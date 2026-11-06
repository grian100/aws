"""API REST sui risultati della pipeline: prezzi, dati settimanali con Google Trends e statistiche.

Avvio, dalla cartella del progetto:
    python -m uvicorn api:app --app-dir src

Documentazione interattiva su http://localhost:8000/docs.

Configurazione tramite variabili d'ambiente:
- CRIPTO_DATI: da dove leggere le tabelle
    - non impostata: le tabelle vengono calcolate all'avvio dai CSV di data/ (meno di un secondo);
    - una cartella locale con <moneta>/<tabella>.parquet, per esempio output/ di analisi_locale.py;
    - s3://<bucket>, il bucket in cui scrivono i job Glue (es. s3://progettoaws-argento),
      con le credenziali AWS configurate.
- CRIPTO_API_KEY: se impostata, ogni richiesta (tranne /health) deve avere l'header X-API-Key
  con questo valore.

I dati vengono letti una sola volta, all'avvio: dopo una nuova esecuzione della pipeline
basta riavviare il servizio.
"""

import os
import secrets
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

import cripto

ROOT = Path(__file__).resolve().parents[1]
CLASSI_VARIAZIONE = [(0, 5, "meno del 5%"), (5, 10, "dal 5 al 10%"), (10, np.inf, "oltre il 10%")]

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


# ---------------------------------------------------------------------------
# Caricamento dei dati
# ---------------------------------------------------------------------------

def tabelle_da_csv(cartella=ROOT / "data"):
    tabelle = {}
    for moneta in cripto.CRIPTO.values():
        prezzi = cripto.pulisci_prezzi(cripto.leggi_prezzi(cartella / moneta.file_prezzi))
        trend = cripto.leggi_trend(cartella / moneta.file_trend, moneta.colonna_trend)
        tabelle[moneta.codice] = cripto.tabelle_finali(prezzi, trend)
    return tabelle


def tabelle_da_parquet(leggi):
    """leggi(moneta, tabella) restituisce il contenuto del file Parquet."""
    return {
        moneta.codice: {tabella: cripto.da_parquet(leggi(moneta, tabella)) for tabella in cripto.TABELLE}
        for moneta in cripto.CRIPTO.values()
    }


def carica_tabelle(sorgente=None, s3=None):
    if not sorgente:
        return tabelle_da_csv()
    if sorgente.startswith("s3://"):
        bucket = sorgente[len("s3://"):].strip("/")
        if s3 is None:
            import boto3
            s3 = boto3.client("s3")
        return tabelle_da_parquet(lambda m, t: cripto.leggi_s3(s3, bucket, cripto.chiave_argento(m, t)))
    cartella = Path(sorgente)
    return tabelle_da_parquet(lambda m, t: cartella / cripto.chiave_argento(m, t))


# ---------------------------------------------------------------------------
# Modelli delle risposte
# ---------------------------------------------------------------------------

class Moneta(BaseModel):
    codice: str
    nome: str
    dal: date = Field(description="primo giorno con un prezzo")
    al: date = Field(description="ultimo giorno con un prezzo")
    giorni: int
    giorni_stimati: int = Field(description="giorni con prezzo non valido (-1) sostituito per interpolazione")
    settimane: int = Field(description="settimane complete unite a Google Trends")


class PrezzoGiornaliero(BaseModel):
    data: date
    prezzo: float
    prezzo_stimato: bool
    media_mobile_10g: Optional[float] = Field(description="null per i primi 9 giorni della serie")


class Settimana(BaseModel):
    settimana: date = Field(description="domenica di inizio della settimana, come in Google Trends")
    prezzo_medio: float
    prezzo_chiusura: float
    variazione_pct: Optional[float] = Field(description="variazione del prezzo di chiusura rispetto alla settimana precedente")
    interesse: int = Field(description="indice di Google Trends (0-100)")
    giorni_stimati: int


class ClasseVariazione(BaseModel):
    variazione_settimanale: str
    settimane: int
    interesse_medio: Optional[float]


class Statistiche(BaseModel):
    moneta: str
    dal: Optional[date]
    al: Optional[date]
    settimane: int
    correlazione_prezzo_interesse: Optional[float]
    correlazione_variazioni: Optional[float] = Field(
        description="correlazione tra variazione settimanale del prezzo e variazione dell'interesse")
    correlazione_variazione_assoluta_interesse: Optional[float] = Field(
        description="correlazione tra ampiezza della variazione del prezzo e interesse")
    interesse_per_variazione: List[ClasseVariazione]


class SettimanaConfronto(BaseModel):
    settimana: date
    btc_prezzo_medio: float
    btc_variazione_pct: Optional[float]
    btc_interesse: int
    xmr_prezzo_medio: float
    xmr_variazione_pct: Optional[float]
    xmr_interesse: int


class Confronto(BaseModel):
    settimane: int
    correlazioni: Dict[str, Optional[float]]
    dati: List[SettimanaConfronto]


# ---------------------------------------------------------------------------
# Funzioni di supporto
# ---------------------------------------------------------------------------

def _numero(valore):
    """NaN e infinito (per esempio una correlazione su troppe poche righe) diventano null."""
    return None if valore is None or not np.isfinite(valore) else float(valore)


def _righe(df):
    """DataFrame -> lista di dizionari con tipi Python nativi, date e null al posto di NaN."""
    colonne = {}
    for nome, serie in df.items():
        valori = serie.dt.date.tolist() if nome in cripto.COLONNE_DATA else serie.tolist()
        colonne[nome] = [None if isinstance(v, float) and not np.isfinite(v) else v for v in valori]
    return [dict(zip(colonne, riga)) for riga in zip(*colonne.values())]


def _periodo(df, colonna, dal, al):
    if dal and al and dal > al:
        raise HTTPException(status_code=422, detail="'dal' deve precedere 'al'")
    if dal:
        df = df[df[colonna] >= pd.Timestamp(dal)]
    if al:
        df = df[df[colonna] <= pd.Timestamp(al)]
    return df


def _moneta(codice):
    codice = codice.lower()
    for moneta in cripto.CRIPTO.values():
        if codice in (moneta.codice, moneta.nome):
            return moneta
    raise HTTPException(status_code=404, detail=f"Moneta sconosciuta: {codice}. Valori ammessi: btc, xmr")


def statistiche(moneta, settimanale):
    base = cripto.statistiche(settimanale)
    con_variazione = settimanale.dropna(subset=["variazione_pct"])
    ampiezza = con_variazione["variazione_pct"].abs()
    classi = []
    for minimo, massimo, nome in CLASSI_VARIAZIONE:
        selezione = con_variazione[(ampiezza >= minimo) & (ampiezza < massimo)]
        classi.append({"variazione_settimanale": nome, "settimane": len(selezione),
                       "interesse_medio": _numero(selezione["interesse"].mean()) if len(selezione) else None})
    vuoto = settimanale.empty
    return {
        "moneta": moneta.nome,
        "dal": None if vuoto else settimanale["settimana"].min().date(),
        "al": None if vuoto else settimanale["settimana"].max().date(),
        "settimane": len(settimanale),
        **{k: _numero(v) for k, v in base.items() if k != "settimane"},
        "interesse_per_variazione": classi,
    }


# ---------------------------------------------------------------------------
# Applicazione
# ---------------------------------------------------------------------------

def create_app(sorgente=None, api_key=None, s3=None):
    """Crea l'applicazione. Senza argomenti legge la configurazione dalle variabili d'ambiente."""

    @asynccontextmanager
    async def lifespan(app):
        app.state.tabelle = carica_tabelle(sorgente if sorgente is not None else os.environ.get("CRIPTO_DATI"), s3)
        yield

    app = FastAPI(
        title="Bitcoin, Monero e Google Trends",
        description=(
            "Risultati della pipeline ETL: prezzi giornalieri puliti con media mobile a 10 giorni, "
            "prezzi settimanali uniti all'indice di Google Trends, statistiche e confronto tra le due monete. "
            "Le date nei filtri sono nel formato AAAA-MM-GG."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    expected_key = api_key if api_key is not None else os.environ.get("CRIPTO_API_KEY")

    def check_api_key(key: Optional[str] = Security(api_key_header)):
        if expected_key and not (key and secrets.compare_digest(key, expected_key)):
            raise HTTPException(status_code=401, detail="API key mancante o non valida")

    protetto = [Depends(check_api_key)]
    Dal = Query(None, description="primo giorno incluso (AAAA-MM-GG)")
    Al = Query(None, description="ultimo giorno incluso (AAAA-MM-GG)")

    def tabella(request, codice, nome):
        moneta = _moneta(codice)
        return moneta, request.app.state.tabelle[moneta.codice][nome]

    @app.get("/health", tags=["servizio"])
    def health():
        """Verifica che il servizio sia attivo."""
        return {"stato": "ok"}

    @app.get("/monete", response_model=List[Moneta], tags=["monete"], dependencies=protetto)
    def monete(request: Request):
        """Monete disponibili, con periodo coperto e numero di giorni con prezzo stimato."""
        risultato = []
        for moneta in cripto.CRIPTO.values():
            giornaliero = request.app.state.tabelle[moneta.codice]["prezzi_giornalieri"]
            risultato.append({
                "codice": moneta.codice, "nome": moneta.nome,
                "dal": giornaliero["data"].min().date(), "al": giornaliero["data"].max().date(),
                "giorni": len(giornaliero), "giorni_stimati": int(giornaliero["prezzo_stimato"].sum()),
                "settimane": len(request.app.state.tabelle[moneta.codice]["settimanale"]),
            })
        return risultato

    @app.get("/monete/{moneta}/prezzi", response_model=List[PrezzoGiornaliero], tags=["monete"],
             dependencies=protetto)
    def prezzi(moneta: str, request: Request, dal: Optional[date] = Dal, al: Optional[date] = Al,
               solo_stimati: bool = Query(False, description="solo i giorni con prezzo stimato")):
        """Prezzi giornalieri puliti e media mobile a 10 giorni (moneta: btc, xmr, bitcoin o monero)."""
        _, df = tabella(request, moneta, "prezzi_giornalieri")
        df = _periodo(df, "data", dal, al)
        if solo_stimati:
            df = df[df["prezzo_stimato"]]
        return _righe(df)

    @app.get("/monete/{moneta}/settimanale", response_model=List[Settimana], tags=["monete"],
             dependencies=protetto)
    def settimanale(moneta: str, request: Request, dal: Optional[date] = Dal, al: Optional[date] = Al):
        """Prezzi aggregati per settimana (da domenica a sabato) e interesse su Google Trends."""
        _, df = tabella(request, moneta, "settimanale")
        return _righe(_periodo(df, "settimana", dal, al))

    @app.get("/monete/{moneta}/statistiche", response_model=Statistiche, tags=["analisi"], dependencies=protetto)
    def statistiche_moneta(moneta: str, request: Request, dal: Optional[date] = Dal, al: Optional[date] = Al):
        """Correlazioni tra prezzo e interesse e interesse medio per ampiezza della variazione settimanale."""
        info, df = tabella(request, moneta, "settimanale")
        return statistiche(info, _periodo(df, "settimana", dal, al))

    @app.get("/confronto", response_model=Confronto, tags=["analisi"], dependencies=protetto)
    def confronto(request: Request, dal: Optional[date] = Dal, al: Optional[date] = Al):
        """Bitcoin e Monero settimana per settimana, con le correlazioni tra le due monete."""
        colonne = ["settimana", "prezzo_medio", "variazione_pct", "interesse"]
        btc, xmr = (request.app.state.tabelle[c]["settimanale"][colonne] for c in ("btc", "xmr"))
        unito = btc.merge(xmr, on="settimana", suffixes=("_btc", "_xmr"))
        unito = _periodo(unito, "settimana", dal, al)
        correlazioni = {
            "prezzi_medi": _numero(unito["prezzo_medio_btc"].corr(unito["prezzo_medio_xmr"])),
            "variazioni_settimanali": _numero(unito["variazione_pct_btc"].corr(unito["variazione_pct_xmr"])),
            "interesse": _numero(unito["interesse_btc"].corr(unito["interesse_xmr"])),
        }
        dati = unito.rename(columns={f"{c}_{s}": f"{s}_{c}" for c in colonne[1:] for s in ("btc", "xmr")})
        return {"settimane": len(dati), "correlazioni": correlazioni, "dati": _righe(dati)}

    return app


app = create_app()
