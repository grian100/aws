"""Pipeline ETL per i prezzi di Bitcoin e Monero e il loro interesse su Google Trends.

Il modulo contiene tutta la logica della pipeline, usata sia dai job AWS Glue (cartella glue/)
sia dall'esecuzione in locale (analisi_locale.py) e dai test:

1. prepara_dati: legge i CSV grezzi da S3, ordina per data, sostituisce i prezzi non validi
   (-1) per interpolazione e scrive i dati puliti in Parquet;
2. unisci_trend: calcola la media mobile a 10 giorni e unisce i prezzi, aggregati per
   settimana, con l'indice settimanale di Google Trends;
3. carica_redshift: carica le tabelle finali in Redshift Serverless con la Data API.

È un unico file perché i job Python shell di Glue accettano librerie aggiuntive solo come
.py, .whl o .egg (parametro --extra-py-files). Compatibile con Python 3.9 e pandas >= 1.3,
le versioni disponibili nei job Python shell.
"""

import io
import re
import time
from collections import namedtuple

import numpy as np
import pandas as pd

Cripto = namedtuple("Cripto", "codice nome file_prezzi file_trend colonna_trend schema")

CRIPTO = {
    "btc": Cripto("btc", "bitcoin", "BTC_EUR_Historical_Data.csv", "google_trend_bitcoin.csv",
                  "interesse bitcoin", "bitcoin"),
    "xmr": Cripto("xmr", "monero", "XMR_EUR Kraken Historical Data.csv", "google_trend_monero.csv",
                  "Monero_interesse", "monero"),
}

GIORNI_MEDIA_MOBILE = 10

# Colonne delle tabelle finali, nell'ordine dei file Parquet: COPY di Redshift associa le
# colonne per posizione, quindi la DDL (sql/tabelle.sql) viene generata da qui con ddl()
TABELLE = {
    "prezzi_giornalieri": [
        ("data", "DATE"),
        ("prezzo", "DOUBLE PRECISION"),
        ("prezzo_stimato", "BOOLEAN"),
        ("media_mobile_10g", "DOUBLE PRECISION"),
    ],
    "settimanale": [
        ("settimana", "DATE"),
        ("prezzo_medio", "DOUBLE PRECISION"),
        ("prezzo_chiusura", "DOUBLE PRECISION"),
        ("variazione_pct", "DOUBLE PRECISION"),
        ("interesse", "BIGINT"),
        ("giorni_stimati", "BIGINT"),
    ],
}


# ---------------------------------------------------------------------------
# Trasformazioni (solo pandas, senza AWS)
# ---------------------------------------------------------------------------

def leggi_prezzi(sorgente):
    """Legge un CSV di prezzi storici (formato Investing.com): colonne data e prezzo.

    Le date sono nel formato mese/giorno/anno e i prezzi di BTC hanno la virgola come
    separatore delle migliaia ("65,619.5"): entrambi i formati vengono dichiarati
    esplicitamente invece di lasciarli indovinare a pandas.
    """
    raw = pd.read_csv(sorgente, usecols=["Date", "Price"], thousands=",", dtype={"Date": str})
    return pd.DataFrame({
        "data": pd.to_datetime(raw["Date"], format="%m/%d/%Y"),
        "prezzo": pd.to_numeric(raw["Price"], errors="coerce"),
    })


def report_qualita(prezzi):
    """Problemi di qualità dei prezzi grezzi."""
    date = prezzi["data"]
    giorni = pd.date_range(date.min(), date.max(), freq="D")
    return {
        "righe": len(prezzi),
        "dal": date.min().date().isoformat(),
        "al": date.max().date().isoformat(),
        "date_duplicate": int(date.duplicated().sum()),
        "giorni_mancanti": int(len(giorni.difference(date))),
        "prezzi_non_validi": int((~(prezzi["prezzo"] > 0)).sum()),  # -1, 0, negativi o mancanti
    }


def pulisci_prezzi(prezzi):
    """Ordina per data e sostituisce i prezzi non validi.

    I prezzi non validi (-1, zero o mancanti) vengono sostituiti per interpolazione lineare
    nel tempo tra il giorno valido precedente e quello successivo, e segnalati nella colonna
    prezzo_stimato. Il file originale è in ordine di data decrescente: l'ordinamento viene
    fatto prima di qualsiasi calcolo che dipende dall'ordine delle righe.
    """
    df = prezzi.sort_values("data").drop_duplicates("data", keep="first").reset_index(drop=True)
    non_valido = ~(df["prezzo"] > 0)
    serie = df["prezzo"].mask(non_valido)
    serie.index = df["data"]
    df["prezzo"] = serie.interpolate(method="time", limit_direction="both").to_numpy()
    df["prezzo_stimato"] = non_valido.to_numpy()
    return df


def leggi_trend(sorgente, colonna):
    """Legge un CSV di Google Trends: colonne settimana (domenica di inizio) e interesse (0-100)."""
    raw = pd.read_csv(sorgente, dtype=str)
    # Google Trends scrive "<1" per un interesse maggiore di zero ma inferiore a 1
    interesse = pd.to_numeric(raw[colonna].str.strip().replace("<1", "0"), errors="raise")
    trend = pd.DataFrame({"settimana": pd.to_datetime(raw["Settimana"], format="%Y-%m-%d"),
                          "interesse": interesse.astype("int64")})
    return trend.sort_values("settimana").reset_index(drop=True)


def aggiungi_media_mobile(prezzi, giorni=GIORNI_MEDIA_MOBILE):
    """Media mobile del prezzo sul giorno e sui giorni-1 precedenti (mai sui giorni futuri).

    Per i primi giorni-1 giorni la media non è definita (valore mancante).
    """
    df = prezzi.sort_values("data").reset_index(drop=True)
    df["media_mobile_10g"] = df["prezzo"].rolling(giorni, min_periods=giorni).mean()
    return df


def inizio_settimana(date):
    """Domenica con cui inizia la settimana di ogni data, come nelle settimane di Google Trends."""
    return (date - pd.to_timedelta((date.dt.dayofweek + 1) % 7, unit="D")).dt.normalize()


def unisci_settimanale(prezzi, trend):
    """Prezzi aggregati per settimana (da domenica a sabato) e uniti a Google Trends.

    Ogni valore di Google Trends si riferisce alla settimana che inizia quella domenica:
    confrontarlo con il prezzo del solo giorno di domenica, o con una media dei giorni
    precedenti, metterebbe a confronto periodi diversi. Si tengono solo le settimane complete.
    """
    df = prezzi.sort_values("data").assign(settimana=lambda d: inizio_settimana(d["data"]))
    settimane = df.groupby("settimana").agg(
        prezzo_medio=("prezzo", "mean"),
        prezzo_chiusura=("prezzo", "last"),
        giorni=("prezzo", "size"),
        giorni_stimati=("prezzo_stimato", "sum"),
    ).reset_index()
    settimane = settimane[settimane["giorni"] == 7].copy()
    settimane["variazione_pct"] = settimane["prezzo_chiusura"].pct_change() * 100
    unito = settimane.merge(trend, on="settimana", how="inner")
    unito["giorni_stimati"] = unito["giorni_stimati"].astype("int64")
    return unito[[c for c, _ in TABELLE["settimanale"]]].reset_index(drop=True)


def tabelle_finali(prezzi_puliti, trend):
    """Le due tabelle caricate in Redshift, con le colonne nell'ordine di TABELLE."""
    giornaliero = aggiungi_media_mobile(prezzi_puliti)
    return {
        "prezzi_giornalieri": giornaliero[[c for c, _ in TABELLE["prezzi_giornalieri"]]],
        "settimanale": unisci_settimanale(prezzi_puliti, trend),
    }


# ---------------------------------------------------------------------------
# Lettura e scrittura su S3
# ---------------------------------------------------------------------------

COLONNE_DATA = {"data", "settimana"}


def leggi_s3(s3, bucket, chiave):
    return io.BytesIO(s3.get_object(Bucket=bucket, Key=chiave)["Body"].read())


def a_parquet(df):
    """Parquet con le date come DATE (non timestamp in nanosecondi, che Redshift non legge)."""
    df = df.copy()
    for colonna in COLONNE_DATA & set(df.columns):
        df[colonna] = df[colonna].dt.date
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    return buffer.getvalue()


def da_parquet(sorgente):
    df = pd.read_parquet(sorgente, engine="pyarrow")
    for colonna in COLONNE_DATA & set(df.columns):
        df[colonna] = pd.to_datetime(df[colonna])
    return df


def scrivi_parquet_s3(s3, df, bucket, chiave):
    s3.put_object(Bucket=bucket, Key=chiave, Body=a_parquet(df))


def chiave_argento(cripto, nome):
    return f"{cripto.nome}/{nome}.parquet"


def prepara_dati(s3, cripto, bucket_raw, bucket_argento, prefisso_raw="dataset"):
    """Fase 1: CSV grezzi da S3 -> prezzi puliti e trend in Parquet."""
    prefisso = prefisso_raw.strip("/")
    prezzi = leggi_prezzi(leggi_s3(s3, bucket_raw, f"{prefisso}/{cripto.file_prezzi}"))
    qualita = report_qualita(prezzi)
    trend = leggi_trend(leggi_s3(s3, bucket_raw, f"{prefisso}/{cripto.file_trend}"), cripto.colonna_trend)
    scrivi_parquet_s3(s3, pulisci_prezzi(prezzi), bucket_argento, chiave_argento(cripto, "prezzi_puliti"))
    scrivi_parquet_s3(s3, trend, bucket_argento, chiave_argento(cripto, "trend"))
    return qualita


def unisci_trend(s3, cripto, bucket_argento):
    """Fase 2: prezzi puliti e trend -> tabelle prezzi_giornalieri e settimanale in Parquet."""
    prezzi = da_parquet(leggi_s3(s3, bucket_argento, chiave_argento(cripto, "prezzi_puliti")))
    trend = da_parquet(leggi_s3(s3, bucket_argento, chiave_argento(cripto, "trend")))
    tabelle = tabelle_finali(prezzi, trend)
    for nome, df in tabelle.items():
        scrivi_parquet_s3(s3, df, bucket_argento, chiave_argento(cripto, nome))
    return {nome: len(df) for nome, df in tabelle.items()}


# ---------------------------------------------------------------------------
# Redshift
# ---------------------------------------------------------------------------

BUCKET_VALIDO = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
RUOLO_VALIDO = re.compile(r"^arn:aws[a-z\-]*:iam::\d{12}:role/[\w+=,.@\-/]+$")


def ddl():
    """Schemi e tabelle di Redshift (contenuto di sql/tabelle.sql)."""
    righe = ["-- Generato da cripto.ddl(): le colonne devono restare nell'ordine dei file Parquet.", ""]
    for cripto in CRIPTO.values():
        righe.append(f"CREATE SCHEMA IF NOT EXISTS {cripto.schema};")
    for cripto in CRIPTO.values():
        for tabella, colonne in TABELLE.items():
            definizione = ",\n".join(f"    {nome} {tipo}" for nome, tipo in colonne)
            righe += ["", f"CREATE TABLE IF NOT EXISTS {cripto.schema}.{tabella} (\n{definizione}\n);"]
    return "\n".join(righe) + "\n"


def sql_caricamento(cripto, bucket_argento, ruolo_iam="default"):
    """Istruzioni per sostituire il contenuto delle tabelle con i file Parquet.

    Eseguite con batch_execute_statement formano un'unica transazione: se COPY fallisce,
    anche DELETE viene annullato. Si usa DELETE e non TRUNCATE perché in Redshift TRUNCATE
    conferma subito la transazione. Rieseguire la pipeline quindi non duplica i dati.
    """
    if not BUCKET_VALIDO.match(bucket_argento):
        raise ValueError(f"Nome del bucket non valido: {bucket_argento!r}")
    if ruolo_iam != "default" and not RUOLO_VALIDO.match(ruolo_iam):
        raise ValueError(f"ARN del ruolo IAM non valido: {ruolo_iam!r}")
    ruolo = "default" if ruolo_iam == "default" else f"'{ruolo_iam}'"
    istruzioni = []
    for tabella in TABELLE:
        nome = f"{cripto.schema}.{tabella}"
        istruzioni += [
            f"DELETE FROM {nome};",
            f"COPY {nome} FROM 's3://{bucket_argento}/{chiave_argento(cripto, tabella)}' "
            f"IAM_ROLE {ruolo} FORMAT AS PARQUET;",
        ]
    return istruzioni


def carica_redshift(client, cripto, bucket_argento, workgroup, database, ruolo_iam="default",
                    secret_arn=None, attesa=2, timeout=600):
    """Fase 3: carica le tabelle in Redshift Serverless con la Data API (client "redshift-data").

    Con secret_arn le credenziali del database vengono lette da Secrets Manager; senza, la
    Data API usa le credenziali IAM del ruolo del job. Nessuna password nel codice.
    """
    parametri = {"WorkgroupName": workgroup, "Database": database,
                 "Sqls": sql_caricamento(cripto, bucket_argento, ruolo_iam)}
    if secret_arn:
        parametri["SecretArn"] = secret_arn
    identificativo = client.batch_execute_statement(**parametri)["Id"]
    scadenza = time.monotonic() + timeout
    while True:
        stato = client.describe_statement(Id=identificativo)
        if stato["Status"] == "FINISHED":
            return stato
        if stato["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Caricamento in Redshift non riuscito ({stato['Status']}): {stato.get('Error')}")
        if time.monotonic() > scadenza:
            client.cancel_statement(Id=identificativo)
            raise TimeoutError(f"Caricamento in Redshift non completato entro {timeout} secondi")
        time.sleep(attesa)


# ---------------------------------------------------------------------------
# Analisi
# ---------------------------------------------------------------------------

def statistiche(settimanale):
    """Correlazioni tra prezzo e interesse su Google Trends."""
    s = settimanale.dropna(subset=["variazione_pct"])
    return {
        "settimane": len(settimanale),
        "correlazione_prezzo_interesse": float(settimanale["prezzo_medio"].corr(settimanale["interesse"])),
        # Le variazioni settimanali eliminano il trend di lungo periodo comune alle due serie
        "correlazione_variazioni": float(s["variazione_pct"].corr(s["interesse"].diff())),
        "correlazione_variazione_assoluta_interesse": float(
            np.abs(s["variazione_pct"]).corr(s["interesse"])),
    }
