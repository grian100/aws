"""Esegue l'intera pipeline in locale sui CSV di data/, senza AWS.

    python analisi_locale.py

Produce gli stessi file Parquet dei job Glue in output/<moneta>/, un grafico per moneta in
images/ e stampa il controllo di qualità e le correlazioni tra prezzo e interesse.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import cripto  # noqa: E402

DATA, OUTPUT, IMAGES = ROOT / "data", ROOT / "output", ROOT / "images"
COLORI = {"bitcoin": "#f2a900", "monero": "#ff6600"}


def grafico(moneta, giornaliero, settimanale, percorso):
    fig, (alto, basso) = plt.subplots(2, 1, figsize=(11, 7), sharex=True, gridspec_kw={"height_ratios": [3, 2]})
    alto.plot(giornaliero["data"], giornaliero["prezzo"], color="#c8c8c8", lw=0.8, label="prezzo giornaliero")
    alto.plot(giornaliero["data"], giornaliero["media_mobile_10g"], color=COLORI[moneta.nome], lw=1.4,
              label="media mobile a 10 giorni")
    stimati = giornaliero[giornaliero["prezzo_stimato"]]
    alto.scatter(stimati["data"], stimati["prezzo"], color="black", s=12, zorder=3,
                 label=f"prezzi stimati ({len(stimati)})")
    alto.set_ylabel("prezzo (EUR)")
    alto.set_title(f"{moneta.nome.title()}: prezzo e interesse su Google Trends")
    alto.legend(loc="upper left", frameon=False)
    basso.bar(settimanale["settimana"], settimanale["interesse"], width=6, color="#4a6fa5")
    basso.set_ylabel("interesse Google Trends\n(0-100, settimanale)")
    for asse in (alto, basso):
        asse.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(percorso, dpi=110)
    plt.close(fig)


def main():
    IMAGES.mkdir(exist_ok=True)
    risultati = {}
    for moneta in cripto.CRIPTO.values():
        prezzi = cripto.leggi_prezzi(DATA / moneta.file_prezzi)
        qualita = cripto.report_qualita(prezzi)
        trend = cripto.leggi_trend(DATA / moneta.file_trend, moneta.colonna_trend)
        tabelle = cripto.tabelle_finali(cripto.pulisci_prezzi(prezzi), trend)
        cartella = OUTPUT / moneta.nome
        cartella.mkdir(parents=True, exist_ok=True)
        for nome, df in tabelle.items():
            (cartella / f"{nome}.parquet").write_bytes(cripto.a_parquet(df))
        grafico(moneta, tabelle["prezzi_giornalieri"], tabelle["settimanale"], IMAGES / f"{moneta.nome}.png")
        risultati[moneta.nome] = {"qualita": qualita, **cripto.statistiche(tabelle["settimanale"])}
    print(json.dumps(risultati, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
