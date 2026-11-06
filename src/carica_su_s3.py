"""Carica su S3 i CSV grezzi e, con --codice, gli script dei job Glue.

Esempi, dalla cartella del progetto:
    python src/carica_su_s3.py --bucket progettoaws-raw
    python src/carica_su_s3.py --bucket progettoaws-raw --codice

I CSV vanno in s3://<bucket>/<prefisso>/<nome file> (prefisso predefinito: dataset), dove li
leggono i job. Con --codice cripto.py e gli script di glue/ vanno in s3://<bucket>/codice/.
Le credenziali AWS sono quelle configurate in locale (aws configure, variabili d'ambiente o SSO).
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import cripto  # noqa: E402

FILE_CODICE = [ROOT / "src" / "cripto.py", *sorted((ROOT / "glue").glob("*.py"))]


def file_dati(cartella=ROOT / "data"):
    nomi = [n for m in cripto.CRIPTO.values() for n in (m.file_prezzi, m.file_trend)]
    return [cartella / nome for nome in nomi]


def chiave(prefisso, percorso):
    """Chiave S3: sempre con "/" (os.path.join su Windows userebbe "\\")."""
    prefisso = prefisso.strip("/")
    return f"{prefisso}/{percorso.name}" if prefisso else percorso.name


def carica(s3, file, bucket, prefisso):
    """Carica i file e restituisce quelli non caricati."""
    errori = []
    for percorso in file:
        destinazione = chiave(prefisso, percorso)
        try:
            s3.upload_file(str(percorso), bucket, destinazione)
            print(f"{percorso.name} -> s3://{bucket}/{destinazione}")
        except Exception as errore:  # ClientError, ParamValidationError, file mancante...
            print(f"ERRORE {percorso.name}: {errore}", file=sys.stderr)
            errori.append(percorso)
    return errori


def main(argv=None, s3=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bucket", required=True, help="nome del bucket, senza cartelle (es. progettoaws-raw)")
    parser.add_argument("--prefisso", default="dataset", help="cartella dei CSV nel bucket")
    parser.add_argument("--codice", action="store_true", help="carica anche gli script dei job in codice/")
    parser.add_argument("--regione", default="eu-central-1")
    args = parser.parse_args(argv)
    if "/" in args.bucket:
        parser.error("--bucket deve contenere solo il nome del bucket: la cartella va in --prefisso")

    if s3 is None:
        import boto3
        s3 = boto3.client("s3", region_name=args.regione)
    errori = carica(s3, file_dati(), args.bucket, args.prefisso)
    if args.codice:
        errori += carica(s3, FILE_CODICE, args.bucket, "codice")
    if errori:
        print(f"{len(errori)} file non caricati", file=sys.stderr)
        return 1
    print("Caricamento completato")
    return 0


if __name__ == "__main__":
    sys.exit(main())
