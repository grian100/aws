"""Job AWS Glue (Python shell), fase 1: CSV grezzi -> prezzi puliti e trend in Parquet.

Parametri del job:
    --CRIPTO          btc oppure xmr
    --BUCKET_RAW      bucket dei dati grezzi (es. progettoaws-raw)
    --PREFISSO_RAW    cartella dei CSV nel bucket (es. dataset)
    --BUCKET_ARGENTO  bucket dei dati trasformati (es. progettoaws-argento)
    --extra-py-files  s3://<bucket>/codice/cripto.py
"""

import json
import sys

import boto3
from awsglue.utils import getResolvedOptions

import cripto

args = getResolvedOptions(sys.argv, ["CRIPTO", "BUCKET_RAW", "PREFISSO_RAW", "BUCKET_ARGENTO"])
moneta = cripto.CRIPTO[args["CRIPTO"].lower()]
qualita = cripto.prepara_dati(boto3.client("s3"), moneta, args["BUCKET_RAW"], args["BUCKET_ARGENTO"],
                              args["PREFISSO_RAW"])
print(json.dumps({"cripto": moneta.nome, "qualita": qualita}))
