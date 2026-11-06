"""Job AWS Glue (Python shell), fase 2: media mobile e unione settimanale con Google Trends.

Parametri del job:
    --CRIPTO          btc oppure xmr
    --BUCKET_ARGENTO  bucket dei dati trasformati (es. progettoaws-argento)
    --extra-py-files  s3://<bucket>/codice/cripto.py
"""

import json
import sys

import boto3
from awsglue.utils import getResolvedOptions

import cripto

args = getResolvedOptions(sys.argv, ["CRIPTO", "BUCKET_ARGENTO"])
moneta = cripto.CRIPTO[args["CRIPTO"].lower()]
righe = cripto.unisci_trend(boto3.client("s3"), moneta, args["BUCKET_ARGENTO"])
print(json.dumps({"cripto": moneta.nome, "righe": righe}))
