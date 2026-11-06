"""Job AWS Glue (Python shell), fase 3: caricamento delle tabelle in Redshift Serverless.

Usa la Redshift Data API: non serve un driver del database né una connessione Glue nella VPC,
e le credenziali non sono nel codice.

Parametri del job:
    --CRIPTO          btc oppure xmr
    --BUCKET_ARGENTO  bucket con i file Parquet finali
    --WORKGROUP       nome del workgroup di Redshift Serverless
    --DATABASE        nome del database (es. dev)
    --RUOLO_IAM       ARN del ruolo IAM che Redshift usa per leggere da S3, oppure "default"
    --SECRET_ARN      ARN del segreto di Secrets Manager con utente e password, oppure "iam"
                      per autenticarsi con il ruolo IAM del job
    --extra-py-files  s3://<bucket>/codice/cripto.py
"""

import sys

import boto3
from awsglue.utils import getResolvedOptions

import cripto

args = getResolvedOptions(sys.argv, ["CRIPTO", "BUCKET_ARGENTO", "WORKGROUP", "DATABASE", "RUOLO_IAM", "SECRET_ARN"])
moneta = cripto.CRIPTO[args["CRIPTO"].lower()]
secret = None if args["SECRET_ARN"].lower() == "iam" else args["SECRET_ARN"]
cripto.carica_redshift(boto3.client("redshift-data"), moneta, args["BUCKET_ARGENTO"], args["WORKGROUP"],
                       args["DATABASE"], ruolo_iam=args["RUOLO_IAM"], secret_arn=secret)
print(f"Tabelle {moneta.schema}.prezzi_giornalieri e {moneta.schema}.settimanale caricate")
