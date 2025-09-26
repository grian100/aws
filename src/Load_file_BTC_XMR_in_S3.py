import boto3
import os
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.ERROR)

region_name = 'eu-central-1'
bucket_name = 'progettoaws-raw/dataset'

btc_historical = 'data/BTC_EUR_Historical_Data.csv'
monero_historical = "data/XMR_EUR Kraken Historical Data.csv"
btc_trend = "data/google_trend_bitcoin.csv"
monero_trend = "data/google_trend_monero.csv"

path_btc_historical = 'bitcoin/'
path_monero_historical  = "monero/"
path_trend_btc = "trend_bitcoin/"
path_trend_monero = "trend_monero/"

btc = os.path.join(path_btc_historical, os.path.basename(btc_historical))
monero = os.path.join(path_monero_historical, os.path.basename(monero_historical))
btc_tr = os.path.join(path_trend_btc, os.path.basename(btc_trend))
monero_tr = os.path.join(path_trend_monero, os.path.basename(monero_trend))

def upload_file_to_s3(file_name, bucket_name, Saved_file_path, region_name=region_name):
    """Upload a file to an S3 bucket."""
    s3_client = boto3.client('s3', region_name=region_name)
    try:
        s3_client.upload_file(file_name, bucket_name, Saved_file_path)
        print(f"File {file_name} uploaded successfully to {bucket_name}/{Saved_file_path}.")
    except ClientError as e:
        logging.error(e)

upload_file_to_s3(btc_historical, bucket_name, btc, region_name=region_name)
upload_file_to_s3(monero_historical, bucket_name, monero, region_name=region_name)
upload_file_to_s3(btc_trend, bucket_name, btc_tr, region_name=region_name)
upload_file_to_s3(monero_trend, bucket_name, monero_tr, region_name=region_name)

print(f'\n Programma finito con successo! \n')
