import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, upper
import pyarrow.parquet as pq
import pandas as pd
import os
from io import BytesIO

# ========= Parametri da linea di comando =========
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ========= Inizializzazione =========
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ========= Leggi da file Parquet su S3 =========
input_arg_btc = "s3://progettoaws-argento/btcoutparquet/"
input_arg_btctrend="s3://progettoaws-argento/gtroutparquet/"

# ========= Scrive file Parquet su S3 =========
out_arg_joinbtctrend="s3://progettoaws-argento/joinbtct"

# ========= Trasforma in DataFrame =========
btc_df = pd.read_parquet(input_arg_btc, engine='pyarrow')
btc_trenddf = pd.read_parquet(input_arg_btctrend, engine='pyarrow')

#calcola la media mobile a 10 giorni
ma = 10
btc_df['Price_MA'] = btc_df['Price'].rolling(ma).mean()
btc_df.sort_values(by='Date', inplace=True)
btc_df.dropna(inplace=True)
btc_df.reset_index(drop=True, inplace=True)

#join bitcoin
join_tb = btc_df.join(btc_trenddf.set_index('Settimana'), on='Date')
join_tb.rename(columns={'interesse bitcoin':'interesse_bitcoin'},inplace=True)
join_tb.dropna(inplace=True)
join_tb.reset_index(drop=True,inplace=True)
join_btc=join_tb[['Date', 'Price_MA', 'interesse_bitcoin']]

#scrivo il file join in formato parquet
join_btc.to_parquet(out_arg_joinbtctrend)