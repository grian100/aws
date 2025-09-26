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
input_arg_xmr = "s3://progettoaws-argento/xmroutparquet/"
input_arg_xmrtrend="s3://progettoaws-argento/xmrtoutparquet/"

# ========= Scrive file Parquet su S3 =========
out_arg_joinxmrtrend="s3://progettoaws-argento/joinxmrt/"

# ========= Trasforma in DataFrame =========
xmr_df = pd.read_parquet(input_arg_xmr, engine='pyarrow')
xmr_trenddf = pd.read_parquet(input_arg_xmrtrend, engine='pyarrow')

#calcola la media mobile a 10 giorni
ma = 10
xmr_df['Price_MA'] = xmr_df['Price'].rolling(ma).mean()
xmr_df.sort_values(by='Date', inplace=True)
xmr_df.dropna(inplace=True)
xmr_df.reset_index(drop=True, inplace=True)

#join xmr
join_xm = xmr_df.join(xmr_trenddf.set_index('Settimana'), on='Date')
join_xm.dropna(inplace=True)
join_xm.reset_index(drop=True, inplace=True)
join_xmr=join_xm[['Date','Price_MA','Monero_interesse']]

#scrivo il file join in formato parquet
join_xmr.to_parquet(out_arg_joinxmrtrend)