import sys
import pandas as pd
import os

#definire path S3
input_file_xmr='s3://progettoaws-raw/dataset/XMR_EUR Kraken Historical Data.csv'
output_file_xmr='s3://progettoaws-argento/xmroutparquet'

input_file_xmrt='s3://progettoaws-raw/dataset/google_trend_monero.csv'
output_file_xmrt='s3://progettoaws-argento/xmrtoutparquet' 


#read file csv
xmr = pd.read_csv(input_file_xmr)

##Modifiche al file monero
#Convertire in datetime il campo 'Date' 
xmr['Date'] = pd.to_datetime(xmr['Date'], errors='coerce')

#Convertire in numeric il campo 'Price'
xmr['Price'] = pd.to_numeric(xmr['Price'], errors='coerce')

#Sostituire il valore -1 del campo 'Price' con la media dei 5 valori validi
for index, row in xmr.iterrows():
    if row['Price'] == -1:
        #Calcolo la media dei precedenti 5 prezzi validi
        previous_prices = xmr['Price'].iloc[max(0, index-5):index].dropna()
        if not previous_prices.empty:
            xmr.loc[index, 'Price'] = previous_prices.mean()
            xmr.sort_values(by='Date', inplace=True)
        else:
            #Gestione del caso in cui ci sono meno di 5 casi validi con la media globale
            xmr.loc[index, 'Price'] = xmr['Price'].mean()

#creare output file parquet
xmr.to_parquet(output_file_xmr) 


##Modifiche al file google trend
#read file csv
gbm = pd.read_csv(input_file_xmrt)

#Convertire in datetime il campo 'Settimana' 
gbm['Settimana'] = pd.to_datetime(gbm['Settimana'], errors='coerce')
gbm.sort_values(by='Settimana', inplace=True)

#creare output file parquet
gbm.to_parquet(output_file_xmrt)