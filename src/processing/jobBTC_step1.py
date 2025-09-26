import sys
import pandas as pd
import os

#definire path S3
input_file_btc='s3://progettoaws-raw/dataset/BTC_EUR_Historical_Data.csv'
output_file_btc='s3://progettoaws-argento/btcoutparquet'

input_file_gtr='s3://progettoaws-raw/dataset/google_trend_bitcoin.csv'
output_file_gtr='s3://progettoaws-argento/gtroutparquet'

#read file csv
btc = pd.read_csv(input_file_btc)

##Modifiche al file bitcoin
#Convertire in datetime il campo 'Date' 
btc['Date'] = pd.to_datetime(btc['Date'], errors='coerce')

#Convertire in numeric il campo 'Price'
btc['Price'] = btc['Price'].str.replace(',', '', regex=False)
btc['Price'] = pd.to_numeric(btc['Price'], errors='coerce')



#Sostituire il valore -1 del campo 'Price' con la media dei 5 valori validi
for index, row in btc.iterrows():
    if row['Price'] == -1:
        #Calcolo la media dei precedenti 5 prezzi validi
        previous_prices = btc['Price'].iloc[max(0, index-5):index].dropna()
        if not previous_prices.empty:
            btc.loc[index, 'Price'] = previous_prices.mean()
            btc.sort_values(by='Date', inplace=True)
        else:
            #Gestione del caso in cui ci sono meno di 5 casi validi con la media globale
            btc.loc[index, 'Price'] = btc['Price'].mean()

#creare output file parquet
btc.to_parquet(output_file_btc) 


##Modifiche al file google trend
#read file csv
gbc = pd.read_csv(input_file_gtr)

#Convertire in datetime il campo 'Settimana' 
gbc['Settimana'] = pd.to_datetime(gbc['Settimana'], errors='coerce')
gbc.sort_values(by='Settimana', inplace=True)

#creare output file parquet
gbc.to_parquet(output_file_gtr)