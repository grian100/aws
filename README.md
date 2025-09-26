# Una pipeline E2E AWS per analizzare BTC e XMR
Qui troverai un progetto di data engineering che mostra come costruire una pipeline dati utilizzando l'ecosistema AWS, con l’obiettivo di estrarre, trasformare e caricare (ETL) dati che fanno rifermento alle monete elettroniche Bitcoin e Monero, analizzandole in parallelo in un database Serverless Amazon Redshift.

## Servizi utilizzati
- AWS S3 (Servizio cloud di archiviazione object oriented)
- AWS Glue ETL (Servizio Serverless che facilita l'ELT, permette la creazione di job in Python Shell o Spark Shell)
- AWS Secret Manager (Servizio di gestione sicura dei segreti ovvero dei dati sensibili come utenti e password per accedere ad un database)
- AWS Step Functions (Servizio di orchestrazione interno ad AWS simile ad Apache Airflow)
- AWS Redshift Serverless (Servizio di datawarehousing distribuito colonnare, noi useremo però la versione Serverless) -
- AWS IAM Roles (Servizio di Identity and Access Management)

# Obiettivi del progetto
- Acquisizione dei dati
Raccogliere file grezzi su prezzi delle criptovalute (BTC ed XMR) e su Google Trends, caricandoli in un bucket S3 (progettoaws-raw/dataset).

- Pulizia e trasformazione
Gestire valori mancanti nei prezzi (-1) con varie strategie.

- Convertire i dati in formato .Parquet e salvarli in un bucket S3 (progettoaws-argento/).
Applicare smoothing (es. media mobile a 10 giorni) per ridurre il rumore nei prezzi.

- Unificazione dei dataset
Eseguire il join tra prezzi e Google Trends (consapevoli della diversa granularità temporale) in modo da produrre un file strutturato solo con i campi più importanti per l'analisi: data, prezzo, indice_google_trend, ed iserirlo sempre in un bucket S3 (progetto-argento/).

- Caricamento e analisi
Caricare i dati finali su Amazon Redshift, pronti per query SQL e analisi.

- Creare dashboard interattive con Amazon QuickSight per esplorare pattern e trend (opzionale).

- Architettura della pipeline
Due pipeline parallele, una per BTC e una per XMR, indipendenti ma con struttura analoga.

- Orchestrazione tramite AWS Step Functions
Si procede i parallelo sia con BTC che XMR dal punto di partenza del caricamento dei dati, passando per l'analisi e modifica, completando con il join ed il caricamento in Redshift, tutte le sigole fasi sono accompagnate da possibili 'Fail State'

Valore e benefici
Automazione completa del processo end-to-end (da dati grezzi a insight).

Scalabilità, per gestire grandi volumi e aggiungere altre criptovalute facilmente.

Flessibilità nelle trasformazioni (diversi approcci a pulizia e smoothing).

Insight immediati su Redshift/QuickSight a supporto di trader, analisti e aziende.

## Repository
AWS_PROJECT/
- README.md
- requirements.txt
- src/
    - Load_file_in_S3.py     # Upload dati grezzi in bucket progettoaws-raw/dataset
      - processing/
        - jobBTC_step1.py             # 1 Pipeline dataset bitcoin   
        - jobXMR_step1.py             # 1 Pipeline dataset monero
        - join_btctrend job.py        # 2 Pipeline dataset bitcoin
        - join_xmrtrend job.py        # 2 Pipeline dataset monero
        - insert_btc_redshift job.py  # 3 Pipeline dataset bitcoin (Load)
        - insert_xmr_redshift job.py  # 3 Pipeline dataset monero (Load)
- orchestration/
    - stepfunctions_graph.svg  # Immagine dell'orchestrazione      
- data/                # dati originali
    - BTC_EUR_Historical_Data.csv
    - XMR_EUR Kraken Historical Data.csv
    - google_trend_bitcoin.csv
    - google_trend_monero.csv               
- docs/

## Workflow
Upload dei dati grezzi nel bucket S3 'progettoaws-raw/dataset'
Creazione di un secondo bucket 'progettoaws-argento' in cui si salveranno i file .Parquet

1^ Pipeline
- Nei file <strong>jobBTC_step1.py</strong> e <strong>jobXMR_step1.py</strong> si sono realizzate le operazioni di lettura dei dati BTC, XMR e googletrend e modifica di:
1. Date utlizzando il 'to_datetime'
2. Prezzo utilizzando il 'to_numeric' per trasformare la stringa in numero con virgola mobile
3. Sostituire i valori presenti come '-1' con un dato medio dei precedenti 5 valori validi
4. Convertire il campo 'Settimana' dei google trend in un valore numerico
5. Esportare i risultati in formato .Parquet con il comando 'to_parquet'

2^ Pipeline
- Nei file <strong>join_btctrend job.py</strong> e <strong>join_xmrtrend job.py</strong> si relizzano le operazioni finali di:
1. Lettura dei file .Parquet presenti nel bucket 'progettoaws-argento'
2. Trasformazione in Dataframe con <strong>pyarrow.parquet</strong>
3. Calcolo della media mobile a 10 giorni dei prezzi ordianti per data crescente
4. Join dei due file in base alla data presente
5. Creazione del file output <strong>joinbtct</strong> e <strong>joinxmrt</strong> solo con i campi <strong><mark>'Date', 'Price_MA', 'interesse'</mark></strong>

3^ Pipeline 
- Inserimento dei file .Parquet in Redshift utilizzando i paramteri di connessione e i ruoli definiti con IAM
I file sono immediatamente interrogabili tramite SQL

## Orchestrazione tramite Step Function
Descrizione della macchina a stati in cui sono evidenziate i passaggi delle varie Pipeline descritte in <strong>stepfunctions_graph.svg</strong>
