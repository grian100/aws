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
Raccogliere file grezzi su prezzi delle criptovalute (BTC ed XMR) e su Google Trends, caricandoli in un bucket S3 (progetto-raw/dataset).

- Pulizia e trasformazione
Gestire valori mancanti nei prezzi (-1) con varie strategie.

- Convertire i dati in formato .Parquet e salvarli in un bucket S3 (progetto-argento/).
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
