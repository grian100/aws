import psycopg2

# === Parametri Redshift ===
host = 'dev-progetto.978353979911.eu-central-1.redshift-serverless.amazonaws.com'
port = 5439
dbname = 'dev'
user = 'admin'
password = 'xxxx'
iam_role = 'arn:aws:iam::978353979911:role/service-role/AmazonRedshift-CommandsAccessRole-20250721T181820'

# === Parametri S3 ===
s3_path = 's3://progettoaws-argento/joinxmrt'
# === Nome della tabella target ===
table_name = 'bitcoin.joinxmrtrend'
# === Connessione a Redshift ===
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    port=port,
    host=host
)
cursor = conn.cursor()

# === Comando COPY ===
copy_sql = f"""
COPY {table_name}
FROM '{s3_path}'
IAM_ROLE '{iam_role}'
FORMAT AS PARQUET;
"""

try:
    cursor.execute(copy_sql)
    conn.commit()
    print("Dati caricati correttamente.")
except Exception as e:
    conn.rollback()
    print("Errore durante il caricamento:", e)
finally:
    cursor.close()
    conn.close()