import psycopg2
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
import os
import time


db_config = os.getenv('conn_string')

# Configurações do S3
s3_client = boto3.client('s3')
bucket_name = 'simulatedbb-raw-data'

# Função para exportar o log
def export_log_to_s3():
    # Conectando ao PostgreSQL
    conn = psycopg2.connect(db_config)
    query = '''SELECT * FROM transactions_log
    where action_timestamp = (select max(action_timestamp) from transactions_log);'''  
    # Substitua pelo nome da sua tabela e a consulta desejada
    df = pd.read_sql(query, conn)
    conn.close()

    # Convertendo o DataFrame para CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Nome do arquivo com base na data e hora
    file_name = f"cdc/log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Upload para o S3
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())

    print(f"Arquivo {file_name} enviado para o S3 com sucesso!")

# Chamar a função para exportar o log e enviar para o S3
# Definir tempo de execução (2 minutos)
start_time = time.time()
elapsed_time = 0

while elapsed_time < 120:  # 2 minutos em segundos
    export_log_to_s3()
    time.sleep(30)
    # Atualizar o tempo de execução
    elapsed_time = time.time() - start_time

