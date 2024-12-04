import pandas as pd
import numpy as np
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc, text
from datetime import datetime, timedelta

def create_engine_db():
    try:
        # Buscar variáveis de ambiente de forma segura
        user = os.getenv('user')
        dbname = os.getenv('dbname')
        password = os.getenv('password')
        host = os.getenv('host')
        port = os.getenv('port')

        # String de conexão com o banco de dados PostgreSQL
        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(connection_string)
        return engine

    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para criar a tabela no banco de dados (com SQL puro)
def create_table(engine):
    with engine.begin() as conn:
        # SQL para criar a tabela com chave primária
        create_table_query = """
        CREATE TABLE IF NOT EXISTS public.transactions (
            transaction_id INTEGER PRIMARY KEY,
            client_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            date DATE NOT NULL
        );
        """
        conn.execute(text(create_table_query))
        print("Tabela criada com sucesso!")

# Função para gerar o DataFrame com dados aleatórios
def generate_dataframe(num_rows):
    # Intervalo de datas
    data_inicial = datetime(2024, 1, 1)
    data_final = datetime(2024, 11, 30)
    dias_totais = (data_final - data_inicial).days
    # Gerar o DataFrame com dados aleatórios
    data = {
        "transaction_id":np.random.choice(range(1, 101), num_rows, replace=False),
        "client_id": np.random.randint(1, 101, num_rows),       # IDs entre 1 e 100
        "product_id": np.random.randint(1, 51, num_rows),       # IDs entre 1 e 50
        "quantity": np.random.randint(1, 21, num_rows),         # Quantidades entre 1 e 20
        "price": np.round(np.random.uniform(5.0, 100.0, num_rows), 2),  # Preços entre 5.0 e 100.0
        "date": [
            data_inicial + pd.to_timedelta(np.random.randint(0, dias_totais), unit='D')
            for _ in range(num_rows)
        ]
    }
    return pd.DataFrame(data)

# Função para realizar o upsert
def upsert_data(engine, df):
    with engine.begin() as conn:
        for _, row in df.iterrows():
            sql = text("""
                INSERT INTO transactions (transaction_id, client_id, product_id, quantity, price, date)
                VALUES (:transaction_id, :client_id, :product_id, :quantity, :price, :date)
                ON CONFLICT (transaction_id) 
                DO UPDATE SET
                    client_id = EXCLUDED.client_id,
                    product_id = EXCLUDED.product_id,
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    date = EXCLUDED.date
            """)
            # Passando os parâmetros como um dicionário
            params = {
                'transaction_id': row['transaction_id'],
                'client_id': row['client_id'],
                'product_id': row['product_id'],
                'quantity': row['quantity'],
                'price': row['price'],
                'date': row['date']
            }
            conn.execute(sql, params)
        print("Upsert realizado com sucesso!")

# # Função principal para gerenciar a conexão e o fluxo
def main():
    # Conectar ao banco de dados
    engine = create_engine_db()
    if not engine:
        print("Falha na conexão com o banco de dados.")
        return
    # Criar a tabela
    create_table(engine)
    # Definir tempo de execução (2 minutos)
    start_time = time.time()
    elapsed_time = 0
    first_run = True  # Controla o número de linhas a ser gerado na primeira iteração

    while elapsed_time < 120:  # 2 minutos em segundos
        # Gerar o DataFrame com base na iteração
        num_rows = 50 if first_run else 5  # 100 na primeira iteração, 5 nas subsequentes
        df = generate_dataframe(num_rows)
        # Realizar o upsert
        upsert_data(engine, df)
        # Após o primeiro loop, mudar para 5 linhas nas iterações subsequentes
        first_run = False
        # Esperar 1 segundo antes da próxima iteração
        time.sleep(30)
        # Atualizar o tempo de execução
        elapsed_time = time.time() - start_time

# Executando o fluxo completo
if __name__ == "__main__":
    main()
