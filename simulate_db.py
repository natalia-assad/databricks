import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Função para gerar o DataFrame com dados aleatórios
def generate_dataframe(num_rows=100):
    data = {
        "client_id": np.random.randint(1, 101, num_rows),  # IDs entre 1 e 100
        "product_id": np.random.randint(1, 51, num_rows),  # IDs entre 1 e 50
        "quantity": np.random.randint(1, 21, num_rows),    # Quantidades entre 1 e 20
        "price": np.round(np.random.uniform(5.0, 100.0, num_rows), 2)  # Preços entre 5.0 e 100.0
    }
    return pd.DataFrame(data)

# Função para conectar ao banco de dados
def create_db_connection():
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
        connection = engine.connect()
        print("Conexão bem-sucedida!")
        return engine
    except ValueError as ve:
        print(f"Erro de configuração: {ve}")

# Função para inserir o DataFrame no banco de dados
def insert_data_to_db(df, engine):
    try:
        if engine:
            df.to_sql('clients', engine, if_exists='append', index=False)
            print("Dados inseridos com sucesso!")
        else:
            print("Erro: Conexão com o banco de dados não estabelecida.")
    except exc.SQLAlchemyError as e:
        print(f"Erro ao inserir dados no banco de dados: {e}")


# Função principal
def main():
    # Gerar dados
    df = generate_dataframe()
    # Conectar ao banco de dados
    engine = create_db_connection()
    # Inserir dados no banco de dados
    insert_data_to_db(df, engine)

if __name__ == "__main__":
    main()
