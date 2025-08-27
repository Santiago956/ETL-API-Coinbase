import time
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base, BitcoinPrice


load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_table():
    try:
        Base.metadata.create_all(engine)
        print("Tabela criada com sucesso!")
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")

def extract_data_bitcoin():
    try:
        url = "https://api.coinbase.com/v2/prices/spot"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"Erro na extração: {e}")
        return None

def transform_data_bitcoin(data):
    try:
        if not data or "data" not in data:
            print("Dados inválidos recebidos da API")
            return None
            
        valor = float(data["data"]["amount"])
        criptomoeda = data["data"]["base"]
        moeda = data["data"]["currency"]
        timestamp = datetime.now()

        transformed_data = {
            "valor": valor,
            "criptomoeda": criptomoeda,
            "moeda": moeda,
            "timestamp": timestamp
        }

        return transformed_data
    except Exception as e:
        print(f"Erro na transformação: {e}")
        return None

def load_postgres_data(transformed_data):
    if not transformed_data:
        print("Dados inválidos, pulando inserção no banco")
        return
        
    try:
        session = Session()  
        new_register = BitcoinPrice(**transformed_data)
        session.add(new_register)
        session.commit()
        session.close()
        
    
        print(f"[{transformed_data['timestamp']}] Dados salvos no PostgreSQL")
        
    except Exception as e:
        print(f"Erro ao salvar no PostgreSQL: {e}")
        if 'session' in locals():
            session.rollback()
            session.close()


if __name__ == "__main__":
    print("Iniciando pipeline de ETL para Bitcoin... (CTRL-C para interromper)")
    
    # Criar tabela se não existir
    create_table()
    
    #Extração dos dados
    while True:
        try:
            data_json = extract_data_bitcoin()
            if data_json:
                transformed_data = transform_data_bitcoin(data_json)
                if transformed_data:
                    load_postgres_data(transformed_data)
                else:
                    print("Falha na transformação dos dados")
            else:
                print("Falha na extração dos dados")
            
            print("Aguardando 15 segundos para próxima execução...")
            time.sleep(15)
            
        except KeyboardInterrupt:
            print("\nPipeline interrompido pelo usuário")
            break
        except Exception as e:
            print(f"Erro inesperado: {e}")
            print("Aguardando 30 segundos antes de tentar novamente...")
            time.sleep(30)







