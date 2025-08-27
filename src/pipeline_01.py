import requests
from datetime import datetime


def extract_data_bitcoin():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    data = response.json()
    return data

def transform_data_bitcoin(data):
    valor = data["data"]["amount"]
    criptomoeda = data["data"]["base"]
    moeda = data["data"]["currency"]
    timestamp = datetime.now().timestamp()

    transformed_data = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp
    }

    return transformed_data

def load_data_bitcoin(transformed_data):


if __name__ == "__main__":
    #Extração dos dados
    data_json = extract_data_bitcoin()
    transformed_data = transform_data_bitcoin(data_json)
    load_data_bitcoin(transformed_data)







