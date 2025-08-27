# ETL-API-Coinbase

## Descrição

Projeto de extração, transformação e carregamento (ETL) de dados da API da Coinbase utilizando Python e a biblioteca `requests` coletando dados em tempo real.

## Funcionalidades

- Extração de dados da API da Coinbase
- Transformação e limpeza dos dados
- Carregamento em banco de dados
- Execução automatizada

## Tecnologias

- Python (pandas)
- Requests
- SQLAlchemy
- Cloud Azure

## Instalação

1. Clone o repositório
2. Instale as dependências: `pip install -r requirements.txt`
3. Execute o processo ETL:
   ```bash
   python src/extract.py
   python src/transform.py
   python src/load.py
   ```
4. Execute: `python main.py`

## Estrutura

```
ETL-API-Coinbase/
├── src/
│   ├── extract/
│   ├── transform/
│   └── load/
├── config/
├── tests/
└── requirements.txt
```

## Contribuição

Pull requests são bem-vindos. Para mudanças maiores, abra uma issue primeiro.

## Licença

MIT
