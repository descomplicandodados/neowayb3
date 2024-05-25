import requests
from fastapi import FastAPI
from google.cloud import bigquery
import json
import pandas as pd
import uvicorn

json_url = 'https://storage.googleapis.com/carteira1/neowayb3-e91ece58c23c.json'
response = requests.get(json_url)
json_path = response.json()

servidor = FastAPI()

# Configurar cliente BigQuery com as credenciais fornecidas
client = bigquery.Client.from_service_account_info(json_path)

# Endpoint para consultar dados na tabela b3_empresas no BigQuery
@servidor.get("/b3_empresas")
async def query_b3_empresas():
    # Consulta SQL para selecionar todos os dados da tabela b3_empresas
    query = """
    SELECT *
    FROM `neowayb3.silver.b3_empresas`
    """

    # Enviar consulta para o BigQuery
    query_job = client.query(query)
    df = await query_job.result()

    return df.to_dict(orient='records')

if __name__ == "__main__":
    uvicorn.run(servidor, host="127.0.0.1", port=8000)
