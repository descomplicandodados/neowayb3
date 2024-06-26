import requests
from fastapi import FastAPI
from google.cloud import bigquery
import json
import pandas as pd
import uvicorn

# Carrega as credenciais do Google Cloud
json_url = 'https://storage.googleapis.com/carteira1/neowayb3-e91ece58c23c.json'
response = requests.get(json_url)
json_path = response.json()

client = bigquery.Client.from_service_account_info(json_path)

query = """
SELECT *
FROM `neowayb3.silver.b3_empresas`
"""

# Envia a consulta para o BQ
query_job = client.query(query)
df = query_job.result().to_dataframe()

df_dict = df.to_dict(orient='records')

# Criar o servidor FastAPI
app = FastAPI()

@app.get("/b3_empresas")
async def get_b3_empresas():
    return df_dict

