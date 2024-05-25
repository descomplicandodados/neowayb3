from fastapi import FastAPI
from google.cloud import bigquery

json_path = r"C:\Users\danil\OneDrive\Área de Trabalho\neoway\neoway_b3\neowayb3-e91ece58c23c.json"

servidor = FastAPI()

# Configurar cliente BigQuery com as credenciais fornecidas
client = bigquery.Client.from_service_account_json(json_path)

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

    # Aguardar resultados da consulta
    results = query_job.result()

    # Retornar resultados
    return {"results": [dict(row) for row in results]}
