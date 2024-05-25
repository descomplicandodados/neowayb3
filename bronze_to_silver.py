from google.cloud import bigquery

# Configurações do BigQuery
projeto = "neowayb3"
conjunto_dados_bronze = "bronze"
conjunto_dados_silver = "silver"
tabela_origem = "b3_empresas"
tabela_destino = "b3_empresas_silver"
json_path = r"C:\Users\danil\OneDrive\Área de Trabalho\neoway\neoway_b3\neowayb3-e91ece58c23c.json"

# Configurar cliente BigQuery
client = bigquery.Client.from_service_account_json(json_path)

# Obtendo informações sobre a tabela de origem
tabela_ref_origem = client.dataset(conjunto_dados_bronze).table(tabela_origem)
tabela_origem = client.get_table(tabela_ref_origem)

# Criar tabela de destino
schema_destino = [
    bigquery.SchemaField("codigo_empresa", "STRING"),
    bigquery.SchemaField("nome_empresa", "STRING"),
    bigquery.SchemaField("nome_comercial", "STRING"),
    bigquery.SchemaField("cnpj", "INTEGER"),
    bigquery.SchemaField("segmento", "STRING"),
    bigquery.SchemaField("mercado", "STRING")
]
tabela_ref_destino = client.dataset(conjunto_dados_silver).table(tabela_destino)
tabela_destino = bigquery.Table(tabela_ref_destino, schema=schema_destino)
tabela_destino = client.create_table(tabela_destino)

# Consulta para preencher tabela de destino com dados da tabela de origem e aplicar as transformações necessárias
sql = f"""
INSERT INTO `{projeto}.{conjunto_dados_silver}.{tabela_destino}`
SELECT 
    issuingCompany AS codigo_empresa,
    CONCAT(UPPER(SUBSTR(companyName, 1, 1)), LOWER(SUBSTR(companyName, 2))) AS nome_empresa,
    CONCAT(UPPER(SUBSTR(tradingName, 1, 1)), LOWER(SUBSTR(tradingName, 2))) AS nome_comercial,
    cnpj,
    segment,
    market AS mercado
FROM `{projeto}.{conjunto_dados_bronze}.{tabela_origem}`
"""

# Executar a consulta
job = client.query(sql)
job.result()  # Esperar a conclusão do job

print("Tabela de destino criada e preenchida com sucesso na camada Silver do BigQuery!")
