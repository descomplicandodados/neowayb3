import pandas as pd
from google.cloud import bigquery

# Caminhos dos arquivos CSV
csv_file_path_p1 = r"C:\Users\danil\Downloads\company_data_pt1.csv"
csv_file_path_p2 = r"C:\Users\danil\Downloads\company_data_pt2.csv"
csv_file_path_p3 = r"C:\Users\danil\Downloads\company_data_pt3.csv"
csv_file_path_p4 = r"C:\Users\danil\Downloads\company_data_pt4.csv"

# Configurações do BigQuery
projeto = "neowayb3"
conjunto_dados = "bronze"
tabela = "b3_empresas"
json_path = r"C:\Users\danil\OneDrive\Área de Trabalho\neoway\neoway_b3\neowayb3-e91ece58c23c.json"

# Ler os arquivos CSV
df_p1 = pd.read_csv(csv_file_path_p1)
df_p2 = pd.read_csv(csv_file_path_p2)
df_p3 = pd.read_csv(csv_file_path_p3)
df_p4 = pd.read_csv(csv_file_path_p4)


# Concatenar os DataFrames
df_combined = pd.concat([df_p1, df_p2, df_p3, df_p4], ignore_index=True)

# Configurar cliente BigQuery
client = bigquery.Client.from_service_account_json(json_path)

# Nome completo da tabela
table_ref = client.dataset(conjunto_dados).table(tabela)

# Enviar dados para o BigQuery
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

client.load_table_from_dataframe(df_combined, table_ref, job_config=job_config).result()

print("Dados carregados para o BigQuery com sucesso!")
