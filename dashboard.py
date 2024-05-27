import streamlit as st
import pandas as pd
from google.cloud import bigquery
import requests
import plotly.express as px

# Conecta-se ao BigQuery
json_url = 'https://storage.googleapis.com/carteira1/neowayb3-e91ece58c23c.json'
response = requests.get(json_url)
json_path = response.json()
client = bigquery.Client.from_service_account_info(json_path)

# Função para buscar dados no BigQuery
def get_data(query):
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df

# Query para buscar dados das ações
query_acoes = """
SELECT *
FROM neowayb3.gold.fact_historico
WHERE data_pregao >= current_date() - 180
"""

# Query para buscar dados dos segmentos
query_segmentos = """
SELECT *
FROM neowayb3.gold.dim_segmento
"""

# Busca dados do BigQuery
df_acoes = get_data(query_acoes)
df_segmentos = get_data(query_segmentos)

# Realiza a junção entre as tabelas fato e dimensão usando a coluna de chave estrangeira comum
merged_df = pd.merge(df_acoes, df_segmentos, how='inner', left_on='nome_empresa', right_on='nome_fantasia')
merged_df = merged_df.sort_values('data_pregao')

merged_df['month'] = merged_df['data_pregao'].apply(lambda x: str(x.year) + '-' + str(x.month))
month = st.sidebar.selectbox('Mês', merged_df['month'].unique())
company = st.sidebar.selectbox('Empresa', merged_df['nome_fantasia'].unique())
segment = st.sidebar.selectbox('Segmento', merged_df['segmento'].unique())
market = st.sidebar.selectbox('Mercado', merged_df['mercado'].unique())

# Filtro para selecionar múltiplas datas usando um calendário
selected_dates = st.sidebar.date_input('Selecione as datas', 
                                       min_value=min(merged_df['data_pregao']),
                                       max_value=max(merged_df['data_pregao']),
                                       value=[min(merged_df['data_pregao']), max(merged_df['data_pregao'])],
                                       key='date_range')

# Aplicando os filtros encadeados
df_filtered = merged_df[(merged_df['month'] == month) & 
                        (merged_df['segmento'] == segment) & 
                        (merged_df['nome_fantasia'] == company) &
                        (merged_df['mercado'] == market) &
                        (merged_df['data_pregao'].isin(selected_dates))]

df_filtered
