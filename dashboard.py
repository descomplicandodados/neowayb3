import streamlit as st
import pandas as pd
from google.cloud import bigquery
import requests
import plotly.graph_objects as go

# Conecta-se ao BigQuery
json_url = 'https://storage.googleapis.com/carteira1/neowayb3-e91ece58c23c.json'
response = requests.get(json_url)
json_path = response.json()
client = bigquery.Client.from_service_account_info(json_path)

def get_data(query):
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df

query_acoes = """
SELECT *
FROM neowayb3.gold.fact_historico
WHERE data_pregao >= current_date() - 180
"""

query_segmentos = """
SELECT *
FROM neowayb3.gold.dim_segmento
"""

# Busca dados do BigQuery
df_acoes = get_data(query_acoes)
df_segmentos = get_data(query_segmentos)

merged_df = pd.merge(df_acoes, df_segmentos, how='inner', left_on='nome_empresa', right_on='nome_fantasia')
merged_df = merged_df.sort_values('data_pregao')


company = st.sidebar.selectbox('Empresa', merged_df['nome_fantasia'].unique())
tickers = st.sidebar.selectbox('Tickers', merged_df['cod_negociacao'].unique())
segment = st.sidebar.selectbox('Segmento', merged_df['segmento'].unique())
market = st.sidebar.selectbox('Mercado', merged_df['mercado'].unique())
data_inicio = st.sidebar.date_input('Selecione a data de início:', min(merged_df['data_pregao']), key='start_date')
data_fim = st.sidebar.date_input('Selecione a data de término:', max(merged_df['data_pregao']), key='end_date')

# Filtre o DataFrame com base no intervalo de datas selecionado
df_filtrado = merged_df[
                        (merged_df['nome_fantasia'] == company) &
                        (merged_df['cod_negociacao'] == tickers) &
                        (merged_df['segmento'] == segment) &
                        (merged_df['mercado'] == market) &
                        (merged_df['data_pregao'] >= data_inicio) &
                        (merged_df['data_pregao'] <= data_fim)]

st.write(df_filtrado)

merged_df.loc[:, 'data_pregao'] = pd.to_datetime(merged_df['data_pregao'])

# Criação do gráfico de candlestick
fig = go.Figure(data=[go.Candlestick(x=df_filtrado['data_pregao'],
                                     open=df_filtrado['preco_abertura'],
                                     high=df_filtrado['preco_maximo'],
                                     low=df_filtrado['preco_minimo'],
                                     close=df_filtrado['preco_ultimo_negocio'])])

# Atualiza o layout do gráfico
fig.update_layout(title='Gráfico de Candlestick - Preço Médio',
                  xaxis_title='Data',
                  yaxis_title='Preço Médio')

# Exibe o gráfico
st.plotly_chart(fig)
