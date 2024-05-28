**Descrição Geral**

Este projeto tem como objetivo baixar e processar dados históricos de cotações de ações da B3  para os anos de 2023 e 2024, convertendo os dados de formato texto (TXT) para CSV e, posteriormente, extraindo dados adicionais de empresas listadas a partir do site da B3. Os dados processados são então carregados para o BigQuery para análise, será criado uma API de consumo de dados na camada silver e um dashboard usando streamlit.

**Requisitos**

- Python 3.x
- Pandas
- Requests
- Google Cloud BigQuery
- FastAPI
- Streamlit
- Uvicorn

**Passos Iniciais**
- Baixar o histórico das cotações de 2023 e 2024 do [site da B3](https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/), arquivos vem no formato txt.
- Baixar o layout dos arquivos: [SeriesHistoricas_Layout.pdf](https://www.b3.com.br/data/files/33/67/B9/50/D84057102C784E47AC094EA8/SeriesHistoricas_Layout.pdf).

**Conversão do Arquivo TXT para CSV**

O script a seguir converte os dados de cotações do formato TXT para CSV utilizando as regras especificadas no layout:


```python

import pandas as pd

arquivo_bovespa = r"SEUCAMINHO\COTAHIST_A2023.TXT"

tamanho_campos = [2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]

dados_acoes=pd.read_fwf(arquivo_bovespa, widths=tamanho_campos, header=0)


## Nomear as colunas

dados_acoes.columns = [
"tipo_registro",
"data_pregao",
"cod_bdi",
"cod_negociacao",
"tipo_mercado",
"noma_empresa",
"especificacao_papel",
"prazo_dias_merc_termo",
"moeda_referencia",
"preco_abertura",
"preco_maximo",
"preco_minimo",
"preco_medio",
"preco_ultimo_negocio",
"preco_melhor_oferta_compra",
"preco_melhor_oferta_venda",
"numero_negocios",
"quantidade_papeis_negociados",
"volume_total_negociado",
"preco_exercicio",
"ìndicador_correcao_precos",
"data_vencimento" ,
"fator_cotacao",
"preco_exercicio_pontos",
"codigo_isin",
"num_distribuicao_papel"]

# Eliminar a última linha
linha=len(dados_acoes["data_pregao"])
dados_acoes=dados_acoes.drop(linha-1)

# Ajustar valores com virgula (dividir os valores dessas colunas por 100)
listaVirgula=[
"preco_abertura",
"preco_maximo",
"preco_minimo",
"preco_medio",
"preco_ultimo_negocio",
"preco_melhor_oferta_compra",
"preco_melhor_oferta_venda",
"volume_total_negociado",
"preco_exercicio",
"preco_exercicio_pontos"
]

for coluna in listaVirgula:
    dados_acoes[coluna]=[i/100. for i in dados_acoes[coluna]]

file_path = r"SEUCAMINHO.csv"
dados_acoes.to_csv(file_path, index=False)
print(f"Arquivo CSV salvo em: {file_path}")
head = dados_acoes.head()
print(head)

```
Repita o mesmo passo para o arquivo historico de 2024.

**Extração de Dados de Empresas Listadas**

Os scripts a seguir extraem dados de empresas listadas no site da B3. Eles diferem principalmente na regra utilizada para gerar as URLs dinâmicas.

``` python
import requests
import pandas as pd

# Função para gerar sequências de caracteres 
def generate_custom_sequences():
    sequences = []
    for first in ['M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']:
        for second in ['C','S', 'i', 'y', 'C', 'S', 'i', 'y', 'C', 'S', 'i', 'y']:
            sequences.append(f"{first}{second}")
    return sequences

# Função para obter dados da URL
def get_data_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para a URL {url}: {e}")
        return []

# URL base
base_url = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMH0='

# Posição dos caracteres a serem modificados
char_position_start = 138
char_position_end = 140

# Gerar todas as sequências de caracteres
sequences = generate_custom_sequences()

# Lista para armazenar todas as URLs geradas
all_urls = []

# Gerar todas as URLs
for seq in sequences:
    modified_url = base_url[:char_position_start] + seq + base_url[char_position_end:]
    all_urls.append(modified_url)

# Lista para armazenar todos os dados
all_companies_data = []

# Fazer o request da URL base e adicionar os resultados à lista all_companies_data
print(f"Processando URL base: {base_url}")
results = get_data_from_url(base_url)
if results:
    for company in results:
        issuing_company = company['issuingCompany']
        company_name = company['companyName']
        trading_name = company['tradingName']
        cnpj = company['cnpj']
        segment = company['segment']
        market = company['market']
        all_companies_data.append([issuing_company, company_name, trading_name, cnpj, segment, market])
else:
    print(f"Nenhum resultado para a URL base: {base_url}")

# Fazer o request para cada URL gerada e armazenar os resultados
for url in all_urls[1:]:  # Começar da segunda URL, pois a primeira já foi processada
    print(f"Processando URL: {url}")
    results = get_data_from_url(url)
    if not results or any(company in all_companies_data for company in results):
        print(f"Nenhum resultado ou dados repetidos encontrados. Parando.")
        break  # Interromper o loop se não houver resultados ou se os dados forem repetidos
    for company in results:
        issuing_company = company['issuingCompany']
        company_name = company['companyName']
        trading_name = company['tradingName']
        cnpj = company['cnpj']
        segment = company['segment']
        market = company['market']
        all_companies_data.append([issuing_company, company_name, trading_name, cnpj, segment, market])

# Criar um DataFrame com os dados acumulados
df = pd.DataFrame(all_companies_data, columns=['issuingCompany', 'companyName', 'tradingName', 'cnpj', 'segment', 'market'])

df_descuplicado = df.drop_duplicates()

# Exibir o DataFrame
print(df_descuplicado)

# Salvar o DataFrame em um arquivo CSV
file_path = r"SEUCAMINHO.csv"
df_descuplicado.to_csv(file_path, index=False)
print(f"Arquivo CSV salvo em: {file_path}")

```


Scripts 2, 3 e 4
Os scripts seguintes seguem uma lógica parecida, mas com diferentes URLs base e regras de geração de URLs dinâmicas, optei por dividir o script em 4 partes devido a serem 4 diferentes regras para criação das URLS dinâmicas

``` python
# URL base inicial
base_url = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MTAsInBhZ2VTaXplIjoxMjB9'


# Caractere a ser modificado na URL base
char_position = 140

# Lista para armazenar todas as URLs geradas
all_urls = []

# Letra inicial para substituição na URL
start_letter = 'A'

# Gerar as URLs
while True:
    modified_url = base_url[:char_position] + start_letter + 'sIn' + base_url[char_position + 4:]
    all_urls.append(modified_url)
    # Obter a próxima letra do alfabeto
    start_letter = chr(ord(start_letter) + 4)
    if start_letter > 'Z':
        break
```

``` python
# URL base inicial
base_url = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MTcsInBhZ2VTaXplIjoxMjB9'


# Caractere a ser modificado na URL base
char_position = 140

# Lista para armazenar todas as URLs geradas
all_urls = []

# Letra inicial para substituição na URL
start_letter = 'c'

# Gerar as URLs
while True:
    modified_url = base_url[:char_position] + start_letter + 'sIn' + base_url[char_position + 4:]
    all_urls.append(modified_url)
    # Obter a próxima letra do alfabeto
    start_letter = chr(ord(start_letter) + 4)
    if start_letter > 'z':
        break
```
``` python
# URL base inicial
base_url = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MjAsInBhZ2VTaXplIjoxMjB9'


# Caractere a ser modificado na URL base
char_position = 140

# Lista para armazenar todas as URLs geradas
all_urls = []

# Letra inicial para substituição na URL
start_letter = 'A'

# Gerar as URLs
while True:
    modified_url = base_url[:char_position] + start_letter + 'sIn' + base_url[char_position + 4:]
    all_urls.append(modified_url)
    # Obter a próxima letra do alfabeto
    start_letter = chr(ord(start_letter) + 4)
    if start_letter > 'Z':
        break

```


Upload para o BigQuery
Este script carrega os dados processados para o BigQuery.

``` python
import pandas as pd
from google.cloud import bigquery

# Caminhos dos arquivos CSV
csv_file_path_p1 = r"SEUCAMINHO_pt1.csv"
csv_file_path_p2 = r"SEUCAMINHO_pt2.csv"
csv_file_path_p3 = r"SEUCAMINHO_pt3.csv"
csv_file_path_p4 = r"SEUCAMINHO_pt4.csv"

# Configurações do BigQuery
projeto = "neowayb3"
conjunto_dados = "bronze"
tabela = "b3_empresas"
json_path = r"SEU JSON"

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


```
esse script para subir os dados, serve para subir tanto os dados historicos dos pregoes, quanto os dados da empresa.

**Criando uma API com os dados da camada Silver**

esse script irá criar uma API local para consumo dos dados

``` python
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

# Configura o cliente BQ com as credenciais fornecidas
client = bigquery.Client.from_service_account_info(json_path)

# Consulta SQL para selecionar todos os dados da tabela b3_empresas
query = """
SELECT *
FROM `neowayb3.silver.b3_empresas`
"""

# Envia a consulta para o BQ
query_job = client.query(query)
df = query_job.result().to_dataframe()

# Converter DataFrame para um dicionário
df_dict = df.to_dict(orient='records')

# Criar o servidor FastAPI
app = FastAPI()

# Definir um endpoint para obter os dados da tabela b3_empresas
@app.get("/b3_empresas")
async def get_b3_empresas():
    return df_dict

```
Caso queira realizar um deploy, pode-se usar o [render](https://render.com/), exemplo de como ficou uma [API em deploy](https://neowayb3-4.onrender.com/b3_empresas).

**Criação de dashboard em Stramlit**

``` python
import streamlit as st
import pandas as pd
from google.cloud import bigquery
import requests
import plotly.graph_objects as go

# Conecta-se ao BQ
json_url = 'https://storage.googleapis.com/carteira1/neowayb3-e91ece58c23c.json'
response = requests.get(json_url)
json_path = response.json()
client = bigquery.Client.from_service_account_info(json_path)

# Função para buscar dados no BQ
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

# Busca dados do BQ
df_acoes = get_data(query_acoes)
df_segmentos = get_data(query_segmentos)

# Realiza a junção entre a tabela fato e dimensão usando a coluna nome da empresa
merged_df = pd.merge(df_acoes, df_segmentos, how='inner', left_on='nome_empresa', right_on='nome_fantasia')
merged_df = merged_df.sort_values('data_pregao')


# Filtros
company = st.sidebar.selectbox('Empresa', merged_df['nome_fantasia'].unique())
tickers = st.sidebar.selectbox('Tickers', merged_df['cod_negociacao'].unique())
segment = st.sidebar.selectbox('Segmento', merged_df['segmento'].unique())
market = st.sidebar.selectbox('Mercado', merged_df['mercado'].unique())

# Crie a caixa de seleção de intervalo de datas
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

# Exibindo a tabela com os filtros
st.write(df_filtrado)

# Corrigindo a conversão da coluna 'data_pregao' para datetime
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


```


