import requests
import pandas as pd

# Função para obter dados de uma URL
def get_data_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para a URL {url}: {e}")
        return []

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

# Lista para armazenar todos os dados
all_companies_data = []

# Fazer o request para cada URL gerada e armazenar os resultados
for url in all_urls:
    print(f"Processando URL: {url}")
    results = get_data_from_url(url)
    if not results:
        print(f"Nenhum resultado encontrado para a URL: {url}. Parando.")
        break
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
file_path = r"C:\Users\danil\Downloads\company_data_pt4.csv"
df_descuplicado.to_csv(file_path, index=False)
print(f"Arquivo CSV salvo em: {file_path}")
