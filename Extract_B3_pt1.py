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

base_url = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMH0='

char_position_start = 138
char_position_end = 140

# Gerar todas as sequências de caracteres
sequences = generate_custom_sequences()

all_urls = []

# Gerar todas as URLs
for seq in sequences:
    modified_url = base_url[:char_position_start] + seq + base_url[char_position_end:]
    all_urls.append(modified_url)

all_companies_data = []

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

df = pd.DataFrame(all_companies_data, columns=['issuingCompany', 'companyName', 'tradingName', 'cnpj', 'segment', 'market'])

df_descuplicado = df.drop_duplicates()

print(df_descuplicado)

# Salvar o DataFrame em um arquivo CSV
file_path = r"C:\Users\danil\Downloads\company_data_pt1.csv"
df_descuplicado.to_csv(file_path, index=False)
print(f"Arquivo CSV salvo em: {file_path}")
