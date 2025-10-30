import pandas as pd
import os
import requests  # Adicionada a importação do requests
import time

def get_razao_social(cnpj):
    url = f"https://open.cnpja.com/office/{cnpj}"
    
    try:
        response = requests.get(url, verify=False)
        
        if response.status_code == 200:
            try:
                data = response.json()
                nome = data.get('company', {}).get('name')
                return data if nome else "Razão social não encontrada"
            except ValueError:
                return "Erro ao decodificar o JSON"
        else:
            return f"Erro na requisição: {response.status_code}"
    except Exception as e:
        return f"Exceção ocorreu: {str(e)}"

# Ler a lista de CSVs
lista = pd.read_csv('lista_cnpjs.csv')
lista['cnpjs'] = lista['cnpjs'].astype(str)

# Gerar DataFrame
df = pd.DataFrame({
    'RazaoSocial': [],
    'CNPJ': [],
    'Status na RF': [],
    'NaturezaJuridica': [],
    'Rua': [],
    'Numero': [],
    'Cidade': [],
    'UF': [],
    'Pais': []
})

# Nome do arquivo
file_name = 'dados_backup.csv'

for index, row in lista.iterrows():
    print(f"Buscando {row['cnpjs']}...")

    # Aqui o script aguarda 14 segundos a cada iteração para não ultrapassar o limite gratuito da API (5 reqs. p/ minuto)
    time.sleep(14)
    
    try:
        data = get_razao_social(row['cnpjs'])  # Corrigido para usar row['cnpjs']

        if isinstance(data, dict):  # Verifica se a resposta é um dicionário
            razao_social = data['company']['name']
            status_na_rf = data['status']['text']
            natureza_juridica = data['company']['nature']['text']
            rua = data['address'].get('street', 'N/A')
            numero = data['address'].get('number', 'N/A')
            distrito = data['address'].get('district', 'N/A')
            cep = data['address'].get('zip', 'N/A')
            cidade = data['address'].get('city', 'N/A')
            uf = data['address']['state']
            pais = data['address']['country'].get('name', 'N/A')

            nova_linha = pd.DataFrame({'RazaoSocial': [razao_social],
                                       'Status na RF': [status_na_rf],
                                       'CNPJ': [row['cnpjs']],  # Corrigido para usar row['cnpjs']
                                       'NaturezaJuridica': [natureza_juridica],
                                       'Rua': [rua],
                                       'Numero': [numero],
                                       'CEP': [cep],
                                       'Distrito': [distrito],
                                       'Cidade': [cidade],
                                       'UF': [uf],
                                       'Pais': [pais]})
            
            df = pd.concat([df, nova_linha], ignore_index=True)

            # Verifica se o arquivo existe
            if not os.path.isfile(file_name):
                # Se não existe, cria a planilha a partir do DataFrame
                df.to_csv(file_name, index=False)
                print(f'O arquivo {file_name} foi criado.')

        else:
            print(f"Erro ao buscar dados: {data}")
        
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

    print(f"CNPJ {row['cnpjs']} adicionado ao DataFrame carregado em memória.")

# Salva o CSV apenas uma vez no final
df.to_csv(file_name, index=False, encoding='utf-8-sig', sep= ';')