from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import zipfile
from datetime import datetime
import json
import os

app = Flask(__name__)

@app.route("/")
def update_spreadsheet():
    # Substitua o conteúdo do arquivo JSON por uma string no código
    json_creds = os.environ.get("GOOGLE_CREDENTIALS")
    creds_dict = json.loads(json_creds)

    # Autenticação usando as credenciais embutidas
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Acesse a planilha
    spreadsheet = client.open('Dados Fundos')
    worksheet = spreadsheet.worksheet('Dados')  # Acesse a aba chamada 'Dados'

    # Obter dados existentes na planilha
    existing_data = worksheet.get_all_values()

    # Verificar se há dados na planilha
    if existing_data:
        # Converter os dados existentes para DataFrame
        headers = existing_data[0]
        existing_df = pd.DataFrame(existing_data[1:], columns=headers)
        existing_df['Data da Cota'] = pd.to_datetime(existing_df['Data da Cota'], format='%d/%m/%Y', errors='coerce')
    else:
        # Criar DataFrame vazio se a planilha estiver vazia
        existing_df = pd.DataFrame(columns=['Nome do Fundo', 'CNPJ', 'Valor da Cota', 'Data da Cota'])

    # Baixar e processar os dados dos fundos
    hoje = datetime.today()
    ano = hoje.strftime('%Y')
    mes = hoje.strftime('%m')
    url = f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ano}{mes}.zip'

    download = requests.get(url)

    with open(f"inf_diario_fi_{ano}{mes}.zip", "wb") as arquivo_cvm:
        arquivo_cvm.write(download.content)

    arquivo_zip = zipfile.ZipFile(f"inf_diario_fi_{ano}{mes}.zip")
    dados_fundos = pd.read_csv(arquivo_zip.open(arquivo_zip.namelist()[0]), sep=";", encoding='ISO-8859-1')

    dados_cadastro = pd.read_csv('https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv', sep=";", encoding='ISO-8859-1')
    dados_cadastro = dados_cadastro[['CNPJ_FUNDO', 'DENOM_SOCIAL']].drop_duplicates()

    base_final = pd.merge(dados_fundos, dados_cadastro, how="left", left_on="CNPJ_FUNDO", right_on="CNPJ_FUNDO")
    base_final = base_final[['CNPJ_FUNDO', 'DENOM_SOCIAL', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST']]

    fundos_especificos = [
        "42.847.903/0001-52",
        "35.471.498/0001-55",
        "42.774.627/0001-40",
        "19.413.587/0001-99",
        "30.566.221/0001-92",
    ]

    dados_planilha = []

    for cnpj in fundos_especificos:
        fundo = base_final[base_final['CNPJ_FUNDO'] == cnpj]

        if not fundo.empty:
            ultima_data = fundo['DT_COMPTC'].max()
            fundo_ultimo_dia = fundo[fundo['DT_COMPTC'] == ultima_data]
            valor_cota_atual = fundo_ultimo_dia['VL_QUOTA'].iloc[-1]
            nome_fundo = fundo_ultimo_dia['DENOM_SOCIAL'].iloc[0]
            data_cota = pd.to_datetime(ultima_data).strftime('%d/%m/%Y')

            # Verificar se o fundo já está na planilha e comparar as datas
            fundo_existente = existing_df[existing_df['CNPJ'] == cnpj]

            if not fundo_existente.empty:
                data_existente = fundo_existente['Data da Cota'].max()

                # Se a data da nova cota for mais recente, atualize
                if pd.to_datetime(data_cota, format='%d/%m/%Y') > data_existente:
                    dados_planilha.append([nome_fundo, cnpj, f"R$ {valor_cota_atual:.8f}".replace('.', ','), data_cota])
                else:
                    # Manter a cota mais atual existente
                    dados_planilha.append(fundo_existente.iloc[0].values.tolist())
            else:
                # Adicionar o fundo se ele não existir na planilha
                dados_planilha.append([nome_fundo, cnpj, f"R$ {valor_cota_atual:.8f}".replace('.', ','), data_cota])
        else:
            # Fundo não encontrado na base da CVM
            dados_planilha.append(["Fundo não encontrado", cnpj, "N/A", "N/A"])

    # Converter dados_planilha para DataFrame
    novos_dados_df = pd.DataFrame(dados_planilha, columns=['Nome do Fundo', 'CNPJ', 'Valor da Cota', 'Data da Cota'])

    # Preparar os dados para a planilha
    novos_dados_df['Data da Cota'] = pd.to_datetime(novos_dados_df['Data da Cota'], format='%d/%m/%Y', errors='coerce')
    novos_dados_df['Data da Cota'] = novos_dados_df['Data da Cota'].dt.strftime('%d/%m/%Y')
    dados_planilha_final = novos_dados_df.fillna('').values.tolist()

    # Limpar a planilha existente
    worksheet.clear()

    # Atualizar a planilha com os dados mais recentes
    worksheet.update('A1', [novos_dados_df.columns.tolist()] + dados_planilha_final)

    return "Dados atualizados na planilha com sucesso!"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)

# Testando o gatilho do Cloud Build
# pengo teste
# teste três
#teste 02
#teste 04
#cinco
#seis
#seis
#sete
