from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import zipfile
from datetime import datetime, timedelta
import json
import os

app = Flask(__name__)

def baixar_e_processar_dados(ano, mes):
    try:
        url = f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ano}{mes}.zip'
        download = requests.get(url)

        if download.status_code == 200:
            with open(f"inf_diario_fi_{ano}{mes}.zip", "wb") as arquivo_cvm:
                arquivo_cvm.write(download.content)

            arquivo_zip = zipfile.ZipFile(f"inf_diario_fi_{ano}{mes}.zip")
            dados_fundos = pd.read_csv(arquivo_zip.open(arquivo_zip.namelist()[0]), sep=";", encoding='ISO-8859-1')

            # Ajuste de cabeçalhos para dados_fundos
            dados_fundos.rename(columns={
                "CNPJ_FUNDO_CLASSE": "CNPJ_FUNDO",
                "VL_QUOTA": "VL_QUOTA",
                "DT_COMPTC": "DT_COMPTC"
            }, inplace=True)

            dados_cadastro = pd.read_csv(
                'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv',
                sep=";",
                encoding='ISO-8859-1'
            )
            dados_cadastro = dados_cadastro[['CNPJ_FUNDO', 'DENOM_SOCIAL']].drop_duplicates()

            # Ajuste de colunas e merge
            base_final = pd.merge(dados_fundos, dados_cadastro, how="left", left_on="CNPJ_FUNDO", right_on="CNPJ_FUNDO")
            base_final = base_final[['CNPJ_FUNDO', 'DENOM_SOCIAL', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST']]

            return base_final
        else:
            return pd.DataFrame()  # Retorna DataFrame vazio se não conseguir baixar o arquivo
    except Exception as e:
        print(f"Erro ao baixar ou processar os dados: {str(e)}")
        return pd.DataFrame()

@app.route("/")
def update_spreadsheet():
    try:
        json_creds = os.environ.get("GOOGLE_CREDENTIALS")
        creds_dict = json.loads(json_creds)

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open('Dados Fundos')
        worksheet = spreadsheet.worksheet('Dados')

        existing_data = worksheet.get_all_values()

        if existing_data:
            headers = existing_data[0]
            existing_df = pd.DataFrame(existing_data[1:], columns=headers)
            existing_df['Data da Cota'] = pd.to_datetime(existing_df['Data da Cota'], format='%d/%m/%Y', errors='coerce')
        else:
            existing_df = pd.DataFrame(columns=['Nome do Fundo', 'CNPJ', 'Valor da Cota', 'Data da Cota'])

        hoje = datetime.today()
        ano_atual = hoje.strftime('%Y')
        mes_atual = hoje.strftime('%m')

        base_atual = baixar_e_processar_dados(ano_atual, mes_atual)

        fundos_especificos = [
            "26.673.556/0001-32",
            "10.347.493/0001-94",
            "23.272.391/0001-07", 
            "08.830.947/0001-31",
            "37.910.132/0001-60",
            "32.990.051/0001-02", 
            "30.566.221/0001-92",
            "34.583.819/0001-40",
            "34.780.531/0001-66", 
            "45.278.833/0001-57",
            "32.893.503/0001-20",
            "35.471.498/0001-55", 
            "30.509.221/0001-50",
            "12.154.412/0001-65",
            "39.959.025/0001-52", 
            "32.892.827/0001-43",
            "39.586.835/0001-00",
            "33.520.968/0001-06", 
            "22.918.359/0001-85",
            "42.794.534/0001-87",
            "44.211.851/0001-59", 
            "42.922.205/0001-74",
            "35.956.641/0001-07",
            "37.053.502/0001-90",
            "10.843.445/0001-97",
        ]

        dados_planilha = []
        for cnpj in fundos_especificos:
            fundo = base_atual[base_atual['CNPJ_FUNDO'] == cnpj]

            if fundo.empty:
                mes_anterior = (hoje - timedelta(days=30)).strftime('%m')
                ano_anterior = (hoje - timedelta(days=30)).strftime('%Y')
                base_anterior = baixar_e_processar_dados(ano_anterior, mes_anterior)
                fundo = base_anterior[base_anterior['CNPJ_FUNDO'] == cnpj]

            if not fundo.empty:
                ultima_data = fundo['DT_COMPTC'].max()
                fundo_ultimo_dia = fundo[fundo['DT_COMPTC'] == ultima_data]
                valor_cota_atual = fundo_ultimo_dia['VL_QUOTA'].iloc[-1]
                nome_fundo = fundo_ultimo_dia['DENOM_SOCIAL'].iloc[0]
                data_cota = pd.to_datetime(ultima_data).strftime('%d/%m/%Y')

                fundo_existente = existing_df[existing_df['CNPJ'] == cnpj]

                if not fundo_existente.empty:
                    data_existente = fundo_existente['Data da Cota'].max()
                    if pd.to_datetime(data_cota, format='%d/%m/%Y') > data_existente:
                        dados_planilha.append([nome_fundo, cnpj, f"R$ {valor_cota_atual:.8f}".replace('.', ','), data_cota])
                else:
                    dados_planilha.append([nome_fundo, cnpj, f"R$ {valor_cota_atual:.8f}".replace('.', ','), data_cota])
            else:
                dados_planilha.append(["Fundo não encontrado", cnpj, "N/A", "N/A"])

        novos_dados_df = pd.DataFrame(dados_planilha, columns=['Nome do Fundo', 'CNPJ', 'Valor da Cota', 'Data da Cota'])
        worksheet.clear()
        worksheet.update('A1', [novos_dados_df.columns.tolist()] + novos_dados_df.fillna('').values.tolist())

        return "Dados atualizados na planilha com sucesso!"
    except Exception as e:
        return f"Ocorreu um erro: {str(e)}"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
