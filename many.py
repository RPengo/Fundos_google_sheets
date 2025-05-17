from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import zipfile
from datetime import datetime, timedelta
import json
import os
import time

app = Flask(__name__)

def baixar_e_processar_dados(ano, mes):
    try:
        url = f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ano}{mes}.zip'
        download = requests.get(url)

        if download.status_code == 200:
            zip_path = f"inf_diario_fi_{ano}{mes}.zip"
            with open(zip_path, "wb") as arquivo_cvm:
                arquivo_cvm.write(download.content)

            with zipfile.ZipFile(zip_path) as arquivo_zip:
                dados_fundos = pd.read_csv(arquivo_zip.open(arquivo_zip.namelist()[0]), sep=";", encoding='ISO-8859-1')

            os.remove(zip_path)

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

            base_final = pd.merge(dados_fundos, dados_cadastro, how="left", on="CNPJ_FUNDO")
            base_final = base_final[['CNPJ_FUNDO', 'DENOM_SOCIAL', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST']]

            return base_final
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Erro ao baixar ou processar os dados: {str(e)}")
        return pd.DataFrame()

def buscar_dados_mais_recentes(cnpjs, max_meses=2):
    hoje = datetime.today()
    meses = [(hoje - timedelta(days=30 * i)).strftime('%Y%m') for i in range(max_meses)]

    df_completo = pd.DataFrame()
    for ym in meses:
        ano, mes = ym[:4], ym[4:]
        df_mes = baixar_e_processar_dados(ano, mes)
        df_completo = pd.concat([df_completo, df_mes], ignore_index=True)

    df_completo.sort_values("DT_COMPTC", ascending=False, inplace=True)
    return df_completo[df_completo["CNPJ_FUNDO"].isin(cnpjs)]

def verificar_faltantes(planilha, fundos_especificos):
    try:
        cnpjs_atualizados = [row[1] for row in planilha.get_all_values()[1:]]  # Coluna B: CNPJs
        cnpjs_faltantes = [cnpj for cnpj in fundos_especificos if cnpj not in cnpjs_atualizados]
        return cnpjs_faltantes
    except Exception as e:
        print(f"Erro ao verificar fundos faltantes: {str(e)}")
        return fundos_especificos

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

        fundos_especificos = [
            "26.673.556/0001-32", "10.347.493/0001-94", "23.272.391/0001-07", "08.830.947/0001-31",
            "37.910.132/0001-60", "32.990.051/0001-02", "30.566.221/0001-92", "34.583.819/0001-40",
            "34.780.531/0001-66", "45.278.833/0001-57", "32.893.503/0001-20", "35.471.498/0001-55",
            "30.509.221/0001-50", "12.154.412/0001-65", "39.959.025/0001-52", "32.892.827/0001-43",
            "39.586.858/0001-15", "33.520.968/0001-06", "22.918.359/0001-85", "42.794.534/0001-87",
            "44.211.851/0001-59", "42.922.205/0001-74", "35.956.641/0001-07", "37.053.502/0001-90",
            "10.843.445/0001-97", "25.213.405/0001-39", "49.227.982/0001-48"
        ]

        tentativas = 0
        max_tentativas = 5
        cnpjs_faltantes = fundos_especificos

        while cnpjs_faltantes and tentativas < max_tentativas:
            print(f"Tentativa {tentativas + 1}: Processando {len(cnpjs_faltantes)} fundos restantes.")

            base_dados = buscar_dados_mais_recentes(cnpjs_faltantes)
            dados_planilha = []

            for cnpj in cnpjs_faltantes:
                try:
                    fundo = base_dados[base_dados['CNPJ_FUNDO'] == cnpj]
                    if not fundo.empty:
                        ultima_data = fundo['DT_COMPTC'].max()
                        fundo_ultimo_dia = fundo[fundo['DT_COMPTC'] == ultima_data]
                        if not fundo_ultimo_dia.empty:
                            nome_fundo = fundo_ultimo_dia['DENOM_SOCIAL'].iloc[0]
                            valor_cota = fundo_ultimo_dia['VL_QUOTA'].iloc[0]
                            data_cota = pd.to_datetime(ultima_data).strftime('%d/%m/%Y')
                            dados_planilha.append([nome_fundo, cnpj, f"R$ {valor_cota:.8f}".replace('.', ','), data_cota])
                except Exception as e:
                    print(f"Erro ao processar fundo {cnpj}: {e}")

            if dados_planilha:
                novos_dados_df = pd.DataFrame(dados_planilha, columns=['Nome do Fundo', 'CNPJ', 'Valor da Cota', 'Data da Cota'])
                worksheet.update('A1', [novos_dados_df.columns.tolist()] + novos_dados_df.fillna('').values.tolist())

            cnpjs_faltantes = verificar_faltantes(worksheet, fundos_especificos)
            tentativas += 1
            time.sleep(5)

        if cnpjs_faltantes:
            try:
                aba_falhas = spreadsheet.worksheet('Falhas')
            except:
                aba_falhas = spreadsheet.add_worksheet(title='Falhas', rows='30', cols='2')

            falhas_data = [[cnpj] for cnpj in cnpjs_faltantes]
            aba_falhas.update('A1', [['CNPJ não encontrado']] + falhas_data)

            return f"Não foi possível atualizar todos os fundos. Faltantes: {', '.join(cnpjs_faltantes)}"

        return "Todos os fundos foram atualizados com sucesso!"
    except Exception as e:
        return f"Ocorreu um erro: {str(e)}"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
