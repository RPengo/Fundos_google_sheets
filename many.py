from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import zipfile
from datetime import datetime
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

            if 'ID_SUBCLASSE' not in dados_fundos.columns:
                dados_fundos['ID_SUBCLASSE'] = ''
            dados_fundos['ID_SUBCLASSE'] = dados_fundos['ID_SUBCLASSE'].fillna('').astype(str)

            dados_cadastro = pd.read_csv(
                'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv',
                sep=";",
                encoding='ISO-8859-1'
            )
            dados_cadastro = dados_cadastro[['CNPJ_FUNDO', 'DENOM_SOCIAL']].drop_duplicates()

            base_final = pd.merge(dados_fundos, dados_cadastro, how="left", on="CNPJ_FUNDO")
            base_final = base_final[['CNPJ_FUNDO', 'ID_SUBCLASSE', 'DENOM_SOCIAL', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST']]

            return base_final
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Erro ao baixar ou processar os dados: {str(e)}")
        return pd.DataFrame()

def buscar_ultima_cota_fundo(cnpj, subclasse='', max_anos=2):
    hoje = datetime.today()
    ano_atual = hoje.year
    mes_atual = hoje.month

    for ano in range(ano_atual, ano_atual - max_anos, -1):
        mes_final = 12 if ano < ano_atual else mes_atual
        for mes in range(mes_final, 0, -1):
            mes_str = str(mes).zfill(2)
            print(f"Buscando {cnpj} {subclasse} em {ano}{mes_str} ...")
            df_mes = baixar_e_processar_dados(str(ano), mes_str)
            if not df_mes.empty:
                if subclasse:
                    fundo_df = df_mes[(df_mes['CNPJ_FUNDO'] == cnpj) & (df_mes['ID_SUBCLASSE'] == subclasse)]
                else:
                    fundo_df = df_mes[df_mes['CNPJ_FUNDO'] == cnpj]
                if not fundo_df.empty:
                    fundo_df['DT_COMPTC'] = pd.to_datetime(fundo_df['DT_COMPTC'], errors='coerce')
                    fundo_df = fundo_df.sort_values('DT_COMPTC', ascending=False)
                    return fundo_df.iloc[0]
    return None

def verificar_faltantes(planilha, fundos_info):
    try:
        linhas = planilha.get_all_values()[1:]
        atualizados = {(row[1], row[2] if len(row) > 2 else '') for row in linhas}
        faltantes = [f for f in fundos_info if (f['CNPJ'], f['SUBCLASSE']) not in atualizados]
        return faltantes
    except Exception as e:
        print(f"Erro ao verificar fundos faltantes: {str(e)}")
        return fundos_info

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

        # Lê CNPJs e subclasses da aba "Fundos"
        fundos_sheet = spreadsheet.worksheet('Fundos')
        valores = fundos_sheet.get_all_values()[1:]
        fundos_info = []
        for row in valores:
            if row and row[0].strip():
                cnpj = row[0].strip()
                subclasse = row[2].strip() if len(row) > 2 else ''
                fundos_info.append({'CNPJ': cnpj, 'SUBCLASSE': subclasse})

        tentativas = 0
        max_tentativas = 5
        faltantes = fundos_info

        while faltantes and tentativas < max_tentativas:
            print(f"Tentativa {tentativas + 1}: Processando {len(faltantes)} fundos restantes.")

            dados_planilha = []

            for fundo in faltantes:
                try:
                    cnpj = fundo['CNPJ']
                    subclasse = fundo['SUBCLASSE']
                    registro = buscar_ultima_cota_fundo(cnpj, subclasse, max_anos=2)
                    if registro is not None:
                        nome_fundo = registro['DENOM_SOCIAL']
                        valor_cota = registro['VL_QUOTA']
                        data_cota = pd.to_datetime(registro['DT_COMPTC']).strftime('%d/%m/%Y')
                        dados_planilha.append([
                            nome_fundo, cnpj, subclasse, f"R$ {valor_cota:.8f}".replace('.', ','), data_cota
                        ])
                except Exception as e:
                    print(f"Erro ao processar fundo {fundo}: {e}")

            if dados_planilha:
                novos_dados_df = pd.DataFrame(dados_planilha, columns=['Nome do Fundo', 'CNPJ', 'ID_SUBCLASSE', 'Valor da Cota', 'Data da Cota'])
                worksheet.update('A1', [novos_dados_df.columns.tolist()] + novos_dados_df.fillna('').values.tolist())

            faltantes = verificar_faltantes(worksheet, fundos_info)
            tentativas += 1
            time.sleep(5)

        if faltantes:
            try:
                aba_falhas = spreadsheet.worksheet('Falhas')
            except:
                aba_falhas = spreadsheet.add_worksheet(title='Falhas', rows='30', cols='2')

            falhas_data = [[f['CNPJ'], f['SUBCLASSE']] for f in faltantes]
            aba_falhas.update('A1', [['CNPJ não encontrado', 'ID_SUBCLASSE']] + falhas_data)

            msg = "Não foi possível atualizar todos os fundos. Faltantes: "
            msg += ', '.join(
                [f"{f['CNPJ']} (subclasse {f['SUBCLASSE']})" if f['SUBCLASSE'] else f['CNPJ'] for f in faltantes]
            )
            return msg

        return "Todos os fundos foram atualizados com sucesso!"
    except Exception as e:
        return f"Ocorreu um erro: {str(e)}"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
