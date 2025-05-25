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

def filtrar_fundos_interesse(df, fundos_interesse):
    if 'ID_SUBCLASSE' not in df.columns:
        df['ID_SUBCLASSE'] = ''
    df['ID_SUBCLASSE'] = df['ID_SUBCLASSE'].fillna('').astype(str)
    mascara = pd.Series(False, index=df.index)
    for fundo in fundos_interesse:
        cnpj = fundo['CNPJ_FUNDO']
        subclasse = fundo['ID_SUBCLASSE']
        if not subclasse:
            mascara |= (df['CNPJ_FUNDO'] == cnpj)
        else:
            mascara |= (df['CNPJ_FUNDO'] == cnpj) & (df['ID_SUBCLASSE'] == subclasse)
    return df[mascara].copy()

def buscar_dados_mais_recentes(fundos_interesse, max_meses=2):
    hoje = datetime.today()
    meses = [(hoje - timedelta(days=30 * i)).strftime('%Y%m') for i in range(max_meses)]

    df_completo = pd.DataFrame()
    for ym in meses:
        ano, mes = ym[:4], ym[4:]
        df_mes = baixar_e_processar_dados(ano, mes)
        df_completo = pd.concat([df_completo, df_mes], ignore_index=True)

    df_completo.sort_values("DT_COMPTC", ascending=False, inplace=True)
    return filtrar_fundos_interesse(df_completo, fundos_interesse)

def verificar_faltantes(planilha, fundos_interesse):
    try:
        planilha_values = planilha.get_all_values()
        cnpjs_atualizados = [row[1] for row in planilha_values[1:]]
        subclasses_atualizadas = [row[2] if len(row) > 2 else '' for row in planilha_values[1:]]
        faltantes = []
        for fundo in fundos_interesse:
            cnpj = fundo['CNPJ_FUNDO']
            subclasse = fundo['ID_SUBCLASSE']
            found = False
            for cnpj_plan, sub_plan in zip(cnpjs_atualizados, subclasses_atualizadas):
                if cnpj == cnpj_plan and (str(subclasse) == str(sub_plan)):
                    found = True
                    break
            if not found:
                faltantes.append(fundo)
        return faltantes
    except Exception as e:
        print(f"Erro ao verificar fundos faltantes: {str(e)}")
        return fundos_interesse

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

        # Lê CNPJ e subclasse da aba "Fundos"
        fundos_sheet = spreadsheet.worksheet('Fundos')
        fundos_values = fundos_sheet.get_all_values()
        fundos_interesse = []
        for row in fundos_values[1:]:
            if len(row) >= 2 and row[1].strip():
                cnpj = row[1].strip()
                subclasse = row[2].strip() if len(row) > 2 else ''
                fundos_interesse.append({'CNPJ_FUNDO': cnpj, 'ID_SUBCLASSE': subclasse})

        tentativas = 0
        max_tentativas = 5
        faltantes = fundos_interesse

        while faltantes and tentativas < max_tentativas:
            print(f"Tentativa {tentativas + 1}: Processando {len(faltantes)} fundos restantes.")

            base_dados = buscar_dados_mais_recentes(faltantes)
            dados_planilha = []

            for fundo in faltantes:
                try:
                    if fundo['ID_SUBCLASSE']:
                        filtro = (
                            (base_dados['CNPJ_FUNDO'] == fundo['CNPJ_FUNDO']) &
                            (base_dados['ID_SUBCLASSE'] == fundo['ID_SUBCLASSE'])
                        )
                    else:
                        filtro = (base_dados['CNPJ_FUNDO'] == fundo['CNPJ_FUNDO'])
                    fundo_df = base_dados[filtro]
                    if not fundo_df.empty:
                        ultima_data = fundo_df['DT_COMPTC'].max()
                        fundo_ultimo_dia = fundo_df[fundo_df['DT_COMPTC'] == ultima_data]
                        if not fundo_ultimo_dia.empty:
                            nome_fundo = fundo_ultimo_dia['DENOM_SOCIAL'].iloc[0]
                            cnpj = fundo['CNPJ_FUNDO']
                            subclasse = fundo['ID_SUBCLASSE']
                            valor_cota = fundo_ultimo_dia['VL_QUOTA'].iloc[0]
                            data_cota = pd.to_datetime(ultima_data).strftime('%d/%m/%Y')
                            dados_planilha.append([nome_fundo, cnpj, subclasse, f"R$ {valor_cota:.8f}".replace('.', ','), data_cota])
                except Exception as e:
                    print(f"Erro ao processar fundo {fundo}: {e}")

            if dados_planilha:
                novos_dados_df = pd.DataFrame(dados_planilha, columns=['Nome do Fundo', 'CNPJ', 'ID_SUBCLASSE', 'Valor da Cota', 'Data da Cota'])
                worksheet.update('A1', [novos_dados_df.columns.tolist()] + novos_dados_df.fillna('').values.tolist())

            faltantes = verificar_faltantes(worksheet, fundos_interesse)
            tentativas += 1
            time.sleep(5)

        if faltantes:
            try:
                aba_falhas = spreadsheet.worksheet('Falhas')
            except:
                aba_falhas = spreadsheet.add_worksheet(title='Falhas', rows='30', cols='2')

            falhas_data = [[fundo['CNPJ_FUNDO'], fundo['ID_SUBCLASSE']] for fundo in faltantes]
            aba_falhas.update('A1', [['CNPJ não encontrado', 'ID_SUBCLASSE']] + falhas_data)

            faltantes_list = []
            for f in faltantes:
                if f['ID_SUBCLASSE']:
                    faltantes_list.append(f"{f['CNPJ_FUNDO']} (subclasse {f['ID_SUBCLASSE']})")
                else:
                    faltantes_list.append(f["CNPJ_FUNDO"])
            return "Não foi possível atualizar todos os fundos. Faltantes: " + ", ".join(faltantes_list)

        return "Todos os fundos foram atualizados com sucesso!"
    except Exception as e:
        return f"Ocorreu um erro: {str(e)}"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
