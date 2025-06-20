import streamlit as st
import pandas as pd
import numpy as np
# from PIL import Image # Não precisamos mais para QR Code de URL direta
# import cv2 # Não precisamos mais para QR Code de URL direta
# from pyzbar.pyzbar import decode # Não precisamos mais para QR Code de URL direta
from google.oauth2.service_account import Credentials
import gspread
import datetime
import re
import locale
import requests
# from io import BytesIO # Não precisamos mais para QR Code de URL direta

# --- Configuração de Localização para Formato de Moeda e Data ---
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil')
    except locale.Error:
        st.warning("Não foi possível definir a localização para pt_BR. O formato de moeda e data pode não estar correto.")

# --- Autenticação Google Sheets ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

try:
    credentials_info = {
        "type": st.secrets["type"],
        "project_id": st.secrets["project_id"],
        "private_key_id": st.secrets["private_key_id"],
        "private_key": st.secrets["private_key"],
        "client_email": st.secrets["client_email"],
        "client_id": st.secrets["client_id"],
        "auth_uri": st.secrets["auth_uri"],
        "token_uri": st.secrets["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["client_x509_cert_url"],
        "universe_domain": st.secrets["universe_domain"]
    }

    credentials = Credentials.from_service_account_info(credentials_info, scopes=scope)
    client = gspread.authorize(credentials)
except KeyError as e:
    st.error(f"Erro: O segredo '{e}' não foi encontrado nos segredos do Streamlit.")
    st.info("Por favor, verifique se todos os campos do JSON de credenciais estão configurados como segredos individuais no Streamlit Cloud.")
    st.stop()
except Exception as e:
    st.error(f"Ocorreu um erro inesperado ao autenticar com o Google Sheets: {e}")
    st.info("Verifique se as credenciais estão corretas e se a API do Google Sheets está ativada no Google Cloud Console.")
    st.stop()

# Nome da planilha de despesas
try:
    sheet_url = "https://docs.google.com/spreadsheets/d/1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U/edit#gid=0"
    sheet = client.open_by_url(sheet_url).sheet1
except gspread.exceptions.SpreadsheetNotFound:
    st.error(f"Erro: Planilha não encontrada no URL: {sheet_url}")
    st.info("Verifique se o URL da planilha está correto e se a conta de serviço tem acesso de 'Editor'.")
    st.stop()
except Exception as e:
    st.error(f"Ocorreu um erro ao tentar abrir a planilha: {e}")
    st.info("Verifique sua conexão com a internet e as permissões da conta de serviço.")
    st.stop()


# --- Configuração da Página Streamlit ---
st.set_page_config(page_title="Controle de Despesas - Vulcano", layout="centered")
st.title("🔗 Leitor de NFC-e (URL)")

st.write("Cole a URL completa do QR Code (link da NFC-e) para extrair os dados da compra.")

# --- Função para extrair dados da URL do QR Code (link da NFC-e) ---
def extract_data_from_nfce_url(nfce_url):
    # As URLs de NFC-e geralmente contêm os dados no formato de parâmetros ou na chave de acesso
    # Ex: https://www.sefaz.rs.gov.br/NFCE/NFCE-CONS.aspx?chave=43230107297921000109650010000000001000000000&data=20231201&cnpj=07297921000109&valor=150.50&tpamb=1
    
    # Regex para a chave de acesso (44 dígitos)
    match_chave = re.search(r'chave=(\d{44})', nfce_url)
    chave_acesso = match_chave.group(1) if match_chave else "N/A"

    # Regex para o CNPJ (14 dígitos)
    match_cnpj = re.search(r'cnpj=(\d{14})', nfce_url)
    cnpj = match_cnpj.group(1) if match_cnpj else "N/A"

    # Regex para o valor (formato numérico com ponto decimal)
    match_valor = re.search(r'(valor|vICMS|vProd|vLiq|vNF|vCFe)=([\d.]+)', nfce_url, re.IGNORECASE)
    valor_total = "N/A"
    if match_valor:
        valor_total = match_valor.group(2).replace('.', ',') # Converte para padrão BR

    # Regex para a data (formato ASDFDDMMYYYY ou DD/MM/YYYY)
    match_data = re.search(r'data=(\d{8})', nfce_url) # Para data=YYYYMMDD
    if match_data:
        data_str = match_data.group(1)
        data_compra = f"{data_str[6:8]}/{data_str[4:6]}/{data_str[0:4]}" # Converte para DD/MM/YYYY
    else:
        match_data = re.search(r'(\d{2})/(\d{2})/(\d{4})', nfce_url)
        if match_data:
            data_compra = match_data.group(0) # Retorna a data completa DD/MM/YYYY
        else:
            data_compra = "N/A"

    return valor_total, data_compra, cnpj, chave_acesso

# --- Input da URL e Processamento ---
nfce_url = st.text_input("Cole a URL da NFC-e (link do QR Code) aqui:")

if nfce_url:
    st.info("Tentando extrair dados da URL...")
    try:
        # Aqui, a "url do QR Code" é o link para a consulta da NFC-e
        valor_total, data_compra, cnpj, chave_acesso = extract_data_from_nfce_url(nfce_url)

        # Para as colunas adicionais, podemos preencher com valores padrão ou solicitar ao usuário
        descricao = st.text_input("Descrição da Compra (Opcional)", value="")
        categoria = st.selectbox("Categoria", ["Alimentação", "Transporte", "Lazer", "Contas da Casa", "Outros"], index=0)
        sub_categoria = st.text_input("Sub-Categoria (Opcional)", value="")
        forma_pagamento = st.selectbox("Forma de Pagamento", ["Cartão de Crédito", "Cartão de Débito", "Dinheiro", "PIX"], index=0)
        
        # Data Pagamento: por padrão, igual à Data Compra, mas editável
        data_pagamento_dt = datetime.datetime.strptime(data_compra, "%d/%m/%Y").date() if data_compra != "N/A" else datetime.date.today()
        data_pagamento = st.date_input("Data do Pagamento", value=data_pagamento_dt)
        data_pagamento_str = data_pagamento.strftime("%d/%m/%Y")


        st.subheader("Dados Extraídos e Sugeridos:")
        
        # Crie um DataFrame para exibir os dados e facilitar a inserção
        data_for_display = {
            "Data Compra": [data_compra],
            "Descrição": [descricao],
            "Categoria": [categoria],
            "Sub-Categoria": [sub_categoria],
            "Forma de Pagamento": [forma_pagamento],
            "Valor": [valor_total],
            "Data Pagamento": [data_pagamento_str],
            "CNPJ Emissor (Auxiliar)": [cnpj], # Manter como auxiliar, não vai para a planilha
            "Chave de Acesso (Auxiliar)": [chave_acesso] # Manter como auxiliar
        }
        df_display = pd.DataFrame(data_for_display)
        st.dataframe(df_display.iloc[:, :7]) # Exibe apenas as colunas da planilha
        
        # Apenas para mostrar ao usuário os dados auxiliares
        st.write(f"CNPJ do Emissor (extraído da URL): {cnpj}")
        st.write(f"Chave de Acesso (extraído da URL): {chave_acesso}")


        # --- Adicionar ao Google Sheets ---
        if st.button("Adicionar Dados à Planilha Google"):
            try:
                # Prepare a linha para inserção, na ordem exata das suas colunas
                valor_para_sheet = valor_total
                if isinstance(valor_para_sheet, str):
                    # Garante que 'R$' ou espaços não sejam enviados
                    valor_para_sheet = valor_para_sheet.replace('R$', '').strip()

                row_to_insert = [
                    data_compra,        # Data Compra
                    descricao,          # Descrição
                    categoria,          # Categoria
                    sub_categoria,      # Sub-Categoria
                    forma_pagamento,    # Forma de Pagamento
                    valor_para_sheet,   # Valor
                    data_pagamento_str  # Data Pagamento
                ]
                
                sheet.append_row(row_to_insert)
                st.success("Dados adicionados com sucesso à planilha Google!")
            except Exception as e:
                st.error(f"Erro ao adicionar dados à planilha: {e}")
                st.info("Verifique se a conta de serviço tem permissão de escrita e se as colunas estão corretas.")

    except requests.exceptions.MissingSchema:
        st.error("URL inválida. Certifique-se de incluir 'http://' ou 'https://'.")
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao acessar a URL: {e}. Verifique se a URL está correta e é acessível.")
    except Exception as e:
        st.error(f"Erro inesperado ao processar a URL: {e}")
        st.info("O formato da URL pode não ser o esperado. Tente copiar o link completo do QR Code.")

else:
    st.info("Cole a URL da NFC-e no campo acima para começar.")


# --- Seção de Visualização de Dados Existentes ---
st.markdown("---")
st.header("Dados Atuais da Planilha")

@st.cache_data(ttl=600) # Cache os dados por 10 minutos
def load_data_from_sheets():
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    # Adaptação para as novas colunas
    if "Data Compra" in df.columns:
        df["Data Compra"] = pd.to_datetime(df["Data Compra"], format="%d/%m/%Y", errors='coerce')
    if "Valor" in df.columns: # Agora a coluna é "Valor"
        df["Valor"] = df["Valor"].astype(str).str.replace(',', '.', regex=False)
        # CORREÇÃO AQUI: aspas simples para 'coerce'
        df["Valor"] = pd.to_numeric(df["Valor"], errors='coerce')
    if "Data Pagamento" in df.columns:
        df["Data Pagamento"] = pd.to_datetime(df["Data Pagamento"], format="%d/%m/%Y", errors='coerce')

    return df

if st.button("Recarregar Dados da Planilha"):
    st.cache_data.clear()
    df_existing = load_data_from_sheets()
    st.success("Dados recarregados!")
else:
    df_existing = load_data_from_sheets()

if not df_existing.empty:
    st.dataframe(df_existing)

    st.subheader("Resumo das Despesas")
    # A coluna de valor agora é "Valor"
    total_despesas = df_existing["Valor"].sum() 
    st.metric(label="Total de Despesas Registradas", value=locale.currency(total_despesas, grouping=True))

    st.subheader("Despesas por Mês")
    # Usa a coluna "Data Compra" para o mês
    df_existing['Mês'] = df_existing['Data Compra'].dt.to_period('M')
    despesas_por_mes = df_existing.groupby('Mês')['Valor'].sum().reset_index() # Usa a coluna "Valor"
    despesas_por_mes['Mês'] = despesas_por_mes['Mês'].astype(str)

    st.bar_chart(despesas_por_mes.set_index('Mês'))

else:
    st.info("Nenhum dado encontrado na planilha ainda.")
