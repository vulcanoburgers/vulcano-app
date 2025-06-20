import streamlit as st
import pandas as pd
import numpy as np
from PIL import Image
import cv2
from pyzbar.pyzbar import decode
from google.oauth2.service_account import Credentials
import gspread
import datetime
import re
import locale
import requests # Importar para fazer requisições HTTP (baixar imagem de URL)
from io import BytesIO # Importar para lidar com bytes da imagem

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
st.title("📸 Leitor de NFC-e (QR Code)")

st.write("Envie uma imagem da NFC-e, tire uma foto ou cole a URL de um QR Code.")

# --- Função para extrair dados do QR Code ---
def extract_data_from_qr_code(qr_data):
    # Regex para encontrar o valor total (geralmente depois de "vICMS=XX.YY" ou "vLiq=XX.YY")
    match_valor = re.search(r'(vICMS|vProd|vLiq|vNF|vCFe)=([\d.]+)', qr_data)
    valor_total = "N/A"
    if match_valor:
        valor_total = match_valor.group(2).replace('.', ',')

    # Regex para encontrar a data (formato YYYYMMDD ou DD/MM/YYYY)
    match_data = re.search(r'dhEmi=.*?(\d{4})(\d{2})(\d{2})', qr_data)
    if match_data:
        ano = match_data.group(1)
        mes = match_data.group(2)
        dia = match_data.group(3)
        data_compra = f"{dia}/{mes}/{ano}"
    else:
        match_data = re.search(r'(\d{2})/(\d{2})/(\d{4})', qr_data)
        if match_data:
            data_compra = match_data.group(0)
        else:
            data_compra = "N/A"

    # Regex para encontrar o CNPJ (14 dígitos)
    match_cnpj = re.search(r'CNPJ=(\d{14})', qr_data)
    cnpj = match_cnpj.group(1) if match_cnpj else "N/A"

    # Regex para encontrar a chave de acesso da NFC-e (44 dígitos)
    match_chave = re.search(r'chNFe=(\d{44})', qr_data)
    chave_acesso = match_chave.group(1) if match_chave else "N/A"

    return valor_total, data_compra, cnpj, chave_acesso

# --- Processamento de Imagem ---
def process_image(image_to_process):
    if image_to_process is None:
        return

    # Converter imagem para formato OpenCV (numpy array)
    img_cv = np.array(image_to_process)
    if len(img_cv.shape) == 3:
        img_gray = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY)
    else:
        img_gray = img_cv

    decoded_objects = decode(img_gray)

    if decoded_objects:
        st.subheader("Dados Extraídos:")
        all_data = []
        for obj in decoded_objects:
            qr_data = obj.data.decode('utf-8')
            st.write(f"QR Code Conteúdo: {qr_data}")

            valor_total, data_compra, cnpj, chave_acesso = extract_data_from_qr_code(qr_data)

            st.write(f"**Valor Total:** R$ {valor_total}")
            st.write(f"**Data da Compra:** {data_compra}")
            st.write(f"**CNPJ do Emissor:** {cnpj}")
            st.write(f"**Chave de Acesso:** {chave_acesso}")

            all_data.append({
                "Data da Compra": data_compra,
                "Valor Total (R$)": valor_total,
                "CNPJ do Emissor": cnpj,
                "Chave de Acesso": chave_acesso
            })

        if all_data:
            df = pd.DataFrame(all_data)
            st.subheader("Dados Prontos para Adicionar:")
            st.dataframe(df)

            # --- Adicionar ao Google Sheets ---
            if st.button("Adicionar Dados à Planilha Google"):
                try:
                    for index, row in df.iterrows():
                        valor_para_sheet = row["Valor Total (R$)"]
                        if isinstance(valor_para_sheet, str):
                            valor_para_sheet = valor_para_sheet.replace('R$', '').strip()

                        sheet.append_row([
                            row["Data da Compra"],
                            valor_para_sheet,
                            row["CNPJ do Emissor"],
                            row["Chave de Acesso"]
                        ])
                    st.success("Dados adicionados com sucesso à planilha Google!")
                except Exception as e:
                    st.error(f"Erro ao adicionar dados à planilha: {e}")
                    st.info("Verifique se a conta de serviço tem permissão de escrita e se as colunas estão corretas.")
    else:
        st.warning("Nenhum QR Code detectado na imagem. Por favor, tente outra imagem.")

# --- Seleção de Input ---
input_option = st.radio(
    "Como você gostaria de fornecer a imagem do QR Code?",
    ("Upload de Arquivo", "Colar URL da Imagem"),
    key="input_option_radio"
)

image_to_process = None

if input_option == "Upload de Arquivo":
    uploaded_file = st.file_uploader("Escolha uma imagem de NFC-e...", type=["jpg", "jpeg", "png"])
    if uploaded_file is not None:
        image_to_process = Image.open(uploaded_file)
        st.image(image_to_process, caption='Imagem Carregada', use_column_width=True)
elif input_option == "Colar URL da Imagem":
    qr_code_url = st.text_input("Cole a URL da imagem do QR Code aqui:")
    if qr_code_url:
        st.info("Tentando carregar imagem da URL...")
        try:
            response = requests.get(qr_code_url)
            response.raise_for_status() # Levanta um erro para códigos de status HTTP ruins (4xx ou 5xx)
            image_to_process = Image.open(BytesIO(response.content))
            st.image(image_to_process, caption='Imagem da URL', use_column_width=True)
        except requests.exceptions.MissingSchema:
            st.error("URL inválida. Certifique-se de incluir 'http://' ou 'https://'.")
        except requests.exceptions.RequestException as e:
            st.error(f"Erro ao carregar a imagem da URL: {e}. Verifique se a URL está correta e é acessível.")
        except Exception as e:
            st.error(f"Erro inesperado ao processar a URL da imagem: {e}")

# Processa a imagem se uma foi fornecida (ou por upload ou por URL)
if image_to_process is not None:
    process_image(image_to_process)


# --- Seção de Visualização de Dados Existentes ---
st.markdown("---")
st.header("Dados Atuais da Planilha")

@st.cache_data(ttl=600) # Cache os dados por 10 minutos
def load_data_from_sheets():
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    if "Data da Compra" in df.columns:
        df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y", errors='coerce')
    if "Valor Total (R$)" in df.columns:
        df["Valor Total (R$)"] = df["Valor Total (R$)"].astype(str).str.replace(',', '.', regex=False)
        df["Valor Total (R$)"] = pd.to_numeric(df["Valor Total (R$)"], errors='coerce')
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
    total_despesas = df_existing["Valor Total (R$)"].sum()
    st.metric(label="Total de Despesas Registradas", value=locale.currency(total_despesas, grouping=True))

    st.subheader("Despesas por Mês")
    df_existing['Mês'] = df_existing['Data da Compra'].dt.to_period('M')
    despesas_por_mes = df_existing.groupby('Mês')['Valor Total (R$)'].sum().reset_index()
    despesas_por_mes['Mês'] = despesas_por_mes['Mês'].astype(str)

    st.bar_chart(despesas_por_mes.set_index('Mês'))

else:
    st.info("Nenhum dado encontrado na planilha ainda.")
