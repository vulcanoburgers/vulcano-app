import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from PIL import Image
from pyzbar.pyzbar import decode
import cv2
import tempfile
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# Autenticação Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
import json
# ...
try:
    # Tenta carregar as credenciais de `st.secrets`
    credentials_info = st.secrets["google_credentials"]
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scope)
except KeyError:
    st.error("Credenciais do Google não encontradas nos segredos do Streamlit.")
    st.stop() # Para o app se as credenciais não estiverem configuradas
client = gspread.authorize(credentials)

# Nome da planilha de despesas
sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U/edit#gid=0").sheet1

st.set_page_config(page_title="Controle de Despesas - Vulcano", layout="centered")
st.title("📸 Leitor de NFC-e (QR Code)")

# Função para extrair link do QR Code
def extract_qr_code(image_path):
    img = cv2.imread(image_path)
    barcodes = decode(img)
    for barcode in barcodes:
        data = barcode.data.decode("utf-8")
        if "sefaz" in data:
            return data
    return None

# Função para raspar dados da NFC-e
def extract_nfe_data_from_url(url):
    response = requests.get(url)
    if response.status_code != 200:
        return None, "Erro ao acessar a página da SEFAZ."

    soup = BeautifulSoup(response.content, "html.parser")
    texto = soup.get_text(separator=" ").replace("\n", " ")
    padrao = r"(.+?)\(Código:\s+(\d+)\)\s+Qtde\.:([\d,]+)\s+UN:([A-Z]+)\s+Vl\. Unit\.:([\d,]+)\s+Vl\. Total([\d,]+)"
    matches = re.findall(padrao, texto)
    
    items = []
    for match in matches:
        nome, cod, qtde, un, unit, total = match
        items.append({
            "Data Compra": datetime.now().strftime("%d/%m/%Y"),
            "Descrição": nome.strip(),
            "Categoria": "Mercado",
            "Sub-Categoria": "Não definido",
            "Forma de Pagamento": "Desconhecido",
            "Valor": float(total.replace(",", ".")),
            "Data Pagamento": datetime.now().strftime("%d/%m/%Y")
        })
    return items, None

# Upload de imagem do QR Code
uploaded_file = st.file_uploader("Envie uma imagem com o QR Code da nota", type=["png", "jpg", "jpeg"])
if uploaded_file is not None:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
        temp_file.write(uploaded_file.read())
        qr_link = extract_qr_code(temp_file.name)

    if qr_link:
        st.success(f"Link encontrado: {qr_link}")
        with st.spinner("Consultando nota..."):
            itens, erro = extract_nfe_data_from_url(qr_link)
            if erro:
                st.error(erro)
            elif itens:
                df = pd.DataFrame(itens)
                st.dataframe(df)
                if st.button("✅ Enviar itens para planilha"):
                    for row in df.itertuples(index=False):
                        sheet.append_row(list(row))
                    st.success("Itens enviados com sucesso!")
            else:
                st.warning("Nenhum item encontrado na nota.")
    else:
        st.error("Não foi possível identificar um QR Code válido na imagem.")
