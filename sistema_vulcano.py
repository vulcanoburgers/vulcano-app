# NOVO CÓDIGO COM FUNCIONALIDADE DE EXTRAÇÃO VIA HTML (REQUESTS + BeautifulSoup)

import streamlit as st
import pandas as pd
import datetime
import locale
import re
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread

# Locale para pt-BR
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    locale.setlocale(locale.LC_ALL, '')

# Google Sheets auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials_info = {key: st.secrets[key] for key in st.secrets}
credentials = Credentials.from_service_account_info(credentials_info, scopes=scope)
client = gspread.authorize(credentials)
sheet_url = "https://docs.google.com/spreadsheets/d/1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U/edit#gid=0"
sheet = client.open_by_url(sheet_url).sheet1

# Streamlit UI
st.set_page_config(page_title="NFC-e Vulcano", layout="centered")
st.title("📥 Leitor de NFC-e por Link")

url = st.text_input("Cole o link completo da NFC-e")

if url:
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'html.parser')

        full_text = soup.get_text(" ", strip=True)

        produtos = re.findall(r"(.+?)\(Código: (\d+)\)Qtde.:(\d+[\.,]?\d*)UN:(\w+)Vl. Unit.:(\d+[\.,]?\d*)Vl. Total(\d+[\.,]?\d*)", full_text)

        if produtos:
            st.subheader("Produtos na nota")
            df = pd.DataFrame(produtos, columns=["Produto", "Código", "Quantidade", "Unidade", "Valor Unitário", "Valor Total"])

            for col in ["Quantidade", "Valor Unitário", "Valor Total"]:
                df[col] = df[col].str.replace(",", ".").astype(float)

            st.dataframe(df)

            if st.button("Enviar produtos para Google Sheets"):
                hoje = datetime.date.today().strftime("%d/%m/%Y")
                for i, row in df.iterrows():
                    nova_linha = [hoje, row['Produto'], "Compras", "Supermercado", "PIX", row['Valor Total'], hoje]
                    sheet.append_row(nova_linha)
                st.success("Produtos adicionados com sucesso!")
        else:
            st.warning("Nenhum produto encontrado. Verifique se a URL é de uma nota válida da SEFAZ RS.")

    except Exception as e:
        st.error(f"Erro ao acessar a nota: {e}")

st.markdown("---")

st.header("📊 Histórico de Despesas")

@st.cache_data(ttl=600)
def carregar_dados():
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    if "Valor" in df.columns:
        df["Valor"] = df["Valor"].astype(str).str.replace(',', '.').astype(float)
    if "Data Compra" in df.columns:
        df["Data Compra"] = pd.to_datetime(df["Data Compra"], format="%d/%m/%Y", errors='coerce')
    return df

if st.button("🔄 Atualizar Dados"):
    st.cache_data.clear()

df_planilha = carregar_dados()

if not df_planilha.empty:
    st.dataframe(df_planilha)
    total = df_planilha['Valor'].sum()
    st.metric("Total de Despesas", locale.currency(total, grouping=True))

    df_planilha['Mês'] = df_planilha['Data Compra'].dt.to_period('M').astype(str)
    gastos_mes = df_planilha.groupby('Mês')['Valor'].sum().reset_index()
    st.bar_chart(gastos_mes.set_index('Mês'))
else:
    st.info("Nenhum dado registrado ainda.")
