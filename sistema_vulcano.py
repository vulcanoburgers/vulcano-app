# NOVO CÓDIGO CORRIGIDO PARA HTML REAL DA NFC-E

import streamlit as st
import pandas as pd
import datetime
import locale
import re
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread

# Locale pt-BR
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

# Função de parser adaptada para o HTML real

def extrair_itens_nfe(soup):
    tabela = soup.find("table", {"id": "tabResult"})
    linhas = tabela.find_all("tr")
    dados = []
    for linha in linhas:
        try:
            nome = linha.find("span", class_="txtTit").get_text(strip=True)
            codigo = linha.find("span", class_="RCod").get_text(strip=True).replace("Código:", "").strip()
            qtd = linha.find("span", class_="Rqtd").get_text(strip=True).replace("Qtde.:", "").strip().replace(",", ".")
            unidade = linha.find("span", class_="RUN").get_text(strip=True).replace("UN:", "").strip()
            unit = linha.find("span", class_="RvlUnit").get_text(strip=True).replace("Vl. Unit.:", "").strip().replace(",", ".")
            total = linha.find("span", class_="valor").get_text(strip=True).replace(",", ".")
            dados.append({
                "Descrição": nome,
                "Código": codigo,
                "Quantidade": float(qtd),
                "Unidade": unidade,
                "Valor Unitário": float(unit),
                "Valor Total": float(total)
            })
        except:
            continue
    return pd.DataFrame(dados)

# UI
st.set_page_config(page_title="NFC-e Vulcano", layout="centered")
st.title("📥 Leitor de NFC-e por Link")

url = st.text_input("Cole o link completo da NFC-e")

if url:
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'html.parser')

        df = extrair_itens_nfe(soup)

        if not df.empty:
            st.subheader("Produtos na nota")
            st.dataframe(df)
            if st.button("Enviar produtos para Google Sheets"):
                hoje = datetime.date.today().strftime("%d/%m/%Y")
                for _, row in df.iterrows():
                    nova_linha = [hoje, row['Descrição'], "Compras", "Supermercado", "PIX", row['Valor Total'], hoje]
                    sheet.append_row(nova_linha)
                st.success("Produtos adicionados com sucesso!")
        else:
            st.warning("Nenhum produto encontrado.")

    except Exception as e:
        st.error(f"Erro ao acessar a nota: {e}")

# Histórico
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
