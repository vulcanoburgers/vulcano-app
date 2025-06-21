# VULCANO APP - Leitura NFC-e com Menu, Dashboard e Fluxo de Caixa

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

# Parser NFC-e

def extrair_itens_por_texto(soup):
    tabela = soup.find("table", {"id": "tabResult"})
    if not tabela:
        return pd.DataFrame()

    linhas = tabela.find_all("tr")
    dados = []

    for linha in linhas:
        texto = linha.get_text(" ", strip=True)

        if all(k in texto for k in ["Código:", "Qtde.:", "UN:", "Vl. Unit.:", "Vl. Total"]):
            try:
                nome = texto.split("(Código:")[0].strip()
                codigo = re.search(r"Código:\s*(\d+)", texto).group(1)
                qtd = re.search(r"Qtde\.\:\s*([\d.,]+)", texto).group(1)
                unitario = re.search(r"Vl\. Unit\.\:\s*([\d.,]+)", texto).group(1)
                total = re.search(r"Vl\. Total\s*([\d.,]+)", texto).group(1)

                qtd = qtd.replace('.', '').replace(',', '.')
                unitario = unitario.replace('.', '').replace(',', '.')
                total = total.replace('.', '').replace(',', '.')

                unidade = re.search(r"UN\:\s*(\w+)", texto).group(1)
            
                dados.append({
                    "Descrição": nome,
                    "Código": codigo,
                    "Quantidade": float(qtd),
                    "Unidade": unidade,
                    "Valor Unitário": round(float(unitario), 2),
                    "Valor Total": round(float(total), 2)
})

            except:
                continue

    return pd.DataFrame(dados)

# Config página e menu lateral
st.set_page_config(page_title="Vulcano App", layout="wide")
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"])

# NFC-e
if menu == "📥 Inserir NFC-e":
    st.title("📥 Leitor de NFC-e por Link")
    url = st.text_input("Cole o link completo da NFC-e")

    if url:
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.content, 'html.parser')

            df = extrair_itens_por_texto(soup)

            if not df.empty:
                st.subheader("Produtos na nota")
                df["Valor Total"] = df["Valor Total"].astype(float)
            df["Valor Unitário"] = df["Valor Unitário"].astype(float)
            st.dataframe(df)
                if st.button("Enviar produtos para Google Sheets"):
                    hoje = datetime.date.today().strftime("%d/%m/%Y")
                    for _, row in df.iterrows():
                        nova_linha = [hoje, "Supermercado - Bistek", "Compras", "Supermercado", "PIX", row['Valor Total'], hoje]
                        sheet.append_row(nova_linha)
                    st.success("Produtos adicionados com sucesso!")
            else:
                st.warning("Nenhum produto encontrado.")

        except Exception as e:
            st.error(f"Erro ao acessar a nota: {e}")

# Dashboard
elif menu == "📊 Dashboard":
    st.title("📊 Dashboard Vulcano")
    st.info("Em construção... em breve você verá gráficos lindões aqui 🔨")

# Fluxo de Caixa
elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")

    @st.cache_data(ttl=600)
    def carregar_dados():
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        if "Valor" in df.columns:
            df["Valor"] = df["Valor"].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
        if "Data Compra" in df.columns:
            df["Data Compra"] = pd.to_datetime(df["Data Compra"], format="%d/%m/%Y", errors='coerce')
        return df

    if st.button("🔄 Atualizar Dados"):
        st.cache_data.clear()

    df_planilha = carregar_dados()
    df_planilha["Valor"] = df_planilha["Valor"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False).astype(float)
    df_planilha["Valor"] = df_planilha["Valor"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False).astype(float)

    if not df_planilha.empty:
        df_planilha["Data Compra"] = df_planilha["Data Compra"].dt.date

        # Classificação automática de tipo
        df_planilha["Tipo"] = df_planilha["Categoria"].apply(lambda x: "Entrada" if x.strip().lower() in ["receita", "venda", "ifood", "ticket", "stone", "sodexo"] else "Despesa")

        entradas = df_planilha[df_planilha["Tipo"] == "Entrada"]
        despesas = df_planilha[df_planilha["Tipo"] == "Despesa"]

        total_entradas = entradas["Valor"].sum()
        total_despesas = despesas["Valor"].sum()
        saldo = total_entradas - total_despesas

        col1, col2, col3 = st.columns(3)
        col1.metric("Total de Entradas", f"R$ {total_entradas:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
        col2.metric("Total de Despesas", f"R$ {total_despesas:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
        col3.metric("Saldo Atual", f"R$ {saldo:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))

        st.subheader("💸 Despesas")
        st.dataframe(despesas, use_container_width=True)

        st.subheader("💰 Entradas")
        st.dataframe(entradas, use_container_width=True)

        df_planilha['Mês'] = pd.to_datetime(df_planilha['Data Compra'], errors='coerce').dt.to_period('M').astype(str)
        gastos_mes = df_planilha.groupby('Mês')['Valor'].sum().reset_index()
        st.bar_chart(gastos_mes.set_index('Mês'))
    else:
        st.info("Nenhum dado registrado ainda.")

# Aba Estoque
elif menu == "📦 Estoque":
    st.title("📦 Estoque Atual")

    @st.cache_data(ttl=600)
    def carregar_estoque():
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        if "Valor" in df.columns:
            df["Valor"] = df["Valor"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False).astype(float)
        if "Descrição" not in df.columns or "Valor" not in df.columns:
            return pd.DataFrame()

        return df  # Retorna os dados crus sem agrupar

    if not df_estoque.empty:
        st.dataframe(df_estoque, use_container_width=True)
        total_estoque = df_estoque["Valor"].sum()
        st.metric("Valor Total Estimado em Estoque", f"R$ {total_estoque:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
    else:
        st.info("Nenhum dado disponível para estoque.")
