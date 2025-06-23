# VULCANO APP - Com leitor de NFC-e integrado e correção de estoque

import streamlit as st
import pandas as pd
import datetime
from google.oauth2.service_account import Credentials
import gspread
import re
import requests
from bs4 import BeautifulSoup

# --- Configuração Inicial ---
st.set_page_config(page_title="Vulcano App", layout="wide")

# --- Conexão Google Sheets ---
@st.cache_resource(ttl=3600)
def conectar_google_sheets():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(st.secrets, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").sheet1
        return sheet
    except Exception as e:
        st.error(f"Erro na conexão com o Google Sheets: {str(e)}")
        st.stop()

# --- Funções Auxiliares ---
def formatar_br(valor, is_quantidade=False):
    try:
        if is_quantidade:
            return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return valor

def converter_valor(valor):
    try:
        return float(str(valor).replace(",", "."))
    except (ValueError, TypeError):
        return 0.0

# --- Leitor de NFC-e por URL ---
def extrair_produtos_nfe(url):
    try:
        resposta = requests.get(url, timeout=10)
        sopa = BeautifulSoup(resposta.text, 'html.parser')
        texto = sopa.get_text()
        padrao_produto = re.compile(r"(.+?)\(Código:.*?\)Qtde.:(.*?)UN:.*?Vl. Unit.:(.*?)Vl. Total(\d+,\d+)", re.DOTALL)
        produtos = padrao_produto.findall(texto)
        lista_produtos = []
        for descricao, qtde, unitario, total in produtos:
            descricao = descricao.strip().replace('\n', ' ')
            lista_produtos.append({
                "Descrição": descricao,
                "Quantidade": converter_valor(qtde.strip()),
                "Valor Unit": converter_valor(unitario.strip()),
                "Valor Total": converter_valor(total.strip()),
                "Unid": "UN"
            })
        return lista_produtos
    except Exception as e:
        st.error(f"Erro ao extrair produtos: {e}")
        return []

# --- Menu Principal ---
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"])

sheet = conectar_google_sheets()

if menu == "📥 Inserir NFC-e":
    st.title("📥 Inserir NFC-e")
    url_nfce = st.text_input("Cole o link da NFC-e:")

    if st.button("Buscar produtos da nota") and url_nfce:
        produtos = extrair_produtos_nfe(url_nfce)
        if produtos:
            df = pd.DataFrame(produtos)
            df['Data Compra'] = datetime.date.today().strftime("%d/%m/%Y")
            df['Fornecedor'] = "Bistek"
            df['Valor Total'] = df['Valor Unit'] * df['Quantidade']
            st.dataframe(df)

            if st.button("Enviar produtos para Google Sheets"):
                for _, row in df.iterrows():
                    sheet.append_row([
                        row['Data Compra'], row['Descrição'], row['Fornecedor'],
                        row['Unid'], str(row['Quantidade']), str(row['Valor Unit'])
                    ])
                st.success("Produtos adicionados à planilha com sucesso!")

elif menu == "📊 Dashboard":
    st.title("📊 Dashboard")
    st.info("Funcionalidade em desenvolvimento")

elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")

    @st.cache_data(ttl=600)
    def carregar_dados():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        df['Valor Unit'] = df['Valor Unit'].apply(converter_valor)
        df['Quantidade'] = df['Quantidade'].apply(converter_valor)
        df['Valor Total'] = df['Valor Unit'] * df['Quantidade']
        df['Data Compra'] = pd.to_datetime(df['Data Compra'], dayfirst=True, errors='coerce').dt.date
        df = df.dropna(subset=['Data Compra'])
        return df

    df = carregar_dados()

    if not df.empty:
        min_date = df['Data Compra'].min()
        max_date = df['Data Compra'].max()
        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input("De", min_date, min_value=min_date, max_value=max_date)
        with col2:
            data_fim = st.date_input("Até", max_date, min_value=min_date, max_value=max_date)

        df_filtrado = df[(df['Data Compra'] >= data_inicio) & (df['Data Compra'] <= data_fim)]
        df_exibir = df_filtrado.copy()
        df_exibir['Valor Unit'] = df_exibir['Valor Unit'].apply(formatar_br)
        df_exibir['Valor Total'] = df_exibir['Valor Total'].apply(formatar_br)
        df_exibir['Quantidade'] = df_exibir['Quantidade'].apply(lambda x: formatar_br(x, is_quantidade=True))

        st.dataframe(
            df_exibir.sort_values('Data Compra', ascending=False),
            hide_index=True,
            use_container_width=True
        )
    else:
        st.warning("Nenhum dado encontrado para o período selecionado.")

elif menu == "📦 Estoque":
    st.title("📦 Gestão de Estoque")

    @st.cache_data(ttl=3600)
    def carregar_estoque():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        df['Valor Unit'] = df['Valor Unit'].apply(converter_valor)
        df['Quantidade'] = df['Quantidade'].apply(converter_valor)
        df['Valor Total'] = df['Valor Unit'] * df['Quantidade']

        df_grouped = df.groupby(['Descrição', 'Unid']).agg(
            Quantidade=('Quantidade', 'sum'),
            Valor_Total_Sum=('Valor Total', 'sum')
        ).reset_index()

        df_grouped['Valor Unit'] = df_grouped['Valor_Total_Sum'] / df_grouped['Quantidade']
        df_grouped['Valor Unit'] = df_grouped['Valor Unit'].fillna(0)
        df_grouped.rename(columns={'Valor_Total_Sum': 'Valor Total'}, inplace=True)

        return df_grouped

    df_estoque = carregar_estoque()

    if not df_estoque.empty:
        df_exibir = df_estoque.copy()
        df_exibir['Valor Unit'] = df_exibir['Valor Unit'].apply(formatar_br)
        df_exibir['Valor Total'] = df_exibir['Valor Total'].apply(formatar_br)
        df_exibir['Quantidade'] = df_exibir['Quantidade'].apply(lambda x: formatar_br(x, is_quantidade=True))

        st.dataframe(
            df_exibir[['Descrição', 'Unid', 'Quantidade', 'Valor Unit', 'Valor Total']],
            hide_index=True,
            use_container_width=True
        )

        valor_total_estoque = df_estoque['Valor Total'].sum()
        st.metric("Valor Total em Estoque", formatar_br(valor_total_estoque))
    else:
        st.warning("Nenhum item em estoque encontrado.")
