# VULCANO APP - VERSÃO FINAL ORGANIZADA
import streamlit as st
import pandas as pd
import datetime
from google.oauth2.service_account import Credentials
import gspread

# Configuração inicial
st.set_page_config(page_title="Vulcano App", layout="wide")

# Conexão com Google Sheets
def conectar_google_sheets():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(st.secrets, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").sheet1
        return sheet
    except Exception as e:
        st.error(f"Erro na conexão: {str(e)}")
        st.stop()

# Funções auxiliares
def formatar_br(valor, is_quantidade=False):
    try:
        if is_quantidade:
            return f"{float(valor):,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return valor

def converter_valor(valor, unidade, is_valor_unitario=False):
    try:
        valor_str = str(valor).strip()
        valor_str = valor_str.replace(".", "").replace(",", ".")
        valor_float = float(valor_str)
        
        if is_valor_unitario and unidade == 'KG':
            return valor_float / 100
        return valor_float
    except:
        return 0.0

# Menu principal - DEFINIDO ANTES DE QUALQUER USO
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"])

# Carrega a planilha APÓS definir o menu
sheet = conectar_google_sheets()

# Páginas do aplicativo
if menu == "📥 Inserir NFC-e":
    st.title("📥 Inserir NFC-e")
    st.info("Funcionalidade em desenvolvimento")

elif menu == "📊 Dashboard":
    st.title("📊 Dashboard")
    st.info("Funcionalidade em desenvolvimento")

elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")
    
    @st.cache_data(ttl=600)
    def carregar_dados():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        df['Valor Unit'] = df.apply(lambda x: converter_valor(x['Valor Unit'], x['Unid'], is_valor_unitario=True), axis=1)
        df['Quantidade'] = df.apply(lambda x: converter_valor(x['Quantidade'], x['Unid']), axis=1)
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
            column_config={
                "Data Compra": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Unid": st.column_config.TextColumn("Unidade")
            },
            hide_index=True,
            use_container_width=True
        )

elif menu == "📦 Estoque":
    st.title("📦 Gestão de Estoque")
    
    @st.cache_data(ttl=3600)
    def carregar_estoque():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        df['Valor Unit'] = df.apply(lambda x: converter_valor(x['Valor Unit'], x['Unid'], is_valor_unitario=True), axis=1)
        df['Quantidade'] = df.apply(lambda x: converter_valor(x['Quantidade'], x['Unid']), axis=1)
        df['Valor Total'] = df['Valor Unit'] * df['Quantidade']
        
        return df.groupby(['Descrição', 'Unid']).agg({
            'Quantidade': 'sum',
            'Valor Unit': 'first',
            'Valor Total': 'sum'
        }).reset_index()

    df_estoque = carregar_estoque()
    
    if not df_estoque.empty:
        df_exibir = df_estoque.copy()
        df_exibir['Valor Unit'] = df_exibir['Valor Unit'].apply(formatar_br)
        df_exibir['Valor Total'] = df_exibir['Valor Total'].apply(formatar_br)
        df_exibir['Quantidade'] = df_exibir['Quantidade'].apply(lambda x: formatar_br(x, is_quantidade=True))
        
        st.dataframe(
            df_exibir[['Descrição', 'Unid', 'Quantidade', 'Valor Unit', 'Valor Total']],
            column_config={
                "Unid": st.column_config.TextColumn("Unidade"),
                "Quantidade": st.column_config.NumberColumn("Qtd", format="%.3f")
            },
            hide_index=True,
            use_container_width=True
        )
        
        valor_total = df_estoque['Valor Total'].sum()
        st.metric("Valor Total em Estoque", formatar_br(valor_total))
