# VULCANO APP - VERSÃO FINAL CORRIGIDA
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

sheet = conectar_google_sheets()

# Funções auxiliares
def formatar_br(valor):
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return valor

def converter_valor(valor):
    try:
        if isinstance(valor, (int, float)):
            return float(valor)
        return float(str(valor).replace(".", "").replace(",", "."))
    except:
        return 0.0

# Menu principal
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"])

# Página Fluxo de Caixa
if menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")
    
    @st.cache_data(ttl=600)
    def carregar_dados():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Conversão de valores
        for col in ['Valor Unit', 'Valor Total', 'Quantidade']:
            if col in df.columns:
                df[col] = df[col].apply(converter_valor)
        
        # Conversão de datas
        df['Data Compra'] = pd.to_datetime(df['Data Compra'], dayfirst=True, errors='coerce').dt.date
        df = df.dropna(subset=['Data Compra'])
        
        return df

    df = carregar_dados()
    
    if not df.empty:
        # Filtros
        min_date = df['Data Compra'].min()
        max_date = df['Data Compra'].max()
        
        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input("De", min_date, min_value=min_date, max_value=max_date)
        with col2:
            data_fim = st.date_input("Até", max_date, min_value=min_date, max_value=max_date)
        
        df_filtrado = df[(df['Data Compra'] >= data_inicio) & 
                         (df['Data Compra'] <= data_fim)]
        
        # Exibição
        st.dataframe(
            df_filtrado.sort_values('Data Compra', ascending=False),
            column_config={
                "Data Compra": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Valor Unit": st.column_config.NumberColumn("Valor Unitário", format="%.2f"),
                "Valor Total": st.column_config.NumberColumn("Total", format="%.2f"),
                "Quantidade": st.column_config.NumberColumn("Qtd", format="%.3f")
            },
            hide_index=True,
            use_container_width=True
        )

# Página Estoque
elif menu == "📦 Estoque":
    st.title("📦 Gestão de Estoque")
    
    @st.cache_data(ttl=3600)
    def carregar_estoque():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Conversão de valores
        for col in ['Valor Unit', 'Valor Total', 'Quantidade']:
            if col in df.columns:
                df[col] = df[col].apply(converter_valor)
        
        # Garante coluna de unidade
        if 'Unid' not in df.columns:
            df['Unid'] = 'UN'
        
        return df.groupby(['Descrição', 'Unid']).agg({
            'Quantidade': 'sum',
            'Valor Unit': 'first',
            'Valor Total': 'sum'
        }).reset_index()

    df_estoque = carregar_estoque()
    
    if not df_estoque.empty:
        # Formatação
        df_exibir = df_estoque.copy()
        df_exibir['Valor Unit'] = df_exibir['Valor Unit'].apply(formatar_br)
        df_exibir['Valor Total'] = df_exibir['Valor Total'].apply(formatar_br)
        
        st.dataframe(
            df_exibir,
            column_config={
                "Quantidade": st.column_config.NumberColumn("Qtd", format="%.3f"),
                "Unid": st.column_config.TextColumn("Unidade")
            },
            hide_index=True,
            use_container_width=True
        )

# Outras páginas (placeholders)
elif menu == "📥 Inserir NFC-e":
    st.title("📥 Inserir NFC-e")
    st.info("Funcionalidade em desenvolvimento")

elif menu == "📊 Dashboard":
    st.title("📊 Dashboard")
    st.info("Funcionalidade em desenvolvimento")
