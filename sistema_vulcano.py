import streamlit as st
import pandas as pd
import datetime
import numpy as np
from google.oauth2.service_account import Credentials
import gspread
import requests
from bs4 import BeautifulSoup
import re

# Configuração
st.set_page_config(page_title="Vulcano App - Sistema de Gestão", layout="wide")

# CSS básico
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF4B4B;
        text-align: center;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# Mapeamento de colunas (seu código original)
COLUNAS_COMPRAS = {
    'data': 'Data Compra',
    'fornecedor': 'Fornecedor', 
    'categoria': 'Categoria',
    'descricao': 'Descrição',
    'quantidade': 'Quantidade',
    'unidade': 'Unid',
    'valor_unitario': 'Valor Unit',
    'valor_total': 'Valor Total',
    'forma_pagamento': 'Forma de Pagamento'
}

COLUNAS_PEDIDOS = {
    'codigo': 'Código',
    'data': 'Data',
    'nome': 'Nome', 
    'canal': 'Canal',
    'motoboy': 'Motoboy',
    'status': 'Status',
    'metodo_entrega': 'Método de entrega',
    'total': 'Total',
    'distancia': 'Distancia'
}

# Suas funções originais
@st.cache_resource(ttl=3600)
def conectar_google_sheets():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(st.secrets, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Erro na conexão com o Google Sheets: {str(e)}")
        return None

def formatar_br(valor, is_quantidade=False):
    try:
        if pd.isna(valor):
            return "R$ 0,00"
        if is_quantidade:
            return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(valor)

def limpar_valor_brasileiro(valor_str):
    try:
        if pd.isna(valor_str) or valor_str == '':
            return 0.0
        valor_clean = re.sub(r'[^0-9,.]', '', str(valor_str))
        valor_clean = valor_clean.replace(',', '.')
        return float(valor_clean) if valor_clean else 0.0
    except:
        return 0.0

def mapear_colunas(df, tipo_planilha):
    if df.empty:
        return df
    
    if tipo_planilha == 'COMPRAS':
        mapeamento = {v: k for k, v in COLUNAS_COMPRAS.items()}
    elif tipo_planilha == 'PEDIDOS':
        mapeamento = {v: k for k, v in COLUNAS_PEDIDOS.items()}
    else:
        return df
    
    colunas_existentes = {col: mapeamento[col] for col in df.columns if col in mapeamento}
    return df.rename(columns=colunas_existentes)

@st.cache_data(ttl=300)
def carregar_dados_sheets():
    client = conectar_google_sheets()
    if not client:
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        sheet_compras = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("COMPRAS")
        sheet_pedidos = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("PEDIDOS")
        
        df_compras = pd.DataFrame(sheet_compras.get_all_records())
        df_pedidos = pd.DataFrame(sheet_pedidos.get_all_records())
        
        df_compras_norm = mapear_colunas(df_compras, 'COMPRAS')
        df_pedidos_norm = mapear_colunas(df_pedidos, 'PEDIDOS')
        
        return df_compras_norm, df_pedidos_norm
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

def main():
    st.markdown('<h1 class="main-header">🔥 VULCANO - Sistema de Gestão</h1>', unsafe_allow_html=True)
    
    st.sidebar.title("📋 Menu Principal")
    menu = st.sidebar.radio(
        "Selecione uma opção:",
        [
            "🏠 Dashboard Principal",
            "📦 Gestão de Estoque",
            "📊 Análise de Pedidos",
            "🛵 Fechamento Motoboys",
            "⚙️ Configurações"
        ]
    )
    
    if menu == "🏠 Dashboard Principal":
        st.title("📊 Dashboard Principal")
        st.write("Dashboard funcionando!")
        
    elif menu == "📦 Gestão de Estoque":
        st.title("📦 Gestão de Estoque")
        st.write("Módulo de estoque em desenvolvimento...")
        
    elif menu == "📊 Análise de Pedidos":
        st.title("📊 Análise de Pedidos")
        st.write("Análise de pedidos funcionando!")
        
    elif menu == "🛵 Fechamento Motoboys":
        st.title("🛵 Fechamento de Motoboys")
        st.write("Fechamento de motoboys funcionando!")
        
    elif menu == "⚙️ Configurações":
        st.title("⚙️ Configurações")
        st.write("Configurações funcionando!")

if __name__ == "__main__":
    m
