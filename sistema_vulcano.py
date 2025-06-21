# VULCANO APP - Versão Corrigida (SyntaxError resolvido)
import streamlit as st
import pandas as pd
import datetime
import re
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread

# --- Configuração Inicial ---
st.set_page_config(page_title="Vulcano App", layout="wide")

# --- Conexão Google Sheets ---
def conectar_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(st.secrets, scopes=scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_url("SUA_URL_DA_PLANILHA").sheet1
    return sheet

sheet = conectar_google_sheets()

# --- Parser NFC-e ---
def parse_nfce(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        tabela = soup.find("table", {"id": "tabResult"})
        
        itens = []
        for linha in tabela.find_all("tr"):
            texto = linha.get_text(" ", strip=True)
            
            if all(k in texto for k in ["Código:", "Qtde.:", "Vl. Unit.:"]):
                try:
                    nome = texto.split("(Código:")[0].strip()
                    qtd = float(re.search(r"Qtde\.?:\s*([\d.,]+)", texto).group(1).replace('.', '').replace(',', '.'))
                    unitario = float(re.search(r"Vl\. Unit\.?:\s*([\d.,]+)", texto).group(1).replace('.', '').replace(',', '.'))
                    total = qtd * unitario
                    unidade = re.search(r"UN:\s*(\w+)", texto).group(1)
                    
                    itens.append({
                        "Descrição": nome,
                        "Quantidade": qtd,
                        "Unid": unidade,
                        "Valor Unit": unitario,
                        "Valor Total": total
                    })
                except Exception as e:
                    st.warning(f"Item ignorado: {texto} | Erro: {str(e)}")
                    continue
        
        return pd.DataFrame(itens)
    
    except Exception as e:
        st.error(f"Erro ao processar NFC-e: {str(e)}")
        return pd.DataFrame()

# --- Menu Principal ---
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"])

# --- Página NFC-e ---
if menu == "📥 Inserir NFC-e":
    st.title("📥 Leitor de NFC-e")
    url = st.text_input("Cole o link da NFC-e:")
    
    if url:
        df_nfce = parse_nfce(url)
        if not df_nfce.empty:
            st.dataframe(df_nfce.style.format({
                "Valor Unit": "R$ {:.2f}",
                "Valor Total": "R$ {:.2f}"
            }))
            
            with st.form("dados_adicionais"):
                st.subheader("Informações Complementares")
                col1, col2, col3 = st.columns(3)
                fornecedor = col1.text_input("Fornecedor")
                categoria = col2.selectbox("Categoria", ["Matéria-Prima", "Embalagens", "Despesas Operacionais"])
                forma_pagamento = col3.selectbox("Forma de Pagamento", ["PIX", "Cartão", "Dinheiro", "Boleto"])
                data_pagamento = st.date_input("Data de Pagamento", datetime.date.today())
                
                if st.form_submit_button("💾 Salvar na Planilha"):
                    hoje = datetime.date.today().strftime("%d/%m/%Y")
                    dados = []
                    
                    for _, row in df_nfce.iterrows():
                        dados.append([
                            hoje,
                            fornecedor,
                            categoria,
                            row["Descrição"],
                            row["Quantidade"],
                            row["Unid"],
                            row["Valor Unit"],
                            row["Valor Total"],
                            forma_pagamento,
                            data_pagamento.strftime("%d/%m/%Y")
                        ])
                    
                    sheet.append_rows(dados)
                    st.success("Dados salvos com sucesso!")
                    st.balloons()

# --- Página Fluxo de Caixa ---
elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")
    
    @st.cache_data(ttl=600)
    def carregar_dados():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Processamento dos dados
        if not df.empty:
            # Conversão de valores
            num_cols = ["Valor Unit", "Valor Total"]
            for col in num_cols:
                if col in df.columns:
                    df[col] = (
                        df[col].astype(str)
                        .str.replace(r'[^\d,]', '', regex=True)
                        .str.replace('.', '', regex=False)
                        .str.replace(',', '.', regex=False)
                        .astype(float, errors='coerce')
                    )
            
            # Classificação Receita/Despesa
            df["Tipo"] = df["Categoria"].apply(
                lambda x: "Receita" if str(x).lower() in ["vendas", "receita", "ifood"] else "Despesa"
            )
            
            # Conversão de datas
            date_cols = ["Data Compra", "Data de pagamento"]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
            
            return df
        return pd.DataFrame()
    
    df = carregar_dados()
    
    if not df.empty:
        # Filtros
        st.sidebar.header("Filtros")
        data_inicio = st.sidebar.date_input("Data Início", df["Data Compra"].min())
        data_fim = st.sidebar.date_input("Data Fim", df["Data Compra"].max())
        
        df_filtrado = df[
            (df["Data Compra"] >= pd.to_datetime(data_inicio)) & 
            (df["Data Compra"] <= pd.to_datetime(data_fim))
        ]
        
        # Formatação BR
        def formatar_br(valor):
            return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        
        # Métricas
        receitas = df_filtrado[df_filtrado["Tipo"] == "Receita"]["Valor Total"].sum()
        despesas = df_filtrado[df_filtrado["Tipo"] == "Despesa"]["Valor Total"].sum()
        saldo = receitas - despesas
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Receitas", formatar_br(receitas))
        col2.metric("Despesas", formatar_br(despesas))
        col3.metric("Saldo", formatar_br(saldo), delta=formatar_br(saldo))
        
        # Tabela
        st.dataframe(
            df_filtrado.sort_values("Data Compra", ascending=False),
            hide_index=True,
            column_config={
                "Data Compra": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Data de pagamento": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Valor Total": st.column_config.NumberColumn(format="R$ %.2f")
            },
            use_container_width=True
        )

# --- Página Estoque ---
elif menu == "📦 Estoque":
    st.title("📦 Gestão de Estoque")
    
    @st.cache_data(ttl=3600)
    def carregar_estoque():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        if not df.empty:
            # Processamento
            num_cols = ["Quantidade", "Valor Unit", "Valor Total"]
            for col in num_cols:
                if col in df.columns:
                    df[col] = (
                        df[col].astype(str)
                        .str.replace(r'[^\d,]', '', regex=True)
                        .str.replace('.', '', regex=False)
                        .str.replace(',', '.', regex=False)
                        .astype(float, errors='coerce')
                    )
            
            # Agrupamento
            df_estoque = df.groupby("Descrição").agg({
                "Quantidade": "sum",
                "Valor Unit": "first",
                "Valor Total": "sum"
            }).reset_index()
            
            return df_estoque.sort_values("Quantidade", ascending=False)
        return pd.DataFrame()
    
    df_estoque = carregar_estoque()
    
    if not df_estoque.empty:
        # Formatação
        def formatar_br(valor):
            return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        
        # Métricas
        total_itens = df_estoque["Quantidade"].sum()
        valor_total = df_estoque["Valor Total"].sum()
        
        col1, col2 = st.columns(2)
        col1.metric("Total de Itens", f"{total_itens:,.2f}".replace(".", ","))
        col2.metric("Valor Total", formatar_br(valor_total))
        
        # Tabela
        st.dataframe(
            df_estoque,
            column_config={
                "Valor Unit": st.column_config.NumberColumn(format="R$ %.2f"),
                "Valor Total": st.column_config.NumberColumn(format="R$ %.2f")
            },
            hide_index=True,
            use_container_width=True
        )

# --- Página Dashboard ---
elif menu == "📊 Dashboard":
    st.title("📊 Dashboard")
    st.info("Em desenvolvimento")
