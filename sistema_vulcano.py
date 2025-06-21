# VULCANO APP - VERSÃO FINAL COMPLETA E CORRIGIDA
import streamlit as st
import pandas as pd
import datetime
import re
import requests
from bs4 import BeautifulSoup
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

def converter_valor(valor, unidade):
    try:
        valor_str = str(valor)
        # Remove todos os pontos (separadores de milhar)
        valor_str = valor_str.replace(".", "")
        # Substitui vírgula decimal por ponto
        valor_str = valor_str.replace(",", ".")
        valor_float = float(valor_str)
        
        # Aplica regra de conversão baseada na unidade
        if unidade == 'UN':
            return valor_float / 100  # Divide por 100 para UN
        return valor_float  # Mantém original para KG
    except:
        return 0.0

# Parser NFC-e
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
                    # Extração dos dados
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
                    st.warning(f"Item ignorado: {texto[:50]}... | Erro: {str(e)}")
                    continue
        
        return pd.DataFrame(itens)
    
    except Exception as e:
        st.error(f"Erro ao processar NFC-e: {str(e)}")
        return pd.DataFrame()

# Menu principal
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"])

# Página Inserir NFC-e
if menu == "📥 Inserir NFC-e":
    st.title("📥 Leitor de NFC-e")
    url = st.text_input("Cole o link da NFC-e abaixo:", placeholder="https://...")
    
    if st.button("🔍 Analisar NFC-e") and url:
        with st.spinner("Processando NFC-e..."):
            df_nfce = parse_nfce(url)
            
            if not df_nfce.empty:
                st.subheader("Itens da Nota Fiscal")
                st.dataframe(
                    df_nfce.style.format({
                        "Valor Unit": formatar_br,
                        "Valor Total": formatar_br
                    }),
                    use_container_width=True
                )
                
                # Seção de dados complementares
                with st.form("dados_complementares"):
                    st.subheader("Informações Adicionais")
                    
                    col1, col2 = st.columns(2)
                    fornecedor = col1.text_input("Fornecedor*", value="Bistek")
                    categoria = col2.selectbox(
                        "Categoria*",
                        ["Matéria-Prima", "Embalagem", "Limpeza", "Despesas"]
                    )
                    
                    col3, col4 = st.columns(2)
                    forma_pagamento = col3.selectbox(
                        "Forma de Pagamento*",
                        ["PIX", "Cartão Crédito", "Cartão Débito", "Dinheiro", "Boleto"]
                    )
                    data_pagamento = col4.date_input(
                        "Data de Pagamento*",
                        datetime.date.today()
                    )
                    
                    if st.form_submit_button("💾 Salvar na Planilha"):
                        if not fornecedor:
                            st.error("Fornecedor é obrigatório!")
                        else:
                            hoje = datetime.date.today().strftime("%d/%m/%Y")
                            dados = []
                            
                            for _, row in df_nfce.iterrows():
                                dados.append([
                                    hoje,  # Data Compra
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
                            
                            try:
                                sheet.append_rows(dados)
                                st.success("✅ Dados salvos com sucesso!")
                                st.balloons()
                            except Exception as e:
                                st.error(f"🔴 Falha ao salvar: {str(e)}")
            else:
                st.warning("Nenhum produto encontrado na NFC-e")

# Página Fluxo de Caixa
elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")
    
    @st.cache_data(ttl=600)
    def carregar_dados():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Conversão de valores considerando a unidade
        df['Valor Unit'] = df.apply(lambda x: converter_valor(x['Valor Unit'], x['Unid']), axis=1)
        df['Quantidade'] = df.apply(lambda x: converter_valor(x['Quantidade'], x['Unid']), axis=1)
        df['Valor Total'] = df['Valor Unit'] * df['Quantidade']
        
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
        
        # Formatação para exibição
        df_exibir = df_filtrado.copy()
        df_exibir['Valor Unit'] = df_exibir['Valor Unit'].apply(formatar_br)
        df_exibir['Valor Total'] = df_exibir['Valor Total'].apply(formatar_br)
        df_exibir['Quantidade'] = df_exibir.apply(
            lambda x: f"{x['Quantidade']:,.3f}".replace(".", "X").replace(",", ".").replace("X", ",") if x['Unid'] == 'KG' 
                      else f"{int(x['Quantidade'])}",
            axis=1
        )
        
        # Exibição
        st.dataframe(
            df_exibir.sort_values('Data Compra', ascending=False),
            column_config={
                "Data Compra": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Unid": st.column_config.TextColumn("Unidade")
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
        
        # Conversão de valores considerando a unidade
        df['Valor Unit'] = df.apply(lambda x: converter_valor(x['Valor Unit'], x['Unid']), axis=1)
        df['Quantidade'] = df.apply(lambda x: converter_valor(x['Quantidade'], x['Unid']), axis=1)
        df['Valor Total'] = df['Valor Unit'] * df['Quantidade']
        
        return df.groupby(['Descrição', 'Unid']).agg({
            'Quantidade': 'sum',
            'Valor Unit': 'first',
            'Valor Total': 'sum'
        }).reset_index()

    df_estoque = carregar_estoque()
    
    if not df_estoque.empty:
        # Formatação para exibição
        df_exibir = df_estoque.copy()
        df_exibir['Valor Unit'] = df_exibir['Valor Unit'].apply(formatar_br)
        df_exibir['Valor Total'] = df_exibir['Valor Total'].apply(formatar_br)
        df_exibir['Quantidade'] = df_exibir.apply(
            lambda x: f"{x['Quantidade']:,.3f}".replace(".", "X").replace(",", ".").replace("X", ",") if x['Unid'] == 'KG' 
                      else f"{int(x['Quantidade'])}",
            axis=1
        )
        
        st.dataframe(
            df_exibir,
            column_config={
                "Unid": st.column_config.TextColumn("Unidade")
            },
            hide_index=True,
            use_container_width=True
        )
        
        # Métricas
        valor_total = df_estoque['Valor Total'].sum()
        st.metric("Valor Total em Estoque", f"R$ {formatar_br(valor_total)}")

# Página Dashboard
elif menu == "📊 Dashboard":
    st.title("📊 Dashboard")
    st.info("Funcionalidade em desenvolvimento")
