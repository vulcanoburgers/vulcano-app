# VULCANO APP - Versão Final (Corrigida e Otimizada)
import streamlit as st
import pandas as pd
import datetime
import re
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread

# --- Configuração Inicial ---
st.set_page_config(
    page_title="Vulcano App",
    page_icon="🔥",
    layout="wide"
)

# --- Conexão Robusta com Google Sheets ---
def conectar_google_sheets():
    try:
        # Configuração do escopo
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Carrega credenciais dos secrets do Streamlit
        creds = {
            "type": "service_account",
            "project_id": st.secrets["project_id"],
            "private_key_id": st.secrets["private_key_id"],
            "private_key": st.secrets["private_key"],
            "client_email": st.secrets["client_email"],
            "client_id": st.secrets["client_id"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": st.secrets["client_x509_cert_url"]
        }
        
        credentials = Credentials.from_service_account_info(creds, scopes=scope)
        client = gspread.authorize(credentials)
        
        # Verifica se a URL da planilha está configurada
        if "PLANILHA_URL" not in st.secrets:
            st.error("🔐 URL da planilha não configurada!")
            st.info("Adicione em: Settings → Secrets → PLANILHA_URL")
            st.stop()
        
        # Extrai o ID da planilha (funciona com URL completa ou apenas ID)
        url = st.secrets["PLANILHA_URL"]
        if "/d/" in url:
            sheet_id = url.split("/d/")[1].split("/")[0]
        else:
            sheet_id = url  # Assume que já é o ID se não tiver URL completa
        
        return client.open_by_key(sheet_id).sheet1
        
    except gspread.exceptions.APIError as e:
        st.error(f"🔴 Erro na API do Google: {str(e)}")
        st.stop()
    except Exception as e:
        st.error(f"🔴 Erro inesperado: {str(e)}")
        st.stop()

sheet = conectar_google_sheets()

# --- Parser NFC-e Atualizado ---
def parse_nfce(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        tabela = soup.find("table", {"id": "tabResult"})
        
        if not tabela:
            st.warning("Nenhuma tabela de produtos encontrada na NFC-e")
            return pd.DataFrame()
        
        itens = []
        for linha in tabela.find_all("tr"):
            texto = linha.get_text(" ", strip=True)
            
            if all(k in texto for k in ["Código:", "Qtde.:", "Vl. Unit.:"]):
                try:
                    # Extração robusta dos dados
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
                    st.warning(f"⚠️ Item ignorado: {texto[:50]}... | Erro: {str(e)}")
                    continue
        
        return pd.DataFrame(itens)
    
    except Exception as e:
        st.error(f"🔴 Falha ao processar NFC-e: {str(e)}")
        return pd.DataFrame()

# --- FUNÇÃO DE FORMATAÇÃO BRASILEIRA ---
def formatar_br(valor):
    try:
        if isinstance(valor, (int, float)):
            # Formata como R$ 1.234,56
            return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return valor
    except:
        return valor

# --- Menu Principal ---
menu = st.sidebar.radio(
    "Navegação",
    ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"],
    horizontal=True
)

# --- Página NFC-e ---
if menu == "📥 Inserir NFC-e":
    st.title("📥 Leitor de NFC-e")
    url = st.text_input("Cole o link da NFC-e abaixo:", placeholder="https://...")
    
    if st.button("🔍 Analisar NFC-e") and url:
        with st.spinner("Processando NFC-e..."):
            df_nfce = parse_nfce(url)
            
            if not df_nfce.empty:
                # Exibe os itens com formatação
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

# --- NOVA VERSÃO DO FLUXO DE CAIXA ---
elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")
    
    @st.cache_data(ttl=600)
    def carregar_dados():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Conversão segura de datas
        def parse_date(date_str):
            try:
                if isinstance(date_str, str):
                    # Tenta parsear no formato brasileiro
                    if "/" in date_str:
                        return datetime.datetime.strptime(date_str, "%d/%m/%Y").date()
                    # Tenta parsear como datetime
                    return pd.to_datetime(date_str).date()
                return date_str
            except:
                return None
        
        df["Data Compra"] = df["Data Compra"].apply(parse_date)
        df = df.dropna(subset=["Data Compra"])  # Remove linhas com datas inválidas
        
        # Conversão de valores
        for col in ["Valor Unit", "Valor Total"]:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(r'[^\d,]', '', regex=True)
                    .str.replace('.', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .astype(float)
                )
        
        return df

    df = carregar_dados()
    
    if not df.empty:
        # Filtros de data - agora com tratamento seguro
        min_date = df["Data Compra"].min()
        max_date = df["Data Compra"].max()
        
        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input(
                "De",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
                format="DD/MM/YYYY"
            )
        with col2:
            data_fim = st.date_input(
                "Até",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                format="DD/MM/YYYY"
            )
        
        # Filtragem dos dados
        df_filtrado = df[
            (df["Data Compra"] >= data_inicio) & 
            (df["Data Compra"] <= data_fim)
        ]
        
        # Restante do seu código...

# --- Página Estoque ---
elif menu == "📦 Estoque":
    st.title("📦 Gestão de Estoque")
    
    @st.cache_data(ttl=3600)
    def carregar_estoque():
        try:
            dados = sheet.get_all_records()
            df = pd.DataFrame(dados)
            
            if not df.empty:
                # Processamento dos valores
                for col in ["Quantidade", "Valor Unit", "Valor Total"]:
                    if col in df.columns:
                        df[col] = (
                            df[col]
                            .astype(str)
                            .str.replace(r'[^\d,]', '', regex=True)
                            .str.replace('.', '', regex=False)
                            .str.replace(',', '.', regex=False)
                            .astype(float)
                        )
                
                # Calcula valor total se necessário
                if "Valor Total" not in df.columns and all(c in df.columns for c in ["Quantidade", "Valor Unit"]):
                    df["Valor Total"] = df["Quantidade"] * df["Valor Unit"]
                
                # Agrupa por produto
                df_estoque = df.groupby("Descrição").agg({
                    "Quantidade": "sum",
                    "Valor Unit": "first",
                    "Valor Total": "sum"
                }).reset_index()
                
                return df_estoque.sort_values("Quantidade", ascending=False)
            return pd.DataFrame()
        
        except Exception as e:
            st.error(f"Erro ao carregar estoque: {str(e)}")
            return pd.DataFrame()
    
    df_estoque = carregar_estoque()
    
    if not df_estoque.empty:
        # Métricas gerais
        total_itens = df_estoque["Quantidade"].sum()
        valor_total = df_estoque["Valor Total"].sum()
        
        st.subheader("Visão Geral")
        col1, col2 = st.columns(2)
        col1.metric("Total de Itens", f"{total_itens:,.2f}".replace(".", ","))
        col2.metric("Valor Total em Estoque", formatar_br(valor_total))
        
        # Tabela de estoque
        st.dataframe(
            df_estoque.style.format({
                "Valor Unit": formatar_br,
                "Valor Total": formatar_br
            }),
            column_config={
                "Quantidade": st.column_config.NumberColumn("Qtd", format="%.3f")
            },
            hide_index=True,
            use_container_width=True
        )
        
        # Seção de contagem física (prévia)
        with st.expander("🔄 Contagem Física"):
            item = st.selectbox("Selecione o item", df_estoque["Descrição"])
            qtd_fisica = st.number_input("Quantidade física contada", min_value=0.0, step=0.001, format="%.3f")
            
            if st.button("Comparar com sistema"):
                qtd_sistema = df_estoque[df_estoque["Descrição"] == item]["Quantidade"].values[0]
                diferenca = qtd_fisica - qtd_sistema
                
                st.write(f"**Sistema:** {qtd_sistema:,.3f} | **Físico:** {qtd_fisica:,.3f}")
                if diferenca == 0:
                    st.success("✅ Contagem compatível!")
                else:
                    st.error(f"❌ Diferença: {diferenca:,.3f}")
    else:
        st.warning("Nenhum dado de estoque encontrado")

# --- Página Dashboard ---
elif menu == "📊 Dashboard":
    st.title("📊 Dashboard Analítico")
    st.info("Em desenvolvimento - versão em breve!")
    st.image("https://via.placeholder.com/800x400?text=Dashboard+em+Construção", use_column_width=True)

# --- Rodapé ---
st.sidebar.markdown("---")
st.sidebar.info("Vulcano App v2.0 | Desenvolvido para gestão integrada")
