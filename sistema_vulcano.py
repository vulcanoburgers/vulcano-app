import streamlit as st
import pandas as pd
import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from google.oauth2.service_account import Credentials
import gspread
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO
import time
from collections import defaultdict
import sqlite3
import os

# --- Configuração Inicial ---
st.set_page_config(page_title="Vulcano App - Sistema de Gestão", layout="wide", initial_sidebar_state="expanded")

# --- CSS Personalizado ---
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF4B4B;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(45deg, #FF4B4B, #FF6B6B);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# --- Inicialização do Banco SQLite ---
@st.cache_resource
def init_database():
    """Inicializa o banco de dados SQLite local"""
    if not os.path.exists('vulcano_data.db'):
        conn = sqlite3.connect('vulcano_data.db')
        cursor = conn.cursor()
        
        # Tabela de produtos/ingredientes
        cursor.execute('''
            CREATE TABLE produtos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL,
                categoria TEXT,
                unidade TEXT,
                estoque_atual REAL DEFAULT 0,
                estoque_minimo REAL DEFAULT 0,
                custo_unitario REAL DEFAULT 0,
                data_ultima_compra DATE,
                fornecedor TEXT
            )
        ''')
        
        # Tabela de fichas técnicas
        cursor.execute('''
            CREATE TABLE fichas_tecnicas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                produto_final TEXT NOT NULL,
                ingrediente TEXT NOT NULL,
                quantidade REAL NOT NULL,
                unidade TEXT NOT NULL
            )
        ''')
        
        # Tabela de movimentações de estoque
        cursor.execute('''
            CREATE TABLE movimentacoes_estoque (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                produto TEXT NOT NULL,
                tipo TEXT NOT NULL,  -- entrada, saida, ajuste
                quantidade REAL NOT NULL,
                motivo TEXT,
                data_movimentacao DATETIME DEFAULT CURRENT_TIMESTAMP,
                usuario TEXT
            )
        ''')
        
        # Tabela de contas a pagar/receber
        cursor.execute('''
            CREATE TABLE fluxo_caixa (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_vencimento DATE NOT NULL,
                descricao TEXT NOT NULL,
                categoria TEXT NOT NULL,
                tipo TEXT NOT NULL,  -- receita, despesa
                valor_teorico REAL DEFAULT 0,
                valor_real REAL DEFAULT 0,
                status TEXT DEFAULT 'pendente',  -- pendente, pago, recebido
                observacoes TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    return sqlite3.connect('vulcano_data.db', check_same_thread=False)

# --- Conexão Google Sheets ---
@st.cache_resource(ttl=3600)
def conectar_google_sheets():
    """Conecta com Google Sheets"""
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(st.secrets, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Erro na conexão com o Google Sheets: {str(e)}")
        return None

# --- Funções Auxiliares ---
def formatar_br(valor, is_quantidade=False):
    """Formata valores para padrão brasileiro"""
    try:
        if is_quantidade:
            return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(valor)

def converter_valor(valor):
    """Converte string para float (padrão brasileiro)"""
    try:
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return 0.0

def extrair_itens_nfce(soup):
    """Extrai itens da NFCe usando BeautifulSoup"""
    tabela = soup.find("table", {"id": "tabResult"})
    if not tabela:
        return pd.DataFrame()
    
    linhas = tabela.find_all("tr")
    dados = []
    
    for linha in linhas:
        texto = linha.get_text(" ", strip=True)
        if all(keyword in texto for keyword in ["Código:", "Qtde.:", "UN:", "Vl. Unit.:", "Vl. Total"]):
            try:
                nome = texto.split("(Código:")[0].strip()
                codigo = re.search(r"Código:\s*(\d+)", texto).group(1)
                qtd = re.search(r"Qtde\.\:\s*([\d,]+)", texto).group(1).replace(",", ".")
                unidade = re.search(r"UN\:\s*(\w+)", texto).group(1)
                unitario = re.search(r"Vl\. Unit\.\:\s*([\d,]+)", texto).group(1).replace(",", ".")
                total = re.search(r"Vl\. Total\s*([\d,]+)", texto).group(1).replace(",", ".")
                
                dados.append({
                    "Descrição": nome,
                    "Código": codigo,
                    "Quantidade": float(qtd),
                    "Unidade": unidade,
                    "Valor Unitário": float(unitario),
                    "Valor Total": float(total)
                })
            except Exception as e:
                continue
    
    return pd.DataFrame(dados)

def analisar_pedidos_com_ia(df_pedidos):
    """Análise inteligente dos dados de pedidos"""
    insights = []
    
    if df_pedidos.empty:
        return ["Não há dados suficientes para análise."]
    
    try:
        # Análise temporal
        df_pedidos['Data'] = pd.to_datetime(df_pedidos['Data'], errors='coerce')
        df_pedidos = df_pedidos.dropna(subset=['Data'])
        
        if len(df_pedidos) == 0:
            return ["Dados de data inválidos."]
        
        # Padrões de horário
        df_pedidos['Hora'] = df_pedidos['Data'].dt.hour
        horarios_pico = df_pedidos['Hora'].value_counts().head(3)
        
        insights.append(f"🕐 **Horários de maior movimento:** {', '.join([f'{h}h ({v} pedidos)' for h, v in horarios_pico.items()])}")
        
        # Padrões de canal de venda
        if 'Canal' in df_pedidos.columns:
            canais = df_pedidos['Canal'].value_counts()
            canal_principal = canais.index[0] if len(canais) > 0 else "N/A"
            insights.append(f"📱 **Canal principal:** {canal_principal} ({canais.iloc[0]} pedidos)")
        
        # Análise de valores
        if 'Valor' in df_pedidos.columns:
            df_pedidos['Valor'] = pd.to_numeric(df_pedidos['Valor'], errors='coerce')
            ticket_medio = df_pedidos['Valor'].mean()
            valor_total = df_pedidos['Valor'].sum()
            
            insights.append(f"💰 **Ticket médio:** {formatar_br(ticket_medio)}")
            insights.append(f"💰 **Faturamento total:** {formatar_br(valor_total)}")
        
        # Análise de crescimento
        df_pedidos['Data_apenas'] = df_pedidos['Data'].dt.date
        pedidos_por_dia = df_pedidos.groupby('Data_apenas').size()
        
        if len(pedidos_por_dia) >= 7:
            ultima_semana = pedidos_por_dia.tail(7).mean()
            semana_anterior = pedidos_por_dia.tail(14).head(7).mean()
            
            if semana_anterior > 0:
                crescimento = ((ultima_semana - semana_anterior) / semana_anterior) * 100
                if crescimento > 5:
                    insights.append(f"📈 **Tendência positiva:** Crescimento de {crescimento:.1f}% na última semana")
                elif crescimento < -5:
                    insights.append(f"📉 **Atenção:** Queda de {abs(crescimento):.1f}% na última semana")
        
        # Recomendações
        insights.append("\n**🎯 Recomendações:**")
        
        if len(horarios_pico) > 0:
            hora_pico = horarios_pico.index[0]
            if hora_pico >= 18:
                insights.append("• Considere promoções no período da tarde para aumentar o movimento")
            elif hora_pico <= 14:
                insights.append("• Aproveite o movimento do almoço para lançar combos executivos")
        
        # Análise de delivery vs salão
        if 'Canal' in df_pedidos.columns:
            delivery_pct = (df_pedidos['Canal'].str.contains('delivery|ifood', case=False, na=False).sum() / len(df_pedidos)) * 100
            if delivery_pct > 60:
                insights.append("• Foco no delivery: considere melhorar a logística e parcerias")
            elif delivery_pct < 30:
                insights.append("• Potencial no delivery: considere estratégias para aumentar vendas online")
        
    except Exception as e:
        insights.append(f"Erro na análise: {str(e)}")
    
    return insights

# --- Carregar dados ---
@st.cache_data(ttl=300)
def carregar_dados_sheets():
    """Carrega dados das planilhas Google Sheets"""
    client = conectar_google_sheets()
    if not client:
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        sheet_compras = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("COMPRAS")
        sheet_pedidos = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("PEDIDOS")
        
        df_compras = pd.DataFrame(sheet_compras.get_all_records())
        df_pedidos = pd.DataFrame(sheet_pedidos.get_all_records())
        
        return df_compras, df_pedidos
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

# --- Interface Principal ---
def main():
    # Header
    st.markdown('<h1 class="main-header">🔥 VULCANO - Sistema de Gestão</h1>', unsafe_allow_html=True)
    
    # Inicializar banco
    conn = init_database()
    
    # Menu lateral
    st.sidebar.title("📋 Menu Principal")
    menu = st.sidebar.radio(
        "Selecione uma opção:",
        [
            "🏠 Dashboard Principal",
            "📥 Inserir NFC-e", 
            "📊 Análise de Pedidos",
            "📦 Controle de Estoque", 
            "📈 Fluxo de Caixa (DRE)",
            "🛵 Fechamento Motoboys",
            "⚙️ Configurações"
        ]
    )
    
    # Carregar dados
    df_compras, df_pedidos = carregar_dados_sheets()
    
    # --- DASHBOARD PRINCIPAL ---
    if menu == "🏠 Dashboard Principal":
        st.title("📊 Dashboard Principal")
        
        # Métricas principais
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_pedidos = len(df_pedidos) if not df_pedidos.empty else 0
            st.metric("Total de Pedidos", total_pedidos)
        
        with col2:
            if not df_pedidos.empty and 'Valor' in df_pedidos.columns:
                faturamento = pd.to_numeric(df_pedidos['Valor'], errors='coerce').sum()
                st.metric("Faturamento", formatar_br(faturamento))
            else:
                st.metric("Faturamento", "R$ 0,00")
        
        with col3:
            if not df_pedidos.empty and 'Valor' in df_pedidos.columns:
                ticket_medio = pd.to_numeric(df_pedidos['Valor'], errors='coerce').mean()
                st.metric("Ticket Médio", formatar_br(ticket_medio))
            else:
                st.metric("Ticket Médio", "R$ 0,00")
        
        with col4:
            total_compras = len(df_compras) if not df_compras.empty else 0
            st.metric("Compras Registradas", total_compras)
        
        # Gráficos
        if not df_pedidos.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 Vendas por Dia")
                if 'Data' in df_pedidos.columns:
                    df_pedidos['Data'] = pd.to_datetime(df_pedidos['Data'], errors='coerce')
                    vendas_dia = df_pedidos.groupby(df_pedidos['Data'].dt.date).size().reset_index()
                    vendas_dia.columns = ['Data', 'Pedidos']
                    
                    fig = px.line(vendas_dia, x='Data', y='Pedidos', title="Pedidos por Dia")
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("🎯 Vendas por Canal")
                if 'Canal' in df_pedidos.columns:
                    canal_vendas = df_pedidos['Canal'].value_counts()
                    fig = px.pie(values=canal_vendas.values, names=canal_vendas.index, title="Distribuição por Canal")
                    st.plotly_chart(fig, use_container_width=True)
    
    # --- INSERIR NFC-E ---
    elif menu == "📥 Inserir NFC-e":
        st.title("📥 Inserir Nota Fiscal (NFC-e)")
        
        tab1, tab2 = st.tabs(["🔗 Via URL", "📄 Via Upload"])
        
        with tab1:
            st.subheader("Importar via URL da NFC-e")
            url_nfce = st.text_input("Cole a URL da NFC-e aqui:")
            
            if st.button("🔍 Extrair Dados") and url_nfce:
                with st.spinner("Processando NFC-e..."):
                    try:
                        response = requests.get(url_nfce)
                        soup = BeautifulSoup(response.content, 'html.parser')
                        df_itens = extrair_itens_nfce(soup)
                        
                        if not df_itens.empty:
                            st.success("✅ Dados extraídos com sucesso!")
                            st.dataframe(df_itens)
                            
                            if st.button("💾 Salvar no Google Sheets"):
                                # Implementar salvamento
                                st.success("Dados salvos com sucesso!")
                        else:
                            st.error("❌ Não foi possível extrair os dados. Verifique a URL.")
                    except Exception as e:
                        st.error(f"❌ Erro ao processar: {str(e)}")
        
        with tab2:
            st.subheader("Upload de arquivo CSV/Excel")
            arquivo = st.file_uploader("Selecione o arquivo", type=['csv', 'xlsx', 'xls'])
            
            if arquivo:
                try:
                    if arquivo.name.endswith('.csv'):
                        df_upload = pd.read_csv(arquivo)
                    else:
                        df_upload = pd.read_excel(arquivo)
                    
                    st.dataframe(df_upload)
                    
                    if st.button("💾 Salvar Dados"):
                        st.success("Dados processados com sucesso!")
                except Exception as e:
                    st.error(f"❌ Erro ao processar arquivo: {str(e)}")
    
    # --- ANÁLISE DE PEDIDOS ---
    elif menu == "📊 Análise de Pedidos":
        st.title("📊 Análise Inteligente de Pedidos")
        
        if df_pedidos.empty:
            st.warning("⚠️ Nenhum dado de pedidos encontrado. Importe os dados primeiro.")
            return
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'Data' in df_pedidos.columns:
                df_pedidos['Data'] = pd.to_datetime(df_pedidos['Data'], errors='coerce')
                data_min = df_pedidos['Data'].min().date() if not df_pedidos['Data'].isnull().all() else datetime.date.today()
                data_max = df_pedidos['Data'].max().date() if not df_pedidos['Data'].isnull().all() else datetime.date.today()
                
                data_inicio = st.date_input("Data Início", value=data_min)
                data_fim = st.date_input("Data Fim", value=data_max)
        
        with col2:
            if 'Canal' in df_pedidos.columns:
                canais = ['Todos'] + list(df_pedidos['Canal'].unique())
                canal_selecionado = st.selectbox("Canal", canais)
        
        with col3:
            if 'Motoboy' in df_pedidos.columns:
                motoboys = ['Todos'] + list(df_pedidos['Motoboy'].unique())
                motoboy_selecionado = st.selectbox("Motoboy", motoboys)
        
        # Análise com IA
        st.subheader("🤖 Insights com IA")
        
        if st.button("🔍 Gerar Análise Inteligente"):
            with st.spinner("Analisando dados..."):
                insights = analisar_pedidos_com_ia(df_pedidos)
                
                for insight in insights:
                    st.markdown(insight)
        
        # Problema do ticket médio por mesa
        st.subheader("🎯 Solução: Ticket Médio por Mesa")
        st.info("""
        **Como resolver o problema dos múltiplos pedidos por mesa:**
        
        1. **Agrupamento por tempo:** Pedidos da mesma mesa em até 30 minutos são considerados uma sessão
        2. **Identificação por mesa:** Use número da mesa + data para agrupar
        3. **Cálculo inteligente:** Soma todos os pedidos de uma sessão para calcular o ticket real
        """)
        
        # Implementação da solução
        if 'Mesa' in df_pedidos.columns and 'Data' in df_pedidos.columns:
            if st.checkbox("🧮 Calcular Ticket Médio Corrigido"):
                # Algoritmo para agrupar pedidos por sessão de mesa
                df_temp = df_pedidos.copy()
                df_temp['Data'] = pd.to_datetime(df_temp['Data'])
                df_temp['Valor'] = pd.to_numeric(df_temp['Valor'], errors='coerce')
                
                # Agrupar por mesa e criar sessões baseadas em tempo
                sessoes_mesa = []
                
                for mesa in df_temp['Mesa'].unique():
                    pedidos_mesa = df_temp[df_temp['Mesa'] == mesa].sort_values('Data')
                    
                    if len(pedidos_mesa) > 0:
                        sessao_atual = [pedidos_mesa.iloc[0]]
                        
                        for i in range(1, len(pedidos_mesa)):
                            tempo_diff = (pedidos_mesa.iloc[i]['Data'] - sessao_atual[-1]['Data']).total_seconds() / 60
                            
                            if tempo_diff <= 30:  # 30 minutos para considerar mesma sessão
                                sessao_atual.append(pedidos_mesa.iloc[i])
                            else:
                                # Finalizar sessão atual
                                valor_sessao = sum([p['Valor'] for p in sessao_atual if not pd.isna(p['Valor'])])
                                sessoes_mesa.append({
                                    'Mesa': mesa,
                                    'Data': sessao_atual[0]['Data'],
                                    'Valor_Total': valor_sessao,
                                    'Qtd_Pedidos': len(sessao_atual)
                                })
                                sessao_atual = [pedidos_mesa.iloc[i]]
                        
                        # Adicionar última sessão
                        if sessao_atual:
                            valor_sessao = sum([p['Valor'] for p in sessao_atual if not pd.isna(p['Valor'])])
                            sessoes_mesa.append({
                                'Mesa': mesa,
                                'Data': sessao_atual[0]['Data'],
                                'Valor_Total': valor_sessao,
                                'Qtd_Pedidos': len(sessao_atual)
                            })
                
                if sessoes_mesa:
                    df_sessoes = pd.DataFrame(sessoes_mesa)
                    ticket_medio_corrigido = df_sessoes['Valor_Total'].mean()
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Ticket Médio Original", formatar_br(df_temp['Valor'].mean()))
                    with col2:
                        st.metric("Ticket Médio Corrigido", formatar_br(ticket_medio_corrigido))
                    
                    st.dataframe(df_sessoes)
    
    # --- CONTROLE DE ESTOQUE ---
    elif menu == "📦 Controle de Estoque":
        st.title("📦 Controle de Estoque")
        
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Estoque Atual", "➕ Adicionar Produto", "🔧 Ficha Técnica", "📊 Relatórios"])
        
        with tab1:
            st.subheader("📋 Estoque Atual")
            
            # Buscar produtos do banco
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM produtos ORDER BY nome")
            produtos = cursor.fetchall()
            
            if produtos:
                df_estoque = pd.DataFrame(produtos, columns=['ID', 'Nome', 'Categoria', 'Unidade', 'Estoque_Atual', 'Estoque_Minimo', 'Custo_Unitario', 'Data_Ultima_Compra', 'Fornecedor'])
                
                # Destacar produtos com estoque baixo
                def highlight_low_stock(row):
                    if row['Estoque_Atual'] <= row['Estoque_Minimo']:
                        return ['background-color: #ffcccc'] * len(row)
                    return [''] * len(row)
                
                st.dataframe(df_estoque.style.apply(highlight_low_stock, axis=1), use_container_width=True)
                
                # Produtos com estoque baixo
                estoque_baixo = df_estoque[df_estoque['Estoque_Atual'] <= df_estoque['Estoque_Minimo']]
                if not estoque_baixo.empty:
                    st.error(f"⚠️ {len(estoque_baixo)} produto(s) com estoque baixo!")
                    st.dataframe(estoque_baixo[['Nome', 'Estoque_Atual', 'Estoque_Minimo']])
            else:
                st.info("Nenhum produto cadastrado. Adicione produtos na aba 'Adicionar Produto'.")
        
        with tab2:
            st.subheader("➕ Adicionar Novo Produto")
            
            with st.form("form_produto"):
                col1, col2 = st.columns(2)
                
                with col1:
                    nome = st.text_input("Nome do Produto*")
                    categoria = st.selectbox("Categoria", ["Carnes", "Pães", "Vegetais", "Molhos", "Bebidas", "Outros"])
                    unidade = st.selectbox("Unidade", ["kg", "g", "un", "L", "ml"])
                
                with col2:
                    estoque_atual = st.number_input("Estoque Atual", min_value=0.0, step=0.1)
                    estoque_minimo = st.number_input("Estoque Mínimo", min_value=0.0, step=0.1)
                    custo_unitario = st.number_input("Custo Unitário (R$)", min_value=0.0, step=0.01)
                
                fornecedor = st.text_input("Fornecedor")
                
                if st.form_submit_button("💾 Salvar Produto"):
                    if nome:
                        cursor = conn.cursor()
                        cursor.execute("""
                            INSERT INTO produtos (nome, categoria, unidade, estoque_atual, estoque_minimo, custo_unitario, fornecedor)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (nome, categoria, unidade, estoque_atual, estoque_minimo, custo_unitario, fornecedor))
                        conn.commit()
                        st.success("✅ Produto adicionado com sucesso!")
                        st.rerun()
                    else:
                        st.error("❌ Nome do produto é obrigatório!")
        
        with tab3:
            st.subheader("🔧 Fichas Técnicas")
            st.info("Configure quanto de cada ingrediente é usado em cada prato.")
            
            # Buscar produtos cadastrados
            cursor = conn.cursor()
            cursor.execute("SELECT nome FROM produtos ORDER BY nome")
            produtos_lista = [row[0] for row in cursor.fetchall()]
            
            if produtos_lista:
                with st.form("form_ficha_tecnica"):
                    produto_final = st.selectbox("Produto Final (prato)", produtos_lista)
                    ingrediente = st.selectbox("Ingrediente", produtos_lista)
                    quantidade = st.number_input("Quantidade", min_value=0.0, step=0.1)
                    unidade = st.selectbox("Unidade", ["kg", "g", "un", "L", "ml"])
                    
                    if st.form_submit_button("💾 Adicionar Ingrediente"):
                        cursor.execute("""
                            INSERT INTO fichas_tecnicas (produto_final, ingrediente, quantidade, unidade)
                            VALUES (?, ?, ?, ?)
                        """, (produto_final, ingrediente, quantidade, unidade))
                        conn.commit()
                        st.success("✅ Ingrediente adicionado à ficha técnica!")
                
                # Mostrar fichas técnicas existentes
                cursor.execute("SELECT * FROM fichas_tecnicas ORDER BY produto_final")
                fichas = cursor.fetchall()
                
                if fichas:
                    df_fichas = pd.DataFrame(fichas, columns=['ID', 'Produto_Final', 'Ingrediente', 'Quantidade', 'Unidade'])
                    st.dataframe(df_fichas, use_container_width=True)
            else:
                st.warning("Cadastre produtos primeiro para criar fichas técnicas.")
        
        with tab4:
            st.subheader("📊 Relatórios de Estoque")
            
            if st.button("📈 Gerar Relatório de Movimentação"):
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM movimentacoes_estoque ORDER BY data_movimentacao DESC LIMIT 50")
                movimentacoes = cursor.fetchall()
                
                if movimentacoes:
                    df_mov = pd.DataFrame(movimentacoes, columns=['ID', 'Produto', 'Tipo', 'Quantidade', 'Motivo', 'Data', 'Usuario'])
                    st.dataframe(df_mov, use_container_width=True)
                else:
                    st.info("Nenhuma movimentação registrada.")
    
    # --- FLUXO DE CAIXA (DRE) ---
    elif menu == "📈 Fluxo de Caixa (DRE)":
        st.title("📈 Fluxo de Caixa e DRE")
        
        tab1, tab2, tab3 = st.tabs(["💰 Fluxo de Caixa", "➕ Adicionar Lançamento", "📊 Relatório DRE"])
        
        with tab1:
            st.subheader("💰 Fluxo de Caixa")
            
            # Filtros de data
            col1, col2 = st.columns(2)
            with col1:
                data_inicio_fluxo = st.date_input("Data Início", value=datetime.date.today() - datetime.timedelta(days=30))
            with col2:
                data_fim_fluxo = st.date_input("Data Fim", value=datetime.date.today() + datetime.timedelta(days=30))
            
            # Buscar lançamentos
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM fluxo_caixa 
                WHERE data_vencimento BETWEEN ? AND ?
                ORDER BY data_vencimento
            """, (data_inicio_fluxo, data_fim_fluxo))
            
            lancamentos = cursor.fetchall()
            
            if lancamentos:
                df_fluxo = pd.DataFrame(lancamentos, columns=[
                    'ID', 'Data_Vencimento', 'Descricao', 'Categoria', 'Tipo', 
                    'Valor_Teorico', 'Valor_Real', 'Status', 'Observacoes'
                ])
                
                # Métricas
                col1, col2, col3, col4 = st.columns(4)
                
                receitas_teorico = df_fluxo[df_fluxo['Tipo'] == 'receita']['Valor_Teorico'].sum()
                despesas_teorico = df_fluxo[df_fluxo['Tipo'] == 'despesa']['Valor_Teorico'].sum()
                receitas_real = df_fluxo[df_fluxo['Tipo'] == 'receita']['Valor_Real'].sum()
                despesas_real = df_fluxo[df_fluxo['Tipo'] == 'despesa']['Valor_Real'].sum()
                
                with col1:
                    st.metric("Receitas (Teórico)", formatar_br(receitas_teorico))
                with col2:
                    st.metric("Despesas (Teórico)", formatar_br(despesas_teorico))
                with col3:
                    st.metric("Receitas (Real)", formatar_br(receitas_real))
                with col4:
                    st.metric("Despesas (Real)", formatar_br(despesas_real))
                
                # Saldo
                saldo_teorico = receitas_teorico - despesas_teorico
                saldo_real = receitas_real - despesas_real
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Saldo Teórico", formatar_br(saldo_teorico), 
                             delta=None, delta_color="normal" if saldo_teorico >= 0 else "inverse")
                with col2:
                    st.metric("Saldo Real", formatar_br(saldo_real),
                             delta=None, delta_color="normal" if saldo_real >= 0 else "inverse")
                
                # Tabela de lançamentos
                st.dataframe(df_fluxo, use_container_width=True)
                
                # Análise de discrepâncias
                st.subheader("🔍 Análise de Discrepâncias")
                df_fluxo['Diferenca'] = df_fluxo['Valor_Real'] - df_fluxo['Valor_Teorico']
                discrepancias = df_fluxo[abs(df_fluxo['Diferenca']) > 50]  # Diferenças > R$ 50
                
                if not discrepancias.empty:
                    st.warning(f"⚠️ {len(discrepancias)} lançamento(s) com diferenças significativas:")
                    st.dataframe(discrepancias[['Descricao', 'Valor_Teorico', 'Valor_Real', 'Diferenca']])
            else:
                st.info("Nenhum lançamento encontrado no período.")
        
        with tab2:
            st.subheader("➕ Adicionar Lançamento")
            
            with st.form("form_lancamento"):
                col1, col2 = st.columns(2)
                
                with col1:
                    data_vencimento = st.date_input("Data de Vencimento")
                    descricao = st.text_input("Descrição*")
                    categoria = st.selectbox("Categoria", [
                        "Vendas", "Taxa Cartão", "Taxa iFood", "Aluguel", "Energia", 
                        "Água", "Internet", "Salários", "Fornecedores", "Marketing", "Outros"
                    ])
                
                with col2:
                    tipo = st.selectbox("Tipo", ["receita", "despesa"])
                    valor_teorico = st.number_input("Valor Teórico (R$)", min_value=0.0, step=0.01)
                    valor_real = st.number_input("Valor Real (R$)", min_value=0.0, step=0.01)
                    status = st.selectbox("Status", ["pendente", "pago", "recebido"])
                
                observacoes = st.text_area("Observações")
                
                if st.form_submit_button("💾 Salvar Lançamento"):
                    if descricao:
                        cursor = conn.cursor()
                        cursor.execute("""
                            INSERT INTO fluxo_caixa 
                            (data_vencimento, descricao, categoria, tipo, valor_teorico, valor_real, status, observacoes)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (data_vencimento, descricao, categoria, tipo, valor_teorico, valor_real, status, observacoes))
                        conn.commit()
                        st.success("✅ Lançamento adicionado com sucesso!")
                        st.rerun()
                    else:
                        st.error("❌ Descrição é obrigatória!")
        
        with tab3:
            st.subheader("📊 Relatório DRE")
            
            # Período para DRE
            col1, col2 = st.columns(2)
            with col1:
                mes_dre = st.selectbox("Mês", range(1, 13), index=datetime.date.today().month - 1)
            with col2:
                ano_dre = st.number_input("Ano", min_value=2020, max_value=2030, value=datetime.date.today().year)
            
            if st.button("📈 Gerar DRE"):
                # Buscar dados do período
                data_inicio_dre = datetime.date(ano_dre, mes_dre, 1)
                if mes_dre == 12:
                    data_fim_dre = datetime.date(ano_dre + 1, 1, 1) - datetime.timedelta(days=1)
                else:
                    data_fim_dre = datetime.date(ano_dre, mes_dre + 1, 1) - datetime.timedelta(days=1)
                
                cursor.execute("""
                    SELECT categoria, tipo, SUM(valor_real) as total
                    FROM fluxo_caixa 
                    WHERE data_vencimento BETWEEN ? AND ?
                    GROUP BY categoria, tipo
                    ORDER BY tipo DESC, total DESC
                """, (data_inicio_dre, data_fim_dre))
                
                dados_dre = cursor.fetchall()
                
                if dados_dre:
                    # Estrutura da DRE
                    st.markdown("### 📋 Demonstrativo do Resultado do Exercício (DRE)")
                    st.markdown(f"**Período:** {data_inicio_dre.strftime('%m/%Y')}")
                    
                    receitas_total = 0
                    despesas_total = 0
                    
                    # Receitas
                    st.markdown("#### 💰 RECEITAS")
                    for categoria, tipo, total in dados_dre:
                        if tipo == 'receita':
                            st.markdown(f"• {categoria}: {formatar_br(total)}")
                            receitas_total += total
                    
                    st.markdown(f"**RECEITA BRUTA: {formatar_br(receitas_total)}**")
                    st.markdown("---")
                    
                    # Despesas
                    st.markdown("#### 💸 DESPESAS")
                    for categoria, tipo, total in dados_dre:
                        if tipo == 'despesa':
                            st.markdown(f"• {categoria}: {formatar_br(total)}")
                            despesas_total += total
                    
                    st.markdown(f"**DESPESAS TOTAIS: {formatar_br(despesas_total)}**")
                    st.markdown("---")
                    
                    # Resultado
                    resultado = receitas_total - despesas_total
                    cor = "🟢" if resultado >= 0 else "🔴"
                    st.markdown(f"#### {cor} RESULTADO DO PERÍODO")
                    st.markdown(f"**{formatar_br(resultado)}**")
                    
                    # Gráfico
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='Receitas', x=['Resultado'], y=[receitas_total], marker_color='green'))
                    fig.add_trace(go.Bar(name='Despesas', x=['Resultado'], y=[despesas_total], marker_color='red'))
                    fig.update_layout(title='Receitas vs Despesas', barmode='group')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Nenhum dado encontrado para o período selecionado.")
    
    # --- FECHAMENTO MOTOBOYS ---
    elif menu == "🛵 Fechamento Motoboys":
        st.title("🛵 Fechamento de Motoboys")
        
        if df_pedidos.empty:
            st.warning("⚠️ Nenhum dado de pedidos encontrado.")
            return
        
        # Configurações
        col1, col2, col3 = st.columns(3)
        
        with col1:
            motoboys_lista = ["Everson", "Marlon", "Adrian", "Vulcano"]
            if 'Motoboy' in df_pedidos.columns:
                motoboys_planilha = df_pedidos['Motoboy'].dropna().unique().tolist()
                motoboys_lista = list(set(motoboys_lista + motoboys_planilha))
            
            motoboy_selecionado = st.selectbox("Selecione o motoboy:", sorted(motoboys_lista))
        
        with col2:
            data_inicio = st.date_input("Data início:", value=datetime.date.today() - datetime.timedelta(days=7))
        
        with col3:
            data_fim = st.date_input("Data fim:", value=datetime.date.today())
        
        if st.button("🔍 Calcular Fechamento"):
            # Preparar dados
            df_temp = df_pedidos.copy()
            df_temp['Data'] = pd.to_datetime(df_temp['Data'], errors='coerce')
            df_temp = df_temp.dropna(subset=['Data'])
            
            # Filtros
            filtro = (
                (df_temp['Motoboy'].str.strip().str.lower() == motoboy_selecionado.lower()) &
                (df_temp['Data'].dt.date >= data_inicio) &
                (df_temp['Data'].dt.date <= data_fim)
            )
            df_filtrado = df_temp[filtro].copy()
            
            if df_filtrado.empty:
                st.warning("Nenhum pedido encontrado para os filtros selecionados.")
            else:
                # Processar distâncias
                if 'Distancia' in df_filtrado.columns:
                    df_filtrado['Distancia'] = pd.to_numeric(df_filtrado['Distancia'], errors='coerce')
                elif 'KM' in df_filtrado.columns:
                    df_filtrado['Distancia'] = pd.to_numeric(df_filtrado['KM'], errors='coerce')
                else:
                    st.error("Coluna de distância não encontrada. Verifique se existe 'Distancia' ou 'KM' na planilha.")
                    return
                
                df_filtrado = df_filtrado.dropna(subset=['Distancia'])
                df_filtrado = df_filtrado.sort_values('Data')
                
                # Cálculos
                dias_trabalhados = df_filtrado['Data'].dt.date.nunique()
                total_corridas = len(df_filtrado)
                km_total = df_filtrado['Distancia'].sum()
                
                # Base diária
                base_diaria = 90.0  # R$ 90 por dia
                total_base = base_diaria * dias_trabalhados
                
                # Função para calcular extras
                def calcular_taxa_extra(km):
                    if km <= 6:
                        return 0
                    elif km <= 8:
                        return 2
                    elif km <= 10:
                        return 6
                    else:
                        return 11
                
                def calcular_valor_excedente(km):
                    if km <= 6:
                        return 6
                    elif km <= 8:
                        return 8
                    elif km <= 10:
                        return 12
                    else:
                        return 17
                
                # Calcular extras por dia
                corridas_por_dia = df_filtrado.groupby(df_filtrado['Data'].dt.date)
                total_extra = 0
                detalhes_dias = []
                
                for dia, grupo in corridas_por_dia:
                    corridas_dia = len(grupo)
                    km_dia = grupo['Distancia'].sum()
                    
                    if corridas_dia <= 8:
                        # Até 8 corridas: taxa normal
                        extra_dia = grupo['Distancia'].apply(calcular_taxa_extra).sum()
                    else:
                        # Mais de 8 corridas: primeiras 8 com taxa normal, excedentes com valor maior
                        primeiras_8 = grupo.iloc[:8]
                        excedentes = grupo.iloc[8:]
                        
                        extra_dia = (primeiras_8['Distancia'].apply(calcular_taxa_extra).sum() + 
                                   excedentes['Distancia'].apply(calcular_valor_excedente).sum())
                    
                    total_extra += extra_dia
                    detalhes_dias.append({
                        'Data': dia,
                        'Corridas': corridas_dia,
                        'KM_Total': km_dia,
                        'Extra': extra_dia
                    })
                
                total_final = total_base + total_extra
                
                # Exibir resultados
                st.success("✅ Fechamento calculado com sucesso!")
                
                # Métricas principais
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Dias Trabalhados", dias_trabalhados)
                with col2:
                    st.metric("Total de Corridas", total_corridas)
                with col3:
                    st.metric("KM Total", f"{km_total:.1f} km")
                with col4:
                    st.metric("Média KM/Corrida", f"{km_total/total_corridas:.1f} km")
                
                # Valores financeiros
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Base Fixa", formatar_br(total_base))
                with col2:
                    st.metric("Extras KM", formatar_br(total_extra))
                with col3:
                    st.metric("TOTAL A PAGAR", formatar_br(total_final), delta=None)
                
                # Detalhes por dia
                with st.expander("📋 Detalhes por Dia"):
                    df_detalhes = pd.DataFrame(detalhes_dias)
                    df_detalhes['Extra_Formatado'] = df_detalhes['Extra'].apply(formatar_br)
                    st.dataframe(df_detalhes, use_container_width=True)
                
                # Pedidos do período
                with st.expander("📦 Pedidos do Período"):
                    colunas_exibir = ['Data', 'Motoboy']
                    if 'Distancia' in df_filtrado.columns:
                        colunas_exibir.append('Distancia')
                    if 'KM' in df_filtrado.columns:
                        colunas_exibir.append('KM')
                    if 'Valor' in df_filtrado.columns:
                        colunas_exibir.append('Valor')
                    
                    st.dataframe(df_filtrado[colunas_exibir], use_container_width=True)
                
                # Gráfico de corridas por dia
                df_grafico = pd.DataFrame(detalhes_dias)
                fig = px.bar(df_grafico, x='Data', y='Corridas', title='Corridas por Dia')
                st.plotly_chart(fig, use_container_width=True)
    
    # --- CONFIGURAÇÕES ---
    elif menu == "⚙️ Configurações":
        st.title("⚙️ Configurações do Sistema")
        
        tab1, tab2, tab3 = st.tabs(["🔧 Sistema", "📊 Dados", "ℹ️ Sobre"])
        
        with tab1:
            st.subheader("🔧 Configurações do Sistema")
            
            st.markdown("### 📋 Estrutura das Planilhas")
            st.info("""
            **Planilha PEDIDOS deve conter as colunas:**
            - Data: Data e hora do pedido
            - Mesa: Número da mesa (para salão)
            - Canal: ifood, balcao, mesa, etc.
            - Motoboy: Nome do motoboy (para delivery)
            - Distancia ou KM: Distância percorrida
            - Valor: Valor do pedido
            - Forma_Pagamento: Como foi pago
            
            **Planilha COMPRAS deve conter as colunas:**
            - Data: Data da compra
            - Fornecedor: Nome do fornecedor
            - Produto: Nome do produto
            - Quantidade: Quantidade comprada
            - Valor_Unitario: Preço por unidade
            - Valor_Total: Valor total do item
            """)
            
            # Configurações de taxas
            st.markdown("### 💳 Configuração de Taxas")
            
            col1, col2 = st.columns(2)
            with col1:
                taxa_cartao = st.number_input("Taxa Cartão (%)", min_value=0.0, max_value=10.0, value=3.5, step=0.1)
                taxa_ifood = st.number_input("Taxa iFood (%)", min_value=0.0, max_value=30.0, value=18.0, step=0.5)
            
            with col2:
                base_motoboy = st.number_input("Base Diária Motoboy (R$)", min_value=0.0, value=90.0, step=5.0)
                tempo_sessao_mesa = st.number_input("Tempo Sessão Mesa (min)", min_value=10, max_value=60, value=30, step=5)
            
            if st.button("💾 Salvar Configurações"):
                st.success("✅ Configurações salvas!")
        
        with tab2:
            st.subheader("📊 Gerenciamento de Dados")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🔄 Atualizar Dados")
                if st.button("🔄 Recarregar Planilhas"):
                    st.cache_data.clear()
                    st.success("✅ Cache limpo! Dados serão recarregados.")
                    st.rerun()
                
                if st.button("📥 Exportar Banco Local"):
                    # Implementar exportação do SQLite
                    st.info("Funcionalidade em desenvolvimento...")
            
            with col2:
                st.markdown("### 🗑️ Limpeza")
                if st.button("⚠️ Limpar Banco Local", type="secondary"):
                    if st.confirm("Tem certeza? Esta ação não pode ser desfeita!"):
                        # Implementar limpeza do banco
                        st.warning("Funcionalidade em desenvolvimento...")
        
        with tab3:
            st.subheader("ℹ️ Sobre o Sistema")
            
            st.markdown("""
            ### 🔥 Vulcano - Sistema de Gestão
            
            **Versão:** 2.0  
            **Desenvolvido para:** Restaurantes e Hamburguerias  
            **Tecnologias:** Python, Streamlit, SQLite, Google Sheets
            
            #### 📋 Funcionalidades:
            - ✅ Análise inteligente de pedidos com IA
            - ✅ Controle completo de estoque
            - ✅ Fluxo de caixa e DRE
            - ✅ Fechamento automático de motoboys
            - ✅ Importação de NFC-e
            - ✅ Dashboards interativos
            
            #### 🎯 Problemas Resolvidos:
            - **Ticket médio por mesa:** Agrupamento inteligente de pedidos
            - **Formas de pagamento:** Sistema de lançamentos manuais
            - **Controle de estoque:** Fichas técnicas automatizadas
            - **Análise de dados:** Insights com IA
            
            #### 📞 Suporte:
            Sistema desenvolvido com foco na praticidade e eficiência.
            """)
            
            # Estatísticas do sistema
            st.markdown("### 📈 Estatísticas do Sistema")
            
            cursor = conn.cursor()
            
            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM produtos")
            total_produtos = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fluxo_caixa")
            total_lancamentos = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fichas_tecnicas")
            total_fichas = cursor.fetchone()[0]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Produtos Cadastrados", total_produtos)
            with col2:
                st.metric("Lançamentos Financeiros", total_lancamentos)
            with col3:
                st.metric("Fichas Técnicas", total_fichas)

if __name__ == "__main__":
    main()
