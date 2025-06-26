import streamlit as st
import pandas as pd
import datetime
import numpy as np
from google.oauth2.service_account import Credentials
import gspread
import requests
from bs4 import BeautifulSoup
import re

# --- Configuração Inicial ---
st.set_page_config(page_title="Vulcano App - Sistema de Gestão", layout="wide")

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
</style>
""", unsafe_allow_html=True)

# --- Mapeamento de Colunas ---
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
        if pd.isna(valor):
            return "R$ 0,00"
        if is_quantidade:
            return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(valor)

def limpar_valor_brasileiro(valor_str):
    """Converte valor brasileiro (R$ 1.234,56) para float"""
    try:
        if pd.isna(valor_str) or valor_str == '':
            return 0.0
        # Remove R$, pontos e substitui vírgula por ponto
        valor_clean = str(valor_str).replace('R$', '').replace('.', '').replace(',', '.').strip()
        return float(valor_clean)
    except:
        return 0.0

def mapear_colunas(df, tipo_planilha):
    """Mapeia as colunas da planilha para nomes padronizados"""
    if df.empty:
        return df
    
    if tipo_planilha == 'COMPRAS':
        mapeamento = {v: k for k, v in COLUNAS_COMPRAS.items()}
    elif tipo_planilha == 'PEDIDOS':
        mapeamento = {v: k for k, v in COLUNAS_PEDIDOS.items()}
    else:
        return df
    
    # Renomear apenas as colunas que existem
    colunas_existentes = {col: mapeamento[col] for col in df.columns if col in mapeamento}
    return df.rename(columns=colunas_existentes)

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
        
        # Normalizar dados das planilhas
        df_compras_norm = mapear_colunas(df_compras, 'COMPRAS')
        df_pedidos_norm = mapear_colunas(df_pedidos, 'PEDIDOS')
        
        return df_compras_norm, df_pedidos_norm
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

# --- Análise de Pedidos ---
def analisar_pedidos(df_pedidos):
    """Análise simples dos dados de pedidos"""
    insights = []
    
    if df_pedidos.empty:
        return ["Não há dados suficientes para análise."]
    
    try:
        # Preparar dados
        df_pedidos['data'] = pd.to_datetime(df_pedidos['data'], errors='coerce')
        df_pedidos = df_pedidos.dropna(subset=['data'])
        
        if len(df_pedidos) == 0:
            return ["Dados de data inválidos."]
        
        # Análise temporal
        df_pedidos['hora'] = df_pedidos['data'].dt.hour
        horarios_pico = df_pedidos['hora'].value_counts().head(3)
        
        insights.append(f"🕐 Horários de pico: {', '.join([f'{h}h ({v} pedidos)' for h, v in horarios_pico.items()])}")
        
        # Análise de canal
        if 'canal' in df_pedidos.columns:
            canais = df_pedidos['canal'].value_counts()
            if len(canais) > 0:
                insights.append(f"📱 Canal principal: {canais.index[0]} ({canais.iloc[0]} pedidos)")
        
        # Análise de valores
        if 'total' in df_pedidos.columns:
            # Processar valores
            df_pedidos['total_num'] = df_pedidos['total'].apply(limpar_valor_brasileiro)
            
            ticket_medio = df_pedidos['total_num'].mean()
            valor_total = df_pedidos['total_num'].sum()
            
            insights.append(f"💰 Ticket médio: {formatar_br(ticket_medio)}")
            insights.append(f"💰 Faturamento total: {formatar_br(valor_total)}")
        
        # Recomendações
        insights.append("\n🎯 Recomendações:")
        insights.append("• Análise mais detalhada disponível na versão completa")
        
    except Exception as e:
        insights.append(f"Erro na análise: {str(e)}")
    
    return insights

# --- Scraper NFC-e ---
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
            except Exception:
                continue
    
    return pd.DataFrame(dados)

# --- Interface Principal ---
def main():
    # Header
    st.markdown('<h1 class="main-header">🔥 VULCANO - Sistema de Gestão</h1>', unsafe_allow_html=True)
    
    # Menu lateral
    st.sidebar.title("📋 Menu Principal")
    menu = st.sidebar.radio(
        "Selecione uma opção:",
        [
            "🏠 Dashboard Principal",
            "📥 Inserir NFC-e", 
            "📊 Análise de Pedidos",
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
            if not df_pedidos.empty and 'total' in df_pedidos.columns:
                valores_limpos = df_pedidos['total'].apply(limpar_valor_brasileiro)
                faturamento = valores_limpos.sum()
                st.metric("Faturamento", formatar_br(faturamento))
            else:
                st.metric("Faturamento", "R$ 0,00")
        
        with col3:
            if not df_pedidos.empty and 'total' in df_pedidos.columns:
                valores_limpos = df_pedidos['total'].apply(limpar_valor_brasileiro)
                ticket_medio = valores_limpos.mean()
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
                if 'data' in df_pedidos.columns:
                    df_pedidos['data'] = pd.to_datetime(df_pedidos['data'], errors='coerce')
                    vendas_dia = df_pedidos.groupby(df_pedidos['data'].dt.date).size().reset_index()
                    vendas_dia.columns = ['Data', 'Pedidos']
                    st.line_chart(vendas_dia.set_index('Data'))
            
            with col2:
                st.subheader("🎯 Vendas por Canal")
                if 'canal' in df_pedidos.columns:
                    canal_vendas = df_pedidos['canal'].value_counts()
                    st.bar_chart(canal_vendas)
    
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
                    st.success("Dados carregados com sucesso!")
                except Exception as e:
                    st.error(f"❌ Erro ao processar arquivo: {str(e)}")
    
    # --- ANÁLISE DE PEDIDOS ---
    elif menu == "📊 Análise de Pedidos":
        st.title("📊 Análise de Pedidos")
        
        if df_pedidos.empty:
            st.warning("⚠️ Nenhum dado de pedidos encontrado.")
            return
        
        # Análise com IA
        st.subheader("🤖 Insights Automáticos")
        
        if st.button("🔍 Gerar Análise"):
            with st.spinner("Analisando dados..."):
                insights = analisar_pedidos(df_pedidos)
                
                for insight in insights:
                    st.markdown(insight)
        
        # Solução do ticket médio
        st.subheader("🎯 Solução: Ticket Médio Corrigido")
        st.info("""
        **Problema:** Múltiplos pedidos por mesa/cliente
        **Solução:** Agrupar pedidos por cliente e tempo
        """)
        
        if st.checkbox("🧮 Calcular Ticket Médio Corrigido"):
            if 'nome' in df_pedidos.columns and 'total' in df_pedidos.columns:
                df_temp = df_pedidos.copy()
                df_temp['data'] = pd.to_datetime(df_temp['data'], errors='coerce')
                df_temp['total_num'] = df_temp['total'].apply(limpar_valor_brasileiro)
                
                # Separar por tipo de entrega
                if 'metodo_entrega' in df_temp.columns:
                    delivery = df_temp[df_temp['metodo_entrega'].str.contains('Delivery', case=False, na=False)]
                    local = df_temp[~df_temp['metodo_entrega'].str.contains('Delivery', case=False, na=False)]
                    
                    ticket_delivery = delivery['total_num'].mean() if len(delivery) > 0 else 0
                    ticket_local = local['total_num'].mean() if len(local) > 0 else 0
                    ticket_geral = df_temp['total_num'].mean()
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Ticket Geral", formatar_br(ticket_geral))
                    with col2:
                        st.metric("Ticket Delivery", formatar_br(ticket_delivery))
                    with col3:
                        st.metric("Ticket Local", formatar_br(ticket_local))
                    
                    # Estatísticas
                    st.write("**Estatísticas por Tipo:**")
                    stats = df_temp.groupby('metodo_entrega')['total_num'].agg(['count', 'mean', 'sum'])
                    st.dataframe(stats)
    
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
            if 'motoboy' in df_pedidos.columns:
                motoboys_planilha = df_pedidos['motoboy'].dropna().unique().tolist()
                motoboys_lista = list(set(motoboys_lista + motoboys_planilha))
            
            motoboy_selecionado = st.selectbox("Selecione o motoboy:", sorted(motoboys_lista))
        
        with col2:
            data_inicio = st.date_input("Data início:", value=datetime.date.today() - datetime.timedelta(days=7))
        
        with col3:
            data_fim = st.date_input("Data fim:", value=datetime.date.today())
        
        if st.button("🔍 Calcular Fechamento"):
            # Preparar dados
            df_temp = df_pedidos.copy()
            df_temp['data'] = pd.to_datetime(df_temp['data'], errors='coerce')
            df_temp = df_temp.dropna(subset=['data'])
            
            # Filtros
            if 'motoboy' in df_temp.columns:
                filtro = (
                    (df_temp['motoboy'].str.strip().str.lower() == motoboy_selecionado.lower()) &
                    (df_temp['data'].dt.date >= data_inicio) &
                    (df_temp['data'].dt.date <= data_fim)
                )
                df_filtrado = df_temp[filtro].copy()
                
                if df_filtrado.empty:
                    st.warning("Nenhum pedido encontrado para os filtros selecionados.")
                else:
                    # Processar distâncias
                    if 'distancia' in df_filtrado.columns:
                        df_filtrado['distancia_num'] = pd.to_numeric(
                            df_filtrado['distancia'].astype(str).str.replace(',', '.'), 
                            errors='coerce'
                        )
                        df_filtrado = df_filtrado.dropna(subset=['distancia_num'])
                        
                        # Cálculos
                        dias_trabalhados = df_filtrado['data'].dt.date.nunique()
                        total_corridas = len(df_filtrado)
                        km_total = df_filtrado['distancia_num'].sum()
                        
                        # Base e extras
                        base_diaria = 90.0
                        total_base = base_diaria * dias_trabalhados
                        
                        # Calcular extras (simplificado)
                        total_extra = 0
                        for _, pedido in df_filtrado.iterrows():
                            km = pedido['distancia_num']
                            if km > 6:
                                if km <= 8:
                                    total_extra += 2
                                elif km <= 10:
                                    total_extra += 6
                                else:
                                    total_extra += 11
                        
                        total_final = total_base + total_extra
                        
                        # Exibir resultados
                        st.success("✅ Fechamento calculado!")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Dias", dias_trabalhados)
                        with col2:
                            st.metric("Corridas", total_corridas)
                        with col3:
                            st.metric("KM Total", f"{km_total:.1f}")
                        with col4:
                            st.metric("TOTAL", formatar_br(total_final))
                        
                        # Detalhes
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Base Fixa", formatar_br(total_base))
                        with col2:
                            st.metric("Extras", formatar_br(total_extra))
                    else:
                        st.error("Coluna 'Distancia' não encontrada.")
            else:
                st.error("Coluna 'Motoboy' não encontrada.")
    
    # --- CONFIGURAÇÕES ---
    elif menu == "⚙️ Configurações":
        st.title("⚙️ Configurações do Sistema")
        
        st.subheader("📋 Estrutura das Planilhas")
        st.info("""
        **✅ COMPRAS (Configurada):**
        Data Compra, Fornecedor, Categoria, Descrição, Quantidade, Unid, Valor Unit, Valor Total
        
        **✅ PEDIDOS (Configurada):**  
        Código, Data, Nome, Canal, Motoboy, Status, Método de entrega, Total, Distancia
        
        O sistema está configurado para sua estrutura atual!
        """)
        
        # Mostrar estrutura atual
        if not df_pedidos.empty:
            st.write("**Colunas encontradas na planilha PEDIDOS:**")
            st.write(", ".join(df_pedidos.columns.tolist()))
        
        # Configurações
        st.subheader("⚙️ Configurações")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Recarregar Dados"):
                st.cache_data.clear()
                st.success("✅ Cache limpo! Dados serão recarregados.")
                st.rerun()
        
        with col2:
            if st.button("📊 Verificar Conexão"):
                client = conectar_google_sheets()
                if client:
                    st.success("✅ Conexão com Google Sheets OK!")
                else:
                    st.error("❌ Erro na conexão com Google Sheets.")

if __name__ == "__main__":
    main()
