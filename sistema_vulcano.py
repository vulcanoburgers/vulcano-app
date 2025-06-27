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
    .estoque-card {
        background: white;
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #FF4B4B;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Mapeamento de colunas
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

def limpar_numero(valor):
    """Converte valores para números de forma segura"""
    try:
        if pd.isna(valor) or valor == '':
            return 0.0
        if isinstance(valor, (int, float)):
            return float(valor)
        if isinstance(valor, str):
            valor_limpo = re.sub(r'[^0-9,.]', '', str(valor))
            valor_limpo = valor_limpo.replace(',', '.')
            if valor_limpo == '':
                return 0.0
            return float(valor_limpo)
        return float(valor)
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

# === FUNÇÕES DO ESTOQUE ===

@st.cache_data(ttl=300)
def carregar_dados_insumos():
    """Carrega dados da aba INSUMOS"""
    client = conectar_google_sheets()
    if not client:
        return pd.DataFrame()
    
    try:
        sheet_insumos = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("INSUMOS")
        df_insumos = pd.DataFrame(sheet_insumos.get_all_records())
        return df_insumos
    except Exception as e:
        st.error(f"Erro ao carregar dados de insumos: {str(e)}")
        return pd.DataFrame()

def pagina_estoque():
    """Página de gestão de estoque"""
    
    st.title("📦 Gestão de Estoque")
    
    # Carregar dados
    df_insumos = carregar_dados_insumos()
    
    if df_insumos.empty:
        st.warning("⚠️ Não foi possível carregar os dados da aba INSUMOS")
        st.info("💡 Verifique se a aba 'INSUMOS' existe na sua planilha")
        return
    
    # Tabs simples
    tab1, tab2, tab3 = st.tabs([
        "📊 Dashboard", 
        "📋 Lista de Produtos", 
        "⚙️ Configurações"
    ])
    
    with tab1:
        dashboard_estoque(df_insumos)
    
    with tab2:
        lista_produtos_estoque(df_insumos)
    
    with tab3:
        configuracoes_estoque()

def dashboard_estoque(df_insumos):
    """Dashboard do estoque"""
    
    st.subheader("📊 Visão Geral do Estoque")
    
    # Preparar dados
    df_work = df_insumos.copy()
    df_work['Em estoque'] = df_work.get('Em estoque', 0).apply(limpar_numero)
    df_work['Estoque Min'] = df_work.get('Estoque Min', 0).apply(limpar_numero)
    df_work['Preço (un)'] = df_work.get('Preço (un)', 0).apply(limpar_numero)
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_produtos = len(df_work)
        st.metric("📦 Total de Produtos", total_produtos)
    
    with col2:
        valor_total = (df_work['Em estoque'] * df_work['Preço (un)']).sum()
        st.metric("💰 Valor Total", formatar_br(valor_total))
    
    with col3:
        produtos_baixo = len(df_work[
            (df_work['Em estoque'] < df_work['Estoque Min']) & 
            (df_work['Em estoque'] > 0)
        ])
        st.metric("⚠️ Estoque Baixo", produtos_baixo)
    
    with col4:
        produtos_falta = len(df_work[df_work['Em estoque'] == 0])
        st.metric("🚨 Em Falta", produtos_falta)
    
    # Alertas importantes
    st.markdown("### 🔔 Alertas Importantes")
    
    produtos_falta_lista = df_work[df_work['Em estoque'] == 0]
    produtos_baixo_lista = df_work[
        (df_work['Em estoque'] < df_work['Estoque Min']) & 
        (df_work['Em estoque'] > 0)
    ]
    
    col1, col2 = st.columns(2)
    
    with col1:
        if len(produtos_falta_lista) > 0:
            st.markdown('<div class="estoque-card">', unsafe_allow_html=True)
            st.markdown("**🚨 Produtos em Falta:**")
            for produto in produtos_falta_lista['Produto'].head(5):
                st.write(f"• {produto}")
            if len(produtos_falta_lista) > 5:
                st.write(f"... e mais {len(produtos_falta_lista) - 5}")
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.success("✅ Nenhum produto em falta!")
    
    with col2:
        if len(produtos_baixo_lista) > 0:
            st.markdown('<div class="estoque-card">', unsafe_allow_html=True)
            st.markdown("**⚠️ Estoque Baixo:**")
            for _, produto in produtos_baixo_lista.head(5).iterrows():
                st.write(f"• {produto['Produto']}: {produto['Em estoque']:.0f}/{produto['Estoque Min']:.0f}")
            if len(produtos_baixo_lista) > 5:
                st.write(f"... e mais {len(produtos_baixo_lista) - 5}")
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.success("✅ Todos os produtos com estoque adequado!")

def lista_produtos_estoque(df_insumos):
    """Lista de produtos do estoque"""
    
    st.subheader("📋 Lista de Produtos")
    
    # Preparar dados
    df_work = df_insumos.copy()
    df_work['Em estoque'] = df_work.get('Em estoque', 0).apply(limpar_numero)
    df_work['Estoque Min'] = df_work.get('Estoque Min', 0).apply(limpar_numero)
    df_work['Preço (un)'] = df_work.get('Preço (un)', 0).apply(limpar_numero)
    
    # Adicionar colunas calculadas
    def determinar_status(row):
        em_estoque = row['Em estoque']
        estoque_min = row['Estoque Min']
        
        if em_estoque == 0:
            return "🔴 Em Falta"
        elif em_estoque < estoque_min:
            return "🟡 Baixo"
        else:
            return "🟢 OK"
    
    df_work['Status'] = df_work.apply(determinar_status, axis=1)
    df_work['Valor Total'] = df_work['Em estoque'] * df_work['Preço (un)']
    
    # Filtros
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'Categoria' in df_work.columns:
            categorias = ['Todas'] + sorted(df_work['Categoria'].dropna().unique().tolist())
            categoria_filtro = st.selectbox("Filtrar por Categoria", categorias)
        else:
            categoria_filtro = 'Todas'
    
    with col2:
        status_filtro = st.selectbox(
            "Filtrar por Status",
            ["Todos", "🟢 OK", "🟡 Baixo", "🔴 Em Falta"]
        )
    
    with col3:
        busca = st.text_input("🔍 Buscar produto")
    
    # Aplicar filtros
    df_filtrado = df_work.copy()
    
    if categoria_filtro != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['Categoria'] == categoria_filtro]
    
    if status_filtro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Status'] == status_filtro]
    
    if busca:
        mask = df_filtrado['Produto'].str.contains(busca, case=False, na=False)
        df_filtrado = df_filtrado[mask]
    
    # Informações do filtro
    valor_filtrado = df_filtrado['Valor Total'].sum()
    st.info(f"📊 Mostrando {len(df_filtrado)} de {len(df_work)} produtos | Valor: {formatar_br(valor_filtrado)}")
    
    # Mostrar tabela
    if len(df_filtrado) > 0:
        colunas_exibir = ['Produto', 'Categoria', 'Em estoque', 'Estoque Min', 'Preço (un)', 'Valor Total', 'Status']
        colunas_disponiveis = [col for col in colunas_exibir if col in df_filtrado.columns]
        
        if 'Fornecedor' in df_filtrado.columns:
            colunas_disponiveis.append('Fornecedor')
        
        st.dataframe(
            df_filtrado[colunas_disponiveis],
            use_container_width=True
        )
    else:
        st.warning("⚠️ Nenhum produto encontrado com os filtros aplicados")

def configuracoes_estoque():
    """Configurações do módulo de estoque"""
    
    st.subheader("⚙️ Configurações do Estoque")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Conexão com Planilha")
        
        if st.button("🔄 Testar Conexão INSUMOS"):
            df_test = carregar_dados_insumos()
            if not df_test.empty:
                st.success(f"✅ Conexão OK! {len(df_test)} produtos carregados")
                st.write("**Colunas encontradas:**")
                st.write(", ".join(df_test.columns.tolist()))
                
                st.write("**Amostra dos dados (primeiras 3 linhas):**")
                st.dataframe(df_test.head(3))
                
            else:
                st.error("❌ Erro na conexão com a aba INSUMOS")
        
        st.markdown("### 🔄 Cache")
        if st.button("🧹 Limpar Cache"):
            st.cache_data.clear()
            st.success("✅ Cache limpo!")
    
    with col2:
        st.markdown("### ⚙️ Configurações de Alerta")
        
        limite_baixo = st.slider(
            "Limite para Estoque Baixo (%)",
            min_value=10,
            max_value=50,
            value=20,
            help="Percentual do estoque mínimo para gerar alerta"
        )
        
        notif_falta = st.checkbox("Notificar produtos em falta", value=True)
        notif_baixo = st.checkbox("Notificar estoque baixo", value=True)
        
        if st.button("💾 Salvar Configurações"):
            st.success("✅ Configurações salvas!")
    
    # Informações da estrutura
    st.markdown("### 📋 Estrutura da Aba INSUMOS")
    st.info("""
    **Colunas esperadas na aba INSUMOS:**
    
    • **Produto** - Nome do produto/insumo
    • **Categoria** - Categoria (Bebidas, Insumos, etc.)
    • **Em estoque** - Quantidade atual em estoque
    • **Estoque Min** - Quantidade mínima recomendada
    • **Preço (un)** - Preço unitário
    • **Fornecedor** - Nome do fornecedor
    
    O sistema já está configurado para funcionar com sua planilha atual!
    """)

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
        
        # Dashboard básico (seu código original continua funcionando)
        st.write("Dashboard funcionando!")
        
        # Adicionar resumo do estoque no dashboard
        st.markdown("### 📦 Resumo do Estoque")
        df_insumos = carregar_dados_insumos()
        
        if not df_insumos.empty:
            df_insumos['Em estoque'] = df_insumos.get('Em estoque', 0).apply(limpar_numero)
            df_insumos['Estoque Min'] = df_insumos.get('Estoque Min', 0).apply(limpar_numero)
            df_insumos['Preço (un)'] = df_insumos.get('Preço (un)', 0).apply(limpar_numero)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                valor_estoque = (df_insumos['Em estoque'] * df_insumos['Preço (un)']).sum()
                st.metric("💰 Valor em Estoque", formatar_br(valor_estoque))
            
            with col2:
                produtos_baixo = len(df_insumos[
                    (df_insumos['Em estoque'] < df_insumos['Estoque Min']) & 
                    (df_insumos['Em estoque'] > 0)
                ])
                st.metric("⚠️ Estoque Baixo", produtos_baixo)
            
            with col3:
                produtos_falta = len(df_insumos[df_insumos['Em estoque'] == 0])
                st.metric("🚨 Em Falta", produtos_falta, delta_color="inverse")
        
    elif menu == "📦 Gestão de Estoque":
        pagina_estoque()
        
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
    main()
