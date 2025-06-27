import streamlit as st
import pandas as pd
import datetime
import numpy as np
from google.oauth2.service_account import Credentials
import gspread
import requests
from bs4 import BeautifulSoup
import re

# Importar plotly apenas se disponível
try:
    import plotly.express as px
    PLOTLY_DISPONIVEL = True
except ImportError:
    PLOTLY_DISPONIVEL = False
    st.warning("⚠️ Plotly não está instalado. Alguns gráficos não serão exibidos.")
    st.info("💡 Para instalar: pip install plotly")

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
    """Converte valor brasileiro para float"""
    try:
        if pd.isna(valor_str) or valor_str == '':
            return 0.0
        valor_clean = re.sub(r'[^0-9,.]', '', str(valor_str))
        valor_clean = valor_clean.replace(',', '.')
        return float(valor_clean) if valor_clean else 0.0
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
        
        df_compras_norm = mapear_colunas(df_compras, 'COMPRAS')
        df_pedidos_norm = mapear_colunas(df_pedidos, 'PEDIDOS')
        
        return df_compras_norm, df_pedidos_norm
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

# --- Funções do Estoque ---
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

def criar_dicionario_alias():
    """Dicionário de alias para normalizar nomes de produtos"""
    return {
        'agua com gas': ['agua com gás', 'agua c/ gas', 'agua fonte da pedra com gas', 'agua crystal com gas', 'agua gasosa'],
        'agua sem gas': ['agua sem gás', 'agua s/ gas', 'agua fonte da pedra sem gas', 'agua crystal sem gas', 'agua natural'],
        'coca cola lata': ['coca cola 350ml', 'coca cola lata 350ml', 'coca-cola lata', 'coca lata'],
        'coca cola zero lata': ['coca zero 350ml', 'coca zero lata 350ml', 'coca-cola zero lata', 'coca zero'],
        'fanta laranja lata': ['fanta laranja 350ml', 'fanta laranja lata 350ml', 'fanta laranja'],
        'fanta uva lata': ['fanta uva 350ml', 'fanta uva lata 350ml', 'fanta uva'],
        'sprite lata': ['sprite 350ml', 'sprite lata 350ml'],
        'budweiser 550ml': ['budweiser 600ml', 'budweiser garrafa', 'budweiser long'],
        'budweiser long neck': ['budweiser 330ml', 'budweiser ln', 'budweiser longneck'],
        'oleo de soja': ['oleo soja', 'óleo de soja', 'óleo soja', 'oleo'],
        'sal': ['sal refinado', 'sal 1kg', 'sal de cozinha'],
        'açucar': ['açúcar', 'açucar cristal', 'açúcar cristal', 'açucar refinado'],
        'arroz': ['arroz branco', 'arroz tipo 1', 'arroz agulhinha'],
        'feijao': ['feijão', 'feijão preto', 'feijao preto'],
        'file de sobrecoxa': ['filé de sobrecoxa', 'sobrecoxa', 'file sobrecoxa'],
        'bife de coxao de dentro': ['bife coxão dentro', 'coxão dentro', 'bife coxao'],
        'queijo cheddar fatiado': ['cheddar fatiado', 'queijo cheddar', 'cheddar'],
        'queijo mussarela fatiado': ['mussarela fatiada', 'queijo mussarela', 'mussarela'],
        'queijo provolone fatiado': ['provolone fatiado', 'queijo provolone', 'provolone'],
        'pao brioche': ['pão brioche', 'brioche', 'pao hamburguer brioche'],
        'pao tradicional com gergelim': ['pão com gergelim', 'pão gergelim', 'pao gergelim'],
        'pao australiano': ['pão australiano', 'australiano'],
        'bisnaga de cheddar': ['cheddar bisnaga', 'molho cheddar', 'cheddar cremoso'],
        'bisnaga de requeijao': ['requeijão bisnaga', 'molho requeijão', 'requeijão cremoso'],
        'barbecue': ['molho barbecue', 'bbq', 'molho bbq'],
        'mostarda rustica': ['mostarda rústica', 'mostarda'],
        'cebola': ['cebola branca', 'cebola amarela'],
        'tomate': ['tomate maduro', 'tomate vermelho'],
        'alface': ['alface americana', 'alface lisa'],
        'couve': ['couve folha', 'couve manteiga']
    }

def normalizar_nome_produto(nome_entrada):
    """Normaliza o nome do produto usando o dicionário de alias"""
    if not nome_entrada:
        return nome_entrada
    
    nome_lower = str(nome_entrada).lower().strip()
    alias_dict = criar_dicionario_alias()
    
    for produto_base, aliases in alias_dict.items():
        if nome_lower == produto_base:
            return produto_base
        if nome_lower in aliases:
            return produto_base
    
    for produto_base, aliases in alias_dict.items():
        for alias in aliases:
            if alias in nome_lower or nome_lower in alias:
                return produto_base
        
        if produto_base in nome_lower or nome_lower in produto_base:
            return produto_base
    
    return nome_entrada

def determinar_status_estoque(row):
    """Determina o status do estoque de um produto"""
    try:
        em_estoque = limpar_numero(row.get('Em estoque', 0))
        estoque_min = limpar_numero(row.get('Estoque Min', 0))
        
        if em_estoque == 0:
            return "🔴 Em Falta"
        elif em_estoque < estoque_min:
            return "🟡 Baixo"
        else:
            return "🟢 OK"
    except:
        return "❓ Indefinido"

def pagina_estoque():
    """Página de gestão de estoque"""
    
    st.title("📦 Gestão de Estoque")
    
    df_insumos = carregar_dados_insumos()
    
    if df_insumos.empty:
        st.warning("⚠️ Não foi possível carregar os dados da aba INSUMOS")
        st.info("💡 Verifique se a aba 'INSUMOS' existe na sua planilha")
        return
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Dashboard", 
        "📋 Lista de Produtos", 
        "📥 Entrada de Produtos",
        "📈 Análise de Custos",
        "⚙️ Configurações"
    ])
    
    with tab1:
        dashboard_estoque(df_insumos)
    
    with tab2:
        lista_produtos_estoque(df_insumos)
    
    with tab3:
        entrada_produtos_estoque()
    
    with tab4:
        analise_custos_estoque(df_insumos)
    
    with tab5:
        configuracoes_estoque()

def dashboard_estoque(df_insumos):
    """Dashboard do estoque"""
    
    st.subheader("📊 Visão Geral do Estoque")
    
    df_work = df_insumos.copy()
    df_work['Em estoque'] = df_work.get('Em estoque', 0).apply(limpar_numero)
    df_work['Estoque Min'] = df_work.get('Estoque Min', 0).apply(limpar_numero)
    df_work['Preço (un)'] = df_work.get('Preço (un)', 0).apply(limpar_numero)
    
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
    
    df_work = df_insumos.copy()
    df_work['Em estoque'] = df_work.get('Em estoque', 0).apply(limpar_numero)
    df_work['Estoque Min'] = df_work.get('Estoque Min', 0).apply(limpar_numero)
    df_work['Preço (un)'] = df_work.get('Preço (un)', 0).apply(limpar_numero)
    
    df_work['Status'] = df_work.apply(determinar_status_estoque, axis=1)
    df_work['Valor Total'] = df_work['Em estoque'] * df_work['Preço (un)']
    
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
    
    df_filtrado = df_work.copy()
    
    if categoria_filtro != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['Categoria'] == categoria_filtro]
    
    if status_filtro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Status'] == status_filtro]
    
    if busca:
        mask = df_filtrado['Produto'].str.contains(busca, case=False, na=False)
        df_filtrado = df_filtrado[mask]
    
    valor_filtrado = df_filtrado['Valor Total'].sum()
    st.info(f"📊 Mostrando {len(df_filtrado)} de {len(df_work)} produtos | Valor: {formatar_br(valor_filtrado)}")
    
    colunas_exibir = ['Produto', 'Categoria', 'Em estoque', 'Estoque Min', 'Preço (un)', 'Valor Total', 'Status', 'Fornecedor']
    colunas_disponiveis = [col for col in colunas_exibir if col in df_filtrado.columns]
    
    if len(df_filtrado) > 0:
        df_display = df_filtrado[colunas_disponiveis].copy()
        
        df_editado = st.data_editor(
            df_display,
            column_config={
                "Produto": st.column_config.TextColumn("Produto", width="medium"),
                "Categoria": st.column_config.TextColumn("Categoria", width="small"),
                "Em estoque": st.column_config.NumberColumn(
                    "Em Estoque",
                    help="Quantidade atual em estoque",
                    min_value=0,
                    step=1,
                    format="%.1f"
                ),
                "Estoque Min": st.column_config.NumberColumn(
                    "Estoque Mínimo", 
                    help="Quantidade mínima recomendada",
                    min_value=0,
                    step=1,
                    format="%.0f"
                ),
                "Preço (un)": st.column_config.NumberColumn(
                    "Preço (R$)",
                    help="Preço unitário",
                    min_value=0.0,
                    step=0.01,
                    format="R$ %.2f"
                ),
                "Valor Total": st.column_config.NumberColumn(
                    "Valor Total",
                    help="Em estoque × Preço",
                    format="R$ %.2f"
                ),
                "Status": st.column_config.TextColumn("Status", width="small"),
                "Fornecedor": st.column_config.TextColumn("Fornecedor", width="small")
            },
            hide_index=True,
            use_container_width=True
        )
        
        if st.button("💾 Salvar Alterações"):
            st.success("✅ Funcionalidade de salvamento será implementada na próxima versão!")
            st.info("💡 Por enquanto, edite diretamente no Google Sheets")
    
    else:
        st.warning("⚠️ Nenhum produto encontrado com os filtros aplicados")

def entrada_produtos_estoque():
    """Entrada de produtos via NFCe, CSV ou manual"""
    
    st.subheader("📥 Entrada de Produtos")
    
    st.info("💡 Aqui você pode registrar a entrada de novos produtos no estoque")
    
    tab1, tab2, tab3 = st.tabs(["🔗 Via NFCe (URL)", "📄 Via CSV/Excel", "✍️ Entrada Manual"])
    
    with tab1:
        st.subheader("Importar via URL da NFC-e")
        st.write("Cole a URL da nota fiscal eletrônica para importar automaticamente os produtos")
        
        url_nfce = st.text_input("Cole a URL da NFC-e aqui:")
        
        if st.button("🔍 Extrair Dados da NFCe") and url_nfce:
            with st.spinner("Processando NFC-e..."):
                try:
                    response = requests.get(url_nfce)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    df_itens = extrair_itens_nfce(soup)
                    
                    if not df_itens.empty:
                        st.success("✅ Dados extraídos com sucesso!")
                        st.subheader("📦 Produtos encontrados:")
                        
                        df_itens['Produto_Normalizado'] = df_itens['Descrição'].apply(normalizar_nome_produto)
                        
                        st.write("**🔗 Normalização de Produtos:**")
                        df_comparacao = df_itens[['Descrição', 'Produto_Normalizado']].copy()
                        df_comparacao.columns = ['Nome na NFCe', 'Produto no Sistema']
                        
                        df_comparacao['Status'] = df_comparacao.apply(
                            lambda row: '✅ Normalizado' if row['Nome na NFCe'].lower() != row['Produto no Sistema'].lower() else '📝 Mantido',
                            axis=1
                        )
                        
                        st.dataframe(df_comparacao, use_container_width=True)
                        
                        st.subheader("📊 Dados processados:")
                        st.dataframe(df_itens, use_container_width=True)
                        
                        if st.button("💾 Salvar no Estoque"):
                            st.success("✅ Funcionalidade de salvamento será implementada!")
                            st.info("💡 Os produtos normalizados serão adicionados ao estoque teórico")
                            
                            st.write("**📋 Resumo do que seria salvo:**")
                            for _, item in df_itens.iterrows():
                                st.write(f"• **{item['Produto_Normalizado']}**: {item['Quantidade']} {item['Unidade']} - R$ {item['Valor Unitário']:.2f}")
                    else:
                        st.error("❌ Não foi possível extrair os dados. Verifique a URL.")
                        st.info("💡 Certifique-se que a URL é de uma NFCe válida")
                except Exception as e:
                    st.error(f"❌ Erro ao processar: {str(e)}")
    
    with tab2:
        st.subheader("Upload de arquivo CSV/Excel")
        st.write("Faça upload de um arquivo com os dados dos produtos comprados")
        
        arquivo = st.file_uploader(
            "Selecione o arquivo", 
            type=['csv', 'xlsx', 'xls'],
            help="Formatos aceitos: CSV, Excel (.xlsx, .xls)"
        )
        
        if arquivo:
            try:
                if arquivo.name.endswith('.csv'):
                    df_upload = pd.read_csv(arquivo)
                else:
                    df_upload = pd.read_excel(arquivo)
                
                st.success("✅ Arquivo carregado com sucesso!")
                st.subheader("📊 Dados do arquivo:")
                st.dataframe(df_upload, use_container_width=True)
                
                st.subheader("🔗 Mapeamento de Colunas")
                st.write("Associe as colunas do seu arquivo com os campos do sistema:")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    coluna_produto = st.selectbox("Produto/Descrição:", df_upload.columns)
                    coluna_quantidade = st.selectbox("Quantidade:", df_upload.columns)
                    coluna_preco = st.selectbox("Preço Unitário:", df_upload.columns)
                
                with col2:
                    coluna_fornecedor = st.selectbox("Fornecedor:", [""] + list(df_upload.columns))
                    coluna_categoria = st.selectbox("Categoria:", [""] + list(df_upload.columns))
                    coluna_unidade = st.selectbox("Unidade:", [""] + list(df_upload.columns))
                
                if st.button("💾 Processar e Salvar"):
                    produtos_normalizados = df_upload[coluna_produto].apply(normalizar_nome_produto)
                    
                    st.success("✅ Dados processados!")
                    
                    st.subheader("🔗 Produtos Normalizados:")
                    df_norm = pd.DataFrame({
                        'Nome Original': df_upload[coluna_produto],
                        'Nome Normalizado': produtos_normalizados,
                        'Quantidade': df_upload[coluna_quantidade],
                        'Preço': df_upload[coluna_preco]
                    })
                    
                    df_norm['Status'] = df_norm.apply(
                        lambda row: '✅ Normalizado' if row['Nome Original'].lower() != row['Nome Normalizado'].lower() else '📝 Mantido',
                        axis=1
                    )
                    
                    st.dataframe(df_norm, use_container_width=True)
                    st.info("💡 Os produtos normalizados serão adicionados ao estoque")
                    
            except Exception as e:
                st.error(f"❌ Erro ao processar arquivo: {str(e)}")
    
    with tab3:
        st.subheader("✍️ Entrada Manual de Produtos")
        st.write("Adicione produtos manualmente ao estoque")
        
        with st.form("entrada_manual"):
            col1, col2 = st.columns(2)
            
            with col1:
                produto_nome = st.text_input("Nome do Produto*", placeholder="Ex: Coca Cola Lata 350ml")
                quantidade = st.number_input("Quantidade*", min_value=0.0, step=1.0, value=1.0)
                preco_unitario = st.number_input("Preço Unitário (R$)*", min_value=0.0, step=0.01, value=0.0)
            
            with col2:
                fornecedor = st.text_input("Fornecedor", placeholder="Ex: Coca Cola")
                categoria = st.selectbox("Categoria", ["Bebidas", "Insumos", "Higiene e Limp", "Embalagens"])
                unidade = st.selectbox("Unidade", ["un", "kg", "g", "l", "ml", "pc"])
            
            observacoes = st.text_area("Observações", placeholder="Informações adicionais sobre a compra...")
            
            submitted = st.form_submit_button("➕ Adicionar ao Estoque")
            
            if submitted:
                if produto_nome and quantidade > 0 and preco_unitario > 0:
                    st.success(f"✅ Produto '{produto_nome}' adicionado ao estoque!")
                    st.info("💡 O produto será registrado na planilha INSUMOS")
                    
                    st.markdown("**📋 Resumo da Entrada:**")
                    st.write(f"• **Produto:** {produto_nome}")
                    st.write(f"• **Quantidade:** {quantidade} {unidade}")
                    st.write(f"• **Preço:** {formatar_br(preco_unitario)}")
                    st.write(f"• **Valor Total:** {formatar_br(quantidade * preco_unitario)}")
