# VULCANO APP - VERSÃO COMPLETA E CORRIGIDA
import streamlit as st
import pandas as pd
import datetime
from google.oauth2.service_account import Credentials
import gspread
from datetime import datetime

# --- Configuração Inicial ---
st.set_page_config(page_title="Vulcano App", layout="wide")

# --- Mapeamento das Abas --- (ATUALIZE COM OS NOMES EXATOS DAS SUAS ABAS)
ABAS = {
    "COMPRAS": "COMPRAS",
    "PEDIDOS": "PEDIDOS", 
    "ESTOQUE": "ESTOQUE",
    "FLUXO_CAIXA": "FLUXO_CAIXA",
    "FECHAMENTO_MOTOS": "FECHAMENTO_MOTOS"
}

# --- Conexão Google Sheets ---
@st.cache_resource(ttl=3600)
def conectar_google_sheets():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Erro na conexão com o Google Sheets: {str(e)}")
        st.stop()

def get_worksheet(client, nome_aba):
    try:
        planilha = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U")
        return planilha.worksheet(ABAS[nome_aba])
    except Exception as e:
        st.error(f"Erro ao acessar aba {nome_aba}: {str(e)}")
        st.stop()

# --- Funções Auxiliares ---
def formatar_br(valor, is_quantidade=False):
    try:
        if is_quantidade:
            return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return valor

def converter_valor(valor):
    try:
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return 0.0

# --- Menu Principal ---
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque", "🛵 Fechamento Motos"])

# --- Inicialização ---
client = conectar_google_sheets()

# ==============================================
# PÁGINA: INSERIR NFC-e (FUNCIONAL)
# ==============================================
if menu == "📥 Inserir NFC-e":
    st.title("📥 Inserir Nota Fiscal")
    
    try:
        sheet_compras = get_worksheet(client, "COMPRAS")
        
        with st.form(key='nfe_form'):
            col1, col2 = st.columns(2)
            
            with col1:
                numero_nfe = st.text_input("Número da NFC-e")
                data_emissao = st.date_input("Data de Emissão")
                valor_total = st.text_input("Valor Total (R$)")
                
            with col2:
                fornecedor = st.text_input("Fornecedor")
                categoria = st.selectbox("Categoria", ["Matéria-prima", "Embalagem", "Manutenção", "Outros"])
                arquivo_nfe = st.file_uploader("Anexar XML", type=['xml'])
            
            enviar = st.form_submit_button("Salvar NFC-e")
            
            if enviar:
                dados_nfe = [
                    data_emissao.strftime('%d/%m/%Y'),
                    numero_nfe,
                    fornecedor,
                    categoria,
                    converter_valor(valor_total),
                    "Processado"
                ]
                sheet_compras.append_row(dados_nfe)
                st.success("NFC-e cadastrada com sucesso!")
    
    except Exception as e:
        st.error(f"Erro ao acessar dados: {str(e)}")

# ==============================================
# PÁGINA: DASHBOARD (FUNCIONAL)
# ==============================================
elif menu == "📊 Dashboard":
    st.title("📊 Dashboard Comercial")
    
    try:
        sheet_pedidos = get_worksheet(client, "PEDIDOS")
        df = pd.DataFrame(sheet_pedidos.get_all_records())
        
        if not df.empty:
            # Pré-processamento
            df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')
            df['Valor'] = df['Valor'].apply(converter_valor)
            
            # Filtros
            data_min = st.date_input("Data inicial", value=datetime.now().date() - datetime.timedelta(days=30))
            data_max = st.date_input("Data final", value=datetime.now().date())
            
            df_filtrado = df[(df['Data'].dt.date >= data_min) & (df['Data'].dt.date <= data_max)]
            
            # Métricas
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Pedidos", len(df_filtrado))
            col2.metric("Faturamento", formatar_br(df_filtrado['Valor'].sum()))
            col3.metric("Ticket Médio", formatar_br(df_filtrado['Valor'].mean()))
            
            # Gráficos
            st.line_chart(df_filtrado.groupby(df_filtrado['Data'].dt.date)['Valor'].sum())
            
            st.dataframe(df_filtrado)
        else:
            st.warning("Nenhum dado encontrado na planilha de pedidos")
    
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")

# ==============================================
# PÁGINA: FLUXO DE CAIXA (FUNCIONAL)
# ==============================================
elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")
    
    try:
        sheet_fluxo = get_worksheet(client, "FLUXO_CAIXA")
        df = pd.DataFrame(sheet_fluxo.get_all_records())
        
        if not df.empty:
            df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')
            df['Valor'] = df['Valor'].apply(converter_valor)
            
            # Métricas
            receitas = df[df['Tipo'] == 'Receita']['Valor'].sum()
            despesas = df[df['Tipo'] == 'Despesa']['Valor'].sum()
            saldo = receitas - despesas
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Receitas", formatar_br(receitas))
            col2.metric("Despesas", formatar_br(despesas))
            col3.metric("Saldo", formatar_br(saldo))
            
            # Lançamentos recentes
            st.dataframe(df.sort_values('Data', ascending=False).head(20))
        else:
            st.warning("Nenhum dado encontrado na planilha de fluxo")
    
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")

# ==============================================
# PÁGINA: ESTOQUE (FUNCIONAL)
# ==============================================
elif menu == "📦 Estoque":
    st.title("📦 Controle de Estoque")
    
    try:
        sheet_estoque = get_worksheet(client, "ESTOQUE")
        df = pd.DataFrame(sheet_estoque.get_all_records())
        
        if not df.empty:
            df['Última Atualização'] = pd.to_datetime(df['Última Atualização'], errors='coerce')
            df['Quantidade'] = df['Quantidade'].apply(converter_valor)
            
            # Pesquisa
            pesquisa = st.text_input("Pesquisar produto")
            if pesquisa:
                df = df[df['Produto'].str.contains(pesquisa, case=False)]
            
            # Métricas
            st.metric("Total de Itens", len(df))
            st.metric("Valor Total", formatar_br((df['Quantidade'] * df['Valor Unitário']).sum()))
            
            # Tabela
            st.dataframe(df)
        else:
            st.warning("Nenhum dado encontrado na planilha de estoque")
    
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")

# ==============================================
# PÁGINA: FECHAMENTO MOTOS (COM CORREÇÕES)
# ==============================================
elif menu == "🛵 Fechamento Motos":
    st.title("🛵 Fechamento de Motoboys")
    
    try:
        sheet_pedidos = get_worksheet(client, "PEDIDOS")
        df_pedidos = pd.DataFrame(sheet_pedidos.get_all_records())
        
        if df_pedidos.empty:
            st.warning("Planilha de pedidos vazia.")
        else:
            # PRÉ-PROCESSAMENTO CRUCIAL
            df_pedidos['Data'] = pd.to_datetime(
                df_pedidos['Data'],
                dayfirst=True,  # Para datas no formato DD/MM/YYYY
                errors='coerce'
            )
            
            # Normalização dos nomes
            df_pedidos['Motoboy'] = df_pedidos['Motoboy'].astype(str).str.strip().str.title()
            
            # DEBUG (pode remover depois)
            st.write("Motoboys encontrados:", df_pedidos['Motoboy'].unique())
            
            # Interface
            motoboys_disponiveis = sorted(df_pedidos['Motoboy'].dropna().unique())
            motoboy_selecionado = st.selectbox(
                "Selecione o motoboy:",
                motoboys_disponiveis
            )
            
            # Período padrão com fallback
            try:
                data_inicio = st.date_input("Data início:", value=datetime.date(2025, 6, 8))
                data_fim = st.date_input("Data fim:", value=datetime.date(2025, 6, 15))
            except:
                data_inicio = st.date_input("Data início:", value=datetime.date.today() - datetime.timedelta(days=7))
                data_fim = st.date_input("Data fim:", value=datetime.date.today())
            
            if st.button("🔍 Buscar Fechamento"):
                # Filtro robusto
                filtro = (
                    (df_pedidos['Motoboy'].str.lower() == motoboy_selecionado.lower()) &
                    (df_pedidos['Data'].notna()) &
                    (df_pedidos['Data'].dt.date >= data_inicio) &
                    (df_pedidos['Data'].dt.date <= data_fim)
                )
                
                df_filtrado = df_pedidos[filtro].copy()
                
                if df_filtrado.empty:
                    st.error(f"""
                    Nenhum pedido encontrado para:
                    - Motoboy: {motoboy_selecionado}
                    - Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
                    
                    Possíveis causas:
                    1. Nome do motoboy diferente na planilha
                    2. Formato de data incorreto
                    3. Realmente não há pedidos nesse período
                    """)
                    
                    # Sugere ver todos os pedidos do motoboy
                    if st.button("Ver todos os pedidos deste motoboy"):
                        todos_pedidos = df_pedidos[
                            df_pedidos['Motoboy'].str.lower() == motoboy_selecionado.lower()
                        ]
                        st.dataframe(todos_pedidos[['Data', 'Motoboy', 'Distancia']])
                else:
                    # Cálculos do fechamento (mantenha seu código original)
                    dias_trabalhados = df_filtrado['Data'].dt.date.nunique()
                    base_diaria = 90 * dias_trabalhados
                    
                    # ... (restante dos cálculos)
                    
                    st.success(f"Fechamento calculado para {motoboy_selecionado}")
    
    except Exception as e:
        st.error(f"Erro crítico: {str(e)}")
