# VULCANO APP - VERSÃO DEFINITIVA PARA MOTOBOYS
import streamlit as st
import pandas as pd
import datetime
from google.oauth2.service_account import Credentials
import gspread

# --- Configuração Inicial ---
st.set_page_config(page_title="Vulcano App - Motoboys", layout="wide")

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
        st.error(f"Erro na conexão: {str(e)}")
        st.stop()

# --- Página Motoboys ---
def pagina_motoboys():
    st.title("🛵 Fechamento de Motoboys")
    
    try:
        # Conexão com a planilha
        client = conectar_google_sheets()
        planilha = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U")
        sheet_pedidos = planilha.worksheet("PEDIDOS")
        
        # Debug: Verificar abas disponíveis
        st.sidebar.write("Abas disponíveis:")
        for aba in planilha.worksheets():
            st.sidebar.write(f"- {aba.title}")
        
        # Carregar dados
        dados = sheet_pedidos.get_all_records()
        df = pd.DataFrame(dados)
        
        if df.empty:
            st.warning("Planilha de pedidos vazia!")
            return
            
        # Pré-processamento CRUCIAL
        df['Motoboy'] = df['Motoboy'].astype(str).str.strip().str.title()
        df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')
        
        # Debug: Mostrar amostra dos dados
        with st.expander("🔍 Ver dados brutos (amostra)"):
            st.write(df.head(3))
            st.write(f"Motoboys encontrados: {df['Motoboy'].unique()}")
            st.write(f"Range de datas: {df['Data'].min()} até {df['Data'].max()}")
        
        # Interface
        motoboys_disponiveis = sorted(df['Motoboy'].dropna().unique())
        motoboy_selecionado = st.selectbox("Selecione o motoboy:", motoboys_disponiveis)
        
        # Datas com fallback seguro
        try:
            data_inicio = st.date_input("Data início:", value=datetime.date(2025, 6, 1))
            data_fim = st.date_input("Data fim:", value=datetime.date(2025, 6, 8))
        except:
            data_inicio = st.date_input("Data início:", value=datetime.date.today() - datetime.timedelta(days=7))
            data_fim = st.date_input("Data fim:", value=datetime.date.today())
        
        if st.button("🔍 Buscar Fechamento", type="primary"):
            # Filtro robusto
            mask = (
                (df['Motoboy'].str.lower() == motoboy_selecionado.lower()) &
                (df['Data'].notna()) &
                (df['Data'].dt.date >= data_inicio) & 
                (df['Data'].dt.date <= data_fim)
            )
            
            df_filtrado = df[mask].copy()
            
            if df_filtrado.empty:
                st.error(f"""
                ⚠️ Nenhum pedido encontrado para:
                - Motoboy: **{motoboy_selecionado}**
                - Período: **{data_inicio.strftime('%d/%m/%Y')}** a **{data_fim.strftime('%d/%m/%Y')}**
                
                🔍 **Possíveis causas:**
                1. O nome **"{motoboy_selecionado}"** está diferente na planilha
                2. As datas estão em formato incorreto na planilha
                3. Realmente não há pedidos nesse período
                """)
                
                # Sugere ver todos os pedidos do motoboy
                if st.button("👀 Ver TODOS os pedidos deste motoboy"):
                    todos_pedidos = df[df['Motoboy'].str.lower() == motoboy_selecionado.lower()]
                    st.dataframe(
                        todos_pedidos[['Data', 'Motoboy', 'Distancia']].sort_values('Data'),
                        column_config={
                            "Data": st.column_config.DatetimeColumn(format="DD/MM/YYYY"),
                            "Distancia": "Distância (km)"
                        }
                    )
            else:
                # Cálculos do fechamento
                dias_trabalhados = df_filtrado['Data'].dt.date.nunique()
                base_diaria = 90 * dias_trabalhados
                
                # Cálculo de extras (exemplo)
                df_filtrado['Distancia'] = pd.to_numeric(df_filtrado['Distancia'], errors='coerce')
                total_extra = df_filtrado['Distancia'].sum() * 1.5  # Exemplo: R$1,50 por km
                
                total_pagar = base_diaria + total_extra
                
                # Exibir resultados
                st.success(f"✅ Fechamento calculado para {motoboy_selecionado}")
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Dias trabalhados", dias_trabalhados)
                col2.metric("Valor Fixo", f"R$ {base_diaria:,.2f}")
                col3.metric("Total a Pagar", f"R$ {total_pagar:,.2f}")
                
                st.dataframe(
                    df_filtrado[['Data', 'Motoboy', 'Distancia']].sort_values('Data'),
                    column_config={
                        "Data": st.column_config.DatetimeColumn(format="DD/MM/YYYY"),
                        "Distancia": "Distância (km)"
                    },
                    hide_index=True
                )
    
    except Exception as e:
        st.error(f"ERRO CRÍTICO: {str(e)}")
        st.stop()

# --- Executar Página ---
pagina_motoboys()
