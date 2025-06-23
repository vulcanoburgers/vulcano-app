# VULCANO APP - Atualizado para ponto como separador decimal + Módulo Motoboys

import streamlit as st
import pandas as pd
import datetime
from google.oauth2.service_account import Credentials
import gspread

# --- Configuração Inicial ---
st.set_page_config(page_title="Vulcano App", layout="wide")

# --- Conexão Google Sheets ---
@st.cache_resource(ttl=3600)
def conectar_google_sheets():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(st.secrets, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Erro na conexão com o Google Sheets: {str(e)}")
        st.stop()

# --- Funções Auxiliares ---
def formatar_br(valor, is_quantidade=False):
    try:
        if is_quantidade:
            return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return valor

def converter_valor(valor, *args, **kwargs):
    try:
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return 0.0

# --- Menu Principal ---
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque", "🛵 Fechamento Motos"])

client = conectar_google_sheets()
sheet_compras = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("COMPRAS")
sheet_pedidos = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("PEDIDOS")

# --- Página Motoboys ---
if menu == "🛵 Fechamento Motos":
    st.title("🛵 Fechamento de Motoboys")
    df_pedidos = pd.DataFrame(sheet_pedidos.get_all_records())

    if df_pedidos.empty:
        st.warning("Planilha de pedidos vazia.")
    else:
        df_pedidos['Data'] = pd.to_datetime(df_pedidos['Data'], errors='coerce')
        df_pedidos.dropna(subset=['Data'], inplace=True)

        motoboys_fixos = ["Lucas", "Rafael", "Felipe", "Gabriel"]
        motoboy_selecionado = st.selectbox("Selecione o motoboy:", motoboys_fixos)

        data_inicio = st.date_input("Data início:", value=datetime.date.today() - datetime.timedelta(days=7))
        data_fim = st.date_input("Data fim:", value=datetime.date.today())

        if st.button("🔍 Buscar Fechamento"):
            filtro = (
                (df_pedidos['Motoboy'] == motoboy_selecionado) &
                (df_pedidos['Data'].dt.date >= data_inicio) &
                (df_pedidos['Data'].dt.date <= data_fim)
            )
            df_filtrado = df_pedidos[filtro].copy()

            if df_filtrado.empty:
                st.warning("Nenhum pedido encontrado para o motoboy nesse período.")
            else:
                df_filtrado['KM'] = pd.to_numeric(df_filtrado['KM'], errors='coerce')
                df_filtrado.dropna(subset=['KM'], inplace=True)
                df_filtrado.sort_values('Data', inplace=True)

                dias_trabalhados = df_filtrado['Data'].dt.date.nunique()
                base_diaria = 90 * dias_trabalhados

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

                corridas_dia = df_filtrado.groupby(df_filtrado['Data'].dt.date).size()
                total_extra = 0

                for dia, count in corridas_dia.items():
                    df_dia = df_filtrado[df_filtrado['Data'].dt.date == dia]
                    extras = 0
                    if count <= 8:
                        extras += df_dia['KM'].apply(calcular_taxa_extra).sum()
                    else:
                        excedente = df_dia.iloc[8:]
                        extras += excedente['KM'].apply(calcular_valor_excedente).sum()
                        extras += df_dia.iloc[:8]['KM'].apply(calcular_taxa_extra).sum()
                    total_extra += extras

                total_final = base_diaria + total_extra

                st.metric("Dias trabalhados", dias_trabalhados)
                st.metric("Total fixo (R$)", formatar_br(base_diaria))
                st.metric("Extras por KM (R$)", formatar_br(total_extra))
                st.metric("Total a pagar (R$)", formatar_br(total_final))

                with st.expander("📋 Ver pedidos filtrados"):
                    st.dataframe(df_filtrado[['Data', 'Motoboy', 'KM']])

# --- Outras páginas (Inserir NFC-e, Dashboard, etc) podem seguir abaixo normalmente.
