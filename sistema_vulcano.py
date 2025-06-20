import streamlit as st
import pandas as pd
import gspread
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import re

st.title("Controle de Despesas - Vulcano Burgers (Cloud 🌩️)")

# Autenticação com Google Sheets
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

credentials = Credentials.from_service_account_info(
    st.secrets["GOOGLE_CREDENTIALS"], scopes=scope
)
gc = gspread.authorize(credentials)
sheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U/edit?usp=sharing")
ws = sheet.sheet1

# Função para puxar HTML da NFC-e
def get_nfe_html(url_qr):
    try:
        resp = requests.get(url_qr, timeout=10)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        st.error(f"Erro ao acessar a URL da NFC-e: {e}")
        return None

# Função para extrair itens do HTML da nota
def parse_nfe_html(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    pattern = re.compile(
        r'(.+?) \(Código: (\d+)\)\s*Qtde\.:([\d\.,]+)(UN|KG):.*?Unit\.: *([\d\.,]+).*?Total *([\d\.,]+)',
        re.MULTILINE
    )
    produtos = []
    for m in pattern.finditer(text):
        nome, cod, qtd, un, unit, tot = m.groups()
        produtos.append({
            "Produto": nome.strip(),
            "Código": cod,
            "Quantidade": float(qtd.replace(",", ".")),
            "Unidade": un,
            "Preço Unit.": float(unit.replace(",", ".")),
            "Preço Total": float(tot.replace(",", "."))
        })
    return pd.DataFrame(produtos)

# Interface com abas
tabs = st.tabs(["Despesas Manuais", "Importar NFC-e (via URL QR)"])

# Aba 1 - Despesas Manuais
with tabs[0]:
    st.header("Despesas registradas")
    df = pd.DataFrame(ws.get_all_records())
    st.dataframe(df)

    st.subheader("Registrar manualmente")
    with st.form("form_despesa"):
        d_compra = st.date_input("Data Compra")
        desc = st.text_input("Descrição")
        cat = st.text_input("Categoria")
        subcat = st.text_input("Sub-Categoria")
        forma = st.text_input("Forma de Pagamento")
        val = st.number_input("Valor", min_value=0.0, format="%.2f")
        d_pag = st.date_input("Data Pagamento")
        if st.form_submit_button("Registrar"):
            ws.append_row([
                d_compra.strftime("%Y-%m-%d"),
                desc,
                cat,
                subcat,
                forma,
                val,
                d_pag.strftime("%Y-%m-%d")
            ])
            st.success("Despesa registrada!")
            st.experimental_rerun()

# Aba 2 - Importar NFC-e
with tabs[1]:
    st.header("Importar dados da NFC-e")
    url_qr = st.text_input("Cole aqui a URL completa do QR Code")
    if st.button("Buscar e importar"):
        if url_qr:
            html = get_nfe_html(url_qr)
            if html:
                df_nfe = parse_nfe_html(html)
                if not df_nfe.empty:
                    st.dataframe(df_nfe)
                    for _, r in df_nfe.iterrows():
                        ws.append_row([
                            pd.Timestamp.now().strftime("%Y-%m-%d"),
                            r["Produto"],
                            "Insumos",
                            "",
                            "NFC-e Importada",
                            r["Preço Total"],
                            pd.Timestamp.now().strftime("%Y-%m-%d")
                        ])
                    st.success(f"{len(df_nfe)} produtos importados com sucesso!")
                else:
                    st.warning("Não encontrei produtos na nota. Pode ser que o layout tenha mudado.")
        else:
            st.warning("Insira a URL do QR Code da NFC-e.")
