
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

st.title("Controle de Despesas - Vulcano Burgers")

# Autenticação Google Sheets
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive.file",
         "https://www.googleapis.com/auth/drive"]

# Substitua pelo caminho do seu arquivo JSON com credenciais da API Google
# Exemplo: 'vulcano-credentials.json'
# Para funcionar, você precisará criar esse arquivo e carregar no mesmo diretório
# do app, com acesso à planilha compartilhada.
credentials = Credentials.from_service_account_file('vulcano-credentials.json', scopes=scope)
gc = gspread.authorize(credentials)

SHEET_URL = "https://docs.google.com/spreadsheets/d/1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U/edit?usp=sharing"
sheet = gc.open_by_url(SHEET_URL)
worksheet = sheet.sheet1

data = worksheet.get_all_records()
df = pd.DataFrame(data)

st.write("Dados da Planilha de Despesas")
st.dataframe(df)

# Formulário simples para adicionar nova despesa
st.header("Registrar nova despesa")
with st.form("form_despesa"):
    data_compra = st.date_input("Data Compra")
    descricao = st.text_input("Descrição")
    categoria = st.text_input("Categoria")
    sub_categoria = st.text_input("Sub-Categoria")
    forma_pagamento = st.text_input("Forma de Pagamento")
    valor = st.number_input("Valor", min_value=0.0, format="%.2f")
    data_pagamento = st.date_input("Data Pagamento")
    submit = st.form_submit_button("Registrar")

if submit:
    nova_linha = [data_compra.strftime("%Y-%m-%d"), descricao, categoria, sub_categoria, forma_pagamento, valor, data_pagamento.strftime("%Y-%m-%d")]
    worksheet.append_row(nova_linha)
    st.success("Despesa registrada com sucesso!")
    st.experimental_rerun()
