# VULCANO APP - Versão Atualizada (Correções + Novos Recursos)
import streamlit as st
import pandas as pd
import datetime
import locale
import re
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread

# --- Configurações Iniciais ---
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    locale.setlocale(locale.LC_ALL, '')

# --- Conexão Google Sheets ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_info(st.secrets, scopes=scope)
client = gspread.authorize(credentials)
sheet_url = "https://docs.google.com/spreadsheets/d/1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U/edit#gid=0"
sheet = client.open_by_url(sheet_url).sheet1

# --- Parser NFC-e Atualizado ---
def extrair_itens_por_texto(soup):
    tabela = soup.find("table", {"id": "tabResult"})
    if not tabela:
        return pd.DataFrame()

    linhas = tabela.find_all("tr")
    dados = []
    
    for linha in linhas:
        texto = linha.get_text(" ", strip=True)
        
        if all(k in texto for k in ["Código:", "Qtde.:", "Vl. Unit.:"]):
            try:
                # Extração dos dados
                nome = texto.split("(Código:")[0].strip()
                codigo = re.search(r"Código:\s*(\d+)", texto).group(1)
                
                # Tratamento de valores (agora corrigido)
                qtd = re.search(r"Qtde\.?:\s*([\d.,]+)", texto).group(1)
                qtd = float(qtd.replace('.', '').replace(',', '.'))
                
                unitario = re.search(r"Vl\. Unit\.?:\s*([\d.,]+)", texto).group(1)
                unitario = float(unitario.replace('.', '').replace(',', '.'))
                
                total = re.search(r"Vl\. Total\s*([\d.,]+)", texto)
                total = float(total.group(1).replace('.', '').replace(',', '.')) if total else qtd * unitario
                
                unidade = re.search(r"UN:\s*(\w+)", texto).group(1)
                
                dados.append({
                    "Descrição": nome,
                    "Código": codigo,
                    "Quantidade": qtd,
                    "Unidade": unidade,
                    "Valor Unitário": unitario,
                    "Valor Total": total
                })
            except Exception as e:
                st.warning(f"Item ignorado (erro no parser): {texto}")
                continue
    
    return pd.DataFrame(dados)

# --- Interface Streamlit ---
st.set_page_config(page_title="Vulcano App", layout="wide")
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"])

# --- Página NFC-e ---
if menu == "📥 Inserir NFC-e":
    st.title("📥 Leitor de NFC-e por Link")
    url = st.text_input("Cole o link completo da NFC-e")

    if url:
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.content, 'html.parser')
            df = extrair_itens_por_texto(soup)

            if not df.empty:
                st.subheader("Produtos na Nota Fiscal")
                st.dataframe(df.style.format({
                    "Valor Unitário": "R$ {:.2f}",
                    "Valor Total": "R$ {:.2f}"
                }))

                # Seção de Dados Adicionais
                with st.form("dados_adicionais"):
                    st.subheader("Informações Complementares")
                    col1, col2, col3 = st.columns(3)
                    fornecedor = col1.text_input("Fornecedor", value="Bistek")
                    categoria = col2.selectbox("Categoria", ["Compras", "Matéria-Prima", "Despesas Operacionais"])
                    pagamento = col3.selectbox("Forma de Pagamento", ["PIX", "Cartão Crédito", "Cartão Débito", "Dinheiro"])
                    
                    if st.form_submit_button("✅ Enviar para Planilha"):
                        hoje = datetime.date.today().strftime("%d/%m/%Y")
                        dados_para_sheets = []
                        
                        for _, row in df.iterrows():
                            dados_para_sheets.append([
                                hoje,
                                fornecedor,
                                categoria,
                                row['Descrição'],
                                row['Quantidade'],
                                row['Unidade'],
                                row['Valor Unitário'],
                                row['Valor Total'],
                                pagamento
                            ])
                        
                        sheet.append_rows(dados_para_sheets)
                        st.success("Dados enviados com sucesso para o Google Sheets!")
                        st.balloons()
            else:
                st.warning("Nenhum produto encontrado na NFC-e.")
                
        except Exception as e:
            st.error(f"Erro ao processar a NFC-e: {str(e)}")

# --- Página Fluxo de Caixa (DRE) ---
elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Demonstração de Resultados")
    
    # Carregar dados com cache
    @st.cache_data(ttl=600)
    def carregar_dados():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Converter colunas numéricas
        num_cols = ['Quantidade', 'Valor Unitário', 'Valor Total']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace('.', '').str.replace(',', '.'), errors='coerce')
        
        # Classificar Receitas/Despesas
        df['Tipo'] = df['Categoria'].apply(lambda x: 'Receita' if x in ['Vendas', 'Ifood'] else 'Despesa')
        return df

    # Widgets de controle
    st.sidebar.header("Filtros")
    data_inicio = st.sidebar.date_input("Data Início", datetime.date.today().replace(day=1))
    data_fim = st.sidebar.date_input("Data Fim", datetime.date.today())
    
    # Dados filtrados
    df = carregar_dados()
    df['Data'] = pd.to_datetime(df[df.columns[0]], format='%d/%m/%Y', errors='coerce')  # Assume que a primeira coluna é a data
    df_filtrado = df[(df['Data'] >= pd.to_datetime(data_inicio)) & (df['Data'] <= pd.to_datetime(data_fim))]
    
    # Layout do DRE
    tab1, tab2 = st.tabs(["📊 Resumo Financeiro", "🔍 Detalhes"])
    
    with tab1:
        # Card de Saldo
        saldo_inicial = st.number_input("Saldo Inicial (R$)", value=0.0, step=100.0)
        receitas = df_filtrado[df_filtrado['Tipo'] == 'Receita']['Valor Total'].sum()
        despesas = df_filtrado[df_filtrado['Tipo'] == 'Despesa']['Valor Total'].sum()
        saldo_final = saldo_inicial + receitas - despesas
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Receitas", f"R$ {receitas:,.2f}")
        col2.metric("Total Despesas", f"R$ {despesas:,.2f}")
        col3.metric("Saldo Final", f"R$ {saldo_final:,.2f}", delta=f"R$ {(saldo_final-saldo_inicial):,.2f}")
        
        # Gráfico de evolução
        st.line_chart(df_filtrado.groupby('Data')['Valor Total'].sum())
    
    with tab2:
        # Abas para Receitas/Despesas
        sub_tab1, sub_tab2 = st.tabs(["Receitas", "Despesas"])
        
        with sub_tab1:
            st.dataframe(
                df_filtrado[df_filtrado['Tipo'] == 'Receita'].sort_values('Data'),
                hide_index=True,
                column_order=['Data', 'Descrição', 'Valor Total', 'Forma de Pagamento']
            )
        
        with sub_tab2:
            st.dataframe(
                df_filtrado[df_filtrado['Tipo'] == 'Despesa'].sort_values('Data'),
                hide_index=True,
                column_order=['Data', 'Fornecedor', 'Descrição', 'Valor Total', 'Categoria']
            )

# --- Página Estoque ---
elif menu == "📦 Estoque":
    st.title("📦 Gestão de Estoque")
    
    @st.cache_data(ttl=3600)
    def carregar_estoque():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Processamento dos dados
        if not df.empty:
            df['Valor Unitário'] = pd.to_numeric(df['Valor Unitário'].astype(str).str.replace('.', '').str.replace(',', '.'), errors='coerce')
            df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce')
            df['Valor Total'] = df['Quantidade'] * df['Valor Unitário']
            
            # Agrupamento por produto
            df_estoque = df.groupby('Descrição').agg({
                'Quantidade': 'sum',
                'Valor Unitário': 'first',
                'Valor Total': 'sum'
            }).reset_index()
            
            return df_estoque.sort_values('Quantidade', ascending=False)
        return pd.DataFrame()

    df_estoque = carregar_estoque()
    
    if not df_estoque.empty:
        # Métricas principais
        total_itens = df_estoque['Quantidade'].sum()
        valor_total = df_estoque['Valor Total'].sum()
        
        col1, col2 = st.columns(2)
        col1.metric("Total de Itens em Estoque", int(total_itens))
        col2.metric("Valor Total Estimado", f"R$ {valor_total:,.2f}")
        
        # Tabela de estoque
        st.dataframe(
            df_estoque.style.format({
                'Valor Unitário': 'R$ {:.2f}',
                'Valor Total': 'R$ {:.2f}'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # Seção de contagem manual (prévia do futuro recurso)
        with st.expander("🔍 Contagem Manual (Beta)"):
            item_selecionado = st.selectbox("Selecione o Item", df_estoque['Descrição'])
            qtd_real = st.number_input("Quantidade Física", min_value=0.0, step=1.0)
            
            if st.button("Registrar Contagem"):
                qtd_sistema = float(df_estoque[df_estoque['Descrição'] == item_selecionado]['Quantidade'].iloc[0])
                diferenca = qtd_real - qtd_sistema
                
                if diferenca != 0:
                    st.warning(f"Diferença encontrada: {diferenca} unidades")
                    # Aqui você pode adicionar a lógica para salvar no Google Sheets
                else:
                    st.success("Contagem compatível com o sistema!")
    else:
        st.warning("Nenhum dado de estoque disponível.")

# --- Página Dashboard (Placeholder) ---
elif menu == "📊 Dashboard":
    st.title("📊 Painel de Controle")
    st.info("Em desenvolvimento - versão em breve!")
    st.image("https://via.placeholder.com/800x400?text=Dashboard+em+Construção", use_column_width=True)

# --- Rodapé ---
st.sidebar.markdown("---")
st.sidebar.info("Vulcano App v1.1 | Desenvolvido para gestão integrada")
