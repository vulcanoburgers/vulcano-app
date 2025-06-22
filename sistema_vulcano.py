import streamlit as st
import pandas as pd
import datetime
from google.oauth2.service_account import Credentials
import gspread

# --- Configuração Inicial ---
# Define o título da página e o layout para a aplicação Streamlit.
st.set_page_config(page_title="Vulcano App", layout="wide")

# --- Função de Conexão ao Google Sheets ---
# Estabelece uma conexão com o Google Sheets usando credenciais de conta de serviço.
# O objeto 'st.secrets' é usado para acessar com segurança as credenciais configuradas no Streamlit.
@st.cache_resource(ttl=3600) # Armazena o objeto de conexão em cache por 1 hora para evitar reautenticação
def conectar_google_sheets():
    try:
        # Define o escopo para o acesso à API do Google Sheets.
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        # Carrega as credenciais a partir dos segredos do Streamlit.
        # As credenciais estão no nível raiz de 'st.secrets'.
        creds = Credentials.from_service_account_info(st.secrets, scopes=scope)
        # Autoriza o cliente gspread com as credenciais carregadas.
        client = gspread.authorize(creds)
        # Abre a planilha específica pela sua chave (ID) e seleciona a primeira aba.
        # Certifique-se de que o ID da planilha está configurado corretamente nos seus segredos do Streamlit ou diretamente aqui.
        sheet = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").sheet1
        return sheet
    except Exception as e:
        # Exibe uma mensagem de erro se a conexão falhar e interrompe a execução da aplicação Streamlit.
        st.error(f"Erro na conexão com o Google Sheets: {str(e)}")
        st.stop()

# --- Funções Auxiliares ---

# Formata um valor numérico para o formato de moeda brasileira (R$) ou de quantidade.
# O flag 'is_quantidade' determina se é uma quantidade (3 casas decimais) ou moeda (2 casas decimais).
def formatar_br(valor, is_quantidade=False):
    try:
        # Converte o valor para float e trata potenciais problemas de formatação (ex: entrada não numérica).
        float_valor = float(valor)
        if is_quantidade:
            # Formata como quantidade com 3 casas decimais, usando vírgula como separador decimal e ponto como separador de milhar.
            # O truque de 'X' é usado para evitar conflitos na substituição de ponto e vírgula.
            return f"{float_valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        else:
            # Formata como moeda com 2 casas decimais, usando vírgula como separador decimal e ponto como separador de milhar.
            return f"R$ {float_valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        # Retorna o valor original se a formatação falhar (ex: se o valor não for um número).
        return valor
        
# Converte Valor
def converter_valor(valor, *args, **kwargs):
    try:
        return float(valor)
    except :
        return 0.0

    except (ValueError, TypeError):
        st.error(f"Erro ao converter valor: {valor} (unidade: {unidade})")
        return 0.0
# Formatar em R$        
def formatar_br(valor, is_quantidade=False):
    try:
        if is_quantidade:
            return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return valor
        
# --- Definição do Menu Principal ---
# Define o menu de navegação para a aplicação Streamlit usando um botão de rádio na barra lateral.
menu = st.sidebar.radio("Menu", ["📥 Inserir NFC-e", "📊 Dashboard", "📈 Fluxo de Caixa", "📦 Estoque"])

# --- Conexão ao Google Sheets (após a definição do menu para garantir o fluxo adequado da aplicação) ---
# Isso garante que o objeto 'sheet' esteja disponível globalmente para as páginas da aplicação.
sheet = conectar_google_sheets()

# --- Páginas da Aplicação ---

# Página: Inserir NFC-e
if menu == "📥 Inserir NFC-e":
    st.title("📥 Inserir NFC-e")
    st.info("Funcionalidade em desenvolvimento")

# Página: Dashboard
elif menu == "📊 Dashboard":
    st.title("📊 Dashboard")
    st.info("Funcionalidade em desenvolvimento")

# Página: Fluxo de Caixa
elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")
    
    # Armazena em cache a função de carregamento de dados para melhorar o desempenho.
    # Os dados serão recarregados apenas se os parâmetros de entrada mudarem ou após 600 segundos (10 minutos).
    @st.cache_data(ttl=600)
    def carregar_dados():
        # Obtém todos os registros da planilha do Google Sheets.
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Processa 'Valor Unit' e 'Quantidade' usando a função converter_valor.
        df['Valor Unit'] = df.apply(lambda x: converter_valor(x['Valor Unit'], x['Unid'], is_valor_unitario=True), axis=1)
        df['Quantidade'] = df.apply(lambda x: converter_valor(x['Quantidade'], x['Unid']), axis=1)
        
        # Calcula 'Valor Total' com base no valor unitário e quantidade processados.
        df['Valor Total'] = df['Valor Unit'] * df['Quantidade']
        
        # Converte 'Data Compra' para objetos datetime, tratando potenciais erros.
        df['Data Compra'] = pd.to_datetime(df['Data Compra'], dayfirst=True, errors='coerce').dt.date
        # Remove as linhas onde 'Data Compra' não pôde ser analisada (valores NaT).
        df = df.dropna(subset=['Data Compra'])
        
        return df

    df = carregar_dados() # Carrega os dados em um DataFrame.
    
    if not df.empty:
        # Determina as datas mínima e máxima no conjunto de dados para filtragem por data.
        min_date = df['Data Compra'].min()
        max_date = df['Data Compra'].max()
        
        # Cria colunas para widgets de entrada de data.
        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input("De", min_date, min_value=min_date, max_value=max_date)
        with col2:
            data_fim = st.date_input("Até", max_date, min_value=min_date, max_value=max_date)
        
        # Filtra o DataFrame com base no intervalo de datas selecionado.
        df_filtrado = df[(df['Data Compra'] >= data_inicio) & (df['Data Compra'] <= data_fim)]
        
        # Cria uma cópia para exibição para evitar modificar o DataFrame original.
        df_exibir = df_filtrado.copy()
        # Aplica a formatação de moeda brasileira e quantidade para fins de exibição.
        df_exibir['Valor Unit'] = df_exibir['Valor Unit'].apply(formatar_br)
        df_exibir['Valor Total'] = df_exibir['Valor Total'].apply(formatar_br)
        df_exibir['Quantidade'] = df_exibir['Quantidade'].apply(lambda x: formatar_br(x, is_quantidade=True))
        
        # Exibe o DataFrame filtrado e formatado usando o widget dataframe do Streamlit.
        st.dataframe(
            df_exibir.sort_values('Data Compra', ascending=False), # Ordena por data decrescente.
            column_config={
                "Data Compra": st.column_config.DateColumn("Data da Compra", format="DD/MM/YYYY"), # Personaliza a exibição da coluna de data.
                "Unid": st.column_config.TextColumn("Unidade"), # Renomeia a coluna 'Unid'.
                "Descrição": st.column_config.TextColumn("Descrição do Item"), # Renomeia a coluna 'Descrição'.
                "Fornecedor": st.column_config.TextColumn("Fornecedor"),
                "Valor Unit": st.column_config.TextColumn("Valor Unitário"),
                "Valor Total": st.column_config.TextColumn("Valor Total")
            },
            hide_index=True, # Oculta o índice do DataFrame.
            use_container_width=True # Faz o DataFrame expandir para a largura do contêiner.
        )
    else:
        st.warning("Nenhum dado encontrado para o período selecionado.")

# Página: Gestão de Estoque
elif menu == "📦 Estoque":
    st.title("📦 Gestão de Estoque")
    
    # Armazena em cache a função de carregamento de dados do estoque.
    # Os dados serão recarregados apenas se os parâmetros de entrada mudarem ou após 3600 segundos (1 hora).
    @st.cache_data(ttl=3600)
    def carregar_estoque():
        # Obtém todos os registros da planilha do Google Sheets.
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Processa 'Valor Unit' e 'Quantidade'.
        df['Valor Unit'] = df.apply(lambda x: converter_valor(x['Valor Unit'], x['Unid'], is_valor_unitario=True), axis=1)
        df['Quantidade'] = df.apply(lambda x: converter_valor(x['Quantidade'], x['Unid']), axis=1)
        
        # Calcula 'Valor Total' com base no valor unitário e quantidade processados.
        df['Valor Total'] = df['Valor Unit'] * df['Quantidade'] # Recalcula o valor total após a conversão
        
        # Agrupa por 'Descrição' e 'Unid' para somar quantidades e valores totais,
        # e obter o primeiro 'Valor Unit' (assumindo que seja consistente para um dado item/unidade).
        df_grouped = df.groupby(['Descrição', 'Unid']).agg(
            Quantidade=('Quantidade', 'sum'),
            Valor_Unit_First=('Valor Unit', 'first'), # Mantém o primeiro valor unitário para referência
            Valor_Total_Sum=('Valor Total', 'sum') # Soma os valores totais
        ).reset_index()

        # Recalcula 'Valor Unit' como a média ponderada
        # Esta é uma representação mais precisa para o inventário se os preços variarem
        df_grouped['Valor Unit'] = df_grouped['Valor_Total_Sum'] / df_grouped['Quantidade']
        
        # Lida com casos em que a Quantidade pode ser zero para evitar erros de divisão por zero
        df_grouped['Valor Unit'] = df_grouped['Valor Unit'].fillna(0) # ou outro valor adequado
        df_grouped = df_grouped.drop(columns=['Valor_Unit_First'])
        df_grouped.rename(columns={'Valor_Total_Sum': 'Valor Total'}, inplace=True)
        
        return df_grouped

    df_estoque = carregar_estoque() # Carrega os dados do estoque.
    
    if not df_estoque.empty:
        # Cria uma cópia para exibição.
        df_exibir = df_estoque.copy()
        # Aplica a formatação de moeda brasileira e quantidade para exibição.
        df_exibir['Valor Unit'] = df_exibir['Valor Unit'].apply(formatar_br)
        df_exibir['Valor Total'] = df_exibir['Valor Total'].apply(formatar_br)
        df_exibir['Quantidade'] = df_exibir['Quantidade'].apply(lambda x: formatar_br(x, is_quantidade=True))
        
        # Exibe o DataFrame do estoque.
        st.dataframe(
            df_exibir[['Descrição', 'Unid', 'Quantidade', 'Valor Unit', 'Valor Total']], # Seleciona e ordena as colunas para exibição.
            column_config={
                "Unid": st.column_config.TextColumn("Unidade"), # Renomeia a coluna 'Unid'.
                "Quantidade": st.column_config.NumberColumn("Qtd", format="%.3f"), # Personaliza a exibição da quantidade.
                "Descrição": st.column_config.TextColumn("Descrição do Item"),
                "Valor Unit": st.column_config.TextColumn("Valor Unitário Médio"), # Indica que é uma média
                "Valor Total": st.column_config.TextColumn("Valor Total do Estoque")
            },
            hide_index=True, # Oculta o índice do DataFrame.
            use_container_width=True # Faz o DataFrame expandir para a largura do contêiner.
        )
        
        # Calcula e exibe o valor total de todos os itens em estoque.
        valor_total_estoque = df_estoque['Valor Total'].sum()
        st.metric("Valor Total em Estoque", formatar_br(valor_total_estoque))
    else:
        st.warning("Nenhum item em estoque encontrado.")
