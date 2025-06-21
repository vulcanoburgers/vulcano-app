# --- Conexão Google Sheets (Versão Robustecida) ---
def conectar_google_sheets():
    try:
        # Configuração do escopo e credenciais
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Carrega credenciais dos secrets
        creds = {
            "type": "service_account",
            "project_id": st.secrets["project_id"],
            "private_key_id": st.secrets["private_key_id"],
            "private_key": st.secrets["private_key"],
            "client_email": st.secrets["client_email"],
            "client_id": st.secrets["client_id"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": st.secrets["client_x509_cert_url"]
        }
        
        credentials = Credentials.from_service_account_info(creds, scopes=scope)
        client = gspread.authorize(credentials)
        
        # Verificação da URL
        if "PLANILHA_URL" not in st.secrets:
            st.error("🔐 Configure a URL da planilha em: Settings → Secrets")
            st.info("Exemplo: PLANILHA_URL='https://docs.google.com/.../edit#gid=0'")
            st.stop()
        
        # Extração segura do ID
        url = st.secrets["PLANILHA_URL"]
        if "/d/" in url:
            sheet_id = url.split("/d/")[1].split("/")[0]
        else:
            sheet_id = url  # Assume que já é o ID se não tiver URL completa
        
        return client.open_by_key(sheet_id).sheet1
        
    except gspread.exceptions.APIError as e:
        st.error(f"Erro na API do Google: {str(e)}")
        st.stop()
    except Exception as e:
        st.error(f"Erro inesperado: {str(e)}")
        st.stop()
