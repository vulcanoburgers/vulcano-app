
# Vulcano Burgers - Controle de Despesas

Este é o app para controle de despesas da hamburgueria Vulcano, usando Streamlit e Google Sheets.

## Como usar

### 1. Preparar credenciais Google Sheets
- Crie um projeto no Google Cloud e habilite a API do Google Sheets.
- Gere um arquivo JSON de credenciais para "Service Account".
- Compartilhe sua planilha com o e-mail do service account.
- Coloque o arquivo JSON na mesma pasta do app com o nome `vulcano-credentials.json`.

### 2. Criar repositório no GitHub
- Faça login no GitHub.
- Crie um novo repositório chamado `vulcano-app`.
- Faça upload dos arquivos `sistema_vulcano.py`, `requirements.txt`, `vulcano-credentials.json` (opcional, cuidado com privacidade) e `README.md`.

### 3. Rodar no Streamlit Cloud
- Acesse https://streamlit.io/cloud
- Conecte seu GitHub.
- Importe o repositório `vulcano-app`.
- Clique em Deploy.

### 4. Uso
- O app estará disponível online.
- Atualize suas despesas via formulário ou diretamente na planilha.
- Os dados aparecem em tempo real.

---

Qualquer dúvida, me chama!
