# --- Página Fluxo de Caixa (Com formatação BR) ---
elif menu == "📈 Fluxo de Caixa":
    st.title("📈 Fluxo de Caixa")
    
    @st.cache_data(ttl=600)
    def carregar_dados():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Garante todas as colunas necessárias
        colunas_requeridas = ["Data Compra", "Fornecedor", "Categoria", "Valor Total", "Forma de Pagamento", "Data de pagamento"]
        for col in colunas_requeridas:
            if col not in df.columns:
                st.error(f"Coluna faltando: '{col}' - Verifique a planilha!")
                return pd.DataFrame()
        
        # Conversão de valores (tratamento robusto)
        df["Valor Total"] = (
            df["Valor Total"]
            .astype(str)
            .str.replace(r'[^\d,]', '', regex=True)  # Remove tudo exceto números e vírgula
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .astype(float, errors='ignore')
        )
        
        # Classificação Receita/Despesa
        df["Tipo"] = df["Categoria"].apply(
            lambda x: "Receita" if str(x).lower() in ["vendas", "receita", "ifood"] else "Despesa"
        )
        
        # Conversão de datas
        df["Data Compra"] = pd.to_datetime(df["Data Compra"], dayfirst=True, errors='coerce')
        df["Data de pagamento"] = pd.to_datetime(df["Data de pagamento"], dayfirst=True, errors='coerce')
        
        return df.dropna(subset=["Valor Total"])

    df = carregar_dados()
    
    if not df.empty:
        # Filtros por período
        min_date = df["Data Compra"].min().date()
        max_date = df["Data Compra"].max().date()
        
        col1, col2 = st.columns(2)
        data_inicio = col1.date_input("De:", min_date, min_value=min_date, max_value=max_date)
        data_fim = col2.date_input("Até:", max_date, min_value=min_date, max_value=max_date)
        
        df_filtrado = df[
            (df["Data Compra"].dt.date >= data_inicio) & 
            (df["Data Compra"].dt.date <= data_fim)
        ]
        
        # Métricas formatadas (R$ com vírgula)
        def formatar_br(valor):
            return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        
        receitas = df_filtrado[df_filtrado["Tipo"] == "Receita"]["Valor Total"].sum()
        despesas = df_filtrado[df_filtrado["Tipo"] == "Despesa"]["Valor Total"].sum()
        saldo = receitas - despesas
        
        st.subheader("Resumo Financeiro")
        col1, col2, col3 = st.columns(3)
        col1.metric("Receitas", formatar_br(receitas))
        col2.metric("Despesas", formatar_br(despesas))
        col3.metric("Saldo", formatar_br(saldo), delta=formatar_br(saldo))
        
        # Tabela com todas as colunas formatadas
        st.subheader("Detalhamento")
        df_exibir = df_filtrado[[
            "Data Compra", "Fornecedor", "Categoria", "Descrição", 
            "Quantidade", "Valor Unit", "Valor Total", "Forma de Pagamento", 
            "Data de pagamento"
        ]].copy()
        
        df_exibir["Valor Unit"] = df_exibir["Valor Unit"].apply(formatar_br)
        df_exibir["Valor Total"] = df_exibir["Valor Total"].apply(formatar_br)
        
        st.dataframe(
            df_exibir,
            hide_index=True,
            column_config={
                "Data Compra": st.column_config.DateColumn("Compra", format="DD/MM/YYYY"),
                "Data de pagamento": st.column_config.DateColumn("Pagamento", format="DD/MM/YYYY"),
                "Quantidade": st.column_config.NumberColumn("Qtd", format="%.3f")
            },
            use_container_width=True
        )
        
        # Gráfico temporal
        st.subheader("Evolução Diária")
        st.line_chart(
            df_filtrado.groupby("Data Compra")["Valor Total"].sum(),
            height=400
        )
    else:
        st.warning("Nenhum dado encontrado para o período selecionado.")

# --- Página Estoque (Completa e Formatada) ---
elif menu == "📦 Estoque":
    st.title("📦 Estoque Atual")
    
    @st.cache_data(ttl=3600)
    def carregar_estoque():
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        if not df.empty:
            # Processamento seguro
            num_cols = ["Quantidade", "Valor Unit", "Valor Total"]
            for col in num_cols:
                if col in df.columns:
                    df[col] = (
                        df[col].astype(str)
                        .str.replace(r'[^\d,]', '', regex=True)
                        .str.replace('.', '', regex=False)
                        .str.replace(',', '.', regex=False)
                        .astype(float, errors='coerce')
                    )
            
            # Calcula valor total se não existir
            if "Valor Total" not in df.columns and all(c in df.columns for c in ["Quantidade", "Valor Unit"]):
                df["Valor Total"] = df["Quantidade"] * df["Valor Unit"]
            
            # Agrupa por produto
            df_estoque = df.groupby("Descrição").agg({
                "Quantidade": "sum",
                "Valor Unit": "first",
                "Valor Total": "sum"
            }).reset_index()
            
            return df_estoque.sort_values("Quantidade", ascending=False)
        return pd.DataFrame()
    
    df_estoque = carregar_estoque()
    
    if not df_estoque.empty:
        # Formatação BR
        def formatar_br(valor):
            return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        
        # Métricas
        total_itens = df_estoque["Quantidade"].sum()
        valor_total = df_estoque["Valor Total"].sum()
        
        col1, col2 = st.columns(2)
        col1.metric("Total de Itens", f"{total_itens:,.2f}".replace(".", ","))
        col2.metric("Valor Total em Estoque", formatar_br(valor_total))
        
        # Tabela formatada
        df_exibir = df_estoque.copy()
        df_exibir["Valor Unit"] = df_exibir["Valor Unit"].apply(formatar_br)
        df_exibir["Valor Total"] = df_exibir["Valor Total"].apply(formatar_br)
        
        st.dataframe(
            df_exibir,
            column_config={
                "Quantidade": st.column_config.NumberColumn("Qtd", format="%.3f")
            },
            hide_index=True,
            use_container_width=True
        )
        
        # Seção de contagem manual (opcional)
        with st.expander("Contagem Física"):
            item = st.selectbox("Selecione o item", df_estoque["Descrição"])
            qtd_fisica = st.number_input("Quantidade física encontrada", min_value=0.0, step=0.001, format="%.3f")
            
            if st.button("Comparar com sistema"):
                qtd_sistema = df_estoque[df_estoque["Descrição"] == item]["Quantidade"].values[0]
                diferenca = qtd_fisica - qtd_sistema
                
                st.write(f"**Sistema:** {qtd_sistema:,.3f} | **Físico:** {qtd_fisica:,.3f}")
                st.success(f"Diferença: {diferenca:,.3f}") if diferenca == 0 else st.error(f"Diferença: {diferenca:,.3f}")
    else:
        st.warning("Nenhum dado de estoque encontrado.")
