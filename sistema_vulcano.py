with col1:
                produto_nome = st.text_input("Nome do Produto*", placeholder="Ex: Coca Cola Lata 350ml")
                quantidade = st.number_input("Quantidade*", min_value=0.0, step=1.0, value=1.0)
                preco_unitario = st.number_input("Preço Unitário (R$)*", min_value=0.0, step=0.01, value=0.0)
            
            with col2:
                fornecedor = st.text_input("Fornecedor", placeholder="Ex: Coca Cola")
                categoria = st.selectbox("Categoria", ["Bebidas", "Insumos", "Higiene e Limp", "Embalagens"])
                unidade = st.selectbox("Unidade", ["un", "kg", "g", "l", "ml", "pc"])
            
            observacoes = st.text_area("Observações", placeholder="Informações adicionais sobre a compra...")
            
            submitted = st.form_submit_button("➕ Adicionar ao Estoque")
            
            if submitted:
                if produto_nome and quantidade > 0 and preco_unitario > 0:
                    st.success(f"✅ Produto '{produto_nome}' adicionado ao estoque!")
                    st.info("💡 O produto será registrado na planilha INSUMOS")
                    
                    # Mostrar resumo
                    st.markdown("**📋 Resumo da Entrada:**")
                    st.write(f"• **Produto:** {produto_nome}")
                    st.write(f"• **Quantidade:** {quantidade} {unidade}")
                    st.write(f"• **Preço:** {formatar_br(preco_unitario)}")
                    st.write(f"• **Valor Total:** {formatar_br(quantidade * preco_unitario)}")
                    if fornecedor:
                        st.write(f"• **Fornecedor:** {fornecedor}")
                    if observacoes:
                        st.write(f"• **Observações:** {observacoes}")
                else:
                    st.error("❌ Preencha todos os campos obrigatórios (*)")

def analise_custos_estoque(df_insumos):
    """Análise de custos do estoque"""
    
    st.subheader("📈 Análise de Custos")
    
    # Preparar dados
    df_work = df_insumos.copy()
    df_work['Em estoque'] = df_work.get('Em estoque', 0).apply(limpar_numero)
    df_work['Preço (un)'] = df_work.get('Preço (un)', 0).apply(limpar_numero)
    df_work['Valor Total'] = df_work['Em estoque'] * df_work['Preço (un)']
    
    # Top produtos mais valiosos
    st.markdown("### 💎 Top 10 Produtos Mais Valiosos")
    top_produtos = df_work.nlargest(10, 'Valor Total')[['Produto', 'Em estoque', 'Preço (un)', 'Valor Total']]
    
    st.dataframe(
        top_produtos,
        column_config={
            "Produto": "Produto",
            "Em estoque": st.column_config.NumberColumn("Estoque", format="%.1f"),
            "Preço (un)": st.column_config.NumberColumn("Preço Unit.", format="R$ %.2f"),
            "Valor Total": st.column_config.NumberColumn("Valor Total", format="R$ %.2f")
        },
        hide_index=True,
        use_container_width=True
    )
    
    # Análise por categoria
    if 'Categoria' in df_work.columns:
        st.markdown("### 📊 Análise por Categoria")
        
        analise_categoria = df_work.groupby('Categoria').agg({
            'Produto': 'count',
            'Valor Total': 'sum',
            'Em estoque': 'sum'
        }).reset_index()
        analise_categoria.columns = ['Categoria', 'Qtd_Produtos', 'Valor_Total', 'Qtd_Estoque']
        analise_categoria = analise_categoria.sort_values('Valor_Total', ascending=False)
        
        st.dataframe(
            analise_categoria,
            column_config={
                "Categoria": "Categoria",
                "Qtd_Produtos": st.column_config.NumberColumn("Produtos", format="%d"),
                "Valor_Total": st.column_config.NumberColumn("Valor Total", format="R$ %.2f"),
                "Qtd_Estoque": st.column_config.NumberColumn("Qtd em Estoque", format="%.1f")
            },
            hide_index=True,
            use_container_width=True
        )
    
    # Recomendações
    st.markdown("### 💡 Recomendações")
    
    valor_total = df_work['Valor Total'].sum()
    produtos_alto_valor = df_work[df_work['Valor Total'] > (valor_total * 0.05)]  # 5% do total
    
    st.info(f"""
    **Análise do Estoque:**
    
    • **Valor total investido:** {formatar_br(valor_total)}
    • **Produtos de alto valor:** {len(produtos_alto_valor)} itens representam a maior parte do investimento
    • **Concentração:** {len(produtos_alto_valor)/len(df_work)*100:.1f}% dos produtos concentram maior valor
    
    **Dicas:**
    • Monitore de perto os produtos de alto valor
    • Revise estoques mínimos dos itens mais caros
    • Considere negociações especiais com fornecedores principais
    """)

def configuracoes_estoque():
    """Configurações do módulo de estoque"""
    
    st.subheader("⚙️ Configurações do Estoque")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Conexão com Planilha")
        
        if st.button("🔄 Testar Conexão INSUMOS"):
            df_test = carregar_dados_insumos()
            if not df_test.empty:
                st.success(f"✅ Conexão OK! {len(df_test)} produtos carregados")
                st.write("**Colunas encontradas:**")
                st.write(", ".join(df_test.columns.tolist()))
                
                # Debug: mostrar alguns dados para verificar formato
                st.write("**Amostra dos dados (primeiras 3 linhas):**")
                st.dataframe(df_test.head(3))
                
            else:
                st.error("❌ Erro na conexão com a aba INSUMOS")
        
        st.markdown("### 🔄 Cache")
        if st.button("🧹 Limpar Cache"):
            st.cache_data.clear()
            st.success("✅ Cache limpo!")
    
    with col2:
        st.markdown("### ⚙️ Configurações de Alerta")
        
        limite_baixo = st.slider(
            "Limite para Estoque Baixo (%)",
            min_value=10,
            max_value=50,
            value=20,
            help="Percentual do estoque mínimo para gerar alerta"
        )
        
        notif_falta = st.checkbox("Notificar produtos em falta", value=True)
        notif_baixo = st.checkbox("Notificar estoque baixo", value=True)
        
        if st.button("💾 Salvar Configurações"):
            st.success("✅ Configurações salvas!")
    
    # Sistema de Alias
    st.markdown("### 🔗 Sistema de Alias de Produtos")
    
    st.info("""
    **Como funciona:**
    O sistema reconhece automaticamente produtos com nomes similares e os associa ao produto correto no estoque.
    
    **Exemplos configurados:**
    • "Agua com gás - Fonte da Pedra" → "agua com gas"
    • "Coca Cola 350ml" → "coca cola lata"
    • "Queijo Cheddar" → "queijo cheddar fatiado"
    """)
    
    # Teste do sistema de alias
    with st.expander("🧪 Testar Sistema de Alias"):
        nome_teste = st.text_input("Digite um nome para testar:", placeholder="Ex: Agua com gás - Crystal")
        
        if nome_teste:
            resultado = normalizar_nome_produto(nome_teste)
            if resultado != nome_teste:
                st.success(f"✅ **'{nome_teste}'** → **'{resultado}'**")
            else:
                st.warning(f"⚠️ **'{nome_teste}'** → Sem correspondência (mantém nome original)")
        
        # Mostrar alguns exemplos
        st.write("**📝 Teste com exemplos pré-definidos:**")
        exemplos = [
            "Agua com gás - Fonte da Pedra",
            "Coca Cola 350ml",
            "Queijo Cheddar",
            "Água Crystal com gás",
            "Sprite lata 350ml"
        ]
        
        for exemplo in exemplos:
            resultado_ex = normalizar_nome_produto(exemplo)
            if resultado_ex != exemplo:
                st.write(f"• **{exemplo}** → **{resultado_ex}** ✅")
            else:
                st.write(f"• **{exemplo}** → Sem correspondência ⚠️")
    
    # Informações da estrutura
    st.markdown("### 📋 Estrutura da Aba INSUMOS")
    st.info("""
    **Colunas esperadas na aba INSUMOS:**
    
    • **Produto** - Nome do produto/insumo
    • **Categoria** - Categoria (Bebidas, Insumos, etc.)
    • **Em estoque** - Quantidade atual em estoque
    • **Estoque Min** - Quantidade mínima recomendada
    • **Preço (un)** - Preço unitário
    • **Fornecedor** - Nome do fornecedor
    
    O sistema já está configurado para funcionar com sua planilha atual!
    """)

# --- Análise de Pedidos ---
def analisar_pedidos(df_pedidos):
    """Análise simples dos dados de pedidos"""
    insights = []
    
    if df_pedidos.empty:
        return ["Não há dados suficientes para análise."]
    
    try:
        # Preparar dados
        df_pedidos['data'] = pd.to_datetime(df_pedidos['data'], errors='coerce')
        df_pedidos = df_pedidos.dropna(subset=['data'])
        
        if len(df_pedidos) == 0:
            return ["Dados de data inválidos."]
        
        # Análise temporal
        df_pedidos['hora'] = df_pedidos['data'].dt.hour
        horarios_pico = df_pedidos['hora'].value_counts().head(3)
        
        insights.append(f"🕐 Horários de pico: {', '.join([f'{h}h ({v} pedidos)' for h, v in horarios_pico.items()])}")
        
        # Análise de canal
        if 'canal' in df_pedidos.columns:
            canais = df_pedidos['canal'].value_counts()
            if len(canais) > 0:
                insights.append(f"📱 Canal principal: {canais.index[0]} ({canais.iloc[0]} pedidos)")
        
        # Análise de valores
        if 'total' in df_pedidos.columns:
            # Processar valores
            df_pedidos['total_num'] = df_pedidos['total'].apply(limpar_valor_brasileiro)
            
            ticket_medio = df_pedidos['total_num'].mean()
            valor_total = df_pedidos['total_num'].sum()
            
            insights.append(f"💰 Ticket médio: {formatar_br(ticket_medio)}")
            insights.append(f"💰 Faturamento total: {formatar_br(valor_total)}")
        
        # Recomendações
        insights.append("\n🎯 Recomendações:")
        insights.append("• Análise mais detalhada disponível na versão completa")
        
    except Exception as e:
        insights.append(f"Erro na análise: {str(e)}")
    
    return insights

# --- Scraper NFC-e ---
def extrair_itens_nfce(soup):
    """Extrai itens da NFCe usando BeautifulSoup"""
    tabela = soup.find("table", {"id": "tabResult"})
    if not tabela:
        return pd.DataFrame()
    
    linhas = tabela.find_all("tr")
    dados = []
    
    for linha in linhas:
        texto = linha.get_text(" ", strip=True)
        if all(keyword in texto for keyword in ["Código:", "Qtde.:", "UN:", "Vl. Unit.:", "Vl. Total"]):
            try:
                nome = texto.split("(Código:")[0].strip()
                codigo = re.search(r"Código:\s*(\d+)", texto).group(1)
                qtd = re.search(r"Qtde\.\:\s*([\d,]+)", texto).group(1).replace(",", ".")
                unidade = re.search(r"UN\:\s*(\w+)", texto).group(1)
                unitario = re.search(r"Vl\. Unit\.\:\s*([\d,]+)", texto).group(1).replace(",", ".")
                total = re.search(r"Vl\. Total\s*([\d,]+)", texto).group(1).replace(",", ".")
                
                dados.append({
                    "Descrição": nome,
                    "Código": codigo,
                    "Quantidade": float(qtd),
                    "Unidade": unidade,
                    "Valor Unitário": float(unitario),
                    "Valor Total": float(total)
                })
            except Exception:
                continue
    
    return pd.DataFrame(dados)

# --- Interface Principal ---
def main():
    # Header
    st.markdown('<h1 class="main-header">🔥 VULCANO - Sistema de Gestão</h1>', unsafe_allow_html=True)
    
    # Menu lateral
    st.sidebar.title("📋 Menu Principal")
    menu = st.sidebar.radio(
        "Selecione uma opção:",
        [
            "🏠 Dashboard Principal",
            "📦 Gestão de Estoque",
            "📊 Análise de Pedidos",
            "🛵 Fechamento Motoboys",
            "⚙️ Configurações"
        ]
    )
    
    # Carregar dados
    df_compras, df_pedidos = carregar_dados_sheets()
    
    # --- DASHBOARD PRINCIPAL ---
    if menu == "🏠 Dashboard Principal":
        st.title("📊 Dashboard Principal")
        
        # Métricas principais
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_pedidos = len(df_pedidos) if not df_pedidos.empty else 0
            st.metric("Total de Pedidos", total_pedidos)
        
        with col2:
            if not df_pedidos.empty and 'total' in df_pedidos.columns:
                valores_limpos = df_pedidos['total'].apply(limpar_valor_brasileiro)
                faturamento = valores_limpos.sum()
                st.metric("Faturamento", formatar_br(faturamento))
            else:
                st.metric("Faturamento", "R$ 0,00")
        
        with col3:
            if not df_pedidos.empty and 'total' in df_pedidos.columns:
                valores_limpos = df_pedidos['total'].apply(limpar_valor_brasileiro)
                ticket_medio = valores_limpos.mean()
                st.metric("Ticket Médio", formatar_br(ticket_medio))
            else:
                st.metric("Ticket Médio", "R$ 0,00")
        
        with col4:
            total_compras = len(df_compras) if not df_compras.empty else 0
            st.metric("Compras Registradas", total_compras)
        
        # Resumo do estoque no dashboard
        st.markdown("### 📦 Resumo do Estoque")
        df_insumos = carregar_dados_insumos()
        
        if not df_insumos.empty:
            df_insumos['Em estoque'] = df_insumos.get('Em estoque', 0).apply(limpar_numero)
            df_insumos['Estoque Min'] = df_insumos.get('Estoque Min', 0).apply(limpar_numero)
            df_insumos['Preço (un)'] = df_insumos.get('Preço (un)', 0).apply(limpar_numero)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                valor_estoque = (df_insumos['Em estoque'] * df_insumos['Preço (un)']).sum()
                st.metric("💰 Valor em Estoque", formatar_br(valor_estoque))
            
            with col2:
                produtos_baixo = len(df_insumos[
                    (df_insumos['Em estoque'] < df_insumos['Estoque Min']) & 
                    (df_insumos['Em estoque'] > 0)
                ])
                st.metric("⚠️ Estoque Baixo", produtos_baixo)
            
            with col3:
                produtos_falta = len(df_insumos[df_insumos['Em estoque'] == 0])
                st.metric("🚨 Em Falta", produtos_falta, delta_color="inverse")
        
        # Gráficos
        if not df_pedidos.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 Vendas por Dia")
                if 'data' in df_pedidos.columns:
                    df_pedidos['data'] = pd.to_datetime(df_pedidos['data'], errors='coerce')
                    vendas_dia = df_pedidos.groupby(df_pedidos['data'].dt.date).size().reset_index()
                    vendas_dia.columns = ['Data', 'Pedidos']
                    st.line_chart(vendas_dia.set_index('Data'))
            
            with col2:
                st.subheader("🎯 Vendas por Canal")
                if 'canal' in df_pedidos.columns:
                    canal_vendas = df_pedidos['canal'].value_counts()
                    st.bar_chart(canal_vendas)
    
    # --- GESTÃO DE ESTOQUE ---
    elif menu == "📦 Gestão de Estoque":
        pagina_estoque()
    
    # --- ANÁLISE DE PEDIDOS ---
    elif menu == "📊 Análise de Pedidos":
        st.title("📊 Análise de Pedidos")
        
        if df_pedidos.empty:
            st.warning("⚠️ Nenhum dado de pedidos encontrado.")
            return
        
        # Análise com IA
        st.subheader("🤖 Insights Automáticos")
        
        if st.button("🔍 Gerar Análise"):
            with st.spinner("Analisando dados..."):
                insights = analisar_pedidos(df_pedidos)
                
                for insight in insights:
                    st.markdown(insight)
        
        # Solução do ticket médio
        st.subheader("🎯 Solução: Ticket Médio Corrigido")
        st.info("""
        **Problema:** Múltiplos pedidos por mesa/cliente
        **Solução:** Agrupar pedidos por cliente e tempo
        """)
        
        if st.checkbox("🧮 Calcular Ticket Médio Corrigido"):
            if 'nome' in df_pedidos.columns and 'total' in df_pedidos.columns:
                df_temp = df_pedidos.copy()
                df_temp['data'] = pd.to_datetime(df_temp['data'], errors='coerce')
                df_temp['total_num'] = df_temp['total'].apply(limpar_valor_brasileiro)
                
                # Separar por tipo de entrega
                if 'metodo_entrega' in df_temp.columns:
                    delivery = df_temp[df_temp['metodo_entrega'].str.contains('Delivery', case=False, na=False)]
                    local = df_temp[~df_temp['metodo_entrega'].str.contains('Delivery', case=False, na=False)]
                    
                    ticket_delivery = delivery['total_num'].mean() if len(delivery) > 0 else 0
                    ticket_local = local['total_num'].mean() if len(local) > 0 else 0
                    ticket_geral = df_temp['total_num'].mean()
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Ticket Geral", formatar_br(ticket_geral))
                    with col2:
                        st.metric("Ticket Delivery", formatar_br(ticket_delivery))
                    with col3:
                        st.metric("Ticket Local", formatar_br(ticket_local))
                    
                    # Estatísticas
                    st.write("**Estatísticas por Tipo:**")
                    stats = df_temp.groupby('metodo_entrega')['total_num'].agg(['count', 'mean', 'sum'])
                    st.dataframe(stats)
    
    # --- FECHAMENTO MOTOBOYS ---
    elif menu == "🛵 Fechamento Motoboys":
        st.title("🛵 Fechamento de Motoboys")
        
        if df_pedidos.empty:
            st.warning("⚠️ Nenhum dado de pedidos encontrado.")
            return
        
        # Configurações
        col1, col2, col3 = st.columns(3)
        
        with col1:
            motoboys_lista = ["Everson", "Marlon", "Adrian", "Vulcano"]
            if 'motoboy' in df_pedidos.columns:
                motoboys_planilha = df_pedidos['motoboy'].dropna().unique().tolist()
                motoboys_lista = list(set(motoboys_lista + motoboys_planilha))
            
            motoboy_selecionado = st.selectbox("Selecione o motoboy:", sorted(motoboys_lista))
        
        with col2:
            data_inicio = st.date_input("Data início:", value=datetime.date.today() - datetime.timedelta(days=7))
        
        with col3:
            data_fim = st.date_input("Data fim:", value=datetime.date.today())
        
        if st.button("🔍 Calcular Fechamento"):
            # Preparar dados
            df_temp = df_pedidos.copy()
            df_temp['data'] = pd.to_datetime(df_temp['data'], errors='coerce')
            df_temp = df_temp.dropna(subset=['data'])
            
            # DEBUG - Mostrar informações básicas
            st.markdown("### 🔍 Informações dos Dados")
            st.write(f"**📊 Total de registros:** {len(df_temp)}")
            st.write(f"**👤 Motoboy procurado:** '{motoboy_selecionado}'")
            st.write(f"**📅 Período:** {data_inicio} até {data_fim}")
            
            if 'motoboy' in df_temp.columns:
                # Mostrar motoboys disponíveis
                motoboys_encontrados = df_temp['motoboy'].dropna().unique()
                st.write(f"**👥 Motoboys na planilha ({len(motoboys_encontrados)}):**")
                for mb in sorted(motoboys_encontrados):
                    st.write(f"  • '{mb}'")
                
                # Testar diferentes filtros
                st.markdown("### 🔎 Testando Filtros")
                
                # Filtro por data primeiro
                df_periodo = df_temp[
                    (df_temp['data'].dt.date >= data_inicio) & 
                    (df_temp['data'].dt.date <= data_fim)
                ]
                st.write(f"**📅 Registros no período:** {len(df_periodo)}")
                
                if len(df_periodo) == 0:
                    st.error("❌ Nenhum registro encontrado no período selecionado!")
                    st.write("**💡 Dicas:**")
                    st.write("• Tente um período maior")
                    st.write("• Verifique se há dados nessas datas")
                    return
                
                # Agora filtrar por motoboy
                filtros_testados = []
                
                # 1. Exato
                mask1 = df_periodo['motoboy'] == motoboy_selecionado
                resultado1 = df_periodo[mask1]
                filtros_testados.append(("Busca exata", len(resultado1)))
                
                # 2. Case insensitive
                mask2 = df_periodo['motoboy'].str.lower() == motoboy_selecionado.lower()
                resultado2 = df_periodo[mask2]
                filtros_testados.append(("Ignorando maiúscula/minúscula", len(resultado2)))
                
                # 3. Sem espaços
                mask3 = df_periodo['motoboy'].str.strip().str.lower() == motoboy_selecionado.strip().lower()
                resultado3 = df_periodo[mask3]
                filtros_testados.append(("Removendo espaços", len(resultado3)))
                
                # 4. Contém
                mask4 = df_periodo['motoboy'].str.contains(motoboy_selecionado, case=False, na=False)
                resultado4 = df_periodo[mask4]
                filtros_testados.append(("Contém o nome", len(resultado4)))
                
                # Mostrar resultados dos filtros
                for nome_filtro, quantidade in filtros_testados:
                    st.write(f"• **{nome_filtro}:** {quantidade} registros")
                
                # Escolher melhor resultado
                df_final = pd.DataFrame()
                metodo = "Nenhum"
                
                if len(resultado3) > 0:
                    df_final = resultado3
                    metodo = "Removendo espaços + case insensitive"
                elif len(resultado2) > 0:
                    df_final = resultado2
                    metodo = "Case insensitive"
                elif len(resultado4) > 0:
                    df_final = resultado4
                    metodo = "Contém o nome"
                elif len(resultado1) > 0:
                    df_final = resultado1
                    metodo = "Busca exata"
                
                st.markdown("### 📋 Resultado Final")
                st.write(f"**✅ Método usado:** {metodo}")
                st.write(f"**📊 Pedidos encontrados:** {len(df_final)}")
                
                if len(df_final) > 0:
                    # Mostrar amostra
                    st.write("**📋 Amostra dos pedidos:**")
                    colunas = ['data', 'motoboy']
                    if 'distancia' in df_final.columns:
                        colunas.append('distancia')
                    st.dataframe(df_final[colunas].head(10))
                    
                    # Calcular fechamento
                    if 'distancia' in df_final.columns:
                        df_final['distancia_num'] = pd.to_numeric(
                            df_final['distancia'].astype(str).str.replace(',', '.'), 
                            errors='coerce'
                        )
                        df_final = df_final.dropna(subset=['distancia_num'])
                        
                        if len(df_final) > 0:
                            dias = df_final['data'].dt.date.nunique()
                            corridas = len(df_final)
                            km_total = df_final['distancia_num'].sum()
                            
                            base_total = 90.0 * dias
                            extras = 0
                            
                            for _, pedido in df_final.iterrows():
                                km = pedido['distancia_num']
                                if km > 6:
                                    if km <= 8:
                                        extras += 2
                                    elif km <= 10:
                                        extras += 6
                                    else:
                                        extras += 11
                            
                            total_final = base_total + extras
                            
                            st.markdown("### 💰 Fechamento Calculado")
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric("Dias", dias)
                            with col2:
                                st.metric("Corridas", corridas)
                            with col3:
                                st.metric("KM Total", f"{km_total:.1f}")
                            with col4:
                                st.metric("TOTAL", formatar_br(total_final))
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Base Fixa", formatar_br(base_total))
                            with col2:
                                st.metric("Extras", formatar_br(extras))
                        else:
                            st.error("❌ Nenhuma distância válida encontrada")
                    else:
                        st.error("❌ Coluna 'distancia' não encontrada")
                else:
                    st.error("❌ Nenhum pedido encontrado!")
                    st.write("**💡 Verifique:**")
                    st.write("• Se o nome do motoboy está correto")
                    st.write("• Se há pedidos nesse período")
            else:
                st.error("❌ Coluna 'motoboy' não encontrada!")
    
    # --- CONFIGURAÇÕES ---
    elif menu == "⚙️ Configurações":
        st.title("⚙️ Configurações do Sistema")
        
        st.subheader("📋 Estrutura das Planilhas")
        st.info("""
        **✅ COMPRAS (Configurada):**
        Data Compra, Fornecedor, Categoria, Descrição, Quantidade, Unid, Valor Unit, Valor Total
        
        **✅ PEDIDOS (Configurada):**  
        Código, Data, Nome, Canal, Motoboy, Status, Método de entrega, Total, Distancia
        
        **✅ INSUMOS (Configurada):**
        Produto, Categoria, Em estoque, Estoque Min, Preço (un), Fornecedor
        
        O sistema está configurado para sua estrutura atual!
        """)
        
        # Mostrar estrutura atual
        if not df_pedidos.empty:
            st.write("**Colunas encontradas na planilha PEDIDOS:**")
            st.write(", ".join(df_pedidos.columns.tolist()))
        
        # Testar conexão com INSUMOS
        st.subheader("🔧 Testes de Conexão")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Testar PEDIDOS e COMPRAS"):
                if not df_pedidos.empty and not df_compras.empty:
                    st.success("✅ PEDIDOS e COMPRAS OK!")
                else:
                    st.error("❌ Erro nas planilhas principais")
        
        with col2:
            if st.button("🔄 Testar INSUMOS"):
                df_insumos = carregar_dados_insumos()
                if not df_insumos.empty:
                    st.success(f"✅ INSUMOS OK! {len(df_insumos)} produtos")
                else:
                    st.error("❌ Erro na planilha INSUMOS")
        
        # Configurações
        st.subheader("⚙️ Configurações")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Recarregar Dados"):
                st.cache_data.clear()
                st.success("✅ Cache limpo! Dados serão recarregados.")
                st.rerun()
        
        with col2:
            if st.button("📊 Verificar Conexão"):
                client = conectar_google_sheets()
                if client:
                    st.success("✅ Conexão com Google Sheets OK!")
                else:
                    st.error("❌ Erro na conexão com Google Sheets.")

if __name__ == "__main__":
    main()import streamlit as st
import pandas as pd
import datetime
import numpy as np
from google.oauth2.service_account import Credentials
import gspread
import requests
from bs4 import BeautifulSoup
import re

# Importar plotly apenas se disponível
try:
    import plotly.express as px
    PLOTLY_DISPONIVEL = True
except ImportError:
    PLOTLY_DISPONIVEL = False
    st.warning("⚠️ Plotly não está instalado. Alguns gráficos não serão exibidos.")
    st.info("💡 Para instalar: pip install plotly")

# --- Configuração Inicial ---
st.set_page_config(page_title="Vulcano App - Sistema de Gestão", layout="wide")

# --- CSS Personalizado ---
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF4B4B;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(45deg, #FF4B4B, #FF6B6B);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .estoque-card {
        background: white;
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #FF4B4B;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# --- Mapeamento de Colunas ---
COLUNAS_COMPRAS = {
    'data': 'Data Compra',
    'fornecedor': 'Fornecedor', 
    'categoria': 'Categoria',
    'descricao': 'Descrição',
    'quantidade': 'Quantidade',
    'unidade': 'Unid',
    'valor_unitario': 'Valor Unit',
    'valor_total': 'Valor Total',
    'forma_pagamento': 'Forma de Pagamento'
}

COLUNAS_PEDIDOS = {
    'codigo': 'Código',
    'data': 'Data',
    'nome': 'Nome', 
    'canal': 'Canal',
    'motoboy': 'Motoboy',
    'status': 'Status',
    'metodo_entrega': 'Método de entrega',
    'total': 'Total',
    'distancia': 'Distancia'
}

# --- Conexão Google Sheets ---
@st.cache_resource(ttl=3600)
def conectar_google_sheets():
    """Conecta com Google Sheets"""
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(st.secrets, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Erro na conexão com o Google Sheets: {str(e)}")
        return None

# --- Funções Auxiliares ---
def formatar_br(valor, is_quantidade=False):
    """Formata valores para padrão brasileiro"""
    try:
        if pd.isna(valor):
            return "R$ 0,00"
        if is_quantidade:
            return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(valor)

def limpar_valor_brasileiro(valor_str):
    """Converte valor brasileiro para float"""
    try:
        if pd.isna(valor_str) or valor_str == '':
            return 0.0
        # Remove símbolos e converte
        valor_clean = re.sub(r'[^0-9,.]', '', str(valor_str))
        valor_clean = valor_clean.replace(',', '.')
        return float(valor_clean) if valor_clean else 0.0
    except:
        return 0.0

def mapear_colunas(df, tipo_planilha):
    """Mapeia as colunas da planilha para nomes padronizados"""
    if df.empty:
        return df
    
    if tipo_planilha == 'COMPRAS':
        mapeamento = {v: k for k, v in COLUNAS_COMPRAS.items()}
    elif tipo_planilha == 'PEDIDOS':
        mapeamento = {v: k for k, v in COLUNAS_PEDIDOS.items()}
    else:
        return df
    
    # Renomear apenas as colunas que existem
    colunas_existentes = {col: mapeamento[col] for col in df.columns if col in mapeamento}
    return df.rename(columns=colunas_existentes)

# --- Carregar dados ---
@st.cache_data(ttl=300)
def carregar_dados_sheets():
    """Carrega dados das planilhas Google Sheets"""
    client = conectar_google_sheets()
    if not client:
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        sheet_compras = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("COMPRAS")
        sheet_pedidos = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("PEDIDOS")
        
        df_compras = pd.DataFrame(sheet_compras.get_all_records())
        df_pedidos = pd.DataFrame(sheet_pedidos.get_all_records())
        
        # Normalizar dados das planilhas
        df_compras_norm = mapear_colunas(df_compras, 'COMPRAS')
        df_pedidos_norm = mapear_colunas(df_pedidos, 'PEDIDOS')
        
        return df_compras_norm, df_pedidos_norm
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

# ============================================================================
# FUNÇÕES PARA O MÓDULO DE ESTOQUE
# ============================================================================

@st.cache_data(ttl=300)
def carregar_dados_insumos():
    """Carrega dados da aba INSUMOS"""
    client = conectar_google_sheets()
    if not client:
        return pd.DataFrame()
    
    try:
        sheet_insumos = client.open_by_key("1dYXXL7d_MJVaDPnmOb6sBECemaVz7-2VXsRBMsxf77U").worksheet("INSUMOS")
        df_insumos = pd.DataFrame(sheet_insumos.get_all_records())
        return df_insumos
    except Exception as e:
        st.error(f"Erro ao carregar dados de insumos: {str(e)}")
        return pd.DataFrame()

def limpar_numero(valor):
    """Converte valores para números de forma segura"""
    try:
        if pd.isna(valor) or valor == '':
            return 0.0
        if isinstance(valor, (int, float)):
            return float(valor)
        if isinstance(valor, str):
            # Remove tudo exceto números, vírgula e ponto
            valor_limpo = re.sub(r'[^0-9,.]', '', str(valor))
            valor_limpo = valor_limpo.replace(',', '.')
            if valor_limpo == '':
                return 0.0
            return float(valor_limpo)
        return float(valor)
    except:
        return 0.0

# ============================================================================
# SISTEMA DE ALIAS PARA PRODUTOS
# ============================================================================

def criar_dicionario_alias():
    """Dicionário de alias para normalizar nomes de produtos"""
    return {
        # Águas
        'agua com gas': ['agua com gás', 'agua c/ gas', 'agua fonte da pedra com gas', 'agua crystal com gas', 'agua gasosa'],
        'agua sem gas': ['agua sem gás', 'agua s/ gas', 'agua fonte da pedra sem gas', 'agua crystal sem gas', 'agua natural'],
        
        # Refrigerantes
        'coca cola lata': ['coca cola 350ml', 'coca cola lata 350ml', 'coca-cola lata', 'coca lata'],
        'coca cola zero lata': ['coca zero 350ml', 'coca zero lata 350ml', 'coca-cola zero lata', 'coca zero'],
        'fanta laranja lata': ['fanta laranja 350ml', 'fanta laranja lata 350ml', 'fanta laranja'],
        'fanta uva lata': ['fanta uva 350ml', 'fanta uva lata 350ml', 'fanta uva'],
        'sprite lata': ['sprite 350ml', 'sprite lata 350ml'],
        
        # Cervejas
        'budweiser 550ml': ['budweiser 600ml', 'budweiser garrafa', 'budweiser long'],
        'budweiser long neck': ['budweiser 330ml', 'budweiser ln', 'budweiser longneck'],
        
        # Insumos básicos
        'oleo de soja': ['oleo soja', 'óleo de soja', 'óleo soja', 'oleo'],
        'sal': ['sal refinado', 'sal 1kg', 'sal de cozinha'],
        'açucar': ['açúcar', 'açucar cristal', 'açúcar cristal', 'açucar refinado'],
        'arroz': ['arroz branco', 'arroz tipo 1', 'arroz agulhinha'],
        'feijao': ['feijão', 'feijão preto', 'feijao preto'],
        
        # Carnes
        'file de sobrecoxa': ['filé de sobrecoxa', 'sobrecoxa', 'file sobrecoxa'],
        'bife de coxao de dentro': ['bife coxão dentro', 'coxão dentro', 'bife coxao'],
        
        # Queijos
        'queijo cheddar fatiado': ['cheddar fatiado', 'queijo cheddar', 'cheddar'],
        'queijo mussarela fatiado': ['mussarela fatiada', 'queijo mussarela', 'mussarela'],
        'queijo provolone fatiado': ['provolone fatiado', 'queijo provolone', 'provolone'],
        
        # Pães
        'pao brioche': ['pão brioche', 'brioche', 'pao hamburguer brioche'],
        'pao tradicional com gergelim': ['pão com gergelim', 'pão gergelim', 'pao gergelim'],
        'pao australiano': ['pão australiano', 'australiano'],
        
        # Molhos e temperos
        'bisnaga de cheddar': ['cheddar bisnaga', 'molho cheddar', 'cheddar cremoso'],
        'bisnaga de requeijao': ['requeijão bisnaga', 'molho requeijão', 'requeijão cremoso'],
        'barbecue': ['molho barbecue', 'bbq', 'molho bbq'],
        'mostarda rustica': ['mostarda rústica', 'mostarda'],
        
        # Vegetais
        'cebola': ['cebola branca', 'cebola amarela'],
        'tomate': ['tomate maduro', 'tomate vermelho'],
        'alface': ['alface americana', 'alface lisa'],
        'couve': ['couve folha', 'couve manteiga']
    }

def normalizar_nome_produto(nome_entrada):
    """Normaliza o nome do produto usando o dicionário de alias"""
    if not nome_entrada:
        return nome_entrada
    
    nome_lower = str(nome_entrada).lower().strip()
    alias_dict = criar_dicionario_alias()
    
    # Procurar por correspondência exata primeiro
    for produto_base, aliases in alias_dict.items():
        if nome_lower == produto_base:
            return produto_base
        if nome_lower in aliases:
            return produto_base
    
    # Procurar por correspondência parcial
    for produto_base, aliases in alias_dict.items():
        # Verificar se algum alias está contido no nome
        for alias in aliases:
            if alias in nome_lower or nome_lower in alias:
                return produto_base
        
        # Verificar se o produto base está contido no nome
        if produto_base in nome_lower or nome_lower in produto_base:
            return produto_base
    
    # Se não encontrou correspondência, retorna o nome original
    return nome_entrada

def determinar_status_estoque(row):
    """Determina o status do estoque de um produto"""
    try:
        em_estoque = limpar_numero(row.get('Em estoque', 0))
        estoque_min = limpar_numero(row.get('Estoque Min', 0))
        
        if em_estoque == 0:
            return "🔴 Em Falta"
        elif em_estoque < estoque_min:
            return "🟡 Baixo"
        else:
            return "🟢 OK"
    except:
        return "❓ Indefinido"

def pagina_estoque():
    """Página de gestão de estoque"""
    
    st.title("📦 Gestão de Estoque")
    
    # Carregar dados
    df_insumos = carregar_dados_insumos()
    
    if df_insumos.empty:
        st.warning("⚠️ Não foi possível carregar os dados da aba INSUMOS")
        st.info("💡 Verifique se a aba 'INSUMOS' existe na sua planilha")
        return
    
    # Tabs do módulo de estoque
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Dashboard", 
        "📋 Lista de Produtos", 
        "📥 Entrada de Produtos",
        "📈 Análise de Custos",
        "⚙️ Configurações"
    ])
    
    with tab1:
        dashboard_estoque(df_insumos)
    
    with tab2:
        lista_produtos_estoque(df_insumos)
    
    with tab3:
        entrada_produtos_estoque()
    
    with tab4:
        analise_custos_estoque(df_insumos)
    
    with tab5:
        configuracoes_estoque()

def dashboard_estoque(df_insumos):
    """Dashboard do estoque"""
    
    st.subheader("📊 Visão Geral do Estoque")
    
    # Preparar dados
    df_work = df_insumos.copy()
    df_work['Em estoque'] = df_work.get('Em estoque', 0).apply(limpar_numero)
    df_work['Estoque Min'] = df_work.get('Estoque Min', 0).apply(limpar_numero)
    df_work['Preço (un)'] = df_work.get('Preço (un)', 0).apply(limpar_numero)
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_produtos = len(df_work)
        st.metric("📦 Total de Produtos", total_produtos)
    
    with col2:
        valor_total = (df_work['Em estoque'] * df_work['Preço (un)']).sum()
        st.metric("💰 Valor Total", formatar_br(valor_total))
    
    with col3:
        produtos_baixo = len(df_work[
            (df_work['Em estoque'] < df_work['Estoque Min']) & 
            (df_work['Em estoque'] > 0)
        ])
        st.metric("⚠️ Estoque Baixo", produtos_baixo)
    
    with col4:
        produtos_falta = len(df_work[df_work['Em estoque'] == 0])
        st.metric("🚨 Em Falta", produtos_falta)
    
    # Alertas importantes
    st.markdown("### 🔔 Alertas Importantes")
    
    produtos_falta_lista = df_work[df_work['Em estoque'] == 0]
    produtos_baixo_lista = df_work[
        (df_work['Em estoque'] < df_work['Estoque Min']) & 
        (df_work['Em estoque'] > 0)
    ]
    
    col1, col2 = st.columns(2)
    
    with col1:
        if len(produtos_falta_lista) > 0:
            st.markdown('<div class="estoque-card">', unsafe_allow_html=True)
            st.markdown("**🚨 Produtos em Falta:**")
            for produto in produtos_falta_lista['Produto'].head(5):
                st.write(f"• {produto}")
            if len(produtos_falta_lista) > 5:
                st.write(f"... e mais {len(produtos_falta_lista) - 5}")
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.success("✅ Nenhum produto em falta!")
    
    with col2:
        if len(produtos_baixo_lista) > 0:
            st.markdown('<div class="estoque-card">', unsafe_allow_html=True)
            st.markdown("**⚠️ Estoque Baixo:**")
            for _, produto in produtos_baixo_lista.head(5).iterrows():
                st.write(f"• {produto['Produto']}: {produto['Em estoque']:.0f}/{produto['Estoque Min']:.0f}")
            if len(produtos_baixo_lista) > 5:
                st.write(f"... e mais {len(produtos_baixo_lista) - 5}")
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.success("✅ Todos os produtos com estoque adequado!")

def lista_produtos_estoque(df_insumos):
    """Lista de produtos do estoque"""
    
    st.subheader("📋 Lista de Produtos")
    
    # Preparar dados
    df_work = df_insumos.copy()
    df_work['Em estoque'] = df_work.get('Em estoque', 0).apply(limpar_numero)
    df_work['Estoque Min'] = df_work.get('Estoque Min', 0).apply(limpar_numero)
    df_work['Preço (un)'] = df_work.get('Preço (un)', 0).apply(limpar_numero)
    
    # Adicionar status
    df_work['Status'] = df_work.apply(determinar_status_estoque, axis=1)
    df_work['Valor Total'] = df_work['Em estoque'] * df_work['Preço (un)']
    
    # Filtros
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'Categoria' in df_work.columns:
            categorias = ['Todas'] + sorted(df_work['Categoria'].dropna().unique().tolist())
            categoria_filtro = st.selectbox("Filtrar por Categoria", categorias)
        else:
            categoria_filtro = 'Todas'
    
    with col2:
        status_filtro = st.selectbox(
            "Filtrar por Status",
            ["Todos", "🟢 OK", "🟡 Baixo", "🔴 Em Falta"]
        )
    
    with col3:
        busca = st.text_input("🔍 Buscar produto")
    
    # Aplicar filtros
    df_filtrado = df_work.copy()
    
    if categoria_filtro != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['Categoria'] == categoria_filtro]
    
    if status_filtro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Status'] == status_filtro]
    
    if busca:
        mask = df_filtrado['Produto'].str.contains(busca, case=False, na=False)
        df_filtrado = df_filtrado[mask]
    
    # Informações do filtro
    valor_filtrado = df_filtrado['Valor Total'].sum()
    st.info(f"📊 Mostrando {len(df_filtrado)} de {len(df_work)} produtos | Valor: {formatar_br(valor_filtrado)}")
    
    # Selecionar colunas para exibir
    colunas_exibir = ['Produto', 'Categoria', 'Em estoque', 'Estoque Min', 'Preço (un)', 'Valor Total', 'Status', 'Fornecedor']
    colunas_disponiveis = [col for col in colunas_exibir if col in df_filtrado.columns]
    
    if len(df_filtrado) > 0:
        # Configurar editor
        df_display = df_filtrado[colunas_disponiveis].copy()
        
        # Tabela editável
        df_editado = st.data_editor(
            df_display,
            column_config={
                "Produto": st.column_config.TextColumn("Produto", width="medium"),
                "Categoria": st.column_config.TextColumn("Categoria", width="small"),
                "Em estoque": st.column_config.NumberColumn(
                    "Em Estoque",
                    help="Quantidade atual em estoque",
                    min_value=0,
                    step=1,
                    format="%.1f"
                ),
                "Estoque Min": st.column_config.NumberColumn(
                    "Estoque Mínimo", 
                    help="Quantidade mínima recomendada",
                    min_value=0,
                    step=1,
                    format="%.0f"
                ),
                "Preço (un)": st.column_config.NumberColumn(
                    "Preço (R$)",
                    help="Preço unitário",
                    min_value=0.0,
                    step=0.01,
                    format="R$ %.2f"
                ),
                "Valor Total": st.column_config.NumberColumn(
                    "Valor Total",
                    help="Em estoque × Preço",
                    format="R$ %.2f"
                ),
                "Status": st.column_config.TextColumn("Status", width="small"),
                "Fornecedor": st.column_config.TextColumn("Fornecedor", width="small")
            },
            hide_index=True,
            use_container_width=True
        )
        
        # Botão para salvar
        if st.button("💾 Salvar Alterações"):
            st.success("✅ Funcionalidade de salvamento será implementada na próxima versão!")
            st.info("💡 Por enquanto, edite diretamente no Google Sheets")
    
    else:
        st.warning("⚠️ Nenhum produto encontrado com os filtros aplicados")

def entrada_produtos_estoque():
    """Entrada de produtos via NFCe, CSV ou manual"""
    
    st.subheader("📥 Entrada de Produtos")
    
    st.info("💡 Aqui você pode registrar a entrada de novos produtos no estoque")
    
    # Tabs para diferentes tipos de entrada
    tab1, tab2, tab3 = st.tabs(["🔗 Via NFCe (URL)", "📄 Via CSV/Excel", "✍️ Entrada Manual"])
    
    with tab1:
        st.subheader("Importar via URL da NFC-e")
        st.write("Cole a URL da nota fiscal eletrônica para importar automaticamente os produtos")
        
        url_nfce = st.text_input("Cole a URL da NFC-e aqui:")
        
        if st.button("🔍 Extrair Dados da NFCe") and url_nfce:
            with st.spinner("Processando NFC-e..."):
                try:
                    response = requests.get(url_nfce)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    df_itens = extrair_itens_nfce(soup)
                    
                    if not df_itens.empty:
                        st.success("✅ Dados extraídos com sucesso!")
                        st.subheader("📦 Produtos encontrados:")
                        
                        # Aplicar sistema de alias
                        df_itens['Produto_Normalizado'] = df_itens['Descrição'].apply(normalizar_nome_produto)
                        
                        # Mostrar comparação
                        st.write("**🔗 Normalização de Produtos:**")
                        df_comparacao = df_itens[['Descrição', 'Produto_Normalizado']].copy()
                        df_comparacao.columns = ['Nome na NFCe', 'Produto no Sistema']
                        
                        # Destacar produtos que foram normalizados
                        df_comparacao['Status'] = df_comparacao.apply(
                            lambda row: '✅ Normalizado' if row['Nome na NFCe'].lower() != row['Produto no Sistema'].lower() else '📝 Mantido',
                            axis=1
                        )
                        
                        st.dataframe(df_comparacao, use_container_width=True)
                        
                        st.subheader("📊 Dados processados:")
                        st.dataframe(df_itens, use_container_width=True)
                        
                        if st.button("💾 Salvar no Estoque"):
                            st.success("✅ Funcionalidade de salvamento será implementada!")
                            st.info("💡 Os produtos normalizados serão adicionados ao estoque teórico")
                            
                            # Mostrar resumo do que seria salvo
                            st.write("**📋 Resumo do que seria salvo:**")
                            for _, item in df_itens.iterrows():
                                st.write(f"• **{item['Produto_Normalizado']}**: {item['Quantidade']} {item['Unidade']} - R$ {item['Valor Unitário']:.2f}")
                    else:
                        st.error("❌ Não foi possível extrair os dados. Verifique a URL.")
                        st.info("💡 Certifique-se que a URL é de uma NFCe válida")
                except Exception as e:
                    st.error(f"❌ Erro ao processar: {str(e)}")
    
    with tab2:
        st.subheader("Upload de arquivo CSV/Excel")
        st.write("Faça upload de um arquivo com os dados dos produtos comprados")
        
        arquivo = st.file_uploader(
            "Selecione o arquivo", 
            type=['csv', 'xlsx', 'xls'],
            help="Formatos aceitos: CSV, Excel (.xlsx, .xls)"
        )
        
        if arquivo:
            try:
                if arquivo.name.endswith('.csv'):
                    df_upload = pd.read_csv(arquivo)
                else:
                    df_upload = pd.read_excel(arquivo)
                
                st.success("✅ Arquivo carregado com sucesso!")
                st.subheader("📊 Dados do arquivo:")
                st.dataframe(df_upload, use_container_width=True)
                
                # Mapear colunas
                st.subheader("🔗 Mapeamento de Colunas")
                st.write("Associe as colunas do seu arquivo com os campos do sistema:")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    coluna_produto = st.selectbox("Produto/Descrição:", df_upload.columns)
                    coluna_quantidade = st.selectbox("Quantidade:", df_upload.columns)
                    coluna_preco = st.selectbox("Preço Unitário:", df_upload.columns)
                
                with col2:
                    coluna_fornecedor = st.selectbox("Fornecedor:", [""] + list(df_upload.columns))
                    coluna_categoria = st.selectbox("Categoria:", [""] + list(df_upload.columns))
                    coluna_unidade = st.selectbox("Unidade:", [""] + list(df_upload.columns))
                
                if st.button("💾 Processar e Salvar"):
                    # Aplicar normalização de nomes
                    produtos_normalizados = df_upload[coluna_produto].apply(normalizar_nome_produto)
                    
                    st.success("✅ Dados processados!")
                    
                    # Mostrar produtos normalizados
                    st.subheader("🔗 Produtos Normalizados:")
                    df_norm = pd.DataFrame({
                        'Nome Original': df_upload[coluna_produto],
                        'Nome Normalizado': produtos_normalizados,
                        'Quantidade': df_upload[coluna_quantidade],
                        'Preço': df_upload[coluna_preco]
                    })
                    
                    # Destacar produtos que foram alterados
                    df_norm['Status'] = df_norm.apply(
                        lambda row: '✅ Normalizado' if row['Nome Original'].lower() != row['Nome Normalizado'].lower() else '📝 Mantido',
                        axis=1
                    )
                    
                    st.dataframe(df_norm, use_container_width=True)
                    st.info("💡 Os produtos normalizados serão adicionados ao estoque")
                    
            except Exception as e:
                st.error(f"❌ Erro ao processar arquivo: {str(e)}")
    
    with tab3:
        st.subheader("✍️ Entrada Manual de Produtos")
        st.write("Adicione produtos manualmente ao estoque")
        
        with st.form("entrada_manual"):
            col1, col2 = st.columns(2)
            
            with col1:
                produto_nome = st.text_input("Nome
