import streamlit as st
import duckdb

# Configuração inicial da página
st.set_page_config(page_title="Consulta de CNPJ", page_icon="🏢", layout="wide")

# ==========================================
# BARRA LATERAL (FILTROS ADICIONAIS - REQUISITO DO DESAFIO)
# ==========================================
st.sidebar.title("🔍 Filtros Adicionais")
st.sidebar.markdown("Use este filtro para refinar a busca de filiais.")
uf_filtro = st.sidebar.text_input("Filtrar filiais por UF (Ex: SC, SP, RJ):", max_chars=2).upper()

st.title("🏢 Consulta de Dados do CNPJ")
st.markdown("Busque por empresas, sócios e estabelecimentos na base de dados da Receita Federal.")

# Conexão com o banco (em cache para performance)
@st.cache_resource
def conectar_banco():
    try:
        return duckdb.connect('banco_cnpj.duckdb', read_only=True)
    except Exception as e:
        st.error(f"Erro ao conectar no banco de dados: {e}")
        return None

conn = conectar_banco()

if conn:
    termo_busca = st.text_input("Digite a Razão Social ou os 8 primeiros dígitos do CNPJ (CNPJ Básico):")

    if termo_busca:
        st.subheader("Resultados da Busca (Empresas)")
        
        # Query 1: EMPRESAS (Fazendo JOIN para traduzir a Natureza Jurídica)
        query_empresas = f"""
            SELECT 
                e.cnpj_basico,
                e.razao_social,
                e.natureza_juridica || ' - ' || COALESCE(n.descricao, 'N/A') AS natureza_juridica,
                e.porte_empresa,
                e.capital_social
            FROM empresas e
            LEFT JOIN naturezas n ON REPLACE(e.natureza_juridica, '"', '') = REPLACE(n.codigo, '"', '')
            WHERE REPLACE(e.cnpj_basico, '"', '') = '{termo_busca}' 
               OR e.razao_social LIKE '%{termo_busca.upper()}%'
            LIMIT 50
        """
        
        try:
            resultado = conn.execute(query_empresas)
            colunas = ["CNPJ Básico", "Razão Social", "Natureza Jurídica", "Cód. Porte", "Capital Social (R$)"]
            dados = resultado.fetchall()
            
            if dados:
                # Limpeza das aspas e formatação para o Streamlit
                dados_limpos = [[str(item).replace('"', '') if item else "" for item in linha] for linha in dados]
                dados_formatados = [dict(zip(colunas, linha)) for linha in dados_limpos]
                
                st.dataframe(dados_formatados, use_container_width=True)
                st.divider()
                
                # Menu de seleção da empresa
                opcoes_dropdown = [f"{empresa['CNPJ Básico']} - {empresa['Razão Social']}" for empresa in dados_formatados]
                empresa_selecionada = st.selectbox(
                    "Selecione uma empresa acima para ver seus Sócios e Estabelecimentos:",
                    options=opcoes_dropdown
                )

                if empresa_selecionada:
                    # Extrai apenas o CNPJ usando nossa técnica à prova de falhas
                    cnpj_selecionado = empresa_selecionada.split(" - ").pop(0).strip()
                    
                    # ==========================================
                    # QUERY 2: SÓCIOS (REQUISITO DO DESAFIO)
                    # ==========================================
                    st.subheader("👥 Quadro de Sócios")
                    query_socios = f"""
                        SELECT nome_socio, qualificacao_socio, faixa_etaria 
                        FROM socios 
                        WHERE REPLACE(cnpj_basico, '"', '') = '{cnpj_selecionado}'
                    """
                    dados_socios = conn.execute(query_socios).fetchall()
                    
                    if dados_socios:
                        col_soc = ["Nome do Sócio / Razão Social", "Cód. Qualificação", "Cód. Faixa Etária"]
                        socios_limpos = [[str(i).replace('"', '') if i else "" for i in l] for l in dados_socios]
                        st.dataframe([dict(zip(col_soc, l)) for l in socios_limpos], use_container_width=True)
                    else:
                        st.info("Nenhum sócio encontrado para esta empresa.")

                    # ==========================================
                    # QUERY 3: ESTABELECIMENTOS COM FILTRO E TRADUÇÃO
                    # ==========================================
                    st.subheader("📍 Filiais e Estabelecimentos")
                    
                    # JOIN com Municipios e Cnaes para traduzir os códigos
                    query_filiais = f"""
                        SELECT 
                            est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj_completo,
                            est.nome_fantasia,
                            est.situacao_cadastral,
                            est.cnae_principal || ' - ' || COALESCE(c.descricao, 'N/A') AS cnae_principal,
                            est.tipo_logradouro || ' ' || est.logradouro || ', ' || est.numero AS endereco,
                            COALESCE(m.descricao, 'N/A') || '/' || est.uf AS cidade_uf
                        FROM estabelecimentos est
                        LEFT JOIN municipios m ON REPLACE(est.municipio, '"', '') = REPLACE(m.codigo, '"', '')
                        LEFT JOIN cnaes c ON REPLACE(est.cnae_principal, '"', '') = REPLACE(c.codigo, '"', '')
                        WHERE REPLACE(est.cnpj_basico, '"', '') = '{cnpj_selecionado}'
                    """
                    
                    # Aplica o filtro lateral se o usuário tiver digitado alguma coisa
                    if uf_filtro:
                        query_filiais += f" AND REPLACE(est.uf, '\"', '') = '{uf_filtro}'"
                    
                    dados_filiais = conn.execute(query_filiais).fetchall()

                    if dados_filiais:
                        col_filiais = ["CNPJ Completo", "Nome Fantasia", "Situação Cadastral", "CNAE Principal", "Endereço", "Cidade/UF"]
                        filiais_limpos = [[str(i).replace('"', '') if i else "" for i in l] for l in dados_filiais]
                        st.dataframe([dict(zip(col_filiais, l)) for l in filiais_limpos], use_container_width=True)
                    else:
                        if uf_filtro:
                            st.warning(f"Nenhum estabelecimento encontrado para a UF: {uf_filtro}")
                        else:
                            st.info("Nenhum estabelecimento encontrado para esta empresa (Pode ser um erro na base da Receita).")

            else:
                st.warning(f"Nenhuma empresa encontrada para o termo '{termo_busca}'.")
                
        except Exception as e:
            st.error(f"Ocorreu um erro na consulta: {e}")