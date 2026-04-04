import streamlit as st
import duckdb

# Configuração inicial da página
st.set_page_config(page_title="Consulta de CNPJ", page_icon="🏢", layout="wide")

# ==========================================
# BARRA LATERAL (FILTROS ADICIONAIS - REQUISITO DO EDITAL)
# ==========================================
st.sidebar.title("🔍 Filtros Avançados")
st.sidebar.markdown("Refine sua busca utilizando os parâmetros do edital.")

st.sidebar.markdown("### 🏢 Filtro de Empresas")
opcoes_porte = {
    "Todos": "", 
    "Não Informado": "00", 
    "Microempresa (ME)": "01", 
    "Empresa de Pequeno Porte (EPP)": "03", 
    "Demais (Grandes/Médias)": "05"
}
porte_selecionado = st.sidebar.selectbox("Porte da Empresa:", list(opcoes_porte.keys()))
porte_filtro = opcoes_porte[porte_selecionado]

st.sidebar.markdown("### 📍 Filtro de Filiais")

# 1. Filtro de UF
lista_ufs = ["Todas", "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
uf_selecionada = st.sidebar.selectbox("Estado (UF):", lista_ufs)
uf_filtro = "" if uf_selecionada == "Todas" else uf_selecionada

# 2. Filtro de Município
municipio_filtro = st.sidebar.text_input("Município (Ex: FLORIANOPOLIS):").upper().strip()

# 3. Filtro de CNAE Principal (Busca por código ou descrição)
cnae_filtro = st.sidebar.text_input("CNAE Principal (Ex: 6201501 ou Computação):").upper().strip()

# 4. Filtro de Situação Cadastral
opcoes_situacao = {
    "Todas": "", 
    "Nula": "01", 
    "Ativa": "02", 
    "Suspensa": "03", 
    "Inapta": "04", 
    "Baixada": "08"
}
situacao_selecionada = st.sidebar.selectbox("Situação Cadastral:", list(opcoes_situacao.keys()))
situacao_filtro = opcoes_situacao[situacao_selecionada]

# ==========================================
# CORPO PRINCIPAL DA APLICAÇÃO
# ==========================================
st.title("🏢 Consulta de Dados do CNPJ")
st.markdown("Busque por empresas, sócios e estabelecimentos com processamento DuckDB.")

@st.cache_resource
def conectar_banco():
    try:
        # Modo read_only garante que o banco não seja corrompido durante consultas
        return duckdb.connect('banco_cnpj.duckdb', read_only=True)
    except Exception as e:
        st.error(f"Erro ao conectar no banco de dados: {e}")
        return None

conn = conectar_banco()

if conn:
    termo_busca = st.text_input("Digite a Razão Social ou os 8 primeiros dígitos do CNPJ:")

    if termo_busca:
        termo_limpo = termo_busca.upper().strip()
        termo_like = f"%{termo_limpo}%"
        
        st.subheader("Resultados da Busca (Empresas)")
        
        # SQL Parametrizado para segurança (SQL Injection)
        query_empresas = """
            SELECT 
                e.cnpj_basico,
                e.razao_social,
                e.natureza_juridica || ' - ' || COALESCE(n.descricao, 'N/A') AS natureza_juridica,
                CASE REPLACE(e.porte_empresa, '"', '')
                    WHEN '00' THEN 'Não Informado'
                    WHEN '01' THEN 'Microempresa (ME)'
                    WHEN '03' THEN 'Empresa de Pequeno Porte (EPP)'
                    WHEN '05' THEN 'Demais'
                    ELSE e.porte_empresa 
                END AS porte_empresa,
                e.capital_social
            FROM empresas e
            LEFT JOIN naturezas n ON REPLACE(e.natureza_juridica, '"', '') = REPLACE(n.codigo, '"', '')
            WHERE (REPLACE(e.cnpj_basico, '"', '') = ? OR e.razao_social LIKE ?)
        """
        parametros_empresas = [termo_limpo, termo_like]

        if porte_filtro:
            query_empresas += " AND REPLACE(e.porte_empresa, '\"', '') = ?"
            parametros_empresas.append(porte_filtro)

        query_empresas += " ORDER BY e.razao_social LIMIT 50"
        
        try:
            resultado = conn.execute(query_empresas, parametros_empresas).fetchall()
            colunas = ["CNPJ Básico", "Razão Social", "Natureza Jurídica", "Porte", "Capital Social (R$)"]
            
            if resultado:
                # Limpeza de aspas literais e formatação
                dados_limpos = [[str(item).replace('"', '') if item else "" for item in linha] for linha in resultado]
                dados_df = [dict(zip(colunas, linha)) for linha in dados_limpos]
                
                st.dataframe(dados_df, use_container_width=True)
                st.divider()
                
                opcoes_dropdown = [f"{emp['CNPJ Básico']} - {emp['Razão Social']}" for emp in dados_df]
                empresa_selecionada = st.selectbox(
                    "Selecione uma empresa para detalhamento:",
                    options=opcoes_dropdown
                )

                if empresa_selecionada:
                    cnpj_selecionado = empresa_selecionada.split(" - ").pop(0).strip()
                    
                    # 👥 QUADRO DE SÓCIOS
                    st.subheader("👥 Quadro de Sócios")
                    query_socios = """
                        SELECT 
                            s.nome_socio, 
                            s.qualificacao_socio || ' - ' || COALESCE(q.descricao, 'N/A') AS qualificacao, 
                            s.faixa_etaria 
                        FROM socios s
                        LEFT JOIN qualificacoes q ON REPLACE(s.qualificacao_socio, '"', '') = REPLACE(q.codigo, '"', '')
                        WHERE REPLACE(s.cnpj_basico, '"', '') = ?
                    """
                    dados_socios = conn.execute(query_socios, [cnpj_selecionado]).fetchall()
                    
                    if dados_socios:
                        col_soc = ["Nome do Sócio / Razão Social", "Qualificação", "Cód. Faixa Etária"]
                        socios_limpos = [[str(i).replace('"', '') if i else "" for i in l] for l in dados_socios]
                        st.dataframe([dict(zip(col_soc, l)) for l in socios_limpos], use_container_width=True)
                    else:
                        st.info("Nenhum sócio encontrado.")

                    # 📍 ESTABELECIMENTOS (COM TODOS OS FILTROS DO EDITAL)
                    st.subheader("📍 Filiais e Estabelecimentos")
                    
                    query_filiais = """
                        SELECT 
                            est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj_completo,
                            est.nome_fantasia,
                            est.situacao_cadastral || ' (Motivo: ' || COALESCE(mot.descricao, 'N/A') || ')' AS situacao,
                            est.cnae_principal || ' - ' || COALESCE(c.descricao, 'N/A') AS cnae,
                            est.tipo_logradouro || ' ' || est.logradouro || ', ' || est.numero AS endereco,
                            COALESCE(m.descricao, 'N/A') || '/' || REPLACE(est.uf, '"', '') AS cidade_uf
                        FROM estabelecimentos est
                        LEFT JOIN municipios m ON REPLACE(est.municipio, '"', '') = REPLACE(m.codigo, '"', '')
                        LEFT JOIN cnaes c ON REPLACE(est.cnae_principal, '"', '') = REPLACE(c.codigo, '"', '')
                        LEFT JOIN motivos mot ON REPLACE(est.motivo_situacao_cadastral, '"', '') = REPLACE(mot.codigo, '"', '')
                        WHERE REPLACE(est.cnpj_basico, '"', '') = ?
                    """
                    
                    parametros_filiais = [cnpj_selecionado]
                    
                    if uf_filtro:
                        query_filiais += " AND REPLACE(est.uf, '\"', '') = ?"
                        parametros_filiais.append(uf_filtro)
                        
                    if municipio_filtro:
                        query_filiais += " AND REPLACE(m.descricao, '\"', '') LIKE ?"
                        parametros_filiais.append(f"%{municipio_filtro}%")
                    
                    if cnae_filtro:
                        # O filtro de CNAE busca tanto no código quanto na descrição traduzida
                        query_filiais += " AND (REPLACE(est.cnae_principal, '\"', '') LIKE ? OR c.descricao LIKE ?)"
                        parametros_filiais.extend([f"%{cnae_filtro}%", f"%{cnae_filtro}%"])
                        
                    if situacao_filtro:
                        query_filiais += " AND REPLACE(est.situacao_cadastral, '\"', '') = ?"
                        parametros_filiais.append(situacao_filtro)
                    
                    dados_filiais = conn.execute(query_filiais, parametros_filiais).fetchall()

                    if dados_filiais:
                        col_filiais = ["CNPJ", "Nome Fantasia", "Situação (Motivo)", "CNAE Principal", "Endereço", "Cidade/UF"]
                        filiais_limpos = [[str(i).replace('"', '') if i else "" for i in l] for l in dados_filiais]
                        st.dataframe([dict(zip(col_filiais, l)) for l in filiais_limpos], use_container_width=True)
                    else:
                        st.warning("Nenhum estabelecimento encontrado com os filtros aplicados.")

            else:
                st.warning(f"Nenhum registro para '{termo_limpo}'.")
                
        except Exception as e:
            st.error(f"Erro na consulta: {e}")