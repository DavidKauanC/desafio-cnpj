import duckdb

conn = duckdb.connect('dados/banco_cnpj.duckdb', read_only=True)

# 1. Busca cirúrgica APENAS pelo CNPJ da UFSC
print("--- BUSCA PELA UFSC ---")
query_ufsc = "SELECT cnpj_basico, razao_social FROM empresas WHERE cnpj_basico = '83899526'"
resultado = conn.execute(query_ufsc).df()

if resultado.empty:
    print("CNPJ não encontrado no banco de dados!")
else:
    print(resultado)

# 2. Teste de Sanidade do Banco (Volume de dados)
print("\n--- TESTE DE VOLUME ---")
query_count = "SELECT COUNT(*) as total_empresas FROM empresas"
total = conn.execute(query_count).fetchone()
print(f"Total de registros na tabela empresas: {total:,}".replace(',', '.'))