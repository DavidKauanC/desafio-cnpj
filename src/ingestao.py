import duckdb
import time
import os
import glob
import shutil

# Garante que a pasta existe 
os.makedirs('dados_raw', exist_ok=True)
print(">>>> TESTE: LENDO A VERSAO NOVA <<<<")
def processar_dados_cnpj():
    print("Iniciando a criação do banco de dados DuckDB...")
    start_time = time.time()

    # Cria o arquivo do banco de dados local
    conn = duckdb.connect('banco_cnpj.duckdb')

    # PRAGMAS: Configurações essenciais para otimizar tempo de carga e uso de memória
    conn.execute("PRAGMA memory_limit='6GB'") 
    conn.execute("PRAGMA threads=4")

    # ==========================================
    # 1. Criação das Tabelas
    # ==========================================
    print("Criando tabelas...")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS empresas (
            cnpj_basico VARCHAR, razao_social VARCHAR, natureza_juridica VARCHAR,
            qualificacao_responsavel VARCHAR, capital_social VARCHAR, porte_empresa VARCHAR,
            ente_federativo_responsavel VARCHAR
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS estabelecimentos (
            cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR, identificador_matriz_filial VARCHAR,
            nome_fantasia VARCHAR, situacao_cadastral VARCHAR, data_situacao_cadastral VARCHAR,
            motivo_situacao_cadastral VARCHAR, nome_cidade_exterior VARCHAR, pais VARCHAR,
            data_inicio_atividade VARCHAR, cnae_principal VARCHAR, cnae_secundaria VARCHAR,
            tipo_logradouro VARCHAR, logradouro VARCHAR, numero VARCHAR, complemento VARCHAR,
            bairro VARCHAR, cep VARCHAR, uf VARCHAR, municipio VARCHAR, ddd_1 VARCHAR,
            telefone_1 VARCHAR, ddd_2 VARCHAR, telefone_2 VARCHAR, ddd_fax VARCHAR,
            fax VARCHAR, correio_eletronico VARCHAR, situacao_especial VARCHAR, data_situacao_especial VARCHAR
        );
    """)

    # ==========================================
    # 2. Função Inteligente de Ingestão e Limpeza
    # ==========================================
    def ingerir_zips(padrao_nome, tabela, definicao_colunas):
        arquivos = glob.glob(f'dados_raw/{padrao_nome}*.zip')
        if not arquivos:
            print(f"Nenhum arquivo encontrado para {padrao_nome}*.zip")
            return

        for arquivo_zip in arquivos:
            print(f"Processando arquivo: {arquivo_zip}...")
            
            # Mapeia o que já existe na pasta antes de extrair
            arquivos_antes = set(glob.glob('dados_raw/*'))
            
            print("  -> Extraindo arquivo...")
            # shutil descompacta sem se importar com a estrutura interna do ZIP
            shutil.unpack_archive(arquivo_zip, extract_dir='dados_raw/')
            
            # Descobre qual foi o arquivo extraído comparando o antes e depois
            arquivos_depois = set(glob.glob('dados_raw/*'))
            arquivos_novos = list(arquivos_depois - arquivos_antes)
            
            if not arquivos_novos:
                print("  -> Erro: Nenhum arquivo novo foi extraído.")
                continue
                
            arquivo_extraido = arquivos_novos.pop()
            caminho_temporario = arquivo_extraido.replace('\\', '/')
            
            print("  -> Ingerindo no banco de dados...")
            conn.execute(f"""
                INSERT INTO {tabela} 
                SELECT * FROM read_csv(
                    '{caminho_temporario}',
                    sep=';',
                    header=false,
                    columns={definicao_colunas},
                    encoding='CP1252',
                    ignore_errors=true,
                    auto_detect=false,
                    strict_mode=false,
                    quote=''
                );
            """)
            
            # Limpeza: Apaga o CSV para não lotar o disco
            if os.path.exists(caminho_temporario):
                os.remove(caminho_temporario)
            print(f"  -> Ingestão concluída. Arquivo temporário apagado.")

    # ==========================================
    # 3. Executando as cargas
    # ==========================================
    print("\n--- Iniciando Carga de Empresas ---")
    colunas_empresas = "{'cnpj_basico': 'VARCHAR', 'razao_social': 'VARCHAR', 'natureza_juridica': 'VARCHAR', 'qualificacao_responsavel': 'VARCHAR', 'capital_social': 'VARCHAR', 'porte_empresa': 'VARCHAR', 'ente_federativo_responsavel': 'VARCHAR'}"
    ingerir_zips('Empresas', 'empresas', colunas_empresas)

    print("\n--- Iniciando Carga de Estabelecimentos ---")
    colunas_estabelecimentos = "{'cnpj_basico': 'VARCHAR', 'cnpj_ordem': 'VARCHAR', 'cnpj_dv': 'VARCHAR', 'identificador_matriz_filial': 'VARCHAR', 'nome_fantasia': 'VARCHAR', 'situacao_cadastral': 'VARCHAR', 'data_situacao_cadastral': 'VARCHAR', 'motivo_situacao_cadastral': 'VARCHAR', 'nome_cidade_exterior': 'VARCHAR', 'pais': 'VARCHAR', 'data_inicio_atividade': 'VARCHAR', 'cnae_principal': 'VARCHAR', 'cnae_secundaria': 'VARCHAR', 'tipo_logradouro': 'VARCHAR', 'logradouro': 'VARCHAR', 'numero': 'VARCHAR', 'complemento': 'VARCHAR', 'bairro': 'VARCHAR', 'cep': 'VARCHAR', 'uf': 'VARCHAR', 'municipio': 'VARCHAR', 'ddd_1': 'VARCHAR', 'telefone_1': 'VARCHAR', 'ddd_2': 'VARCHAR', 'telefone_2': 'VARCHAR', 'ddd_fax': 'VARCHAR', 'fax': 'VARCHAR', 'correio_eletronico': 'VARCHAR', 'situacao_especial': 'VARCHAR', 'data_situacao_especial': 'VARCHAR'}"
    ingerir_zips('Estabelecimentos', 'estabelecimentos', colunas_estabelecimentos)

    end_time = time.time()
    print(f"\nCarga finalizada com sucesso em {round((end_time - start_time) / 60, 2)} minutos!")

if __name__ == "__main__":
    processar_dados_cnpj()