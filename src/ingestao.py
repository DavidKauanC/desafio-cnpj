import os
import zipfile
import duckdb
import time
import glob
import psutil

def obter_memoria_ram():
    """Retorna o uso de memória RAM do processo atual em Megabytes"""
    processo = psutil.Process(os.getpid())
    memoria_bytes = processo.memory_info().rss
    return memoria_bytes / (1024 * 1024)

def extrair_e_carregar():
    inicio = time.time()
    print("Iniciando a criação do banco de dados DuckDB (Carga Completa - Big Data)...")
    
    # Conecta ao banco de dados local
    conn = duckdb.connect('banco_cnpj.duckdb')
    
    # Dicionário com todas as tabelas, mapeamento de múltiplos arquivos (*) e layouts
    tabelas = {
        'empresas': ('Empresas*.zip', {
            'cnpj_basico': 'VARCHAR', 'razao_social': 'VARCHAR', 'natureza_juridica': 'VARCHAR',
            'qualificacao_responsavel': 'VARCHAR', 'capital_social': 'VARCHAR', 'porte_empresa': 'VARCHAR',
            'ente_federativo_responsavel': 'VARCHAR'
        }),
        'estabelecimentos': ('Estabelecimentos*.zip', {
            'cnpj_basico': 'VARCHAR', 'cnpj_ordem': 'VARCHAR', 'cnpj_dv': 'VARCHAR',
            'identificador_matriz_filial': 'VARCHAR', 'nome_fantasia': 'VARCHAR',
            'situacao_cadastral': 'VARCHAR', 'data_situacao_cadastral': 'VARCHAR',
            'motivo_situacao_cadastral': 'VARCHAR', 'nome_cidade_exterior': 'VARCHAR',
            'pais': 'VARCHAR', 'data_inicio_atividade': 'VARCHAR', 'cnae_principal': 'VARCHAR',
            'cnae_secundaria': 'VARCHAR', 'tipo_logradouro': 'VARCHAR', 'logradouro': 'VARCHAR',
            'numero': 'VARCHAR', 'complemento': 'VARCHAR', 'bairro': 'VARCHAR',
            'cep': 'VARCHAR', 'uf': 'VARCHAR', 'municipio': 'VARCHAR',
            'ddd_1': 'VARCHAR', 'telefone_1': 'VARCHAR', 'ddd_2': 'VARCHAR',
            'telefone_2': 'VARCHAR', 'ddd_fax': 'VARCHAR', 'fax': 'VARCHAR',
            'correio_eletronico': 'VARCHAR', 'situacao_especial': 'VARCHAR',
            'data_situacao_especial': 'VARCHAR'
        }),
        'socios': ('Socios*.zip', {
            'cnpj_basico': 'VARCHAR', 'identificador_socio': 'VARCHAR', 'nome_socio': 'VARCHAR',
            'cnpj_cpf_socio': 'VARCHAR', 'qualificacao_socio': 'VARCHAR', 'data_entrada_sociedade': 'VARCHAR',
            'pais': 'VARCHAR', 'representante_legal': 'VARCHAR', 'nome_representante': 'VARCHAR',
            'qualificacao_representante': 'VARCHAR', 'faixa_etaria': 'VARCHAR'
        }),
        'simples': ('Simples*.zip', {
            'cnpj_basico': 'VARCHAR', 'opcao_simples': 'VARCHAR', 'data_opcao_simples': 'VARCHAR',
            'data_exclusao_simples': 'VARCHAR', 'opcao_mei': 'VARCHAR', 'data_opcao_mei': 'VARCHAR',
            'data_exclusao_mei': 'VARCHAR'
        }),
        'municipios': ('Municipios*.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'}),
        'cnaes': ('Cnaes*.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'}),
        'naturezas': ('Naturezas*.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'}),
        'motivos': ('Motivos*.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'}),
        'paises': ('Paises*.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'}),
        'qualificacoes': ('Qualificacoes*.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'})
    }

    for tabela, (padrao_zip, colunas) in tabelas.items():
        caminho_busca = os.path.join('dados_raw', padrao_zip)
        
        # Pega a lista de todos os arquivos que batem com o nome (ex: Empresas0, Empresas1...)
        arquivos_encontrados = glob.glob(caminho_busca)
        
        if not arquivos_encontrados:
            print(f"⚠️ Nenhum arquivo encontrado para a tabela {tabela.upper()}. Pulando...")
            continue
            
        print(f"\n--- Iniciando Carga da tabela: {tabela.upper()} ({len(arquivos_encontrados)} arquivo(s)) ---")
        
        # Apaga a tabela se ela já existir para não duplicar dados
        conn.execute(f"DROP TABLE IF EXISTS {tabela}")
        
        # Variável de controle: O primeiro arquivo cria a tabela, os demais apenas inserem dados
        primeiro_arquivo = True

        for caminho_zip in arquivos_encontrados:
            nome_zip = os.path.basename(caminho_zip)
            print(f"  -> Processando arquivo {nome_zip}...")
            
            # 1. Extração segura (Proteção contra listas)
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                lista_arquivos = zip_ref.namelist()
                nome_arquivo_extraido = lista_arquivos.pop(0)
                zip_ref.extractall('dados_raw')
            
            # Ajuste de barras para o Windows e formatação das colunas para o DuckDB
            caminho_csv = os.path.join('dados_raw', nome_arquivo_extraido).replace('\\', '/')
            colunas_str = ", ".join([f"'{k}': '{v}'" for k, v in colunas.items()])
            
            # 2. Ingestão com tratamento de exceções (try/except)
            try:
                if primeiro_arquivo:
                    # No 1º arquivo nós CRIAMOS a tabela e definimos a estrutura
                    query = f"""
                        CREATE TABLE {tabela} AS 
                        SELECT * FROM read_csv('{caminho_csv}', delim=';', header=False, 
                        columns={{{colunas_str}}}, encoding='ISO_8859_1', ignore_errors=true)
                    """
                    conn.execute(query)
                    primeiro_arquivo = False
                else:
                    # Nos próximos arquivos, nós apenas ANEXAMOS os dados à tabela já existente
                    query = f"""
                        INSERT INTO {tabela} 
                        SELECT * FROM read_csv('{caminho_csv}', delim=';', header=False, 
                        columns={{{colunas_str}}}, encoding='ISO_8859_1', ignore_errors=true)
                    """
                    conn.execute(query)
                
                # 3. Monitoramento de memória profissional
                mem_mb = obter_memoria_ram()
                print(f"    ✅ Tabela atualizada com sucesso. [Uso atual de RAM: {mem_mb:.2f} MB]")
                
            except Exception as e:
                print(f"    ❌ Erro crítico ao tentar processar o CSV do arquivo {nome_zip}: {e}")
            
            # O bloco 'finally' garante que o CSV temporário seja apagado do HD mesmo se der erro
            finally:
                if os.path.exists(caminho_csv):
                    os.remove(caminho_csv)
        
    conn.close()
    tempo_total = (time.time() - inicio) / 60
    print(f"\n🚀 CARGA TOTAL FINALIZADA COM SUCESSO EM {tempo_total:.2f} MINUTOS!")

if __name__ == '__main__':
    extrair_e_carregar()