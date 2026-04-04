import os
import zipfile
import duckdb
import time

def extrair_e_carregar():
    inicio = time.time()
    print("Iniciando a criação do banco de dados DuckDB...")
    
    # Conecta ao banco (se não existir, ele cria)
    conn = duckdb.connect('banco_cnpj.duckdb')
    
    # Dicionário com as configurações de cada arquivo da Receita
    # Estrutura: 'nome_da_tabela': ('NomeDoArquivo.zip', {'coluna1': 'TIPO', 'coluna2': 'TIPO'...})
    tabelas = {
        'empresas': ('Empresas0.zip', {
            'cnpj_basico': 'VARCHAR', 'razao_social': 'VARCHAR', 'natureza_juridica': 'VARCHAR',
            'qualificacao_responsavel': 'VARCHAR', 'capital_social': 'VARCHAR', 'porte_empresa': 'VARCHAR',
            'ente_federativo_responsavel': 'VARCHAR'
        }),
        'estabelecimentos': ('Estabelecimentos0.zip', {
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
        'socios': ('Socios0.zip', {
            'cnpj_basico': 'VARCHAR', 'identificador_socio': 'VARCHAR', 'nome_socio': 'VARCHAR',
            'cnpj_cpf_socio': 'VARCHAR', 'qualificacao_socio': 'VARCHAR', 'data_entrada_sociedade': 'VARCHAR',
            'pais': 'VARCHAR', 'representante_legal': 'VARCHAR', 'nome_representante': 'VARCHAR',
            'qualificacao_representante': 'VARCHAR', 'faixa_etaria': 'VARCHAR'
        }),
        'municipios': ('Municipios.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'}),
        'cnaes': ('Cnaes.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'}),
        'naturezas': ('Naturezas.zip', {'codigo': 'VARCHAR', 'descricao': 'VARCHAR'})
    }

    for tabela, (arquivo_zip, colunas) in tabelas.items():
        caminho_zip = os.path.join('dados_raw', arquivo_zip)
        
        if not os.path.exists(caminho_zip):
            print(f"⚠️ Arquivo {arquivo_zip} não encontrado na pasta dados_raw. Pulando...")
            continue
            
        print(f"\n--- Iniciando Carga de {tabela.capitalize()} ---")
        print(f"  -> Extraindo arquivo {arquivo_zip}...")
        
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            lista_arquivos = zip_ref.namelist()
            nome_arquivo_extraido = lista_arquivos.pop(0)
            zip_ref.extractall('dados_raw')
        
        caminho_csv = os.path.join('dados_raw', nome_arquivo_extraido).replace('\\', '/')
        
        print(f"  -> Ingerindo no banco de dados...")
        
        # Monta a formatação de colunas que o DuckDB exige
        colunas_str = ", ".join([f"'{k}': '{v}'" for k, v in colunas.items()])
        
        # Deleta a tabela se ela já existir (cumpre o requisito de reprocessamento limpo)
        conn.execute(f"DROP TABLE IF EXISTS {tabela}")
        
        # O Pulo do Gato: Lê o CSV direto pro banco usando a velocidade extrema do DuckDB
        query_ingestao = f"""
            CREATE TABLE {tabela} AS 
            SELECT * FROM read_csv('{caminho_csv}', 
                delim=';', 
                header=False, 
                columns={{{colunas_str}}},
                encoding='ISO_8859_1',
                ignore_errors=true
            )
        """
        conn.execute(query_ingestao)
        
        print(f"  -> Ingestão concluída. Apagando arquivo temporário...")
        os.remove(caminho_csv)
        
    conn.close()
    tempo_total = (time.time() - inicio) / 60
    print(f"\n✅ Carga finalizada com sucesso em {tempo_total:.2f} minutos!")

if __name__ == '__main__':
    extrair_e_carregar()