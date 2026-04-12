# Desafio CNPJ/Céos - Sistema de Ingestão e Consulta

Este projeto foi desenvolvido como parte do desafio técnico para o projeto Céos/UFSC. Trata-se de uma solução de Big Data para download, ingestão e visualização dos Dados Abertos do CNPJ da Receita Federal, utilizando Python, DuckDB e Streamlit.

## Funcionalidades

- **Download Automatizado**: Recuperação resiliente dos arquivos ZIP do servidor oficial da Receita Federal.
- **Ingestão Otimizada**: Processamento de milhões de registros com controle dinâmico de memória RAM (limite de 85% do disponível) para evitar travamentos do sistema durante a carga.
- **Tradução de Metadados**: Conversão automática de códigos numéricos (CNAE, Motivos de Situação, Natureza Jurídica) para descrições textuais legíveis.
- **Interface Analítica**: Dashboard interativo com filtros avançados por UF, Município, Porte e Situação Cadastral.

## Tecnologias Utilizadas

- **Linguagem**: Python 3.12
- **Banco de Dados**: DuckDB (Processamento analítico em disco de alta performance)
- **Interface**: Streamlit
- **Bibliotecas Auxiliares**: psutil e tqdm

## Estrutura do Projeto

```text
├── dados/              # Banco de dados DuckDB gerado (.duckdb)
├── dados_raw/          # Arquivos ZIP temporários baixados da Receita
├── src/
│   ├── download.py     # Script de coleta de dados
│   ├── ingestao.py     # Pipeline de ETL e estruturação do banco
│   └── app.py          # Interface Web e lógica de consulta
├── requirements.txt    # Lista de dependências do projeto
└── .gitignore          # Configuração para evitar upload de dados sensíveis/pesados
```

## Como Executar

### 1. Clonar o repositório
```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
```

### 2. Configurar o ambiente Python
Crie e ative um ambiente virtual:

**Linux/macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Windows:**
```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Executar o Pipeline de Dados
A coleta e a estruturação dos dados podem levar tempo dependendo do hardware e da velocidade de conexão com a internet.

```bash
# Passo 1: Download dos arquivos ZIP
python src/download.py

# Passo 2: Ingestão e criação do banco no DuckDB
python src/ingestao.py
```

### 4. Iniciar a Interface de Consulta
```bash
streamlit run src/app.py
```

## Declaração de Uso de IA

Este projeto contou com o apoio da Inteligência Artificial **Gemini** (Google) para auxílio no desenvolvimento. O suporte incluiu:
- Refatoração de scripts Python para implementação de gerenciamento dinâmico de memória RAM.
- Otimização da sintaxe de consultas SQL parametrizadas para o motor DuckDB.
- Auxílio na estruturação e revisão da documentação técnica (README).

A concepção da arquitetura, a modelagem das regras de negócio, a validação dos dados e o cumprimento dos requisitos técnicos do edital foram conduzidos e validados inteiramente pelo desenvolvedor.

## Requisitos do Edital Atendidos

- [x] Coleta automatizada de dados oficiais.
- [x] Modelagem de dados e carga em banco de dados local.
- [x] Consulta por Razão Social ou CNPJ Básico.
- [x] Visualização de dados cadastrais detalhados.
- [x] Filtros avançados por Estado, Município e Situação Cadastral.
- [x] Exibição do Quadro de Sócios e Filiais.

## Licença e Autoria

Projeto acadêmico desenvolvido para fins de avaliação técnica. Os dados consumidos são de domínio público, providos abertamente pela Receita Federal do Brasil.

Desenvolvido por **David Kauan** - Estudante de Sistemas de Informação (UFSC).