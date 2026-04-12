# Desafio CNPJ/Céos - Sistema de Ingestão e Consulta

Este projeto foi desenvolvido como parte do desafio técnico para o projeto Céos/UFSC. Trata-se de uma solução de Big Data para download, ingestão e visualização dos Dados Abertos do CNPJ da Receita Federal, utilizando Python, DuckDB e Streamlit.

## Funcionalidades

- **Gerenciador Interativo**: Interface via terminal para orquestrar o download, a ingestão e a visualização dos dados em um único ponto.
- **Download Automatizado**: Recuperação resiliente dos arquivos ZIP do servidor oficial da Receita Federal (opcional, caso os dados já estejam no disco).
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
├── main.py             # Menu interativo do sistema (Gerenciador)
├── requirements.txt    # Lista de dependências do projeto
└── .gitignore          # Configuração para evitar upload de dados pesados
```

## Como Executar

Siga os passos de acordo com o seu sistema operacional para configurar o ambiente e iniciar a aplicação.

> **Importante: Uso de Dados Locais (Opcional)**
> Se você já possui os arquivos `.zip` dos Dados Abertos do CNPJ e deseja pular a etapa de download, basta criar uma pasta chamada `dados_raw` na raiz do projeto e colar todos os arquivos `.zip` dentro dela antes de executar o passo 3. O sistema reconhecerá os arquivos automaticamente.

### Passo 1: Clonar o repositório (Todos os sistemas)
Abra o seu terminal e execute:
```bash
git clone https://github.com/DavidKauanC/desafio-cnpj.git
cd seu-repositorio
```

---

### Passo 2: Configurar e Executar no Linux / macOS
Abra o terminal na pasta do projeto e execute os comandos abaixo sequencialmente:

```bash
# 1. Criar e ativar o ambiente virtual
python3 -m venv venv
source venv/bin/activate

# 2. Instalar as dependências
pip install -r requirements.txt

# 3. Executar o Gerenciador do Sistema
python3 main.py
```

---

### Passo 2: Configurar e Executar no Windows
Abra o Prompt de Comando (CMD) ou PowerShell na pasta do projeto e execute os comandos abaixo sequencialmente:

```cmd
# 1. Criar e ativar o ambiente virtual
python -m venv venv
venv\Scripts\activate

# 2. Instalar as dependências
pip install -r requirements.txt

# 3. Executar o Gerenciador do Sistema
python main.py
```

---

### Passo 3: Utilizando o Sistema
Ao executar o `main.py`, um menu interativo será exibido no terminal:
1. **Baixar dados:** Executa a raspagem dos arquivos ZIP (Pode ser ignorado se você seguiu o aviso de uso de dados locais acima).
2. **Processar e Ingerir:** Cria o banco de dados `banco_cnpj.duckdb` estruturando as informações.
3. **Iniciar Painel:** Abre a interface gráfica do Streamlit no seu navegador padrão.

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
