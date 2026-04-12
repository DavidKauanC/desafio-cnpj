import os
import sys
import subprocess

def limpar_tela():
    os.system('cls' if os.name == 'nt' else 'clear')

def exibir_menu():
    limpar_tela()
    print("-" * 60)
    print("SISTEMA DE GESTAO DE DADOS CNPJ - CEOS/UFSC")
    print("-" * 60)
    print("1. Baixar dados da Receita Federal (Opcional)")
    print("2. Processar e Ingerir dados para o banco DuckDB")
    print("3. Iniciar Painel de Consulta (Interface Streamlit)")
    print("0. Sair")
    print("-" * 60)

def executar_comando(comando):
    try:
        limpar_tela()
        print(f"Iniciando processo...\n")
        # subprocess.run passa o controle do terminal para o script chamado
        subprocess.run(comando, check=True)
        print("\nOperacao concluida.")
    except subprocess.CalledProcessError as e:
        print(f"\nErro na execucao. Codigo de retorno: {e.returncode}")
    except KeyboardInterrupt:
        print("\nProcesso interrompido pelo usuario.")
    
    input("\nPressione ENTER para voltar ao menu principal...")

def main():
    while True:
        exibir_menu()
        opcao = input("Selecione uma opcao: ").strip()
        
        if opcao == '1':
            executar_comando([sys.executable, "src/download.py"])
        
        elif opcao == '2':
            executar_comando([sys.executable, "src/ingestao.py"])
        
        elif opcao == '3':
            # Executa o Streamlit chamando o modulo nativo do Python dentro do venv
            executar_comando([sys.executable, "-m", "streamlit", "run", "src/app.py"])
        
        elif opcao == '0':
            limpar_tela()
            print("Sistema encerrado.")
            break
        
        else:
            print("\nOpcao invalida.")
            input("Pressione ENTER para tentar novamente...")

if __name__ == "__main__":
    main()