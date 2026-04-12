import os
import requests
import time
from datetime import datetime, timedelta
from tqdm import tqdm

def obter_ultimo_mes_disponivel(base_url, token):
    print("[INFO] Verificando a competencia mais recente disponivel no servidor...")
    data_teste = datetime.now()
    headers = {"User-Agent": "Mozilla/5.0 FDM/6.20"}
    
    for _ in range(6):
        mes_str = data_teste.strftime("%Y-%m")
        url_teste = f"{base_url}/{token}/{mes_str}/Municipios.zip"
        
        try:
            resposta = requests.head(url_teste, headers=headers, timeout=15)
            if resposta.status_code == 200:
                print(f"[INFO] Pasta base localizada: {mes_str}")
                return mes_str
            else:
                print(f"[INFO] Pasta {mes_str} indisponivel. Buscando anterior...")
        except requests.exceptions.RequestException as e:
            print(f"[WARN] Erro ao testar {mes_str}: {e}")
            
        primeiro_dia = data_teste.replace(day=1)
        data_teste = primeiro_dia - timedelta(days=1)
        
    raise ValueError("[ERROR] Nao foi possivel encontrar dados no portal.")

def sincronizar_dados():
    inicio_download = time.time() # INÍCIO DO CRONÔMETRO
    print("[INFO] Iniciando motor de sincronizacao nativa via API direta...")
    
    pasta_destino = os.path.abspath("dados_raw")
    if not os.path.exists(pasta_destino): 
        os.makedirs(pasta_destino)

    base_url = "https://arquivos.receitafederal.gov.br/public.php/dav/files"
    token = "YggdBLfdninEJX9"
    mes_ref = obter_ultimo_mes_disponivel(base_url, token)
    
    print(f"[INFO] Iniciando downloads para: {mes_ref} | Diretorio: {pasta_destino}")

    arquivos_alvo = (
        [f"Empresas{i}.zip" for i in range(10)] +
        [f"Estabelecimentos{i}.zip" for i in range(10)] +
        [f"Socios{i}.zip" for i in range(10)] +
        ["Simples.zip", "Municipios.zip", "Cnaes.zip", "Naturezas.zip", 
         "Motivos.zip", "Paises.zip", "Qualificacoes.zip"]
    )

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FDM/6.20", "Accept": "*/*"}

    for nome_arq in arquivos_alvo:
        caminho_local = os.path.join(pasta_destino, nome_arq)
        
        if os.path.exists(caminho_local) and os.path.getsize(caminho_local) > 1000:
            continue
            
        url_dinamica = f"{base_url}/{token}/{mes_ref}/{nome_arq}"
        
        try:
            with requests.get(url_dinamica, headers=headers, stream=True, timeout=60) as r:
                if r.status_code == 200:
                    total_size = int(r.headers.get('content-length', 0))
                    
                    with tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"[DOWNLOAD] {nome_arq}") as barra:
                        with open(caminho_local, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024 * 1024): 
                                if chunk:
                                    f.write(chunk)
                                    barra.update(len(chunk))
                else:
                    print(f"[WARN] Arquivo ignorado: {nome_arq} (HTTP {r.status_code})")
        except Exception as e:
            print(f"[ERROR] Falha de conexao ao transferir {nome_arq}: {e}")
        
        time.sleep(1)

    # CÁLCULO E IMPRESSÃO DO TEMPO TOTAL
    tempo_total_minutos = (time.time() - inicio_download) / 60
    print(f"\n[DONE] Sincronizacao de dados finalizada com sucesso em {tempo_total_minutos:.2f} minutos.")

if __name__ == "__main__":
    sincronizar_dados()