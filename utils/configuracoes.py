import os
import sys
import psutil
import datetime
import pytz
import logging
import tempfile
import shutil
import requests
import socket
import sqlparse

def buscar_dados_vcpu_so() ->  tuple:
    """Busca informações do sistema operacional relacionados a CPU
    Retorno:
        - `nucleos_cpu`: Número de núcleos lógicos da máquina
        - `memoria_gb`: Retorna 70% da memória para uso do spark
    """
    # Obtém o número total de núcleos (físicos + virtuais)
    nucleos_cpu = psutil.cpu_count(logical=True)

    # Obtém total de memória em em bits, calcula e retorna em GB
    mem_info = psutil.virtual_memory()
    memoria_gb = int(mem_info.total / (1024 ** 3) * 0.70 )

    return nucleos_cpu, memoria_gb

def buscar_data_hoje() -> str:
    """retorna a data de hoje no formanto texto ano-mês-dia"""
    hoje = datetime.date.today()
    return hoje.strftime("%Y-%m-%d")

def buscar_hora_agora() -> str:
    """Retorna a hora de agora no formato texto hora-minuto"""
    agora = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))
    return agora.strftime('%d-%m-%Y %H:%M:%S')


def definir_variaveis_ambiente()-> None:
    """
    O objetivo é definir variáveis de ambiente necessárias para a configuração do 
    ambiente de execução do Spark.
    """
    # Define o diretório do executável e o executável
    caminho_python = sys.executable
    diretorio_python = sys.exec_prefix

    # Pega a variável`SPARK_HOME`
    spark_home = os.environ.get('SPARK_HOME')
    # Define o caminho para o `py4j-0.10.9.7-src.zip`
    py4j_path = os.path.join(spark_home, "python\lib\py4j-0.10.9.7-src.zip")

    # Define o python Path
    pythonpath = os.environ.get("PYTHONPATH", "")
    pythonpath = f"{spark_home}/python;{py4j_path};{pythonpath}"
    
    # Inputa o `SPARK_HOME` e `PYTHONPATH` na variável `PATH`
    path = os.environ.get("PATH", "")
    path = f"{spark_home}/bin:{spark_home}/python:{path}"

    os.environ["PATH"] = path

    # Define as variáveis de ambientes necessárias
    os.environ["PYTHONPATH"] = pythonpath
    os.environ['PYSPARK_PYTHON'] = caminho_python
    os.environ['PYSPARK_DRIVER_PYTHON'] = caminho_python

def limpar_diretorio_temporario() -> None:
    """
    Mapeia o diretório temporário do sistema operacional, e remove todos os arquivos e subdiretórios.
    """
    TMP_PATH = tempfile.gettempdir()

    # Lista todos os arquivos e diretórios temporários
    arquivos_diretorios = os.listdir(TMP_PATH)

    for arquivo_diretorio in arquivos_diretorios:
        if arquivo_diretorio.startswith("spark-"):
            try:
                caminho = os.path.join(TMP_PATH, arquivo_diretorio)

                # Verifica se é um diretório ou um arquivo
                if os.path.isfile(path=caminho):
                    # Remove o arquivo
                    os.remove(caminho)
                elif os.path.isdir(s=caminho):
                    # Remove o diretório
                    shutil.rmtree(path=caminho)
            except Exception as e:
                logging.info(f"Erro ao deletar {caminho}: {e}")

def formatar_sql(query:str):
    """ Retorna a query no formato padrão SQL"""
    query_sql = sqlparse.format(query, reindent=True, keyword_case="upper")
    return query_sql


def obter_ip_publico() -> str:
    """Essa função tenta enviar um endereço para monitoramento do spark no webUI"""
    try:
        resposta = requests.get('https://httpbin.org/ip')
        ip_publico = resposta.json()['origin']
        logging.info(f'O endereço IP público da máquina é: {ip_publico}')
        logging.info(f"Tenta acessar: http://{ip_publico}:4040")
    except Exception as e:
        logging.info(f'Erro ao obter o endereço IP público: {e}')
    try: 
        host_name = socket.gethostname() 
        host_ip = socket.gethostbyname(host_name) 
        logging.info(f"Nome do computador :  {host_name}") 
        logging.info(f"IP do computador : {host_ip}") 
        logging.info(f"Tenta acessar: http://{host_ip}:4040")
    except Exception as e: 
        logging.info(f'Erro ao obter o endereço IP público: {e}')

def preparar_mover(origem:str, destino:str):
    """Recebe duas pastas ou arquivos e move para outro lugar.
    Parâmetros:
        - `origem`: caminho de origem
        - `destino`: caminho de destino
    """
    
    if os.path.exists(destino):
        # Se a pasta de destino existir, move os arquivos da pasta de origem para a pasta de destino
        for arquivo in os.listdir(origem):
            shutil.move(os.path.join(origem, arquivo), destino)
    else:
        # Se a pasta de destino não existir, mover a pasta de origem para o destino
        shutil.move(origem, destino)