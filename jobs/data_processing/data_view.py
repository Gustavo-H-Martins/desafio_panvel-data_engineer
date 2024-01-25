import os
import tempfile
import pandas as pd
from deltalake import DeltaTable
import duckdb
from tabulate import tabulate
import logging

log_format = '%(asctime)s||Resultado:%(message)s'
date_format = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=log_format, datefmt=date_format)

# Função para obter o caminho completo do arquivo temporário
def obter_caminho_do_arquivo(nome_do_arquivo):
    return os.path.join(tempfile.gettempdir(), nome_do_arquivo)

# Função para carregar dados na tabela Delta
def carregar_tabela_delta_duckdb(caminho_da_tabela:str) -> duckdb.connect: 
    """Tenta carregar a tabela delta dentro do diretório local fornecido e retornar um dataframe pandas.
    Parâmetro:
        - `caminho_da_tabela`: uma uri para a tabela delta.
    """
    df_sql = duckdb.connect(database=':memory:')

    # Caminho para a tabela Delta
    caminho_da_tabela = os.path.normpath(caminho_da_tabela)
    # Tentando inferir o nome da tabela
    nome_tabela = caminho_da_tabela.split("\\")[-1].replace(".", "_")
    df = pd.DataFrame
    try:
        # Tentando carregar os dados existentes na tabela Delta para um DataFrame Pandas
        df = DeltaTable(caminho_da_tabela).to_pandas()
    except Exception as e:
        logging.error(f"Não foi possível carregar a tabela Delta do diretório: {caminho_da_tabela} Exceção: {e}")
        return 
    try:
        df_sql.register(nome_tabela, df)
        logging.info(f"Retornando a tabela DuckDB com o nome: {nome_tabela}")
    except Exception as e:
        logging.error(f"Não foi converter a tabela: {nome_tabela} para duckDB Exceção: {e}")
        return 
    return df_sql

def consultar_tabela_duck_db(tabela_duckdb:duckdb.connect, query_sql:str):
    """Realiza uma consulta SQL em uma tabela delta mapeada pelo DuckDB
    Parâmetros:
        - `query_sql`: Consulta SQL
    Retorno:
        - Mostra o resultado da consulta formatado como tabela
    """

    # Consulte a tabela delta como uma tabela SQL comum.
    resultado = tabela_duckdb.execute(query_sql).fetch_df()
    logging.info(f"""\n\n{tabulate(resultado, tablefmt="fancy_grid", showindex=False, numalign="left")}""")