### Libs
import os
import sys
import logging
from utils import tables_config_dict, transform_data, gold_operations, datalake_paths, transient_config_dict, criar_log ,limpar_diretorio_temporario
from jobs import DeltaProcessingBronze, DeltaProcessingSilver, DeltaProcessingGold, Sparkinit

# Definir o nível de log para "INFO"
logging.getLogger().setLevel(logging.INFO)

# Define o formato da mensagem do logger
formato_mensagem = f'{__name__}'
logger = criar_log(formato_mensagem)

# Verifica a existência das variáveis de ambiente, uteis para o projeto
logger.info(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON')}")
logger.info(f"PYSPARK_DRIVER_PYTHON: {os.environ.get('PYSPARK_DRIVER_PYTHON')}")
logger.info(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}")
logger.info(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")


if __name__ == "__main__":
    
    # Instanciando e chamando a sessão do spark
    spark_start = Sparkinit()
    # Obtém a sessão Spark
    spark = spark_start.buscar_sessao_spark()
    logger.info(f"Versão do spark: {spark.version}")
    # Imprimir o link do Spark Web UI
    logger.info(f"Interface gráfica do usuário do spark: {spark.sparkContext.uiWebUrl}")
    
    # Obtém todos os nomes das tabelas
    data_base_list = [keydb for keydb in tables_config_dict.keys()]

    # Instancia a classe Delta para as camadasdo datalake

    delta_bronze = DeltaProcessingBronze(ambientes_dados=datalake_paths, spark=spark)

    delta_silver = DeltaProcessingSilver(ambientes_dados=datalake_paths, spark=spark)

    delta_consumer = DeltaProcessingGold(ambiente_dados=datalake_paths, spark=spark)

    # Realiza o processamento da camada Transient e Refined
    for nome_tabela, dicionario_parametros in list(tables_config_dict.items()):
        # Executa a Bronze
        delta_bronze.run_bronze(nome_tabela=nome_tabela, operacao_delta=transform_data,operacao_transient=transient_config_dict[nome_tabela], sql_query="bronze_query", format_in=dicionario_parametros["format"])
        # Executa a Silver
        delta_silver.run_silver(nome_tabela=nome_tabela, operacao=transform_data, sql_query="silver_query")

    # Realiza o processamento da Gold
    for operacoes in gold_operations.values():
        delta_consumer.run_gold(operacoes=operacoes, query_sql="gold_query")
    
    logger.info("Operação finalizada")
    
    # Limpart todas as tabelas temporárias após a conclusão
    spark.catalog.clearCache()