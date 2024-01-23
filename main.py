### Libs
import os
import sys
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "data_processing")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "data_setup")))
from spark_init import Sparkinit
from variables import tables_config_dict, transform_data, consumer_operations, datalake_paths, transient_config_dict
import logs
from delta_processing_base import DeltaProcessingRefined
from delta_processing_consumer import DeltaProcessingConsumer
from configuracoes import limpar_diretorio_temporario, obter_ip_publico

# Definir o nível de log para "INFO"
logging.getLogger().setLevel(logging.INFO)

# Define o formato da mensagem do logger
formato_mensagem = f'{__name__}'
logger = logs.criar_log(formato_mensagem)

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
    obter_ip_publico()
    
    # Obtém todos os nomes das tabelas
    data_base_list = [keydb for keydb in tables_config_dict.keys()]

    # Instancia a classe Delta para as camadasdo datalake

    delta_refined = DeltaProcessingRefined(ambientes_dados=datalake_paths, spark=spark)

    delta_consumer = DeltaProcessingConsumer(ambiente_dados=datalake_paths, spark=spark)

    # Realiza o processamento da camada Transient e Refined
    for nome_tabela, dicionario_parametros in list(tables_config_dict.items()):
        # Executa a Refined
        delta_refined.run_refined(nome_tabela=nome_tabela, operacao_delta=transform_data,operacao_transient=transient_config_dict[nome_tabela], sql_query="sql_query", format_in=dicionario_parametros["format"])
    # Realiza o processamento para Consumer
    for operacoes in consumer_operations.values():
        delta_consumer.run_consumer(operacoes=operacoes, query_sql= "sql_query")
    
    logger.info("Operação finalizada")
    
    # Limpart todas as tabelas temporárias após a conclusão
    spark.catalog.clearCache()

    # Limpar tabelas temporária
    limpar_diretorio_temporario()

    # Finaliza a sessão do Spark
    spark.stop()