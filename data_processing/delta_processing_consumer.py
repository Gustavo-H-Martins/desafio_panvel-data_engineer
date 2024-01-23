import findspark
findspark.init()
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "util")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "cloud", "AWS", "athena")))
from variables import datalake_paths
from delta.tables import DataFrame
from  tablehandler import TableHandler
import logs
import re

formato_mensagem = f'{__name__}'
logger = logs.criar_log(formato_mensagem)


class DeltaProcessingConsumer:
    def __init__(self, ambiente_dados : dict, spark, **kwargs):
        """
        Classe para instanciar a execução da camada Consumer
        Parâmetros:
            - `ambiente_dados`: Dicionário de dados com as bases do processamento
            - `spark`: Sessão ativa do spark
            - `kwargs`:  
        """

        self.dataframe_tabela_atual = None
        self.dataframe_tabela_anterior = None
        self.spark = spark
        self.ambiente_dados = ambiente_dados
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.sparkContext.setLogLevel("ERROR")
        self.param = {}
        self.keys = []
        self.kwargs_param(**kwargs)
        self.deltatables = []
        self.deltadim = []
        self.views = []
        self.tablehandler = TableHandler(spark)

    def kwargs_param(self, **kwargs):
        """Parâmetros para execução do processo"""
        self.param = {
            'header': 'true',
            'inferSchema': 'true',
            'format_out': 'delta',
            'mode': 'overwrite',
            'format_in': 'parquet',
            'upsert': True,
            'step': {"refined": 'refined', "consumer": "consumer"}
        }
        self.param.update(kwargs)
        self.keys = list(self.param.keys())
    
    @logs.logs
    def save_update_table(self, sql_df: DataFrame, operacoes: dict, diretorio: str):
        """
        Cria uma camada consumer ou realiza o upsert se a consumer já  existir
        Parâmetros:
            - `sql_df`: Dataframe criado pela consulta SQL do Spark
            - `operacoes`: Dicionário com as operações da camada Consumer
            - `diretorio`: Caminho para a camada Consumer
        """

        rotulo_origem = operacoes["label_orig"]
        rotulo_destino = operacoes["label_destino"]
        condicoes = operacoes["condition"]

        # Instanciando o Delta Table
        tablehandler_consumer = TableHandler(self.spark, diretorio)

        # Verificando se a tabela já existe
        if not tablehandler_consumer.is_deltatable():
            tablehandler_consumer.write_table(dataframe=sql_df, path=diretorio, options=self.param)
        else:
            tablehandler_consumer.upsert_deltatable(dataframe=sql_df, label_origem=rotulo_origem, label_destino=rotulo_destino, condupdate=condicoes)
        
    @logs.logs
    def _update_refined_tables(self, operacoes: dict, diretorio: str):
        """
        Realiza o upsert da Refined usando a Consumer para alterar os valores (ex: processed para True)
        Parâmetros:
             - `operacoes`: Dicionário com as operações da camada Consumer
             - `diretorio`: Caminho para a camada Consumer
        """
        rotulo_origem = operacoes["label_orig"]
        condicoes = operacoes["tables_refined"]["conditions"]
        campos_validos =  operacoes["match_filds"]

        # Instanciando o Delta Table
        tablehandler_consumer = TableHandler(spark=self.spark, localpath=diretorio)

        # Verificando se a tabela já existe
        if tablehandler_consumer.is_deltatable():
            # Iterando entre as Refineds
            for index, valor in enumerate(operacoes["tables_refined"]["tables"]):
                path_table = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", f"{self.ambiente_dados[self.param['step']['refined']]}/{str(valor).upper()}/"))
                tablehandler_refined = TableHandler(spark=self.spark, localpath=path_table)

                deltatable_consumer = tablehandler_consumer.get_deltatable()
                tablehandler_refined.upsert_table(deltatable=deltatable_consumer, label_origem=rotulo_origem, label_destino=valor, condupdate=condicoes[index], match_fields=campos_validos)
    
    def run_consumer(self, operacoes: dict, query_sql: str = "sql_query", **kwargs):
        """
        Executa o processamento da camada Consumer de várias tabelas refineds
        Parâmetros:
            - `operacoes`:  Dicionário com as operações da camada Consumer
            - `query_sql`: Query SQL que será executada pelo  Spark SQL
        """
        nome_tabela = operacoes['table_name']
        formato_mensagem = f'{DeltaProcessingConsumer.__name__}.{self.run_consumer.__name__}'
        logger = logs.criar_log(formato_mensagem)
        logger.info(f"Iniciando processamento da Consumer {nome_tabela}")

        self.kwargs_param(**kwargs)


        # Criando uma lista com as tabelas
        tabelas = operacoes.get("join_operations", {}).get("tables", [])

        # Limpando a lista das deltatables e deltadim's
        self.deltatables.clear()
        self.deltadim.clear()
        # Limpando a lista de Views
        self.views.clear()
        
        # Iterando sobre as bases refined e criando as views SQL para cada uma, e removendo dataframe da memoria
        for index, tabela in enumerate(tabelas):
            path_table = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", f"{self.ambiente_dados[self.param['step']['refined']]}/{str(tabela).upper()}/"))
            self.tablehandler.set_deltatable_path(path_table)
            self.deltatables.append(self.tablehandler.get_deltatable().toDF())
            self.views.append(self.deltatables[index].createOrReplaceTempView(tabela))
            self.deltatables[index].unpersist()

        # Executando a consulta SQL de cruzamentos para a Consumer
        try:
            dataframe = self.spark.sql(operacoes[query_sql])
        except Exception as e:
            retorno_erro_consumer = f"Erro na execução da query sql para consumer da base {nome_tabela}: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada!"
            logger.warning(retorno_erro_consumer)
            return retorno_erro_consumer
        
        primeira_linha = dataframe.first()
        logger.info(f"Primeira linha da tabela {nome_tabela}: {primeira_linha}")

        # Verificando se o dataframe daquery veio vazio
        chave_primaria = operacoes.get("primary_key", None)
        if isinstance(chave_primaria, list):
            dataframe = dataframe.dropDuplicates(subset=chave_primaria)
        else:
            dataframe = dataframe.dropDuplicates(subset=[chave_primaria])
        if dataframe.isEmpty():
            retorno_df_vazio = f"Não há dados da refined para as bases {tabelas} - Processamento Ignorado! "
            logger.warning(retorno_df_vazio)
            return retorno_df_vazio
        for nome in tabelas:
            self.spark.catalog.dropTempView(nome)

        # Caminho em que a consumer será salva
        diretorio_consumer = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", f"{self.ambiente_dados[self.param['step']['consumer']]}/{str(nome_tabela).upper()}/"))

        try:
            self.save_update_table(sql_df=dataframe, operacoes=operacoes, diretorio=diretorio_consumer)
        except Exception as e:
            retorno_erro_atualizacao_consumer = f"Erro ao atualizar a tabela {nome_tabela}: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada!"
            logger.warning(retorno_erro_atualizacao_consumer)
            return retorno_erro_atualizacao_consumer
        
        if operacoes["upsert_refined"] == 'true':
            try:
                self._update_refined_tables(operacoes=operacoes, diretorio=diretorio_consumer)
            except Exception as e:
                logger.error(f"Erro ao atualizar as refineds que geram a tabela {nome_tabela}: {e}")
        
        retorno_sucesso_consumer = f"Consumer da {nome_tabela} - Concluída com Sucesso!"
        logger.info(retorno_sucesso_consumer)

        return retorno_sucesso_consumer