import findspark
findspark.init()
import os
from utils import criar_log, logs
from delta.tables import DataFrame
from jobs.delta_processing import TableHandler

formato_mensagem = f'{__name__}'
logger = criar_log(formato_mensagem)


class DeltaProcessingGold:
    def __init__(self, ambiente_dados : dict, spark, **kwargs):
        """
        Classe para instanciar a execução da camada Gold
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
            'step': {"silver": 'silver', "gold": "gold"}
        }
        self.param.update(kwargs)
        self.keys = list(self.param.keys())
    
    @logs.logs
    def save_update_table(self, sql_df: DataFrame, operacoes: dict, diretorio: str):
        """
        Cria uma camada gold ou realiza o upsert se a gold já  existir
        Parâmetros:
            - `sql_df`: Dataframe criado pela consulta SQL do Spark
            - `operacoes`: Dicionário com as operações da camada Gold
            - `diretorio`: Caminho para a camada Gold
        """

        rotulo_origem = operacoes["label_orig"]
        rotulo_destino = operacoes["label_destino"]
        condicoes = operacoes["condition"]

        # Instanciando o Delta Table
        tablehandler_gold = TableHandler(self.spark, diretorio)

        # Verificando se a tabela já existe
        if not tablehandler_gold.is_deltatable():
            tablehandler_gold.write_table(dataframe=sql_df, path=diretorio, options=self.param)
        else:
            tablehandler_gold.upsert_deltatable(dataframe=sql_df, label_origem=rotulo_origem, label_destino=rotulo_destino, condupdate=condicoes)
        
    @logs.logs
    def _update_silver_tables(self, operacoes: dict, diretorio: str):
        """
        Realiza o upsert da Silver usando a Gold para alterar os valores (ex: processed para True)
        Parâmetros:
             - `operacoes`: Dicionário com as operações da camada Gold
             - `diretorio`: Caminho para a camada Gold
        """
        rotulo_origem = operacoes["label_orig"]
        condicoes = operacoes["tables_silver"]["conditions"]
        campos_validos =  operacoes["match_filds"]

        # Instanciando o Delta Table
        tablehandler_gold = TableHandler(spark=self.spark, localpath=diretorio)

        # Verificando se a tabela já existe
        if tablehandler_gold.is_deltatable():
            # Iterando entre as Silvers
            for index, valor in enumerate(operacoes["tables_silver"]["tables"]):
                path_table = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "..", f"{self.ambiente_dados[self.param['step']['silver']]}/{str(valor).upper()}/"))
                tablehandler_silver = TableHandler(spark=self.spark, localpath=path_table)

                deltatable_gold = tablehandler_gold.get_deltatable()
                tablehandler_silver.upsert_table(deltatable=deltatable_gold, label_origem=rotulo_origem, label_destino=valor, condupdate=condicoes[index], match_fields=campos_validos)
    
    def run_gold(self, operacoes: dict, query_sql: str = "gold_query", **kwargs):
        """
        Executa o processamento da camada Gold de várias tabelas silvers
        Parâmetros:
            - `operacoes`:  Dicionário com as operações da camada Gold
            - `query_sql`: Query SQL que será executada pelo  Spark SQL
        """
        nome_tabela = operacoes['table_name']
        formato_mensagem = f'{DeltaProcessingGold.__name__}.{self.run_gold.__name__}'
        logger = criar_log(formato_mensagem)
        logger.info(f"Iniciando processamento da Gold {nome_tabela}")

        self.kwargs_param(**kwargs)


        # Criando uma lista com as tabelas
        tabelas = operacoes.get("join_operations", {}).get("tables", [])

        # Limpando a lista das deltatables e deltadim's
        self.deltatables.clear()
        self.deltadim.clear()
        # Limpando a lista de Views
        self.views.clear()
        
        # Iterando sobre as bases silver e criando as views SQL para cada uma, e removendo dataframe da memoria
        for index, tabela in enumerate(tabelas):
            path_table = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "..", f"{self.ambiente_dados[self.param['step']['silver']]}/{str(tabela).upper()}/"))
            self.tablehandler.set_deltatable_path(path_table)
            self.deltatables.append(self.tablehandler.get_deltatable().toDF())
            self.views.append(self.deltatables[index].createOrReplaceTempView(tabela))
            self.deltatables[index].unpersist()

        # Executando a consulta SQL de cruzamentos para a Gold
        try:
            dataframe = self.spark.sql(operacoes[query_sql])
        except Exception as e:
            retorno_erro_gold = f"Erro na execução da query sql para gold da base {nome_tabela}: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada!"
            logger.warning(retorno_erro_gold)
            return retorno_erro_gold
        
        primeira_linha = dataframe.first()
        logger.info(f"Primeira linha da tabela {nome_tabela}: {primeira_linha}")

        # Verificando se o dataframe daquery veio vazio
        chave_primaria = operacoes.get("primary_key", None)
        if isinstance(chave_primaria, list):
            dataframe = dataframe.dropDuplicates(subset=chave_primaria)
        else:
            dataframe = dataframe.dropDuplicates(subset=[chave_primaria])
        if dataframe.isEmpty():
            retorno_df_vazio = f"Não há dados da silver para as bases {tabelas} - Processamento Ignorado! "
            logger.warning(retorno_df_vazio)
            return retorno_df_vazio
        for nome in tabelas:
            self.spark.catalog.dropTempView(nome)

        # Caminho em que a gold será salva
        diretorio_gold = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "..", f"{self.ambiente_dados[self.param['step']['gold']]}/{str(nome_tabela).upper()}/"))

        try:
            self.save_update_table(sql_df=dataframe, operacoes=operacoes, diretorio=diretorio_gold)
        except Exception as e:
            retorno_erro_atualizacao_gold = f"Erro ao atualizar a tabela {nome_tabela}: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada!"
            logger.warning(retorno_erro_atualizacao_gold)
            return retorno_erro_atualizacao_gold
        
        if operacoes["upsert_silver"] == 'true':
            try:
                self._update_silver_tables(operacoes=operacoes, diretorio=diretorio_gold)
            except Exception as e:
                logger.error(f"Erro ao atualizar as silvers que geram a tabela {nome_tabela}: {e}")
        
        retorno_sucesso_gold = f"Gold da {nome_tabela} - Concluída com Sucesso!"
        logger.info(retorno_sucesso_gold)

        return retorno_sucesso_gold