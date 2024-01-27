import findspark
findspark.init()
import os
from utils import formatar_sql, criar_log, preparar_mover
from pyspark.sql import functions
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql import DataFrame
from jobs.delta_processing import TableHandler

formato_mensagem = f'{__name__}'
logger = criar_log(formato_mensagem)

# Definindo a classe pai
class DeltraProcessing:
    """Classe para instanciar execução `delta lake` para `preparação``"""
    def __init__(self, ambientes_dados:dict, spark, **kwargs) -> None:
        """
        Aplica a contrução dos parâmetros para `DeltraProcessing`
        Parâmetros:
            - `ambientes_dados`: dicionario com as bases
            - `spark`: sessao spark
            - `kwargs`: kwargs
        """
        self.spark = spark
        self.ambiente_dados = ambientes_dados
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Usando as novas configurações para rebasear os valores de data e hora
        self.spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        self.spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        self.spark.sparkContext.setLogLevel("ERROR")
        self.param = {}
        self.kwargs_param(**kwargs)
        self.tablehandler_bronze = TableHandler(self.spark)
        self.tablehandler_silver = TableHandler(self.spark)
        self.keys = []

    def kwargs_param(self, **kwargs):
        self.param = {
            'header': 'true',
            'inferSchema': 'true',
            'format_out': 'delta',
            'mode': 'overwrite',
            'format_in': 'parquet',
            'upsert': True,
            'upsert_delete': False,
            'multiline': True
        }
        self.param.update(kwargs)
        self.keys = list(self.param.keys())
    
    def run_query(self, df: DataFrame, nome_tabela:str, operacao:dict, sql_query:str):
        """
        Executa a query sql e prepara o dataframe para as camadas seguintes
        Parâmetros:
            - `df`: DataFrame do método
            - `nome_tabela`: nome da tabela
            - `operacao`: dicionário com as tabelas
            - `sql_query`: query_sql que será executada
        Retorno
            `dataframe` dataframe inicial com dois campos adicionais `processed`, `creationDate`
        """
        # Instanciando o log do método
        formato_mensagem = f'{DeltraProcessing.__name__}.{self.run_query.__name__}'
        logger = criar_log(formato_mensagem)

        # Criando a view do spark dataframe
        tabela_temporaria = operacao[nome_tabela]["table_tmp"]
        logger.info(f"Processando a tabela: {tabela_temporaria} referente a tabela: {nome_tabela}")
        df.createOrReplaceTempView(tabela_temporaria)

        # Executando a operacao sql do dataframe
        sql_query_formatada = formatar_sql(operacao[nome_tabela][sql_query])
        df = self.spark.sql(sql_query_formatada)

        # Mostrando a primeira linha
        # print(df.first())
        
        # Extrai o esquema do DataFrame
        schema = df.schema

        # Percorre o esquema e adiciona os nomes das colunas às listas apropriadas
        colunas_date = [field.name for field in schema.fields if str(field.dataType) == "DateType"]
        colunas_timestamp = [field.name for field in schema.fields if str(field.dataType) == "TimestampType"]

        logger.info(f"Colunas do tipo date: {colunas_date}")
        logger.info(f"Colunas do tipo timestamp: {colunas_timestamp}")

        # Converte as datas para date ou timestamp
        for coluna in df.columns:
            if coluna in colunas_date:
                df = df.withColumn(coluna, to_date(col(coluna)))
            elif coluna in colunas_timestamp:
                df = df.withColumn(coluna, to_timestamp(col(coluna)))

        # Inserindo as colunas processed com False e creationDate com a data de hoje
        df = df.withColumn("processed", functions.lit(False)) \
            .withColumn("creationDate", functions.lit(functions.current_timestamp()))
        # Removendo os duplicados com base no _id criado na sql query
        chave_primaria = operacao[nome_tabela]["primary_key"]        
        df = df.dropDuplicates(subset=[chave_primaria])
        self.spark.catalog.dropTempView(tabela_temporaria)
        return df
    
# Definindo a classe bronze
class DeltaProcessingBronze(DeltraProcessing):
    def run_bronze(self, nome_tabela:str, operacao_delta:dict, operacao_transient:dict, sql_query:str= "bronze_query", **kwargs):
        """
        Executa o procesamento da camada bronze em uma tabela específica
        Parâmetros:
            - `nome_tabela`: Nome da tabela que será procesada
            - `operacao_delta`: Dicionário com as operações da camada bronze
            - `operacao_transient`: Dicionário com as operações da camada transient
            - `sql_query`: Query sql do método `run_query` que será executada
        """

        # Instancia os parâmetros
        self.kwargs_param(**kwargs)

        # Instanciando o log do método
        formato_mensagem = f"{DeltaProcessingBronze.__name__}.{self.run_bronze.__name__}"
        logger = criar_log(formato_mensagem)
        
        logger.info(f"Iniciando execução da camada bronze: {nome_tabela}")

        # Declarando localização das bases
        diretorio_transient = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..", f"{self.ambiente_dados['transient']}/{nome_tabela.upper()}/")) # Para rodar em cloud precisaremos alterar esse campo para buscar o repositório remoto
        logger.info(f"Diretório Transient: {diretorio_transient}")
        diretorio_bronze = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..", f"{self.ambiente_dados['bronze']}/{nome_tabela.upper()}/")) # Para rodar em cloud precisaremos alterar esse campo para buscar o repositório remoto
        logger.info(f"Diretório Bronze: {diretorio_bronze}")
        transient_options = {"upsert": True, "upsert_delete": False}
        transient_options.update(operacao_transient)
        # Lendo a camada transient
        tabela_transient = TableHandler(self.spark)

        # Realizando a leitura da camanda transient
        try:
            dataframe = tabela_transient.get_table(path=diretorio_transient, options=transient_options)
        except Exception as e:
            retorno_erro_transient = f"Encontrado erro na base: {nome_tabela} erro: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada!"
            logger.warning(retorno_erro_transient)
            return retorno_erro_transient
        
        # Chamando o método para executar a query sql da camada bronze
        try:
            dataframe = self.run_query(df=dataframe, nome_tabela=nome_tabela, operacao=operacao_delta, sql_query=sql_query)
        except Exception as e:
            retorno_erro_bronze = f"Erro na execução da query sql para camada bronze da base {nome_tabela}: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada"
            logger.warning(retorno_erro_bronze)
            return retorno_erro_bronze
        
        # Criando a camada bronze se ela não existir, ou realizando o upsert caso já exista
        self.tablehandler_bronze.set_deltatable_path(diretorio_bronze)
        if not self.tablehandler_bronze.is_deltatable():
            self.tablehandler_bronze.write_table(dataframe=dataframe, path=diretorio_bronze, options=self.param)
            logger.info(f"Tabela Bronze: {nome_tabela} Criada com sucesso")
        else:
            rotulo_origem = operacao_delta[nome_tabela]["label_orig"]
            rotulo_destino = operacao_delta[nome_tabela]["label_destino"]
            condicoes = operacao_delta[nome_tabela]["condition"]
            self.tablehandler_bronze.upsert_deltatable(dataframe=dataframe, label_origem=rotulo_origem, label_destino=rotulo_destino, condupdate=condicoes)
            logger.info(f"Tabela Bronze: {nome_tabela} Atualizada com sucesso")

        dataframe.unpersist()
        retorno_sucesso_bronze = f"Processamento Camada Bronze da tabela {nome_tabela} - Concluída com Sucesso!"
        logger.info(retorno_sucesso_bronze)

        destino_transient_files = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..", f"{self.ambiente_dados['transient']}/TRANSIENTFILES/{nome_tabela.upper()}"))
        print(destino_transient_files)
        preparar_mover(origem=diretorio_transient, destino=destino_transient_files)

        return retorno_sucesso_bronze
    
# Definindo a classe Silver
class DeltaProcessingSilver(DeltraProcessing):
    def run_silver(self, nome_tabela:str, operacao:dict, sql_query:str= "silver_query", **kwargs):
        """
        Executa o processamento da camada Silver de uma tabela
        Parâmetros:
            - `nome_tabela`: nome da tabela a ser processada
            - `operacao`: Dicionário com as operações da camada Silver
            - `sql_query`: Query SQL que o método `run_query` vai executar
        """

        # Instancia os parâmetros
        self.kwargs_param(**kwargs)

        # Instanciando o log do método
        formato_mensagem = f"{DeltaProcessingSilver.__name__}.{self.run_silver.__name__}"
        logger = criar_log(formato_mensagem)
        logger.info(f"Iniciando a camada silver da tabela: {nome_tabela}")

        # Declarando os locais da silver e bronze
        diretorio_bronze = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..", f"{self.ambiente_dados['bronze']}/{nome_tabela.upper()}/")) # Para rodar em cloud precisaremos alterar esse campo para buscar o repositório remoto
        diretorio_silver = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..", f"{self.ambiente_dados['silver']}/{nome_tabela.upper()}/")) # Para rodar em cloud precisaremos alterar esse campo para buscar o repositório remoto

        # Verificando se a bronze já existe
        self.tablehandler_bronze.set_deltatable_path(diretorio_bronze)
        if not self.tablehandler_bronze.is_deltatable():
            retorno_nao_existe_bronze = f"Camada bronze da tabela {nome_tabela} não existe - Base Ignorada!"
            logger.warning(retorno_nao_existe_bronze)
            return retorno_nao_existe_bronze
        
        # Transoformando a camada bronze em um spark dataframe
        dataframe_bronze = self.tablehandler_bronze.get_deltatable().toDF()

        # Chamando o método para a Query SQL da camada Silver
        try:
            dataframe = self.run_query(df=dataframe_bronze, nome_tabela=nome_tabela, operacao=operacao, sql_query=sql_query)
        except Exception as e:
            # Caso dê algum erro, captura e registra no log
            retorno_erro_silver = f"Erro na execução da Query SQL da camada Silver da base {nome_tabela}: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada!"
            logger.warning(retorno_erro_silver)
            dataframe_bronze.unpersist()
            return retorno_erro_silver
        
        # Verificando se o dataframe está vazio
        if dataframe.isEmpty():
            retorno_df_vazio = f"Não há dados na camada bronze da base {nome_tabela} para o processamento - Base Ignorada!"
            logger.warning(retorno_df_vazio)
            dataframe_bronze.unpersist()
            dataframe.unpersist()
            return retorno_df_vazio
        
        # Criando a camada silver se ela não existir, ou realizando o upsert caso já exista
        self.tablehandler_silver.set_deltatable_path(diretorio_silver)

        # Parâmetros de transição dos dados
        rotulo_origem = operacao[nome_tabela]["label_orig"]
        rotulo_destino = operacao[nome_tabela]["label_destino"]
        condicoes = operacao[nome_tabela]["condition"]
        campos_validado = operacao[nome_tabela]["match_filds"]

        if not self.tablehandler_silver.is_deltatable():
            self.tablehandler_silver.write_table(dataframe=dataframe, path=diretorio_silver, options=self.param)
            logger.info(f"Tabela Silver: {nome_tabela} Criada com sucesso")
        else:
            self.tablehandler_silver.upsert_deltatable(dataframe=dataframe, label_origem=rotulo_origem, label_destino=rotulo_destino, condupdate=condicoes)
            logger.info(f"Tabela Silver: {nome_tabela} Atualizada com sucesso")

        # Verificando se a silver existe, e realizando o upsert na bronze para alterar o processed
        if self.tablehandler_silver.is_deltatable():
            self.tablehandler_bronze.upsert_from_df(dataframe=dataframe, label_origem=rotulo_origem, label_destino=rotulo_destino, condupdate=condicoes,match_fields=campos_validado)
        
        dataframe_bronze.unpersist()
        dataframe.unpersist()
        retorno_sucesso_silver = f"Camada Silver da tabela {nome_tabela} - Concluída com Sucesso!"
        logger.info(retorno_sucesso_silver)

        return retorno_sucesso_silver