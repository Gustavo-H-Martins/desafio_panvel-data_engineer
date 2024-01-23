import findspark
findspark.init()
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "delta_setup")))
from pyspark.sql import functions
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql import DataFrame
from tablehandler import TableHandler
import logs
from configuracoes import formatar_sql

formato_mensagem = f'{__name__}'
logger = logs.criar_log(formato_mensagem)

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
        self.tablehandler_raw = TableHandler(self.spark)
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
        logger = logs.criar_log(formato_mensagem)

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
        self.spark.catalogs.dropTempView(tabela_temporaria)
        return df
    
# Definindo a classe crua
class DeltaProcessingRaw(DeltraProcessing):
    def run_raw(self, nome_tabela:str, operacao:dict, sql_query:str= "sql_create_id", **kwargs):
        """
        Executa o procesamento da camada crua em uma tabela específica
        Parâmetros:
            - `nome_tabela`: Nome da tabela que será procesada
            - `operacao`: Dicionário com as operações da camada raw
            - `sql_query`: Query sql do método `run_query` que será executada
        """

        # Instancia os parâmetros
        self.kwargs_param(**kwargs)

        # Instanciando o log do método
        formato_mensagem = f"{DeltaProcessingRaw.__name__}.{self.run_raw.__name__}"
        logger = logs.criar_log(formato_mensagem)
        
        logger.info(f"Iniciando execução da camada raw: {nome_tabela}")

        # Declarando localização das bases
        diretorio_transient = f"{self.ambiente_dados['transient']}/{nome_tabela.upper()}/"
        logger.info(f"Diretório Transient: {diretorio_transient}")
        diretorio_raw = f"{self.ambiente_dados['raw']}/{nome_tabela.upper()}/"
        logger.info(f"Diretório Raw: {diretorio_raw}")
        transient_options = {'header': 'true', 'inferSchema': 'true', 'format_out': 'delta', 'mode': 'overwrite', 'format_in': 'avro', 'upsert': True, 'upsert_delete': False}
        # Lendo a camada transient
        tabela_transient = TableHandler(self.spark)

        # Realizando a leitura da camanda transient
        try:
            dataframe = tabela_transient.get_table(path=diretorio_transient, options=transient_options)
        except Exception as e:
            retorno_erro_transient = f"Encontrado erro na base: {nome_tabela} erro: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada!"
            logger.warning(retorno_erro_transient)
            return retorno_erro_transient
        
        # Chamando o método para executar a query sql da camada crua
        try:
            dataframe = self.run_query(df=dataframe, nome_tabela=nome_tabela, operacao=operacao, sql_query=sql_query)
        except Exception as e:
            retorno_erro_raw = f"Erro na execução da query sql para camada raw da base {nome_tabela}: {'Diretório Vazio'  if 'Path does not exist' in str(e) else str(e)} - Base Ignorada"
            logger.warning(retorno_erro_raw)
            return retorno_erro_raw
        
        # Criando a camada raw se ela não existir, ou realizando o upsert caso já exista
        self.tablehandler_raw.set_deltatable_path(diretorio_raw)
        if not self.tablehandler_raw.is_deltatable():
            self.tablehandler_raw.write_table(dataframe=dataframe, path=diretorio_raw, options=self.param)
        else:
            rotulo_origem = operacao[nome_tabela]["label_orig"]
            rotulo_destino = operacao[nome_tabela]["label_destino"]
            condicoes = operacao[nome_tabela]["condition"]
            self.tablehandler_raw.upsert_deltatable(dataframe=dataframe, label_origem=rotulo_origem, label_destino=rotulo_destino, condupdate=condicoes)

        dataframe.unpersist()
        retorno_sucesso_raw = f"Camada Raw da tabela {nome_tabela} - Concluída com Sucesso!"
        logger.info(retorno_sucesso_raw)

        return retorno_sucesso_raw