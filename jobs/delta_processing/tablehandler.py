from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from utils import logs


formato_mensagem = f'{__name__}'
logger = logs.criar_log(formato_mensagem)

class TableHandler(object):
    """ Instancia tabelas para operacoes de leitura, escrita, upsert
    """

    def __init__(self, spark: SparkSession, localpath=None) -> None:
        """ Inicia os parâmetros da Classe
        ParÂmetros:
        - `spark`: Sessão do spark atual
        - `localpath`: Quando usando a nuvem, não passar valor
        """
        self.spark = spark
        self.__pyspark_format_available = ['csv', 'avro', 'json', 'parquet', 'delta']
        self.__bool_available = ["true", "false"]
        self.__pyspark_mode = ["append", "overwrite"]
        self._local_path = localpath
        self._deltatable = None

        if self._local_path:
            self.is_deltatable()

    def get_deltatable(self) -> DeltaTable:
        """ Verifica se a tabela delta existe ou não, 
        caso exista a tabela é retornada, caso oposto dá erro.
        - Retorno:  Deltatable
        """
        self._set_deltatable()
        return self._deltatable

    def set_deltatable_path(self, path: str) -> None:
        self._local_path = path

    def is_deltatable(self):
        """ Faz testes se o caminho possui uma tabela delta armazenada ou não 
        = Retorno: True se for uma deltatable caso contrário retorna false
        """
        try:
            is_table = DeltaTable.isDeltaTable(self.spark, self._local_path)
            self._set_deltatable()
        except Exception as e:
            is_table = False
            logger.warning(f"O diretório `{self._local_path}` não é Delta")

        return is_table

    def _set_deltatable(self) -> None:
        """ Define uma tabela delta se existir, caso contrário, retorna uma exceção
        - Retorno: None
        """
        self._deltatable = DeltaTable.forPath(self.spark, self._local_path)

        # Executa o comando OPTIMIZE
        self._deltatable.optimize().executeCompaction()
        
    def get_table(self, path: str, options: dict):
        """ Realiza a leitura de arquivos de diversos formatos como : `['csv', 'avro', 'json', 'parquet', 'delta']`
        Parâmetros: 
        - `path`: Caminho dos arquivos
        - `options`: Lista de opções de leitura dos arquivos
        Retorno: Dataframe como uma tabela Delta
        """
        fmsg = f'{TableHandler.__name__}.{self.get_table.__name__}'
        if options['format_in'] in self.__pyspark_format_available and \
                options['header'] in self.__bool_available and \
                options['inferSchema'] in self.__bool_available:
            if "json" in options['format_in']:
                table = self.spark.read.format(options['format_in']) \
                    .option('inferSchema', options['inferSchema']) \
                    .option('header', options['header']) \
                    .option('multiline', options['multiline']) \
                    .load(path)
            else:
                table = self.spark.read.format(options['format_in']) \
                    .option('inferSchema', options['inferSchema']) \
                    .option('header', options['header']) \
                    .load(path)
        else:
            logs.criar_log(fmsg).error(f"Unsupported format {self.__pyspark_format_available}  or "
                                         f"header differ from {self.__bool_available} or "
                                         f"inferSchema differ from {self.__bool_available} ")
            raise ValueError(f"Unsupported format {self.__pyspark_format_available}  or "
                             f"header differ from {self.__bool_available} or "
                             f"inferSchema differ from {self.__bool_available} ")
        return table

    @logs.logs
    def write_table(self, dataframe, path: str, options: dict) -> None:
        """ Escreve um arquivo em diversos formatos principalmente Delta
        Parâmetros: 
        - `dataframe`: data to be saved
        - `path`: Caminho de destino
        - `options`: Parâmetros úteis para o processamento
        Retorno: None
        """
        fmsg = f'{TableHandler.__name__}.{self.write_table.__name__}'
        if options['format_out'] in self.__pyspark_format_available and \
                options['header'] in self.__bool_available and \
                options['mode'] in self.__pyspark_mode:
            
            dataframe.write.format(options['format_out']) \
                .mode(options['mode']) \
                .option('header', options['header']) \
                .save(path)

        else:
            logs.criar_log(fmsg).error(f"Unsupported format {self.__pyspark_format_available}  or "
                                         f"header differ from {self.__bool_available} or "
                                         f"mode differ from {self.__pyspark_mode}")
            raise ValueError(f"Unsupported format {self.__pyspark_format_available}  or "
                             f"header differ from {self.__bool_available} or "
                             f"mode differ from {self.__pyspark_mode}")

    @logs.logs
    def upsert_deltatable(self, dataframe: DataFrame, label_origem: str,
                          label_destino: str, condupdate: str) -> None:
        """ Realiza o `upsert`, uma operação que insere dados que não existem ou só atualiza dados existentes.
            Parâmetros: 
            - `dataframe`: Dataframe com os dados para atualizar.
            - `condupdate`: Condições a serem atendidas para atualização
            - `label_origem`: alias para o dataframe
            - `label_destino`: alias para a tabela delta de destino
            Retorno: none
        """
        self._deltatable.alias(label_destino) \
            .merge(source=dataframe.alias(label_origem),
                   condition=condupdate) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        # Executa o comando VACUUM
        self._deltatable.vacuum(48)

    @logs.logs
    def upsert_deltatable_with_delete(self, dataframe: DataFrame, label_origem: str,
                                      label_destino: str, condupdate: str, cond_delete: str) -> None:
        """ Realiza o `upsert`, uma operação que insere dados que não existem ou só atualiza dados existentes 
        podendo deletar dados com base nas condições passadas. 
            Parâmetros:
            - `dataframe`: Dataframe com os dados para atualizar.
            - `label_origem`: alias para o dataframe 
            - `label_destino`: alias para a tabela delta de destino
            - `condupdate`: Condições a serem atendidas para atualização
            - `cond_delete`: Condições a serem atendidas para deletar os dados.
            Retorno: none"""

        self._deltatable.alias(label_destino) \
            .merge(source=dataframe.alias(label_origem),
                   condition=condupdate) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .whenMatchedDelete(condition=cond_delete) \
            .execute()

        # Executa o comando VACUUM
        self._deltatable.vacuum(48)

    @logs.logs
    def upsert_table(self, deltatable: DeltaTable,
                     label_origem: str,
                     label_destino: str,
                     condupdate: str,
                     match_fields: dict) -> None:
        """ Realiza o `upsert`, uma operação que insere dados que não existem ou só atualiza dados existentes.
            Parâmetros: 
            - `dataframe`: Dataframe com os dados para atualizar.
            - `condupdate`: Condições a serem atendidas para atualização
            - `label_origem`: alias para o dataframe
            - `label_destino`: alias para a tabela delta de destino
            - `match_fields`: campos para alterar.
        Retorno : none
        """

        self._deltatable.alias(label_destino) \
            .merge(source=deltatable.toDF().alias(label_origem),
                   condition=condupdate) \
            .whenMatchedUpdate(set=match_fields) \
            .execute()

        # Executa o comando VACUUM
        self._deltatable.vacuum(48)

    @logs.logs
    def upsert_from_df(self, dataframe,
                       label_origem: str,
                       label_destino: str,
                       condupdate: str,
                       match_fields: dict) -> None:
        """ Realiza o `upsert`, uma operação que insere dados que não existem ou só atualiza dados existentes em um dataframe.
        Parâmetros: 
        - `dataframe`: Dataframe com os dados para atualizar.
        - `label_origem`: alias para o dataframe
        - `label_destino`: alias para a tabela delta de destino
        - `condupdate`: Condições a serem atendidas para atualização
        - `match_fields`: campos para alterar
        Retorno : none
        """

        self._deltatable.alias(label_destino) \
            .merge(source=dataframe.alias(label_origem),
                   condition=condupdate) \
            .whenMatchedUpdate(set=match_fields) \
            .execute()

        # Executa o comando VACUUM
        self._deltatable.vacuum(48)
