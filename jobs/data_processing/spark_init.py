import findspark
findspark.init()
import logging
from pyspark.sql import SparkSession
from utils import buscar_dados_vcpu_so, definir_variaveis_ambiente

cpu_cores, memoria_gb = buscar_dados_vcpu_so()

# Definindo as variáveis de ambiente
definir_variaveis_ambiente()

# Definir o nível de log para "WARNING"
logging.getLogger("py4j").setLevel(logging.WARNING)

class Sparkinit():
    """Classe para instanciar a sessão operacional do Spark"""
    def __init__(self, processadores:str = cpu_cores, memoria:str = memoria_gb) -> SparkSession:
        """Aplica as configurações do spark para execução local"""
        self.cpu_cores = max(1, int(processadores) - 1)
        self.memoria = memoria
        
        # Configuração dos pacotes Delta Lake
        self.pacotes = ["io.delta:delta-core_2.12:2.4.0", "io.delta:delta-storage:2.4.0",
          "org.apache.spark:spark-core_2.12:3.4.0",  "org.apache.hadoop:hadoop-common:3.3.6"]

        builder = SparkSession.builder \
            .appName("jobGPanvel") \
            .master("local[*]") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.cores.max", self.cpu_cores) \
            .config("spark.driver.cores", f"{self.cpu_cores}") \
            .config("spark.driver.memory", f"{self.memoria}g") \
            .config("spark.driver.maxResultSize", f"{int(self.memoria / 3)}g") \
            .config("spark.driver.memoryOverhead", "3g") \
            .config("spark.io.compression.lz4.blockSize", "512k") \
            .config("spark.shuffle.service.index.cache.size", "100m") \
            .config("spark.jars.packages", ",".join(self.pacotes)) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") \
            .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        self.spark_session = builder.getOrCreate()

    def buscar_sessao_spark(self) -> SparkSession:
        """Retorna a construção de seção do spark construída acima"""
        return self.spark_session
