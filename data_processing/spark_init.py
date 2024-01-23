import findspark
findspark.init()
import logging
from pyspark.sql import SparkSession
from data_setup.configuracoes import buscar_dados_vcpu_so, definir_variaveis_ambiente
import os 

cpu_cores, memoria_gb = buscar_dados_vcpu_so()

# Definindo as variáveis de ambiente
definir_variaveis_ambiente()

jar_path  = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","jars"))
hadoop_jar_path = os.path.abspath(os.path.join(jar_path, "hadoop-common-3.3.6.jar"))
spark_jar_path  = os.path.abspath(os.path.join(jar_path, "spark-core_2.12-3.5.0.jar"))

# Definir o nível de log para "WARNING"
logging.getLogger("py4j").setLevel(logging.WARNING)

from pyspark import SparkContext

# Encerra o SparkContext se existir
SparkContext.getOrCreate().stop()

class Sparkinit():
    """Classe para instanciar a sessão operacional do Spark"""
    def __init__(self,  processadores:str = cpu_cores, memoria:str = memoria_gb) -> SparkSession:
        """Aplica as configurações do spark para execução local"""
        self.cpu_cores = max(1, int(processadores) - 1)
        self.memoria = memoria

        self.spark = SparkSession.builder \
            .appName("jobGPanvel") \
            .master("local[*]") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.cores.max", "8") \
            .config("spark.driver.cores", f"{self.cpu_cores}") \
            .config("spark.driver.memory", f"{self.memoria}g") \
            .config("spark.driver.memoryOverhead", "3g") \
            .config("spark.shuffle.file.buffer", "1m") \
            .config("spark.file.transferTo", "false") \
            .config("spark.shuffle.unsafe.file.output.buffer", "1m") \
            .config("spark.io.compression.lz4.blockSize", "512k") \
            .config("spark.shuffle.service.index.cache.size", "100m") \
            .config("spark.shuffle.registration.timeout", "120000ms") \
            .config("spark.shuffle.registration.maxAttempts", "5") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "2047m") \
            .config("spark.jars", f"{spark_jar_path},{hadoop_jar_path}") \
            .config("spark.jars.packages",
                    "io.delta:delta-core_2.12-2.4.0,io.delta:delta-storage-3.0.0,") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .config("spark.databricks.delta.properties.defaults.logRetentionDuration", "interval 7 days")\
            .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") \
            .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .getOrCreate()

    
    def buscar_sessao_spark(self) -> SparkSession:
        """Retorna a construção de seção do spark constrúida acima"""

        return self.spark