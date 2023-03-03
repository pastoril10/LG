
from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read("config.ini")
acess_key = config["bucket-s3"]["access_key"]
secret_key = config["bucket-s3"]["secret_key"]

def build_new_spark_session(app_name) -> SparkSession: 
     """
    Cria uma nova SparkSession.
        Parametros:
        `app_name`: Nome da sessão ativa do Spark.
    Retorno:
        SparkSession
    """
     return\
     SparkSession\
    .builder \
    .appName(app_name)\
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.access.key", acess_key)\
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)\
    .config("spark.hadoop.fs.s3a.path.style.access", 'true') \
    .config("spark.hadoop.fs.s3a.fast.upload", 'true') \
    .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.parquet.int96RebaseModeInWrite","LEGACY")\
    .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled",'true')\
    .config("spark.sql.inMemoryColumnarStorage.compressed", 'true')\
    .config("spark.memory.fraction",0.2)\
    .config("spark.executor.memory","10g")\
    .config("spark.driver.memory","10g")\
    .config('spark.driver.maxResultSize', '10g')\
    .config("spark.sql.shuffle.partitions",900)\
    .config("spark.memory.offHeap.enabled",'true')\
    .config("spark.memory.offHeap.size","10g")\
    .enableHiveSupport() \
    .getOrCreate()
 
    # .config("spark.sql.inMemoryColumnarStorage.batchSize",10000)\
def get_new_spark_session(app_name) -> SparkSession:
    active_session = SparkSession.getActiveSession()
    if active_session == None:
        return build_new_spark_session(app_name)
    return SparkSession.newSession(active_session) 

def get_active_session() -> SparkSession:
    return SparkSession.getActiveSession()

def close_spark_session(spark_session):
    SparkSession.stop(spark_session)