
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
    .config("spark.hadoop.fs.s3a.access.key", acess_key)\
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)\
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.memory.offHeap.enabled", "true")\
    .config("spark.memory.offHeap.size","14g")\
    .config("spark.memory.fraction", 0.8)\
    .config("spark.executor.memory","14g")\
    .config("spark.driver.memory","14g")\
    .config('spark.driver.maxResultSize', '12g')\
    .config("spark.sql.shuffle.partitions", 800)\
    .getOrCreate()

    # .master("local[*]") \
    # .config("hive.metastore.uris", "thrift://84.46.240.25:9083")\
    # .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse" )\
    # .config('hive.optimize.sort.dynamic.partition',True)\
    # .enableHiveSupport() \

def get_new_spark_session(app_name) -> SparkSession:
    active_session = SparkSession.getActiveSession()
    if active_session == None:
        return build_new_spark_session(app_name)
    return SparkSession.newSession(active_session) 

def get_active_session() -> SparkSession:
    return SparkSession.getActiveSession()

def close_spark_session(spark_session):
    SparkSession.stop(spark_session)