import pandas as pd
import findspark
findspark.init()

import build_spark_session as bss
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = bss.build_new_spark_session("ReceitaFederal")

def tratar_simples():
    simples = spark.read\
                    .options(delimiter = ";")\
                    .schema(StructType([StructField("CNPJ BASICO", LongType(), True),
                                        StructField("OPCAO PELO MEI", StringType(), True),
                                        StructField("DATA DE OPCAO PELO MEI", StringType(), True),
                                        StructField("DATA DE EXCLUSAO DO MEI", StringType(), True)]))\
                    .option("numPartitions", 35)\
                    .option("lowerBound", 0)\
                    .option("upperBound", 35000000)\
                    .csv("s3a://lead-generation-data-raw/Simples.csv")

    simples = simples.na.fill(value="-")\
                    .withColumn("CNPJ BASICO", lpad(col("CNPJ BASICO"), 8, "0"))\
                    .withColumn("DATA DE OPCAO PELO MEI", to_date(col("DATA DE OPCAO PELO MEI"), "yyyyMMdd"))\
                    .withColumn("DATA DE EXCLUSAO DO MEI", to_date(col("DATA DE EXCLUSAO DO MEI"), "yyyyMMdd"))

    return simples