import pandas as pd
import findspark
findspark.init()

import build_spark_session as bss
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = bss.build_new_spark_session("ReceitaFederal")

def tratar_empresas():
        schema = StructType([StructField("CNPJ BASICO", LongType(), True),
                        StructField("RAZAO SOCIAL", StringType(), True),
                        StructField("COD NATUREZA JURIDICA", ShortType(), True),
                        StructField("COD QUALIFICACAO SOCIO", ShortType(), True),
                        StructField("CAPITAL SOCIAL", StringType(), True),
                        StructField("COD PORTE DA EMPRESA", ShortType(), True),
                        ])

        path = ["s3a://lead-generation-data-raw/Empresas"+ str(i) +".csv" for i in range(10)]

        empresas = spark.read.options(delimiter=';')\
                                .schema(schema) \
                                .option("numPartitions", 20)\
                                .option("lowerBound", 0)\
                                .option("upperBound", 4000000)\
                                .csv(path)

        empresas = empresas.na.fill(value="-")

        qualificacao_socios = spark.read.options(delimiter = ";")\
                                        .schema(StructType([StructField("COD QUALIFICACAO SOCIO", ShortType(), True),
                                                        StructField("QUALIFICACAO DO SOCIO", StringType(), True)]))\
                                        .csv("s3a://lead-generation-data-raw/Qualificacoes.csv")

        natureza_juridica = spark.read.options(delimiter = ";")\
                                        .schema(StructType([StructField("COD NATUREZA JURIDICA", ShortType(), True),
                                                        StructField("NATUREZA JURIDICA", StringType(), True)]))\
                                        .csv("s3a://lead-generation-data-raw/Naturezas.csv")

        empresas = empresas.join(qualificacao_socios, ["COD QUALIFICACAO SOCIO"])\
                                .join(natureza_juridica, ["COD NATUREZA JURIDICA"])

        empresas = empresas.na.fill(value="-")\
                        .withColumn("PORTE DA EMPRESA", 
                                when(empresas["COD PORTE DA EMPRESA"] == 0.0, "MICROEMPREENDEDOR INDIVIDUAL (MEI)")\
                                .when(empresas["COD PORTE DA EMPRESA"] == 1.0, "MICRO EMPRESA (ME)")\
                                .when(empresas["COD PORTE DA EMPRESA"] == 3.0, "EMPRESA DE PEQUENO PORTE (EPP)")\
                                .when(empresas["COD PORTE DA EMPRESA"] == 5.0, "DEMAIS"))\
                        .withColumn("FATURAMENTO/ANO", 
                                when(empresas["COD PORTE DA EMPRESA"] == 0.0, "ATÉ 81 MIL")\
                                .when(empresas["COD PORTE DA EMPRESA"] == 1.0, "ATÉ 360 MIL")\
                                .when(empresas["COD PORTE DA EMPRESA"] == 3.0, "360 MIL - 4,8 MILHOES")\
                                .when(empresas["COD PORTE DA EMPRESA"] == 5.0, "MAIS DE 4,8 MILHOES"))\
                        .withColumn("QUALIFICACAO DO SOCIO", upper(col("QUALIFICACAO DO SOCIO")))\
                        .withColumn("NATUREZA JURIDICA", upper(col("NATUREZA JURIDICA")))\
                        .withColumn("CNPJ BASICO", lpad(col("CNPJ BASICO"), 8, "0"))

        empresas = empresas.select(["CNPJ BASICO", "RAZAO SOCIAL", "CAPITAL SOCIAL", 
                                        "NATUREZA JURIDICA", "PORTE DA EMPRESA", "FATURAMENTO/ANO"])
        return empresas

# empresas.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in empresas.columns]).show()
