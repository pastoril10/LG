import pandas as pd
import findspark
findspark.init()


import build_spark_session as bss
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = bss.build_new_spark_session("ReceitaFederal")

schema = StructType([StructField("CNPJ BASICO", LongType(), True),
                      StructField("ID DO SOCIO", ShortType(), True),
                      StructField("NOME SOCIO", StringType(), True),
                      StructField("CNPJ/CPF DO SOCIO", StringType(), True),
                      StructField("COD QUALIFICACAO SOCIO", ShortType(), True),
                      StructField("DT ENTRADA NA SOCIEDADE", StringType(), True),
                      StructField("CODIGO PAIS", ShortType(), True),
                      StructField("REPRESENTANTE LEGAL", StringType(), True),
                      StructField("NOME REPRESENTANTE", StringType(), True),
                      StructField("QUALIFICAÇÃO DO REPRESENTANTE LEGAL", ShortType(), True),
                      StructField("COD FAIXA ETARIA", ShortType(), True),
                    ])

path = ["s3a://lead-generation-data-raw/Socios"+ str(i) +".csv" for i in range(10)]

def tratar_socios():

  socios = spark.read.options(delimiter=';')\
                      .schema(schema) \
                      .option("numPartitions", 20)\
                      .option("lowerBound", 0)\
                      .option("upperBound", 4000000)\
                      .csv(path)

  socios = socios.na.fill(value=105, subset=["CODIGO PAIS"])\
                    .na.fill(value="-")


  qualificacao_socios = spark.read.options(delimiter = ";")\
                                  .schema(StructType([StructField("COD QUALIFICACAO SOCIO", ShortType(), True),
                                                      StructField("QUALIFICACAO DO SOCIO", StringType(), True)]))\
                                  .csv("s3a://lead-generation-data-raw/Qualificacoes.csv")

  qualificacao_socios = qualificacao_socios.withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "�",""))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "Scio","SOCIO"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "nan","NAO INFORMADO"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", 'No informada', "NAO INFORMADO"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "Fsica", "FISICA"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "Indstria", "INDRUSTRIA"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "Comanditrio", "COMANDITADO"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "jurdica", "JURIDICA"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "Fsica", "FISICA"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "Administrao", "Administracao"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", regexp_replace("QUALIFICACAO DO SOCIO", "Secretario", "Secretario"))\
                                          .withColumn("QUALIFICACAO DO SOCIO", upper(col("QUALIFICACAO DO SOCIO")))

                                                
  socios = socios.join(qualificacao_socios, ["COD QUALIFICACAO SOCIO"])\
                  .withColumn("CNPJ BASICO", lpad(col("CNPJ BASICO"), 8, "0"))\
                  .withColumn("DT ENTRADA NA SOCIEDADE", to_date(col("DT ENTRADA NA SOCIEDADE"), "yyyyMMdd"))\
                  .withColumn("TIPO SOCIO", 
                              when(socios["ID DO SOCIO"] == 1, "PESSOA JURIDICA")\
                              .when(socios["ID DO SOCIO"] == 2, "PESSOA FISICA")\
                              .when(socios["ID DO SOCIO"] == 3, "ESTRANGEIRO"))\
                  .withColumn("FAIXA ETARIA", 
                              when(socios["COD FAIXA ETARIA"] == 1, "0 - 12 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 2, "13 - 20 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 3, "21 - 30 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 4, "31 - 40 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 5, "41 - 50 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 6, "51 - 60 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 7, "61 - 70 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 8, "71 - 80 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 9, "MAIS DE 80 ANOS")\
                              .when(socios["COD FAIXA ETARIA"] == 0, "NÃO SE APLICA"))\
                  .withColumn('DT ENTRADA NA SOCIEDADE', when(col('DT ENTRADA NA SOCIEDADE')<='1900-01-01',\
                                                  to_date(lit('1900-01-01'),'yyyyMMdd')) \
                                                  .otherwise(col('DT ENTRADA NA SOCIEDADE')))


  socios = socios.select(["CNPJ BASICO", "NOME SOCIO", "CNPJ/CPF DO SOCIO", "DT ENTRADA NA SOCIEDADE", \
                          "QUALIFICACAO DO SOCIO", "TIPO SOCIO", "FAIXA ETARIA"])               

  return socios

