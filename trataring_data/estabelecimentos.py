import pandas as pd

import findspark
findspark.init()

import build_spark_session as bss
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = bss.build_new_spark_session("ReceitaFederal")

def tratar_estabelecimentos():
  schema = StructType([StructField("CNPJ BASICO", LongType(), True),
                        StructField("CNPJ ORDEM", ShortType(), True),
                        StructField("CNPJ DV", ShortType(), True),
                        StructField("IDENTIFICADOR MATRIZ FILIAL", ShortType(), True),
                        StructField("NOME FANTASIA", StringType(), True),
                        StructField("SIT CADASTRAL", ShortType(), True),
                        StructField("DATA SIT CADASTRAL", StringType(), True),
                        StructField("MOTIVO", ShortType(), True),
                        StructField("CIDADE EXTERIOR", ShortType(), True),
                        StructField("CODIGO_PAIS", ShortType(), True),
                        StructField("INICIO ATIVIDADE", StringType(), True),
                        StructField("CNAE PRINCIPAL", StringType(), True),
                        StructField("CNAE SEGUNDARIO", StringType(), True),
                        StructField("TIPO LOGRADOURO", StringType(), True),
                        StructField("LOGRADOURO", StringType(), True),
                        StructField("NUMERO", ShortType(), True),
                        StructField("COMPLEMENTO", StringType(), True),
                        StructField("BAIRRO", StringType(), True),
                        StructField("CEP", LongType(), True),
                        StructField("UF", StringType(), True),
                        StructField("CODIGO_MUNICIPIO", ShortType(), True),
                        StructField("DDD", ShortType(), True),
                        StructField("TELEFONE", LongType(), True),
                        StructField("DDD2", ShortType(), True),
                        StructField("TELEFONE2", DoubleType(), True),
                        StructField("DDD FAX", ShortType(), True),
                        StructField("FAX", StringType(), True),
                        StructField("EMAIL", StringType(), True),
                        StructField("SIT FISCAL", StringType(), True),
                        StructField("DATA SIT FISCAL", DateType(), True),
                      ])

  # path = ["./data/estabelecimento"+ str(i) + ".csv" for i in range(1, 4)]

  path = ["s3a://lead-generation-data-raw/Estabelecimentos"+ str(i) +".csv" for i in range(10)]

  estabelecimentos = spark.read.options(delimiter=';')\
                              .schema(schema)\
                              .option("numPartitions", 25)\
                              .option("lowerBound", 0)\
                              .option("upperBound", 4000000)\
                              .csv(path)

  estabelecimentos_ativos = estabelecimentos.na.fill(value="-")\
                                            .where(col("SIT CADASTRAL") == 2)\
                                            .na.fill(value=105, subset=["CODIGO_PAIS"])\
                                            .na.drop(subset = ["NOME FANTASIA", "CEP", ])\
                                            .withColumn("CNPJ BASICO", lpad(col("CNPJ BASICO"), 8, "0"))\
                                            .withColumn("CNPJ ORDEM", lpad(col("CNPJ ORDEM"), 4, "0"))\
                                            .withColumn("CNPJ DV", lpad(col("CNPJ DV"), 2, "0"))\
                                            .withColumn("EMAIL", upper(col("EMAIL")))\
                                            .withColumn("INICIO ATIVIDADE", to_date(col("INICIO ATIVIDADE"), "yyyyMMdd"))\
                                            .withColumn("ANO INICIO ATIVIDADE", year(to_date(col("INICIO ATIVIDADE"), "yyyyMMdd")))\
                                            .withColumn("MES INICIO ATIVIDADE", month(to_date(col("INICIO ATIVIDADE"), "yyyyMMdd")))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "???",""))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "STIO","SITIO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "PRAA","PRACA"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", 'ESTNCIA', "ESTANCIA"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "CHCARA", "CHACARA"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "CRREGO", "CORREGO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "PTIO", "PATIO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "REA", "AREA"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "CALADA", "CALCADA"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "LIGAO", "LIGACAO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "MDULO", "MODULO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "ESTAO", "ESTACAO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "CONDOMNIO", "CONDOMONIO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "COLNIA", "COLONIA"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "VIRIO", "VIARIO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "NCLEO", "NUCLEO"))\
                                            .withColumn("TIPO LOGRADOURO", regexp_replace("TIPO LOGRADOURO", "ROTATRIA", "ROTATORIA"))

                                            
  codigo_pais = spark.read.options(delimiter = ";")\
                          .schema(StructType([StructField("CODIGO_PAIS", ShortType(), True),
                                              StructField("PAIS", StringType(), True)]))\
                          .csv("s3a://lead-generation-data-raw/Paises.csv")

  codigo_pais = codigo_pais.withColumn("PAIS", regexp_replace("PAIS",'COLIS POSTAUX','BRASIL'))

  codigo_municipio = spark.read.options(delimiter = ";")\
                               .schema(StructType([StructField("CODIGO_MUNICIPIO", ShortType(), True),
                                                  StructField("MUNICIPIO", StringType(), True)]))\
                              .csv("s3a://lead-generation-data-raw/Municipios.csv")

  cnaes = spark.read.options(delimiter = ",")\
                    .schema(StructType([StructField("CNAE PRINCIPAL", StringType(), True),
                                      StructField("DESCRICAO"   , StringType(), True),
                                      StructField("COD SETOR", StringType(), True),
                                      StructField("SETOR", StringType(), True)
                                      ]))\
                  .csv("../data/tabela-cnae.csv")

  cnaes = cnaes.withColumn("DESCRICAO", upper(col("DESCRICAO")))
                       
  estabelecimentos_ativos = estabelecimentos_ativos.join(codigo_pais, ["CODIGO_PAIS"])\
                                                  .join(codigo_municipio, ["CODIGO_MUNICIPIO"])\
                                                  .join(cnaes, ["CNAE PRINCIPAL"])

  estabelecimentos_ativos =  estabelecimentos_ativos.withColumn("CNPJ", concat(col("CNPJ BASICO"), col("CNPJ ORDEM"), col("CNPJ DV")))\
                                                    .withColumn("CNAE DESCRICAO", concat_ws(" - ", col("CNAE PRINCIPAL"), col("DESCRICAO")))\
                                                    .withColumn('INICIO ATIVIDADE', when(col('INICIO ATIVIDADE')<='1900-01-01',\
                                                                                    to_date(lit('1900-01-01'),'yyyyMMdd')) \
                                                                                    .otherwise(col('INICIO ATIVIDADE')))

  
  estabelecimentos_ativos = estabelecimentos_ativos.select(['CNPJ','CNPJ BASICO', 'CNPJ ORDEM', 'CNPJ DV',
                                                    'NOME FANTASIA','INICIO ATIVIDADE','CNAE PRINCIPAL', 'CNAE SEGUNDARIO',
                                                      'TIPO LOGRADOURO', 'LOGRADOURO', 'NUMERO', 'COMPLEMENTO',
                                                      'BAIRRO', 'CEP', 'UF', 'DDD', 'TELEFONE','EMAIL',
                                                      'ANO INICIO ATIVIDADE', 'MES INICIO ATIVIDADE', 'PAIS',
                                                      'MUNICIPIO', 'DESCRICAO','SETOR', "CNAE DESCRICAO"])
  
  df1 = concat_ws(" ", col("TIPO LOGRADOURO"), col("LOGRADOURO"), col("NUMERO"))
  df2 = concat_ws(" - ", col("MUNICIPIO"), col("UF"))

  return estabelecimentos_ativos


# df = tratar_estabelecimentos()

# df_final = df2.repartition('UF','MUNICIPIO')

# # df final.to pandas on spark()

# _# df final.write \
# #   .format('delta') \
# #   .mode('append') \
# #   .option("numPartitions", 300)\
# #   .partitionBy('UF','MUNICIPIO') \
# #_  .saveAsTable('default.estabelecimentos')


# df final.write \
#   .format('parquet') \
#   .mode('append') \
#   .option("numPartitions", 300)\
#   .partitionBy('UF','MUNICIPIO') \
#   .save('/media/pastoril/74A86D55A86D16C0/ReceitaFederal/')


# teste = spark.sql('''SELECT * FROM estabelecimentos''')
# teste.to pandas on spark()
# teste.show()


# # df.write \
# #         .partitionBy("UF", "MUNICIPIO") \
# #         .mode("append") \
# #         .format("parquet")\
# #         .save("/home/lg/LG/data/files")
 

# df PR =spark.read.option("header", True) \
#         .parquet("/media/pastoril/74A86D55A86D16C0/ReceitaFederal/UF=PR")

# df PR.head(5)


# df PR.count()
# df = df PR.to_pandas_on_spark()
