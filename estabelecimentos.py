import pandas as pd

import findspark
findspark.init()

import build_spark_session as bss
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = bss.build_new_spark_session("ReceitaFederal")

def tratar_estabelecimentos():
  schema = StructType([StructField("CNPJ_BASICO", LongType(), True),
                        StructField("CNPJ_ORDEM", ShortType(), True),
                        StructField("CNPJ_DV", ShortType(), True),
                        StructField("IDENTIFICADOR_MATRIZ_FILIAL", ShortType(), True),
                        StructField("NOME_FANTASIA", StringType(), True),
                        StructField("SIT_CADASTRAL", ShortType(), True),
                        StructField("DATA_SIT_CADASTRAL", StringType(), True),
                        StructField("MOTIVO", ShortType(), True),
                        StructField("CIDADE_EXTERIOR", ShortType(), True),
                        StructField("CODIGO_PAIS", ShortType(), True),
                        StructField("INICIO_ATIVIDADE", StringType(), True),
                        StructField("CNAE_PRINCIPAL", StringType(), True),
                        StructField("CNAE_SEGUNDARIO", StringType(), True),
                        StructField("TIPO_LOGRADOURO", StringType(), True),
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
                        StructField("DDD_FAX", ShortType(), True),
                        StructField("FAX", StringType(), True),
                        StructField("EMAIL", StringType(), True),
                        StructField("SIT_FISCAL", StringType(), True),
                        StructField("DATA_SIT_FISCAL", DateType(), True),
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
                                            .where(col("SIT_CADASTRAL") == 2)\
                                            .na.fill(value=105, subset=["CODIGO_PAIS"])\
                                            .na.drop(subset = ["NOME_FANTASIA", "CEP", ])\
                                            .withColumn("CNPJ_BASICO", lpad(col("CNPJ_BASICO"), 8, "0"))\
                                            .withColumn("CNPJ_ORDEM", lpad(col("CNPJ_ORDEM"), 4, "0"))\
                                            .withColumn("CNPJ_DV", lpad(col("CNPJ_DV"), 2, "0"))\
                                            .withColumn("EMAIL", upper(col("EMAIL")))\
                                            .withColumn("INICIO_ATIVIDADE", to_date(col("INICIO_ATIVIDADE"), "yyyyMMdd"))\
                                            .withColumn("ANO_INICIO_ATIVIDADE", year(to_date(col("INICIO_ATIVIDADE"), "yyyyMMdd")))\
                                            .withColumn("MES_INICIO_ATIVIDADE", month(to_date(col("INICIO_ATIVIDADE"), "yyyyMMdd")))

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
                    .schema(StructType([StructField("CNAE_PRINCIPAL", StringType(), True),
                                      StructField("DESCRICAO"   , StringType(), True),
                                      StructField("COD_SETOR", StringType(), True),
                                      StructField("SETOR", StringType(), True)
                                      ]))\
                  .csv("data/tabela-cnae.csv")

  cnaes = cnaes.withColumn("DESCRICAO", upper(col("DESCRICAO")))
                       
  estabelecimentos_ativos = estabelecimentos_ativos.join(codigo_pais, ["CODIGO_PAIS"])\
                                                  .join(codigo_municipio, ["CODIGO_MUNICIPIO"])\
                                                  .join(cnaes, ["CNAE_PRINCIPAL"])

  estabelecimentos_ativos =  estabelecimentos_ativos.withColumn("CNPJ", concat(col("CNPJ_BASICO"), col("CNPJ_ORDEM"), col("CNPJ_DV")))\
                                                    .withColumn("CNAE_DESCRICAO", concat_ws(" - ", col("CNAE_PRINCIPAL"), col("DESCRICAO")))

  
  estabelecimentos_ativos = estabelecimentos_ativos.select(['CNPJ','CNPJ_BASICO', 'CNPJ_ORDEM', 'CNPJ_DV',
                                                    'NOME_FANTASIA','INICIO_ATIVIDADE','CNAE_PRINCIPAL', 'CNAE_SEGUNDARIO',
                                                      'TIPO_LOGRADOURO', 'LOGRADOURO', 'NUMERO', 'COMPLEMENTO',
                                                      'BAIRRO', 'CEP', 'UF', 'DDD', 'TELEFONE','EMAIL',
                                                      'ANO_INICIO_ATIVIDADE', 'MES_INICIO_ATIVIDADE', 'PAIS',
                                                      'MUNICIPIO', 'DESCRICAO','SETOR', "CNAE_DESCRICAO"])
  return estabelecimentos_ativos


df = tratar_estabelecimentos()

df2 = df.withColumn('INICIO_ATIVIDADE',\
                              when(col('INICIO_ATIVIDADE')<='1900-01-01',
                              to_date(lit('1900-01-01'),'yyyyMMdd')) \
                              .otherwise(col('INICIO_ATIVIDADE')))

df_final = df2.repartition('UF','MUNICIPIO')

df_final.to_pandas_on_spark()


df_final.write \
  .format('delta') \
  .mode('append') \
  .option("numPartitions", 300)\
  .partitionBy('UF','MUNICIPIO') \
  .saveAsTable('default.estabelecimentos')



teste = spark.sql('''SELECT * FROM estabelecimentos''')
teste.to_pandas_on_spark()
teste.show()



# df.write \
#         .partitionBy("UF", "MUNICIPIO") \
#         .mode("append") \
#         .format("parquet")\
#         .save("/home/lg/LG/data/files")
 