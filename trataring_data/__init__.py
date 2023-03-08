import pandas as pd
import findspark
import subprocess
findspark.init()

from pyspark.sql.types import *
from pyspark.sql.functions import *

from estabelecimentos import tratar_estabelecimentos
from socios import tratar_socios
from empresas import tratar_empresas

def all_merge(df1, df2, df3):
    df = df1.join(df2, ["CNPJ BASICO"])\
            .join(df3, ["CNPJ BASICO"])
    return df

def export_parquet(df):
    df = df.repartition('UF','MUNICIPIO')
    df.write \
        .format('parquet') \
        .mode('append') \
        .option("numPartitions", 300)\
        .partitionBy('UF','MUNICIPIO') \
        .save('/media/pastoril/74A86D55A86D16C0/empresas_rf/')
        # .save("s3a://lead-generation-data-raw/empresas_rf/")
    
if __name__ == "__main__":
    print("LOADING ESTABELECIMENTOS")
    estabelecimentos = tratar_estabelecimentos()
    print("ESTABELECIMENTOS OK! \n")
    print("LOADING EMPRESAS")
    empresas = tratar_empresas()
    print("EMPRESAS OK! \n")
    print("LOADING SOCIOS")
    socios = tratar_socios()
    print("SOCIOS OK! \n")


    df = all_merge(estabelecimentos, empresas, socios)
    
    print("GERANDO ARQUIVO PARQUET E ARMAZENANDO NO S3")
    export_parquet(df)

# df.cache()
# df.printSchema()

# df_PR = spark.read.option("header", True) \
#         .parquet("/media/pastoril/74A86D55A86D16C0/ReceitaFederal/UF=PR")
