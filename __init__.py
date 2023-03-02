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

def export_csv(df):
    pass

if __name__ == "__main__":
    estabelecimentos = tratar_estabelecimentos()
    empresas = tratar_empresas()
    socios = tratar_socios()

    df = all_merge(estabelecimentos, empresas, socios)

df.cache()

# df.printSchema()
# df.count(col("CNPJ"))
# # export_csv(df)
# print(f'---- SALVANDO ARQUIVO PARQUET PROCESSADO')
# df.write.mode("overwrite").parquet("s3a://lead-generation-data-raw/empresas_RF.parquet")

df.write.mode("overwrite")\
        .format("parquet")\
        .partitionBy("CNAE PRINCIPAL")\
        .save("s3a://lead-generation-data-raw/empresas_RF.parquet")


