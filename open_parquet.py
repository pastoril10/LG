import pandas as pd

import findspark
findspark.init()

import build_spark_session as bss
from pyspark.sql.types import *
from pyspark.sql.functions import *

# spark = bss.build_new_spark_session("ReceitaFederal")


# df_PR = spark.read.option("header", True) \
#         .parquet("/media/pastoril/74A86D55A86D16C0/empresas_rf/UF=PR")

# df_PR.printSchema()

# df_PR.head(2)

df_pr = pd.read_parquet("/media/pastoril/74A86D55A86D16C0/empresas_rf/UF=PR")
df_pr.drop_duplicates(subset=["CNPJ"]).shape


df_pr.columns
df_pr.head(5)

["TIPO LOGRADOURO", "NATUREZA JURIDICA", "QUALIFICACAO DO SOCIO"]

df_pr["QUALIFICACAO DO SOCIO"].unique()

import streamlit as st
st.dataframe(data=df_PR.head(10))