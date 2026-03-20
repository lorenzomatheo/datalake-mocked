# Databricks notebook source
# MAGIC %pip install pandas fsspec s3fs openpyxl xlrd
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.utils.df_utils import normaliza_nomes_colunas

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "cria tabela de produtos genericos",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark

# pastas s3
s3_folder_genericos = env.full_s3_path("1-raw-layer/maggu/produtos_genericos/")
s3_file_genericos = s3_folder_genericos + "LISTA COM 10.438 GENERICOS.xlsx"


# COMMAND ----------

produtos_genericos = pd.read_excel(s3_file_genericos, sheet_name="Planilha1", header=0)
produtos_genericos_df = spark.createDataFrame(produtos_genericos)

print(f"Total de linhas na tabela de produtos: {produtos_genericos_df.count()}")
produtos_genericos_df.display()

# COMMAND ----------

produtos_genericos_df = produtos_genericos_df.dropDuplicates(subset=["CDG_BARRAS 1 "])
print(f"Total de linhas na tabela de produtos: {produtos_genericos_df.count()}")

# COMMAND ----------

produtos_genericos_df = normaliza_nomes_colunas(produtos_genericos_df)
produtos_genericos_df.columns

# COMMAND ----------

produtos_genericos_df = (
    produtos_genericos_df.drop(
        "farma___nao_farma",
        "tipo_de_mercado",
        "tipo_merc_seg",
        "tipo_de_produto",
    )
    .withColumnRenamed("cdg_barras_1_", "ean")
    .withColumnRenamed("descricao_do_produto___familia", "familia_produto")
    .withColumnRenamed("quantidade___vol_", "quantidade_vol")
    .withColumnRenamed("concentracao_+_qtd_ou_vol", "concentracao_qtd_ou_vol")
    .withColumnRenamed("cod___classe_terapeutica_", "cod_classe_terapeutica")
    .withColumnRenamed("droga_padrao_", "droga_padrao")
)

produtos_genericos_df.columns

# COMMAND ----------

produtos_genericos_df.write.mode("overwrite").saveAsTable(
    Table.produtos_genericos.value
)
