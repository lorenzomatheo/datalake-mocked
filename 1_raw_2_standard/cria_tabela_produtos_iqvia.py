# Databricks notebook source
# TODO: migrar para DatabricksEnvironmentBuilder em vez de widgets/SparkConf manuais.
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %pip install pandas fsspec s3fs openpyxl xlrd
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.utils.df_utils import normaliza_nomes_colunas

# COMMAND ----------


# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("analisa planilha industria")
    .config(conf=config)
    .getOrCreate()
)

# ambiente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"
s3_bucket_name = f'maggu-datalake-{stage}'

# pastas s3
s3_folder_iqvia = f's3://{s3_bucket_name}/1-raw-layer/maggu/produtos_iqvia/'
s3_file_iqvia = s3_folder_iqvia + 'TbDinamica_PMB_Full - Abr24.xlsm'


# COMMAND ----------

produtos_iqvia = pd.read_excel(
    s3_file_iqvia, sheet_name='tbDinamica', skiprows=1, header=0
)
produtos_iqvia_df = spark.createDataFrame(produtos_iqvia)

print(f'Total de linhas na tabela de produtos: {produtos_iqvia_df.count()}')
produtos_iqvia_df.display()

# COMMAND ----------

produtos_iqvia_df = produtos_iqvia_df.drop(
    'Unnamed: 0',
    'GGP',
    'Sum of PF (RQTR)',
    'Sum of DESC MEDIO (RQTR)',
    'Sum of PPP (RQTR)',
    'Sum of PMP (RQTR)',
)
produtos_iqvia_df = produtos_iqvia_df.filter(F.col('cd_ean') != 0)
print(f'Total de linhas na tabela de produtos: {produtos_iqvia_df.count()}')
produtos_iqvia_df.display()

# COMMAND ----------

produtos_iqvia_df = produtos_iqvia_df.dropDuplicates(subset=['cd_ean'])
print(f'Total de linhas na tabela de produtos: {produtos_iqvia_df.count()}')
produtos_iqvia_df.display()

# COMMAND ----------

produtos_iqvia_df = normaliza_nomes_colunas(produtos_iqvia_df)
produtos_iqvia_df.display()

# COMMAND ----------

produtos_iqvia_df.write.mode('overwrite').saveAsTable(f'{catalog}.raw.produtos_iqvia')
