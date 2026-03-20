# Databricks notebook source


from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "cria_tabela_eans_processados_guia_farmacia",
    dbutils,
    {"spark.sql.caseSensitive", "true"},
)

# tabelas
produtos_table = Table.produtos_refined.value
eans_processados = Table.eans_processados_guia_da_farmacia.value

# COMMAND ----------

produtos_refined = env.table(produtos_table)

# COMMAND ----------

# carga inicial com todos os eans criados há mais de 30 dias (já processados no workflow do scraping)
# TODO: Esse 2024 ficou hardcoded, mudar depois
produtos_refined = (
    produtos_refined.filter(F.col("ean").isNotNull())
    .filter(F.col("eh_medicamento"))
    .filter(F.col("marca").isNotNull())
    .filter("gerado_em <= '2024-11-30 23:59:59'")
    .select("ean")
    .distinct()
)

# COMMAND ----------

eans_processados_list = [
    row.ean for row in produtos_refined.select("ean").distinct().collect()
]

# COMMAND ----------

current_time = agora_em_sao_paulo()
rows = [Row(ean=ean, data_scraping=current_time) for ean in eans_processados_list]

# COMMAND ----------

# TODO: esse schema deveria ir para utils
schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("data_scraping", T.TimestampType(), False),
    ]
)

# COMMAND ----------

df = spark.createDataFrame(rows, schema)

df.limit(100).display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(eans_processados)
