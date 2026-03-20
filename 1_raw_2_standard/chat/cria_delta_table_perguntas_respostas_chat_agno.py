# Databricks notebook source

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("perguntas_respostas_agno")
    .config(conf=config)
    .getOrCreate()
)

# ambiente
stage = "prod"
catalog = "production" if stage == "prod" else "dev"

# aws
chat_agno_payload_path = (
    f"s3://maggu-datalake-{stage}/1-raw-layer/maggu/perguntas_respostas_agno/"
)

# delta
chat_agno_payload_table = f"{catalog}.raw.chat_agno_{catalog}_payload"


# COMMAND ----------

df_perguntas_respostas = (
    spark.read.option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.json")
    .option("multiline", True)
    .json(chat_agno_payload_path)
)

# COMMAND ----------

df_chat_agno_payload = df_perguntas_respostas.select(
    F.col("client_request_id").cast("string"),
    F.col("databricks_request_id").cast("string"),
    F.to_date(F.col("date")).alias("date"),
    F.col("timestamp_ms").cast("long"),
    F.col("status_code").cast("int"),
    F.col("execution_time_ms").cast("long"),
    F.col("request").cast("string"),
    F.col("response").cast("string"),
    F.col("sampling_fraction").cast("double"),
    F.col("request_metadata"),
)

# COMMAND ----------

dt_chat_agno = (
    DeltaTable.createIfNotExists(spark)
    .tableName(chat_agno_payload_table)
    .addColumns(df_chat_agno_payload.schema)
    .execute()
)

# COMMAND ----------

merge_condition = """
    target.client_request_id = source.client_request_id AND
    target.request = source.request AND
    target.response = source.response AND
    target.timestamp_ms = source.timestamp_ms
"""

dt_chat_agno.alias("target").merge(
    source=df_chat_agno_payload.alias("source"), condition=merge_condition
).whenNotMatchedInsertAll().execute()
