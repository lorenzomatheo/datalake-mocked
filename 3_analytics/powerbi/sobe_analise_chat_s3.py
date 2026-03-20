# Databricks notebook source
# MAGIC %md
# MAGIC ## Objetivo
# MAGIC
# MAGIC Normalizar o payload bruto de chats (campos `request` e `response` em JSON) para consolidar, por conversa (`databricks_request_id`), a **última pergunta do usuário** e as **respostas associadas**. O processo também cria um indicador de **resposta padrão** (`resposta_padrao`) para facilitar análises e filtros posteriores.
# MAGIC
# MAGIC ## Destino dos dados
# MAGIC
# MAGIC O resultado é persistido em **Delta** e gravado no **S3** para posterior ingestão no **BigQuery**.

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports e configurações do ambiente

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.schemas.analytics_chat_milvus_s3 import analytics_chat_milvus_s3

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "sobe_para_s3_analise_uso_chat",
    dbutils,
)

# COMMAND ----------

stage = env.settings.name_short
catalog = env.settings.catalog
s3_bucket_name = env.settings.bucket
DEBUG = dbutils.widgets.get("debug") == "false"

spark = env.spark

# COMMAND ----------

DELTA_TABLE_RAW_CHAT = f"{catalog}.raw.chat_milvus_production_payload"

DELTA_TABLE_ANALYTICS_CHAT = "hive_metastore.pbi.analytics_chat_milvus"

TABELA_S3_FOLDER = (
    f"s3://{s3_bucket_name}/5-sharing-layer/copilot.maggu.ai/analytics_chat_milvus/"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Carrega e trata os dados do chat

# COMMAND ----------

df_chat_milvus = spark.table(DELTA_TABLE_RAW_CHAT)

# COMMAND ----------

request_schema = StructType(
    [
        StructField(
            "messages",
            ArrayType(
                StructType(
                    [
                        StructField("content", StringType(), True),
                        StructField("role", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

# COMMAND ----------

response_schema = ArrayType(
    StructType(
        [
            StructField("answer", StringType(), True),
        ]
    )
)

# COMMAND ----------

df_chat_milvus_filtered = df_chat_milvus.withColumn(
    "request_struct", F.from_json(F.col("request"), request_schema)
)

# COMMAND ----------

df_chat_milvus_selected = (
    df_chat_milvus_filtered.select(
        F.col("databricks_request_id").alias("pergunta_id"),
        F.col("client_request_id").alias("atendente_id"),
        F.col("date").alias("chat_date"),
        F.col("status_code"),
        F.posexplode(F.col("request_struct.messages")).alias("msg_index", "m"),
    )
    .select(
        F.col("pergunta_id"),
        F.col("atendente_id"),
        F.col("chat_date"),
        F.col("msg_index"),
        F.col("status_code"),
        F.col("m.content").alias("pergunta"),
        F.col("m.role").alias("role"),
    )
    .filter(F.col("role") == "user")
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("pergunta_id").orderBy(F.col("msg_index").desc())
        ),
    )
    .filter(F.col("rn") == 1)
)

# COMMAND ----------

df_chat_milvus_ans = df_chat_milvus_filtered.select(
    F.col("databricks_request_id").alias("pergunta_id"),
    F.explode(F.from_json(F.col("response"), response_schema)).alias("answer_struct"),
).select(F.col("pergunta_id"), F.col("answer_struct.answer").alias("resposta"))

# COMMAND ----------

df_chat_milvus_final = (
    df_chat_milvus_selected.join(df_chat_milvus_ans, on="pergunta_id", how="inner")
    .dropDuplicates(
        [
            "pergunta_id",
            "atendente_id",
            "chat_date",
            "msg_index",
            "status_code",
            "pergunta",
            "role",
            "rn",
            "resposta",
        ]
    )
    .withColumn(
        "eh_resposta_padrao",
        F.lower(F.col("resposta")).contains(
            "sou uma atendente virtual especialista em farmácias"
        ),
    )
    .withColumn(
        "topico",
        F.when(
            F.col("pergunta").contains("> "),
            F.trim(F.split(F.col("pergunta"), r">", 2).getItem(0)),
        ).otherwise(F.lit("sem_tag")),
    )
    .withColumn("data_extracao", F.expr("current_timestamp() - interval 3 hours"))
    .select(
        F.col("pergunta_id").cast("string").alias("pergunta_id"),
        F.col("atendente_id").cast("string").alias("atendente_id"),
        F.col("chat_date").cast("date").alias("chat_date"),
        F.col("msg_index").cast("int").alias("msg_index"),
        F.col("status_code").cast("int").alias("status_code"),
        F.col("pergunta").cast("string").alias("pergunta"),
        F.col("topico").cast("string").alias("topico"),
        F.col("role").cast("string").alias("role"),
        F.col("rn").cast("int").alias("rn"),
        F.coalesce(F.col("resposta").cast("string"), F.lit("BAD_REQUEST")).alias(
            "resposta"
        ),
        F.col("eh_resposta_padrao").cast("boolean").alias("eh_resposta_padrao"),
        F.col("data_extracao").cast("timestamp").alias("data_extracao"),
    )
    .filter(F.col("atendente_id").isNotNull())
)

# COMMAND ----------

df_chat_milvus_rdd = spark.createDataFrame(
    df_chat_milvus_final.rdd, analytics_chat_milvus_s3
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salva no delta e no s3

# COMMAND ----------

df_chat_milvus_rdd.write.format("delta").mode("overwrite").saveAsTable(
    DELTA_TABLE_ANALYTICS_CHAT
)

# COMMAND ----------

df_chat_milvus_rdd.write.mode("overwrite").parquet(TABELA_S3_FOLDER)
