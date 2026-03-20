# Databricks notebook source
get_ipython().system("pip install openai")
dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.io.milvus import Milvus
from maggulake.produtos.cria_indice_milvus import create_collection_chat

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "atualiza_collection_conversas_chat",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={"recria_collection": ["false", "false", "true"]},
)

spark = env.spark

# milvus
COLLECTION = "conversas_chat"
RECRIA_COLLECTION = dbutils.widgets.get("recria_collection") == "true"

CACHE_DAYS = 30

# COMMAND ----------

chat_embeddings = env.table(Table.chat_milvus_payload_with_embeddings)

# COMMAND ----------

chat_embeddings = (
    chat_embeddings.filter(
        F.col("chat_date").between(
            F.date_sub(F.current_date(), CACHE_DAYS), F.current_date()
        )
    )
    .withColumnRenamed("databricks_request_id", "id")
    .withColumnRenamed("informacoes_para_embeddings", "pergunta")
    .withColumnRenamed("chat_answer", "resposta")
    .withColumnRenamed("embedding", "vector")
    .drop("chat_date", "last_user_question")
)

# COMMAND ----------

if not chat_embeddings.take(1):
    dbutils.notebook.exit("DataFrame de embeddings está vazio e não será salvo.")

# COMMAND ----------

milvus = Milvus(env.settings.milvus_uri, env.settings.milvus_token)

# COMMAND ----------

if RECRIA_COLLECTION:
    create_collection_chat(milvus.client, COLLECTION, apaga_antiga=True)

# COMMAND ----------

milvus.upsert(chat_embeddings, COLLECTION)

# COMMAND ----------

# MAGIC %md
# MAGIC Teste de busca semântica

# COMMAND ----------

openai_client = env.openai


def get_embeddings(text: str) -> list[float]:
    text = text.replace("\n", " ")
    response = openai_client.embeddings.create(
        input=[text],
        model="text-embedding-3-large",
    )
    return response.data[0].embedding


query = "posologia crestor"

results = milvus.client.search(
    collection_name=COLLECTION,
    data=[get_embeddings(query)],
    anns_field="vector",
    limit=3,
    output_fields=["id", "pergunta", "resposta"],
)

results[0]
