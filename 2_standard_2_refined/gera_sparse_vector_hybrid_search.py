# Databricks notebook source
get_ipython().system("pip install boto3 pymilvus[model] datasets peft FlagEmbedding")
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("catalog", "dev", ["dev", "staging", "production"])
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])
dbutils.widgets.dropdown("compute_device", "cpu", ["cpu", "cuda"])

# COMMAND ----------

from datetime import datetime
from itertools import groupby

import pyspark.sql.functions as F
from pymilvus.model.hybrid import (  # https://milvus.io/docs/embed-with-bgm-m3.md
    BGEM3EmbeddingFunction,
)
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.utils.iters import batched

# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("gera_sparse_vector_hybrid_search")
    .config(conf=config)
    .getOrCreate()
)

# ambiente
stage = dbutils.widgets.get("stage")
catalog = dbutils.widgets.get("catalog")
s3_bucket_name = f"maggu-datalake-{stage}"
compute_device = dbutils.widgets.get("compute_device")

# tabelas delta
produtos_table = f"{catalog}.refined.produtos_refined"
embeddings_table = f"{catalog}.refined.embeddings_hybrid_search"


# COMMAND ----------

produtos_df = spark.read.table(produtos_table)
produtos_df.display()

# COMMAND ----------

textos_embeddings_df = produtos_df.select("informacoes_para_embeddings").dropna()

if spark.catalog.tableExists(embeddings_table):
    embeddings_df = spark.read.table(embeddings_table)
    textos_embeddings_df = textos_embeddings_df.join(
        embeddings_df,
        F.col('informacoes_para_embeddings') == F.col('prompt'),
        'leftanti',
    )

# COMMAND ----------

textos_embeddings_df.cache().count()

# COMMAND ----------

if textos_embeddings_df.count() == 0:
    dbutils.notebook.exit("Sem embeddings para gerar")

# COMMAND ----------

bge_m3_ef = BGEM3EmbeddingFunction(
    use_fp16=False, device=compute_device, return_dense=False
)

# COMMAND ----------


def get_sparse_embeddings(texts):
    # text = text.replace("\n", " ")
    embeddings = bge_m3_ef.encode_documents(texts)
    starse_vector = embeddings["sparse"]

    # todos = [((0, 36), 0.426), ...]
    todos = starse_vector.todok().items()

    def key(x):
        return x[0][0]

    return [
        {int(i[0][1]): float(i[1]) for i in v}
        for k, v in groupby(sorted(todos, key=key), key=key)
    ]


# COMMAND ----------


def to_embeddings_df(batch):
    sparse_vectors = get_sparse_embeddings(
        [p.informacoes_para_embeddings for p in batch]
    )
    return spark.createDataFrame(
        [
            {
                "prompt": p.informacoes_para_embeddings,
                "sparse_vector": v,
                "gerado_em": datetime.now(),
            }
            for p, v in zip(batch, sparse_vectors)
        ],
        "prompt string, sparse_vector map<bigint, double>, gerado_em timestamp",
    )


# COMMAND ----------

to_embeddings_df(textos_embeddings_df.limit(10).collect()).display()

# COMMAND ----------

for i, batch in enumerate(batched(textos_embeddings_df.toLocalIterator(), 1000)):
    print(f"Fazendo batch {i}")
    embeddings_df = to_embeddings_df(batch)
    embeddings_df.write.mode("append").saveAsTable(embeddings_table)
