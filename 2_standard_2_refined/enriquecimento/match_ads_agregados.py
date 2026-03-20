# Databricks notebook source
# MAGIC %pip install langchain_google_genai langchain-google-vertexai openai pymilvus
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from functools import partial

import pandas as pd
from pyspark.sql import functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Environment
from maggulake.utils.iters import batched

env = DatabricksEnvironmentBuilder.build("match_ads_agregados", dbutils)

DEBUG = dbutils.widgets.get("debug") == "true"


# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração Inicial

# COMMAND ----------

spark = env.spark
postgres = env.postgres_replica_adapter
catalog = env.settings.catalog

# delta tables
PRODUTOS_REFINED_TABLE = f"{catalog}.refined.produtos_refined"
PRODUTOS_AGREGADOS = f"{catalog}.standard.produtos_campanha_agregados"

SCORE_THRESHOLD = 0.6
MAX_RESULTS = 200


# COMMAND ----------

# MAGIC %md
# MAGIC ## Identifica produtos em campanhas ativas atualmente

# COMMAND ----------

produtos_foco: list[str] = postgres.get_eans_em_campanha(spark)

if DEBUG:
    print(f"Quantidade de EANs em campanha sem duplicatas: {len(produtos_foco)}")
    print("Lista dos 10 primeiros produtos foco: ", produtos_foco[:10])

# COMMAND ----------

# Usa broadcast para acelerar o processo
produtos_foco_df = spark.createDataFrame(
    [(ean,) for ean in produtos_foco], "ean string"
)
produtos_em_campanha = spark.read.table(PRODUTOS_REFINED_TABLE).join(
    F.broadcast(produtos_foco_df), "ean"
)

# COMMAND ----------

# Seleciona tags de todos os produtos em campanha
eans_e_tags = produtos_em_campanha.select(
    "ean",
    F.explode(
        F.array_distinct(
            F.flatten(
                F.array(
                    "tags_complementares",
                    "tags_potencializam_uso",
                    "tags_atenuam_efeitos",
                    "tags_agregadas",
                )
            )
        )
    ).alias("tag"),
).cache()

# Isso aqui materializa o dataframe
eans_e_tags.limit(100).display()

# COMMAND ----------

# Tags unicas
tags = eans_e_tags.select("tag").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria retriever

# COMMAND ----------

TEXT_COLUMNS = [
    "ean",
    "eans_alternativos",
    "power_phrase",
    "informacoes_para_embeddings",
]

FILTRO = f"ean not in {produtos_foco} and not array_contains_any(eans_alternativos, {produtos_foco})"

# COMMAND ----------


def parse_response(response):
    return [
        {
            "ean": ean,
            "score": r["distance"],
            "frase": r["entity"]["power_phrase"],
        }
        for r in response
        for ean in [r["entity"]["ean"]] + (r["entity"]["eans_alternativos"] or [])
    ]


# COMMAND ----------

# Parece que o Milvus permite so 10 entradas por chamada em staging
batch_size = 100 if catalog == "production" else 10

get_env = partial(Environment, env.settings, env.session_name, env.spark_config)


# pandas_udf nao retorna struct
@F.pandas_udf("string")
def recomendacoes_udf(tags: pd.Series) -> pd.Series:
    env = get_env()

    retriever = env.vector_search.get_data_api_retriever(
        filtro=FILTRO,
        max_results=MAX_RESULTS,
        score_threshold=SCORE_THRESHOLD,
        campos_output=TEXT_COLUMNS,
        format_return=False,
    )

    recomendacoes = [
        json.dumps(parse_response(recomendacao))
        for b in batched(tags, batch_size)
        for recomendacao in retriever.batch(b)
    ]

    return pd.Series(recomendacoes)


# COMMAND ----------

tags_com_recomendacoes = tags.withColumn(
    "recomendacoes",
    F.from_json(
        recomendacoes_udf("tag"),
        "array<struct<ean string, score double, frase string>>",
    ),
).cache()

# Isso aqui materializa o dataframe
tags_com_recomendacoes.limit(100).display()

# COMMAND ----------

recomendacoes_exploded = (
    eans_e_tags.alias("e")
    .join(tags_com_recomendacoes.alias("r"), "tag")
    .selectExpr(
        "e.ean AS ean_em_campanha",
        "explode(r.recomendacoes) AS recomendacao",
    )
    .select(
        "ean_em_campanha",
        "recomendacao.ean",
        "recomendacao.score",
        "recomendacao.frase",
    )
    .sort("ean_em_campanha", F.desc("score"))
    .dropDuplicates(["ean_em_campanha", "ean"])
    .cache()
)

# Isso aqui materializa o dataframe
recomendacoes_exploded.limit(100).display()

# COMMAND ----------

recomendacoes = (
    recomendacoes_exploded.groupBy("ean_em_campanha")
    .agg(
        F.collect_list(F.struct("ean", "score", "frase")).alias("eans_recomendar"),
    )
    .cache()
)

# Isso aqui materializa o dataframe
recomendacoes.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando resultado

# COMMAND ----------

recomendacoes.write.mode("overwrite").saveAsTable(PRODUTOS_AGREGADOS)
