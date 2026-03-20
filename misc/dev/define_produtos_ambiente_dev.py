# Databricks notebook source
# MAGIC %pip install pymilvus langchain langchain-community langchain_core langchain_openai langchain-databricks openai pydantic langchain-google-genai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F

from maggulake.environment import CopilotTable, DatabricksEnvironmentBuilder, Table

# TODO: trocar
env = DatabricksEnvironmentBuilder.build(
    "define_produtos_ambiente_dev",
    dbutils,
    widgets={"environment": ["production", "production"]},
)

spark = env.spark

# COMMAND ----------

SCORE_THRESHOLD = 0.3
MAX_RESULTS = 100


# COMMAND ----------

p = env.table(Table.produtos_refined).filter("status = 'ativo'").cache()
e = env.table(Table.produtos_com_embeddings).cache()
pl = (
    env.table(CopilotTable.produtos_loja)
    .withColumn("conta", F.col("tenant"))
    .withColumn("loja", F.col("codigo_loja"))
    .cache()
)

# COMMAND ----------

produtos_campanha_eans = set(env.postgres_replica_adapter.get_eans_em_campanha(spark))
produtos_campanha = p.filter(F.col("ean").isin(produtos_campanha_eans))

produtos_campanha.display()

# COMMAND ----------

nomes_produtos_pesquisados = [
    "finasterida",
    "crestor",
    "ozempic",
    "rivotril",
]

ids_produtos_pesquisados = [
    p.filter(f"nome ilike '{pp}%'").head().id for pp in nomes_produtos_pesquisados
]

produtos_pesquisados = p.filter(F.col("id").isin(ids_produtos_pesquisados))

# COMMAND ----------

produtos_considerados = produtos_campanha.unionByName(produtos_pesquisados)
produtos_considerados_list = produtos_considerados.collect()

# COMMAND ----------

principios_ativos = {
    p.principio_ativo for p in produtos_considerados_list if p.principio_ativo
}

produtos_substitutos = p.filter(F.col("principio_ativo").isin(principios_ativos))

produtos_substitutos.display()

# COMMAND ----------

retriever = env.vector_search.get_data_api_retriever(
    max_results=MAX_RESULTS,
    score_threshold=SCORE_THRESHOLD,
    campos_output=["ean", "informacoes_para_embeddings"],
)

# COMMAND ----------


def acha_categorias_produto(produto):
    print(f"Pesquisando produto {produto.ean}")
    if produto.tags_complementares:
        yield from produto.tags_complementares

    if produto.tags_atenuam_efeitos:
        yield from produto.tags_atenuam_efeitos

    if produto.tags_potencializam_uso:
        yield from produto.tags_potencializam_uso


# COMMAND ----------

produtos_recomendados_ids = {
    p["id"]
    for pc in produtos_considerados_list
    for response in retriever.batch(list(acha_categorias_produto(pc)))
    for p in response
}

produtos_recomendados = p.filter(F.col("id").isin(produtos_recomendados_ids))

produtos_recomendados.display()

# COMMAND ----------

produtos_todos = (
    produtos_considerados.unionByName(produtos_substitutos)
    .unionByName(produtos_recomendados)
    .dropDuplicates(["ean"])
)

produtos_todos.count()

# COMMAND ----------

# TODO: Tem que apagar do produtos_api, veio tudo nulo
produtos_todos = produtos_todos.filter("ean != '7896637035460'")

produtos_todos.count()

# COMMAND ----------

lojas_para_importar = spark.createDataFrame(
    [
        # ('farmagui', 'loja_4'),
        ("toolspharma", "prototipo"),
    ],
    ["conta", "loja"],
)

pl_lojas = pl.join(lojas_para_importar, ["conta", "loja"], "semi")

pl_lojas.count()

# COMMAND ----------

produtos = produtos_todos.join(pl_lojas.dropDuplicates(["ean"]), "ean", "semi")
eans_set = {p.ean for p in produtos.collect() if p.ean}
eans = spark.createDataFrame([[e] for e in eans_set], ["ean"])

eans.display()

# COMMAND ----------

eans.write.mode("overwrite").saveAsTable("production.utils.eans_dev")
