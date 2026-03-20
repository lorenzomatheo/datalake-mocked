# Databricks notebook source
# MAGIC %md
# MAGIC # Padroniza Categorias e Tags dos Produtos
# MAGIC
# MAGIC Este notebook tem como objetivo padronizar as categorias e tags dos produtos na tabela de produtos em processamento.
# MAGIC Basicamente, ele lê as categorias e tags dos produtos na camada refined, consulta um banco de dados vetorial (Milvus)
# MAGIC para encontrar categorias padronizadas e as aplica aos produtos.
# MAGIC É uma forma interessante de usar IA para trazer uma categoria que já existe (mas não está no enum) para uma categoria que está no enum.

# COMMAND ----------

from functools import partial

import pandas as pd
import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Environment, Table
from maggulake.utils.iters import batched

env = DatabricksEnvironmentBuilder.build("padroniza_categorias_e_tags", dbutils)
spark = env.spark

tabela_padronizacao_categorias = (
    f"{env.settings.catalog}.enriquecimento.padronizacao_categorias"
)

score_threshold = 0.4

DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

produtos = env.table(Table.produtos_em_processamento).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gera padronizacoes de categorias faltantes

# COMMAND ----------


def select_categorias(column):
    return produtos.select(F.explode(column).alias("categoria_original"))


def select_tags(column):
    return select_categorias(F.split(column, r"\|"))


# COMMAND ----------

categorias_originais = (
    select_categorias("categorias")
    .dropna()
    .dropDuplicates()
    .filter("categoria_original != ''")
    .withColumn("tipo", F.lit("categoria"))
    .cache()  # Reused multiple times below
)

recomendacoes_originais = (
    select_tags("tags_agregadas")
    .union(select_tags("tags_complementares"))
    .union(select_tags("tags_potencializam_uso"))
    .union(select_tags("tags_atenuam_efeitos"))
    .dropna()
    .dropDuplicates()
    .filter("categoria_original != ''")
    .withColumn("tipo", F.lit("recomendacao"))
    .cache()  # Reused multiple times below
)

# COMMAND ----------

# NOTE: mantido fora do DEBUG para garantir que o cache seja efetivo
print(f"Categorias originais: {categorias_originais.count()}")
print(f"Recomendações originais: {recomendacoes_originais.count()}")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tabela_padronizacao_categorias} (
        tipo string,
        categoria_original string,
        categoria_padronizada string
    )
    PARTITIONED BY (tipo)
""")

# COMMAND ----------

padronizacao_categorias = spark.read.table(tabela_padronizacao_categorias).cache()

novas_categorias_originais = categorias_originais.alias("c").join(
    F.broadcast(padronizacao_categorias).alias("p"),
    on=["tipo", "categoria_original"],
    how="leftanti",
)

novas_recomendacoes_originais = recomendacoes_originais.alias("c").join(
    F.broadcast(padronizacao_categorias).alias("p"),
    on=["tipo", "categoria_original"],
    how="leftanti",
)

# COMMAND ----------

if DEBUG:
    print(f"Novas categorias originais: {novas_categorias_originais.count()}")
    print(f"Novas recomendações originais: {novas_recomendacoes_originais.count()}")

# COMMAND ----------

# TODO: Generalizar e mover para algum lugar melhor

get_env = partial(Environment, env.settings, env.session_name, env.spark_config)


def texto_to_prompt(texto):
    return f"Categoria de produtos vendidos em uma farmácia: {texto}"


def get_embeddings(openai_client, texts):
    response = openai_client.embeddings.create(
        model="text-embedding-3-large",
        input=[texto_to_prompt(texto) for texto in texts],
        dimensions=1536,
    )
    return [e.embedding for e in response.data]


def get_milvus_response(milvus_client, embeddings, somente_recomendacoes: bool):
    response = milvus_client.search(
        collection_name="categorias",
        anns_field="vector",
        data=embeddings,
        filter="eh_recomendacao == true" if somente_recomendacoes else None,
        limit=1,
        output_fields=["categoria_flat"],
        search_params={"params": {"radius": score_threshold}},
    )

    def get_categorias(hit):
        if not hit:
            return None

        return hit[0]["entity"]["categoria_flat"]

    return [get_categorias(hit) for hit in response]


def get_vectorsearch_udf(somente_recomendacoes: bool):
    @F.pandas_udf("string")
    def vector_search_udf(texts: pd.Series) -> pd.Series:
        env = get_env()

        embeddings = [
            e for b in batched(texts, 1000) for e in get_embeddings(env.openai, b)
        ]

        categorias = [
            c
            for b in batched(embeddings, 10)
            for c in get_milvus_response(
                env.vector_search.milvus,
                list(b),
                somente_recomendacoes,
            )
        ]

        return pd.Series(categorias)

    return vector_search_udf


search_todos = get_vectorsearch_udf(somente_recomendacoes=False)
search_recomendacoes = get_vectorsearch_udf(somente_recomendacoes=True)

# COMMAND ----------


novas_categorias = novas_categorias_originais.withColumn(
    "categoria_padronizada", search_todos(F.col("categoria_original"))
)

if DEBUG:
    novas_categorias.limit(1000).display()

# COMMAND ----------

novas_categorias.write.mode("append").saveAsTable(tabela_padronizacao_categorias)

# COMMAND ----------


novas_recomendacoes = novas_recomendacoes_originais.withColumn(
    "categoria_padronizada", search_recomendacoes(F.col("categoria_original"))
)

# COMMAND ----------

if DEBUG:
    novas_recomendacoes.limit(1000).display()

# COMMAND ----------

novas_recomendacoes.write.mode("append").saveAsTable(tabela_padronizacao_categorias)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Padroniza colunas com categorias na tabela de produtos

# COMMAND ----------

tabela_padronizacao = spark.read.table(tabela_padronizacao_categorias)

# COMMAND ----------

if DEBUG:
    tabela_padronizacao.limit(1000).display()

# COMMAND ----------

padronizacao_categorias = tabela_padronizacao.filter("tipo = 'categoria'").join(
    categorias_originais, on="categoria_original", how="leftsemi"
)

padronizacao_categorias = {
    r.categoria_original: r.categoria_padronizada
    for r in padronizacao_categorias.collect()
}

# COMMAND ----------

padronizacao_recomendacoes = tabela_padronizacao.filter("tipo = 'recomendacao'").join(
    recomendacoes_originais, on="categoria_original", how="leftsemi"
)

padronizacao_recomendacoes = {
    r.categoria_original: r.categoria_padronizada
    for r in padronizacao_recomendacoes.collect()
}

# COMMAND ----------


@F.pandas_udf("string")
def padroniza_string_tags(arrays: pd.Series) -> pd.Series:
    def padroniza(categorias):
        if not categorias or not categorias.strip():
            return None

        return "|".join(
            [
                categoria_padronizada
                for c in categorias.split("|")
                if (categoria_padronizada := padronizacao_recomendacoes.get(c))
            ]
        )

    return arrays.apply(padroniza)


@F.pandas_udf("array<string>")
def padroniza_array_categorias(arrays: pd.Series) -> pd.Series:
    def padroniza(categorias):
        if categorias is None:
            return None

        return [
            categoria_padronizada
            for c in categorias
            if (categoria_padronizada := padronizacao_categorias.get(c))
        ]

    return arrays.apply(padroniza)


# COMMAND ----------

produtos_padronizados = (
    produtos.withColumn(
        "categorias",
        padroniza_array_categorias("categorias"),
    )
    # .withColumn(
    #     "tags_agregadas",
    #     padroniza_string_tags("tags_agregadas"),
    # )
    # .withColumn(
    #     "tags_complementares",
    #     padroniza_string_tags("tags_complementares"),
    # )
    # .withColumn(
    #     "tags_potencializam_uso",
    #     padroniza_string_tags("tags_potencializam_uso"),
    # )
    # .withColumn(
    #     "tags_atenuam_efeitos",
    #     padroniza_string_tags("tags_atenuam_efeitos"),
    # )
)

# COMMAND ----------

if DEBUG:
    produtos_padronizados.filter(
        "categorias_de_complementares_por_indicacao is not null"
    ).select("categorias").limit(1000).display()

# COMMAND ----------

produtos_padronizados.write.mode("overwrite").saveAsTable(
    Table.produtos_em_processamento.value
)
