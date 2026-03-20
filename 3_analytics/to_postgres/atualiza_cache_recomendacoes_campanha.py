# Databricks notebook source
# MAGIC %md
# MAGIC # Atualiza a tabela de cache das recomendações dos produtos em campanhas
# MAGIC
# MAGIC NOTE: No dia 24/apr/25 foram incluídas algumas validações pois o notebook estava
# MAGIC quebrando quando alguém criava um concorrente sem power_phrase nas campanhas de industria.
# MAGIC

# COMMAND ----------

# ambiente
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------


# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])
spark = (
    SparkSession.builder.appName("atualiza_cache_recomendacoes_campanha")
    .config(conf=config)
    .getOrCreate()
)

# delta tables
# TODO: usar Environment do maggulake, usar o Table para definir os nomes das tabelas
PRODUTOS_AGREGADOS = f"{catalog}.standard.produtos_campanha_agregados"
PRODUTOS_SUBSTITUTOS = f"{catalog}.standard.produtos_campanha_substitutos"
POWER_PHRASES = f"{catalog}.enriquecimento.power_phrase_produtos"

# postgres
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, POSTGRES_USER, POSTGRES_PASSWORD)

tabela_agregados = 'v4_produtocampanhaagregado'
tabela_substitutos = 'v4_produtocampanhasubstituto'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura do datalake

# COMMAND ----------

produtos_agregados = spark.read.table(PRODUTOS_AGREGADOS).cache()
produtos_substitutos = spark.read.table(PRODUTOS_SUBSTITUTOS).cache()
power_phrases = (
    spark.read.table(POWER_PHRASES)
    .selectExpr("ean", "power_phrase as frase_fallback")
    .dropDuplicates(["ean"])
    .cache()
)

# COMMAND ----------

produtos_substitutos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tratamento - Agregados

# COMMAND ----------

produtos_agregados_formatados = produtos_agregados.withColumn(
    'eans_recomendar', F.explode('eans_recomendar')
).selectExpr(
    'uuid() as id',
    'eans_recomendar.ean AS ean_buscado',
    'ean_em_campanha AS ean_agregado',
    'eans_recomendar.frase',
    'eans_recomendar.score',
)

# COMMAND ----------

# Faz fallback de power phrase: Caso o time não tenha cadastrado uma power phrase,
# no ProdutoCampanha, utiliza a power phrase do produto em si (mais generalista)

# TODO: Esse fallback era para ser feito em outro notebook, não nessa pasta "3_analytics/to_postgres".
# Porém não estou com tempo sobrando para refatorar o notebook match_ads_substitutos.py agora


produtos_agregados_formatados = (
    produtos_agregados_formatados.alias("pa")
    .join(power_phrases.alias("pp"), F.col("pa.ean_buscado") == F.col("pp.ean"), "left")
    .select(
        F.col("pa.id"),
        F.col("pa.ean_buscado"),
        F.col("pa.ean_agregado"),
        F.coalesce(F.col("pa.frase"), F.col("pp.frase_fallback")).alias("frase"),
        F.col("pa.score"),
    )
)

# COMMAND ----------

produtos_agregados_sem_frase = produtos_agregados_formatados.filter(
    F.col("frase").isNull() | (F.col("frase") == '')
)

if (c := produtos_agregados_sem_frase.count()) > 0:
    print(
        f"Removendo {c} linhas devido o produto estar com frase vazia. Na tabela de concorrentes, defina a `power_frase` dos seguintes produtos:"
    )
    display(produtos_agregados_sem_frase)
else:
    print("Oba, nenhuma linha com frase vazia, muito bem!")

# remover linhas em que "frase" é vazia
produtos_agregados_formatados = produtos_agregados_formatados.filter(
    F.col("frase").isNotNull() & (F.col("frase") != '')
)

# COMMAND ----------

postgres.insert_into_table(
    produtos_agregados_formatados, tabela_agregados, mode='overwrite'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tratamento - Substitutos

# COMMAND ----------

produtos_substitutos_formatados = produtos_substitutos.withColumn(
    "eans_recomendar", F.explode("eans_recomendar")
).selectExpr(
    "uuid() as id",
    "eans_recomendar.ean AS ean_buscado",
    "ean_em_campanha AS ean_substituto",
    "eans_recomendar.frase",
    "eans_recomendar.score",
)

produtos_substitutos_formatados = (
    produtos_substitutos_formatados.alias("ps")
    .join(power_phrases.alias("pp"), F.col("ps.ean_buscado") == F.col("pp.ean"), "left")
    .select(
        F.col("ps.id"),
        F.col("ps.ean_buscado"),
        F.col("ps.ean_substituto"),
        F.coalesce(F.col("ps.frase"), F.col("pp.frase_fallback")).alias("frase"),
        F.col("ps.score"),
    )
)

# COMMAND ----------


produtos_substitutos_sem_frase = produtos_substitutos_formatados.filter(
    (F.col('frase').isNull()) | (F.col('frase') == '')
)

if (c := produtos_substitutos_sem_frase.count()) > 0:
    print(
        f"Removendo {c} linhas devido o produto estar com frase vazia. Na tabela de concorrentes, defina a `power_frase` dos seguintes produtos:"
    )
    display(produtos_substitutos_sem_frase)
else:
    print("Oba, nenhuma linha com frase vazia, muito bem!")

# remover linhas em que "frase" é vazia
produtos_substitutos_formatados = produtos_substitutos_formatados.filter(
    (F.col('frase').isNotNull()) & (F.col('frase') != '')
)

# COMMAND ----------

postgres.insert_into_table(
    produtos_substitutos_formatados, tabela_substitutos, mode='overwrite'
)
