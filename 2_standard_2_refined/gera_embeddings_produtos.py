# Databricks notebook source
import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Table

MODEL = "text-embedding-3-large"
DIMENSION = 1536

env = DatabricksEnvironmentBuilder.build(
    "gera_embeddings_produtos",
    dbutils,
)

spark = env.spark
# Desabilita broadcast em grandes tabelas
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

produtos = env.table(Table.produtos_refined).filter("status = 'ativo'")

# TODO: trimar antes de salvar na produtos_refined
textos = (
    produtos.selectExpr("trim(informacoes_para_embeddings) AS texto")
    .dropna()
    .dropDuplicates()
    .filter("texto != ''")
    .cache()
)

# COMMAND ----------

# textos.count()

# COMMAND ----------

embeddings = (
    env.table(Table.embeddings).filter(f"modelo = '{MODEL}-{DIMENSION}'").cache()
)
textos_embeddings = embeddings.select("texto")

textos_novos = textos.join(
    F.broadcast(embeddings.select("texto")), on="texto", how="leftanti"
).cache()

# textos_novos.count()

# COMMAND ----------

embeddings_udf = env.get_embeddings_udf(MODEL, DIMENSION)

embeddings = (
    textos_novos.withColumn("gerado_em", F.current_timestamp())
    .withColumn("modelo", F.lit(f"{MODEL}-{DIMENSION}"))
    .withColumn("embedding", embeddings_udf(F.col("texto")))
)

embeddings.write.mode("append").saveAsTable(Table.embeddings.value)
