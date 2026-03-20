# Databricks notebook source
# MAGIC %md
# MAGIC # Enriquece tamanho dos produtos
# MAGIC

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from maggulake.enums import TamanhoProduto

spark = SparkSession.builder.appName("enriquecimento_tamanho_produtos").getOrCreate()

# COMMAND ----------

# ambiente e cliente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"
s3_bucket_name = f"maggu-datalake-{stage}"

# pastas s3
s3_refined_folder = f"s3://{s3_bucket_name}/3-refined-layer/produtos_enriquecidos"

# tabelas delta
produtos_em_processamento_path = f"{catalog}.refined._produtos_em_processamento"
produtos_refined_path = f"{catalog}.refined.produtos_refined"
tabela_ja_processada_path = f"{catalog}.utils.medicamentos_forma_volume_quantidade"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calcula tamanho dos produtos
# MAGIC
# MAGIC Simples e fácil: vamos classificar entre `Pequeno`, `Medio` e `Grande` comparando o `volume_quantidade` com todos os outros itens que possuem a mesma forma farmaceutica e unidade de medida.
# MAGIC

# COMMAND ----------

df = spark.read.table(tabela_ja_processada_path)

# Por segurança...
df = df.withColumn("unidade_medida", F.lower(F.col("unidade_medida")))

# COMMAND ----------

df_gp = (
    df.select(["forma_farmaceutica", "unidade_medida", "quantidade"])
    .groupBy("forma_farmaceutica", "unidade_medida")
    .agg(
        F.min("quantidade").alias("min_quantidade"),
        F.expr("percentile_approx(quantidade, 0.33)").alias("p33_quantidade"),
        F.expr("percentile_approx(quantidade, 0.66)").alias("p66_quantidade"),
        F.max("quantidade").alias("max_quantidade"),
    )
)


df_joined = df.join(df_gp, ["forma_farmaceutica", "unidade_medida"], "left")

# COMMAND ----------

df_tamanhos = df_joined.withColumn(
    "tamanho_produto",
    F.when(F.col("quantidade") < F.col("p33_quantidade"), TamanhoProduto.PEQUENO.value)
    .when(F.col("quantidade") > F.col("p66_quantidade"), TamanhoProduto.GRANDE.value)
    .otherwise(TamanhoProduto.MEDIO.value),
)


# df_tamanhos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unir com tabela de produtos em processamento

# COMMAND ----------

df_produtos = spark.read.table(produtos_em_processamento_path)

if "tamanho_produto" in df_produtos.columns:
    df_produtos = df_produtos.drop("tamanho_produto")

print(df_produtos.count())


df_final = df_produtos.join(
    df_tamanhos.select(["forma_farmaceutica", "volume_quantidade", "tamanho_produto"]),
    on=["forma_farmaceutica", "volume_quantidade"],
    how="left",
)

print(df_final.count())


# df_final.filter(F.col("tamanho_produto").isNotNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva no delta

# COMMAND ----------

df_final.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(
    produtos_em_processamento_path
)
