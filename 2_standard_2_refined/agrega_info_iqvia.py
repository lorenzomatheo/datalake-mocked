# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.enums import TipoMedicamento

# COMMAND ----------

# TODO: usar o DatabricksEnvironmentBuilder
# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("agrega_info_iqvia").config(conf=config).getOrCreate()
)

# ambiente e cliente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"
s3_bucket_name = f"maggu-datalake-{stage}"

# pastas s3
s3_refined_folder = f"s3://{s3_bucket_name}/3-refined-layer/produtos_enriquecidos"
tabela_produtos_iqvia = f"{catalog}.raw.produtos_iqvia"

# tabelas delta
produtos_em_processamento_path = f"{catalog}.refined._produtos_em_processamento"

# COMMAND ----------

# MAGIC %md
# MAGIC Atualizando com info da IQVIA

# COMMAND ----------

produtos = spark.read.table(produtos_em_processamento_path).cache()
produtos_iqvia = spark.read.table(tabela_produtos_iqvia).cache()

contagem_antes = produtos.select(
    F.count(
        F.when(
            F.col("nome_comercial").isNull()
            | (F.col("nome_comercial") == "")
            | (F.length(F.col("nome_comercial")) > 140),
            True,
        )
    ).alias("contagem_alterar_nome_comercial"),
    F.count(
        F.when(
            F.col("tipo_medicamento").isNull() | (F.col("tipo_medicamento") == ""), True
        )
    ).alias("contagem_nulos_tipo"),
    F.count(F.when(F.col("eh_otc").isNull(), True)).alias("contagem_nulos_eh_otc"),
).collect()[0]

joined_df = produtos.join(
    produtos_iqvia, produtos["ean"] == produtos_iqvia["cd_ean"], "left"
)


updated_df = (
    joined_df.withColumn(
        "nome_comercial",
        F.when(
            F.col("nome_comercial").isNull()
            | (F.col("nome_comercial") == "")
            | (F.length(F.col("nome_comercial")) > 140),
            F.col("apresentacao"),
        ).otherwise(F.col("nome_comercial")),
    )
    .withColumn(
        "tipo_medicamento",
        F.when(
            F.col("tipo_medicamento").isNull() | (F.col("tipo_medicamento") == ""),
            F.when(F.col("gmrs") == "G", TipoMedicamento.GENERICO.value)
            .when(F.col("gmrs") == "R", TipoMedicamento.REFERENCIA.value)
            .when(F.col("gmrs") == "S", TipoMedicamento.SIMILAR.value)
            .otherwise(F.col("tipo_medicamento")),
        ).otherwise(F.col("tipo_medicamento")),
    )
    .withColumn(
        "eh_otc",
        F.when(
            F.col("eh_otc").isNull(),
            F.when(F.col("visao_ims") == "OTC", F.lit(True))
            .when(F.col("visao_ims") == "RX", F.lit(False))
            .otherwise(F.col("eh_otc")),
        ).otherwise(F.col("eh_otc")),
    )
)

produtos_iqvia_columns = produtos_iqvia.columns
updated_df = updated_df.drop(*produtos_iqvia_columns)

contagem_depois = updated_df.select(
    F.count(
        F.when(
            F.col("nome_comercial").isNull()
            | (F.col("nome_comercial") == "")
            | (F.length(F.col("nome_comercial")) > 140),
            True,
        )
    ).alias("contagem_alterar_nome_comercial"),
    F.count(
        F.when(
            F.col("tipo_medicamento").isNull() | (F.col("tipo_medicamento") == ""), True
        )
    ).alias("contagem_nulos_tipo"),
    F.count(F.when(F.col("eh_otc").isNull(), True)).alias("contagem_nulos_eh_otc"),
).collect()[0]

print("Contagem de valores nulos ou vazios antes da atualização:")
print(f"nome_comercial: {contagem_antes['contagem_alterar_nome_comercial']}")
print(f"tipo_medicamento: {contagem_antes['contagem_nulos_tipo']}")
print(f"eh_otc: {contagem_antes['contagem_nulos_eh_otc']}")

print("\nContagem de valores nulos ou vazios depois da atualização:")
print(f"nome_comercial: {contagem_depois['contagem_alterar_nome_comercial']}")
print(f"tipo: {contagem_depois['contagem_nulos_tipo']}")
print(f"eh_otc: {contagem_depois['contagem_nulos_eh_otc']}")
final_df = updated_df

# COMMAND ----------

# MAGIC %md
# MAGIC Salva no S3 e no delta table

# COMMAND ----------

final_df = final_df.dropDuplicates(["ean"])

# COMMAND ----------

final_df = final_df.withColumn(
    "gerado_em", F.coalesce("gerado_em", F.current_timestamp())
)

# COMMAND ----------


final_df.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(
    produtos_em_processamento_path
)
