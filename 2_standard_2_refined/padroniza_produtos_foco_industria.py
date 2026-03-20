# Databricks notebook source
# MAGIC %pip install pandas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Table

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "padroniza_produtos_foco_industria",
    dbutils,
    spark_config={
        "spark.sql.files.maxPartitionBytes": "128mb",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
    },
)


# info dos produtos foco industria
path_info_produtos_ads = dbutils.secrets.get(
    scope="databricks", key="SHEETS_PRODUTOS_ADS"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Atualiza com informações dos produtos incentivados pela indústria

# COMMAND ----------

produtos = env.table(Table.produtos_em_processamento)

# COMMAND ----------

info_produtos_ads = env.spark.createDataFrame(pd.read_csv(path_info_produtos_ads))
info_produtos_ads.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Padroniza os dados no formato da tabela e aplica correções
# MAGIC Nesse caso a padronização é feita por produto.

# COMMAND ----------


def padroniza_coluna(column_name):
    return F.regexp_replace(
        F.concat_ws(
            "|",
            F.array_remove(
                F.transform(
                    F.split(
                        F.regexp_replace(
                            F.lower(F.col(column_name)).rlike(r"[\n\r]+"),
                            r"[\n\r]+",
                            " ",
                        ),
                        r"[,|]+",
                    ),
                    lambda x: F.trim(x),
                ),
                "",
            ),
        ),
        r"\|$",
        "",
    )


# COMMAND ----------

colunas_para_atualizar = [
    "ean",
    "nome_comercial",
    "tags_substitutos",
    "indicacao",
    "descricao",
]

info_para_atualizar = info_produtos_ads.select(*colunas_para_atualizar)

info_padronizada = info_para_atualizar.withColumn(
    "tags_substitutos", padroniza_coluna("tags_substitutos")
)

info_para_mergear = info_padronizada.select(
    *[F.col(c).alias(f"new_{c}") for c in info_padronizada.columns]
).cache()

joined_df = produtos.join(
    F.broadcast(info_para_mergear), on=F.col("ean") == F.col("new_ean"), how="left"
).cache()

# COMMAND ----------

for column in colunas_para_atualizar:
    if column != "ean":
        joined_df = joined_df.withColumn(
            column, F.coalesce(F.col(f"new_{column}"), F.col(column))
        )

final_df = joined_df.select(produtos.columns).cache()
final_df.limit(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC Salva no S3 e no delta table

# COMMAND ----------


final_df = final_df.dropDuplicates(["id"])

# COMMAND ----------

final_df = final_df.withColumn(
    "gerado_em", F.coalesce("gerado_em", F.current_timestamp())
)

# COMMAND ----------

final_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    Table.produtos_em_processamento.value
)
