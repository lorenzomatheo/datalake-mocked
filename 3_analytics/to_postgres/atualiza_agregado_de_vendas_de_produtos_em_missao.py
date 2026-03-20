# Databricks notebook source
import pyspark.sql.functions as F

from maggulake.environment import (
    BigqueryView,
    DatabricksEnvironmentBuilder,
)

env = DatabricksEnvironmentBuilder.build(
    "atualiza_agregado_de_vendas_de_produtos_em_missao", dbutils
)

# COMMAND ----------

# TODO: mudar

schema = (
    "bigquery.databricks_views_production"
    if env.settings.name == "production"
    else "bigquery.databricks_views_staging"
)

tabela_agregado = f"{schema}.agregado_total_de_vendas_de_produtos_em_missao_por_semana"
tabela_por_missao = f"{schema}.agregado_total_de_produtos_por_missao_por_semana"

# COMMAND ----------

agregado = (
    env.table(BigqueryView.agregado_em_missao)
    .withColumn("id", F.expr("uuid()"))
    .dropna()
    .cache()
)

# COMMAND ----------

agregado.limit(10).display()

# COMMAND ----------

env.postgres_adapter.insert_into_table(
    agregado,
    "area_do_dono_agregadototaldevendasdeprodutosemmissaoporsemana",
    mode="overwrite",
    batch_size=1000,
)

# COMMAND ----------

por_missao = (
    env.table(BigqueryView.agregado_por_missao)
    .withColumn("id", F.expr("uuid()"))
    .dropna()
    .cache()
)

# COMMAND ----------

por_missao.limit(10).display()

# COMMAND ----------

env.postgres_adapter.insert_into_table(
    por_missao,
    "area_do_dono_agregadototaldeprodutospormissaoporsemana",
    mode="overwrite",
    batch_size=1000,
)
