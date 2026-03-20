# Databricks notebook source
# MAGIC %md
# MAGIC # Imports e parâmetros

# COMMAND ----------

import unicodedata

import pyspark.sql.functions as F

from maggulake.enums import (
    FaixaEtaria,
    FormaFarmaceutica,
    SexoRecomendado,
    TiposReceita,
    ViaAdministracao,
)
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas.enums import schema_enums

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "salva_enums_s3_e_delta",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark
DEBUG = dbutils.widgets.get("debug") == "true"
environment = dbutils.widgets.get("environment")

catalog = env.settings.catalog
stage = env.settings.stage

# COMMAND ----------

TABELA_S3_FOLDER = (
    f"s3://maggu-datalake-{stage}/5-sharing-layer/copilot.maggu.ai/enums/"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função auxiliar

# COMMAND ----------


def normalizar_valor(texto: str) -> str:
    if texto is None:
        return None
    # remove acentos e coloca em minúsculo
    texto_sem_acento = ''.join(
        c
        for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
    return texto_sem_acento.lower()


# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando os enums em um dataframe

# COMMAND ----------

df_p_ativos_enum = env.table(Table.enum_principio_ativo_medicamentos)

p_ativos_enum = df_p_ativos_enum.select("principio_ativo").distinct().collect()
p_ativos_list = [(row.principio_ativo) for row in p_ativos_enum]
p_ativos_tuple = tuple(row.principio_ativo for row in p_ativos_enum)

# COMMAND ----------

enums = {
    "tipo_receita": TiposReceita.tuple(),
    "forma_farmaceutica": FormaFarmaceutica.tuple(),
    "via_administracao": ViaAdministracao.tuple(),
    "idade_recomendada": FaixaEtaria.tuple(),
    "sexo_recomendado": SexoRecomendado.tuple(),
    "principio_ativo": p_ativos_tuple,
}

# COMMAND ----------

rows = []
for nome_enum, lista_valores in enums.items():
    for indice, valor in enumerate(lista_valores):
        ordem_logica = indice + 1

        valor_normalizado = normalizar_valor(valor)

        rows.append((nome_enum, valor, valor_normalizado, ordem_logica))

# COMMAND ----------

df_enums_unificado = spark.createDataFrame(rows, schema=schema_enums)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando enums no s3 e catalog

# COMMAND ----------

if not DEBUG and not df_enums_unificado.rdd.isEmpty():
    df_enums_unificado = df_enums_unificado.withColumn(
        "data_execucao", F.current_timestamp()
    )

    df_enums_unificado.write.mode("overwrite").parquet(TABELA_S3_FOLDER)

    df_enums_unificado.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable(Table.enums_completo.value)
