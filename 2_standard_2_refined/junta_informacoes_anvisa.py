# Databricks notebook source
import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Table

env = DatabricksEnvironmentBuilder.build("atualiza_produtos_anvisa", dbutils)

# COMMAND ----------

produtos = env.table(Table.produtos_em_processamento).cache()
medicamentos = env.table(Table.medicamentos_anvisa).cache()

# COMMAND ----------

# Compatibilidade: tabelas podem ter "registro" (antigo) ou "numero_registro" (novo)
p_registro_col = (
    "numero_registro" if "numero_registro" in produtos.columns else "registro"
)
m_registro_col = (
    "numero_registro" if "numero_registro" in medicamentos.columns else "registro"
)

if p_registro_col == "registro":
    produtos = produtos.withColumnRenamed("registro", "numero_registro")
if m_registro_col == "registro":
    medicamentos = medicamentos.withColumnRenamed("registro", "numero_registro")

# COMMAND ----------

colunas_so_produtos = [
    coluna for coluna in produtos.columns if coluna not in medicamentos.columns
]
colunas_so_anvisa = ["tarja"]
# Na anvisa vem só razao social, prefiro o nome mais curto
colunas_melhores_nos_produtos = ["fabricante"]
colunas_talvez_muito_grandes = ["principio_ativo"]
colunas_melhores_na_anvisa = [
    coluna
    for coluna in medicamentos.columns
    if coluna not in colunas_so_produtos
    and coluna not in colunas_so_anvisa
    and coluna not in colunas_melhores_nos_produtos
    and coluna not in colunas_talvez_muito_grandes
]

# COMMAND ----------

produtos_atualizados = (
    produtos.alias("p")
    .join(
        medicamentos.alias("m"),
        F.col("p.numero_registro") == F.col("m.numero_registro"),
        "left",
    )
    .select(
        *[f"p.{coluna}" for coluna in colunas_so_produtos],
        *[f"m.{coluna}" for coluna in colunas_so_anvisa],
        *[
            F.coalesce(f"p.{coluna}", f"m.{coluna}").alias(coluna)
            for coluna in colunas_melhores_nos_produtos
        ],
        *[
            F.when(F.length(F.col(f"m.{coluna}")) >= 1000, F.col(f"p.{coluna}"))
            .otherwise(F.coalesce(f"m.{coluna}", f"p.{coluna}"))
            .alias(coluna)
            for coluna in colunas_talvez_muito_grandes
        ],
        *[
            F.coalesce(f"m.{coluna}", f"p.{coluna}").alias(coluna)
            for coluna in colunas_melhores_na_anvisa
        ],
    )
)

# COMMAND ----------

produtos_atualizados.write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(Table.produtos_em_processamento.value)
