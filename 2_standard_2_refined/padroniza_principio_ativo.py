# Databricks notebook source
# MAGIC %md
# MAGIC # Padroniza princípio ativo
# MAGIC
# MAGIC Aplica regras de padronização na coluna `principio_ativo` dos produtos em processamento
# MAGIC e mantém tabela histórica `refined.principios_ativos_padronizados` com o mapeamento
# MAGIC original → padronizado por EAN.
# MAGIC
# MAGIC Regras (validadas contra DCB ANVISA out/2025 — 70% match):
# MAGIC 1. Separadores: `;`, `,` (seguida de letra), `\n` → ` + `
# MAGIC 2. Formato de sal: "X (sal)", "X sal", "sal X" → "sal de X"
# MAGIC 3. Hidratação: "trihidratado" / "triidratado" → "tri-hidratado"
# MAGIC 4. Ácido invertido: "X acido" → "acido X"
# MAGIC 5. Parênteses não-sal removidos: "(1%)", "(comp rosado)"
# MAGIC 6. Acentos removidos via remove_accents (chave canônica)
# MAGIC 7. Componentes ordenados alfabeticamente e deduplicados

# COMMAND ----------

import pyspark.sql.functions as F
from delta import DeltaTable

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.produtos.padroniza_principio_ativo import padronizar_principio_ativo_udf
from maggulake.schemas import schema_principios_ativos_padronizados as schema
from maggulake.utils.pyspark import to_schema
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "padroniza_principio_ativo",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark
DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê produtos em processamento

# COMMAND ----------

produtos = (
    env.table(Table.produtos_em_processamento)
    .select("ean", "principio_ativo")
    .dropDuplicates(["ean"])
)

if DEBUG:
    distinct_antes = produtos.select("principio_ativo").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria tabela se não existir e aplica padronização incremental

# COMMAND ----------

env.create_table_if_not_exists(Table.principios_ativos_padronizados, schema)

# COMMAND ----------

padronizados_existentes = env.table(Table.principios_ativos_padronizados)

novos_ou_alterados = (
    produtos.alias("p")
    .join(padronizados_existentes.alias("e"), "ean", "left")
    .where(
        F.col("e.principio_ativo_original").isNull()
        | ~F.col("p.principio_ativo").eqNullSafe(F.col("e.principio_ativo_original"))
    )
    .select("p.ean", "p.principio_ativo")
)

# COMMAND ----------

agora = agora_em_sao_paulo()

novos_padronizados = novos_ou_alterados.select(
    F.col("ean"),
    F.col("principio_ativo").alias("principio_ativo_original"),
    padronizar_principio_ativo_udf(F.col("principio_ativo")).alias(
        "principio_ativo_padronizado"
    ),
    F.lit(agora).alias("atualizado_em"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva/merge na tabela de padronização

# COMMAND ----------

novos_padronizados_schema = to_schema(novos_padronizados, schema)

qtd_novos = novos_padronizados_schema.count()

delta_table = DeltaTable.forName(spark, Table.principios_ativos_padronizados.value)
(
    delta_table.alias("target")
    .merge(novos_padronizados_schema.alias("source"), "target.ean = source.ean")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

if DEBUG and qtd_novos > 0:
    print(f"Novos ou alterados: {qtd_novos}")
    novos_padronizados_schema.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplica valor padronizado de volta em `_produtos_em_processamento`

# COMMAND ----------

tabela_padronizados = env.table(Table.principios_ativos_padronizados).select(
    "ean",
    F.col("principio_ativo_padronizado").alias("_pa_padronizado"),
)

produtos_completos = env.table(Table.produtos_em_processamento)

produtos_atualizados = (
    produtos_completos.join(F.broadcast(tabela_padronizados), "ean", "left")
    .withColumn(
        "principio_ativo",
        F.coalesce(F.col("_pa_padronizado"), F.col("principio_ativo")),
    )
    .drop("_pa_padronizado")
)

# COMMAND ----------

produtos_atualizados.write.mode("overwrite").saveAsTable(
    Table.produtos_em_processamento.value
)

# COMMAND ----------

if DEBUG:
    distinct_depois = produtos_atualizados.select("principio_ativo").distinct().count()
    reducao = (
        ((distinct_antes - distinct_depois) / distinct_antes * 100)
        if distinct_antes > 0
        else 0
    )
    print(f"Princípios ativos distintos ANTES: {distinct_antes}")
    print(f"Princípios ativos distintos DEPOIS: {distinct_depois}")
    print(f"Redução: {reducao:.1f}%")
