# Databricks notebook source
# MAGIC %md
# MAGIC # Validação de Compliance OTC/MIP
# MAGIC
# MAGIC Valida a consistência dos dados de produtos OTC com base na LMIP da ANVISA.
# MAGIC
# MAGIC ## Regras de Compliance
# MAGIC
# MAGIC 1. **OTC → Não Controlado**: Se `eh_otc = True`, então `eh_controlado = False`
# MAGIC 2. **OTC → Tarja compatível**: Se `eh_otc = True`, então `tarja IN (NULL, 'Sem Tarja')`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment

# COMMAND ----------

import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Table

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "valida_compliance_otc",
    dbutils,
)

spark = env.spark
DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carrega dados

# COMMAND ----------

produtos_df = spark.read.table(Table.produtos_refined.value).cache()

total_produtos = produtos_df.count()
total_medicamentos = produtos_df.filter("eh_medicamento = true").count()
total_otc = produtos_df.filter("eh_otc = true").count()
total_controlados = produtos_df.filter("eh_controlado = true").count()

print(f"Total de produtos: {total_produtos}")
print(f"Total de medicamentos: {total_medicamentos}")
print(f"Produtos OTC (eh_otc=True): {total_otc}")
print(f"Produtos Controlados: {total_controlados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Regra 1: OTC não pode ser Controlado

# COMMAND ----------

# Produtos que são OTC e Controlado ao mesmo tempo (violação)
violacao_otc_controlado = produtos_df.filter("eh_otc = true AND eh_controlado = true")

qtd_violacao_otc_controlado = violacao_otc_controlado.count()

print("=" * 60)
print("REGRA 1: OTC → Não Controlado")
print("=" * 60)

if qtd_violacao_otc_controlado > 0:
    print(f"⚠️ {qtd_violacao_otc_controlado} produtos violam essa regra!")
    print("\nExemplos:")
    violacao_otc_controlado.select(
        "ean", "nome_comercial", "principio_ativo", "tarja", "eh_otc", "eh_controlado"
    ).limit(20).display()
else:
    print("✅ Nenhuma violação encontrada!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regra 2: OTC deve ter tarja compatível

# COMMAND ----------

# Produtos OTC com tarja que não seja "Sem Tarja" ou NULL (violação)
violacao_otc_tarja = produtos_df.filter(
    "eh_otc = true AND tarja IS NOT NULL AND tarja NOT ILIKE '%sem tarja%' AND tarja NOT LIKE '%Vermelha%'"
)

qtd_violacao_otc_tarja = violacao_otc_tarja.count()

print("=" * 60)
print("REGRA 2: OTC → Tarja compatível (Sem Tarja ou NULL)")
print("=" * 60)

if qtd_violacao_otc_tarja > 0:
    print(f"⚠️ {qtd_violacao_otc_tarja} produtos violam essa regra!")
    print("\nDistribuição por tarja:")
    violacao_otc_tarja.groupBy("tarja").count().orderBy(F.desc("count")).limit(
        20
    ).display()

    print("\nExemplos:")
    violacao_otc_tarja.select(
        "ean", "nome_comercial", "principio_ativo", "tarja", "eh_otc"
    ).limit(20).display()
else:
    print("✅ Nenhuma violação encontrada!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distribuição geral de eh_otc

# COMMAND ----------

print("=" * 60)
print("DISTRIBUIÇÃO DE eh_otc")
print("=" * 60)

# Apenas medicamentos
medicamentos_df = produtos_df.filter("eh_medicamento = true")

medicamentos_df.groupBy("eh_otc").count().orderBy("eh_otc").display()

# COMMAND ----------

# Distribuição de eh_otc por tarja (apenas medicamentos)
if DEBUG:
    print("\nDistribuição de eh_otc por tarja (medicamentos):")
    medicamentos_df.groupBy("tarja", "eh_otc").count().orderBy("tarja", "eh_otc").limit(
        50
    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Relatório Final

# COMMAND ----------

total_violacoes = qtd_violacao_otc_controlado + qtd_violacao_otc_tarja

compliance_rate = (
    ((total_medicamentos - total_violacoes) / total_medicamentos * 100)
    if total_medicamentos > 0
    else 100
)

print("=" * 60)
print("RELATÓRIO FINAL DE COMPLIANCE OTC")
print("=" * 60)
print(f"Total de medicamentos analisados: {total_medicamentos}")
print(f"Total de produtos OTC: {total_otc}")
print(f"Total de produtos controlados: {total_controlados}")
print("-" * 60)
print(f"Violações OTC + Controlado: {qtd_violacao_otc_controlado}")
print(f"Violações OTC + Tarja incompatível: {qtd_violacao_otc_tarja}")
print("-" * 60)
print(f"Total de violações: {total_violacoes}")
print(f"Taxa de compliance: {compliance_rate:.2f}%")
print("=" * 60)

if total_violacoes == 0:
    print("✅ Nenhuma violação de compliance encontrada!")
else:
    print("⚠️ Existem violações que precisam ser corrigidas.")
