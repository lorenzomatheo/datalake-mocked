# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão da LMIP (Lista de Medicamentos Isentos de Prescrição) da ANVISA
# MAGIC
# MAGIC Este notebook carrega os anexos da LMIP e salva na camada raw do datalake.
# MAGIC Ref: https://anvisalegis.datalegis.net/action/ActionDatalegis.php?acao=abrirTextoAto&tipo=INM&numeroAto=00000285&seqAto=000&valorAno=2024&orgao=DC/ANVISA/MS&codTipo=&desItem=&desItemFim=&cod_menu=1696&cod_modulo=134&pesquisa=true
# MAGIC
# MAGIC **Fonte oficial:** IN nº 285, de 7 de março de 2024
# MAGIC - Anexo 1: Fármacos/Medicamentos sintéticos
# MAGIC - Anexo 2: Fitoterápicos
# MAGIC
# MAGIC **Tabela de destino:** `{catalog}.raw.lmip_anvisa`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment

# COMMAND ----------

import pyspark.sql.functions as F
from delta import DeltaTable

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.mappings.anvisa_lmip import LMIP_SCHEMA, AnvisaLmipParser

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "ingestao_lmip_anvisa",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark
settings = env.settings

DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

print(f"Stage: {settings.stage}")
print(f"Catalog: {settings.catalog}")
print(f"Bucket: {settings.bucket}")
print(f"Tabela destino: {Table.lmip_anvisa.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrega os CSVs da LMIP do S3

# COMMAND ----------

# Caminhos dos arquivos no S3
medicamentos_path = f"s3://{settings.bucket}/raw/anvisa/lmip/anexo1.csv"
fitoterapicos_path = f"s3://{settings.bucket}/raw/anvisa/lmip/anexo2.csv"

print(f"Medicamentos: {medicamentos_path}")
print(f"Fitoterápicos: {fitoterapicos_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processa os dados usando o AnvisaLmipParser

# COMMAND ----------

lmip_parser = AnvisaLmipParser.from_csv(
    spark,
    medicamentos_path,
    fitoterapicos_path,
)

lmip_df = lmip_parser.lmip_df

# COMMAND ----------

if DEBUG:
    print(f"Total de registros LMIP: {lmip_df.count()}")
    print(
        f"Medicamentos sintéticos: {lmip_df.filter(F.col('tipo_origem') == 'SINTETICO').count()}"
    )
    print(
        f"Fitoterápicos: {lmip_df.filter(F.col('tipo_origem') == 'FITOTERAPICO').count()}"
    )

# COMMAND ----------

if DEBUG:
    lmip_df.limit(1000).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Valida os dados antes de salvar

# COMMAND ----------

# Verifica se há princípios ativos nulos
nulos = lmip_df.filter(F.col("principio_ativo").isNull()).count()
if nulos > 0:
    print(f"AVISO: {nulos} registros com princípio ativo nulo")


# COMMAND ----------


# Verifica duplicatas por princípio ativo + formas farmacêuticas
duplicatas = (
    lmip_df.groupBy("principio_ativo", "formas_farmaceuticas", "tipo_origem")
    .count()
    .filter(F.col("count") > 1)
)
if duplicatas.count() > 0:
    print(f"AVISO: {duplicatas.count()} duplicadas encontradas")
    duplicatas.display()

    # fazer drop de duplicatas
    lmip_df = lmip_df.dropDuplicates(
        ["principio_ativo", "formas_farmaceuticas", "tipo_origem"]
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva na tabela Delta (camada raw)

# COMMAND ----------

# Cria a tabela se não existir
DeltaTable.createIfNotExists(spark).tableName(Table.lmip_anvisa.value).addColumns(
    LMIP_SCHEMA
).execute()

# COMMAND ----------

# Sobrescreve os dados (a LMIP é uma lista completa que deve ser substituída)
lmip_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    Table.lmip_anvisa.value
)

print(f"Dados salvos em: {Table.lmip_anvisa.value}")

# COMMAND ----------

# Verifica os dados salvos
spark.read.table(Table.lmip_anvisa.value).display()
