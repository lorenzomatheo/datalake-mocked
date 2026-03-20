# Databricks notebook source
# MAGIC %md
# MAGIC # Validação de Produtos - EANs Alternativos
# MAGIC
# MAGIC Este notebook valida a integridade dos dados de `refined.eans_alternativos` e `status`.
# MAGIC
# MAGIC **Validações bloqueantes:**
# MAGIC - Campo `status` não pode ser nulo
# MAGIC - Produtos inativos devem ter `eans_alternativos` preenchidos
# MAGIC - Produtos ativos não podem ter EAN começando com 0
# MAGIC - EAN deve ser único (sem duplicados)
# MAGIC
# MAGIC Em caso de falha, o pipeline é interrompido e notificação é enviada ao Discord.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.errors import PySparkException

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.integrations.discord import enviar_mensagem_discord

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build("valida_produtos_eans_alternativos", dbutils)

spark = env.spark
CATALOG = env.settings.catalog
STAGE = env.settings.stage.value

ID_CANAL_DISCORD = 1211857582939963483  # Canal de alertas Databricks

print(f"STAGE: {STAGE}")
print(f"CATALOG: {CATALOG}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos dados

# COMMAND ----------

produtos_eans = spark.read.table(Table.refined_eans_alternativos.value)

print(f"✅ Dados carregados: {produtos_eans.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução das Validações

# COMMAND ----------

# Executa todas as validações em uma única passada pelo DataFrame
validations = next(
    produtos_eans.agg(
        F.sum(F.when(F.col("status").isNull(), 1).otherwise(0)).alias("status_nulos"),
        F.sum(
            F.when(
                (F.col("status") == "inativo")
                & (
                    F.col("eans_alternativos").isNull()
                    | (F.size(F.col("eans_alternativos")) == 0)
                ),
                1,
            ).otherwise(0)
        ).alias("inativos_sem_alternativos"),
        F.sum(
            F.when(
                (F.col("status") == "ativo") & F.col("ean").startswith("0"), 1
            ).otherwise(0)
        ).alias("ativos_com_zero_esquerda"),
        F.count("ean").alias("total_eans"),
        F.countDistinct("ean").alias("eans_distintos"),
        F.sum(F.when(F.col("status") == "ativo", 1).otherwise(0)).alias("total_ativos"),
        F.sum(F.when(F.col("status") == "inativo", 1).otherwise(0)).alias(
            "total_inativos"
        ),
    ).toLocalIterator()
)

print("\n📊 Métricas de Validação:")
print(f"   Total de EANs: {validations['total_eans']}")
print(f"   EANs distintos: {validations['eans_distintos']}")
print(f"   Produtos ativos: {validations['total_ativos']}")
print(f"   Produtos inativos: {validations['total_inativos']}")
print("\n⚠️  Violações encontradas:")
print(f"   Status nulos: {validations['status_nulos']}")
print(f"   Inativos sem alternativos: {validations['inativos_sem_alternativos']}")
print(f"   Ativos com zero à esquerda: {validations['ativos_com_zero_esquerda']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva métricas de validação

# COMMAND ----------

# Cria DataFrame com métricas para histórico
metricas_df = spark.createDataFrame(
    [
        {
            "stage": STAGE,
            "total_eans": validations['total_eans'],
            "eans_distintos": validations['eans_distintos'],
            "total_ativos": validations['total_ativos'],
            "total_inativos": validations['total_inativos'],
            "status_nulos": validations['status_nulos'],
            "inativos_sem_alternativos": validations['inativos_sem_alternativos'],
            "ativos_com_zero_esquerda": validations['ativos_com_zero_esquerda'],
            "num_duplicados": validations['total_eans'] - validations['eans_distintos'],
            "validacao_passou": (
                validations['status_nulos'] == 0
                and validations['inativos_sem_alternativos'] == 0
                and validations['ativos_com_zero_esquerda'] == 0
                and validations['total_eans'] == validations['eans_distintos']
            ),
        }
    ]
).withColumn("gerado_em", F.current_timestamp())

# Salva em tabela de analytics
table_name = f"{CATALOG}.analytics.view_validation_eans_alternativos"
metricas_df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)

print(f"✅ Métricas salvas em: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações Bloqueantes

# COMMAND ----------

# Lista de erros encontrados
erros = []

if validations["status_nulos"] > 0:
    erros.append(f"❌ Campo 'status' com {validations['status_nulos']} valores nulos")

if validations["inativos_sem_alternativos"] > 0:
    erros.append(
        f"❌ {validations['inativos_sem_alternativos']} produtos inativos sem eans_alternativos"
    )

if validations["ativos_com_zero_esquerda"] > 0:
    erros.append(
        f"❌ {validations['ativos_com_zero_esquerda']} produtos ativos com EAN começando com 0"
    )

if validations["total_eans"] != validations["eans_distintos"]:
    num_duplicados = validations["total_eans"] - validations["eans_distintos"]
    erros.append(f"❌ {num_duplicados} EANs duplicados encontrados")

# COMMAND ----------

# Se houver erros, envia notificação ao Discord e falha o notebook
if erros:
    mensagem_erro = (
        f"🚨 **Validação de EANs Alternativos Falhou ({STAGE})**\n\n" + "\n".join(erros)
    )
    mensagem_erro += f"\n\n📊 Total de registros: {validations['total_eans']}"

    print("\n" + mensagem_erro)

    # Envia notificação ao Discord
    enviar_mensagem_discord(spark, ID_CANAL_DISCORD, mensagem_erro)
    print("✅ Notificação enviada ao Discord")

    # Falha o notebook com mensagem detalhada
    erro_completo = "Validação de integridade falhou:\n" + "\n".join(erros)
    raise PySparkException(erro_completo)

# COMMAND ----------

print("✅ Todas as validações passaram com sucesso!")
print(f"   {validations['total_eans']} EANs validados")
print(
    f"   {validations['total_ativos']} ativos | {validations['total_inativos']} inativos"
)
