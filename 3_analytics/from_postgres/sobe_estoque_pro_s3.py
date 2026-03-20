# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("somente_testes", "true", ["true", "false"])
dbutils.widgets.dropdown("debug", "true", ["false", "true"])

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
)

from maggulake.io.postgres import PostgresAdapter
from maggulake.schemas.estoque_historico import schema_estoque_historico

# COMMAND ----------

# MAGIC %md
# MAGIC #Configurações e parâmetros

# COMMAND ----------

spark = (
    SparkSession.builder.appName("sobe_estoque_pro_s3")
    .config(conf=SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")]))
    .getOrCreate()
)

# COMMAND ----------

STAGE = dbutils.widgets.get("stage")
CATALOG = "production" if STAGE == "prod" else "staging"
SOMENTE_TESTES = dbutils.widgets.get("somente_testes") == "true"
DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

TABELA_S3_FOLDER = f"s3://maggu-datalake-{STAGE}/5-sharing-layer/copilot.maggu.ai/estoque_historico_formatado/"
DELTA_TABLE_ESTOQUE = (
    f"{CATALOG}.analytics.estoque_historico_eans_campanha_e_concorrentes"
)

# COMMAND ----------

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(STAGE, USER, PASSWORD)

# COMMAND ----------

# MAGIC %md
# MAGIC # Leitura dos dados de estoque e campanhas do postgres

# COMMAND ----------

query_estoque_campanha = """
    --select
    WITH produtos_campanha_concorrente AS (
        -- produto campanha
            SELECT DISTINCT(cp.produto_id) AS produto_id, 'campanha' as tipo_ean
            FROM campanhas_campanhaproduto cp
            LEFT JOIN campanhas_campanha c ON cp.campanha_id = c.id
            WHERE c.tipo_campanha = 'INDUSTRIA'
            AND c.status = 'ATIVA'
            AND date(c.inicio_em) <= CURRENT_DATE
            AND date(c.fim_em) >= CURRENT_DATE
            AND cp.removido_em IS NULL
            UNION
        -- produto concorrente
            SELECT DISTINCT(produtov2_id) AS produto_id, 'concorrente' as tipo_ean
            FROM campanhas_campanhaproduto_produtos_concorrentes ccp
            INNER JOIN campanhas_campanhaproduto cp ON ccp.campanhaproduto_id = cp.id
            INNER JOIN campanhas_campanha c ON cp.campanha_id = c.id
            WHERE c.tipo_campanha = 'INDUSTRIA'
            AND c.status = 'ATIVA'
            AND date(c.inicio_em) <= CURRENT_DATE
            AND date(c.fim_em) >= CURRENT_DATE
            AND cp.removido_em IS NULL
        )
    SELECT
        e.criado_em,
        e.atualizado_em,
        e.id,
        e.ean_conta_loja,
        e.produto_id,
        e.ean,
        e.tenant,
        e.codigo_loja,
        e.preco_venda_desconto,
        e.custo_compra,
        e.estoque_unid,
        e.loja_id,
        pc.tipo_ean
    FROM produtos_produtoloja e
    INNER JOIN produtos_campanha_concorrente pc ON e.produto_id = pc.produto_id
"""

df_estoque_campanha = postgres.execute_query(query_estoque_campanha)
df_estoque = spark.createDataFrame(df_estoque_campanha)

# COMMAND ----------

df_estoque = (
    df_estoque.withColumn(
        "custo_compra", F.col("custo_compra").cast(DecimalType(10, 2))
    )
    .withColumn("estoque_unid", (F.col("estoque_unid") * 1.0).cast(DoubleType()))
    .withColumn(
        "preco_venda_desconto", F.col("preco_venda_desconto").cast(DecimalType(10, 2))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando no S3

# COMMAND ----------

df_estoque = df_estoque.withColumn(
    "data_extracao", F.expr("current_timestamp() - interval 3 hours")
).withColumn("ano_mes", F.date_format("data_extracao", "yyyy-MM"))

# COMMAND ----------

df_estoque_write = spark.createDataFrame(df_estoque.rdd, schema_estoque_historico)

# COMMAND ----------

if not SOMENTE_TESTES and df_estoque.take(1):
    if not spark.catalog.tableExists(DELTA_TABLE_ESTOQUE):
        df_existing = spark.read.parquet(TABELA_S3_FOLDER)
        df_existing.write.format("delta").mode("overwrite").saveAsTable(
            DELTA_TABLE_ESTOQUE
        )
    # TODO: Precisa mesmo salvar em 2 lugares (s3 e delta)?
    df_estoque_write.write.mode("append").partitionBy("ano_mes").parquet(
        TABELA_S3_FOLDER, compression="zstd"
    )
    df_estoque_write.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(DELTA_TABLE_ESTOQUE)
