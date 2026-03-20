# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# MAGIC %pip install gspread google-auth

# COMMAND ----------

spark.conf.set("spark.databricks.service.server.enabled", "true")

# COMMAND ----------

import json

import gspread
import pyspark.sql.functions as F
from google.oauth2.service_account import Credentials
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])
spark = (
    SparkSession.builder.appName("cria-produto-loja-campanha")
    .config(conf=config)
    .getOrCreate()
)

stage = "prod"
catalog = "production"
delta_produto_loja_campanha = "hive_metastore.pbi.produto_loja_campanha"

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, USER, PASSWORD, utilizar_read_replica=True)

# COMMAND ----------

json_txt = dbutils.secrets.get("databricks", "GOOGLE_SHEETS_PBI")
creds = Credentials.from_service_account_info(
    json.loads(json_txt),
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)

gc = gspread.authorize(creds)

SHEET_ID = "1n-olIrTv6115vfxB4-zSGxWVjcbne04pHzCdkBX1Fg4"
ABA = "campanha_loja"

rows = gc.open_by_key(SHEET_ID).worksheet(ABA).get_all_records()

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando as lojas das campanhas - Sheets

# COMMAND ----------

df_loja_campanha = spark.createDataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cadastro de lojas do postgres

# COMMAND ----------

df_lojas = postgres.get_lojas(
    spark,
    ativo=None,
    extra_fields=[
        "c.databricks_tenant_name as rede",
        "cnpj",
        "coalesce(cidade, 'sem_informacao') as cidade",
        "coalesce(estado, 'sem_informacao') as estado",
        "erp",
        "status as status_loja",
    ],
).select(
    F.col("id").alias("loja_id"),
    F.col("rede"),
    F.col("name").alias("loja"),
    F.col("cnpj"),
    F.col("cidade"),
    F.col("estado"),
    F.col("erp"),
    F.col("status_loja"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregandos os produtos de campanha e os concorrentes

# COMMAND ----------

query_produto = """
--select
    WITH produtos_campanha AS (
            SELECT DISTINCT
                cp.produto_id,
                p.ean,
                p.nome AS nome_produto,
                p.marca AS marca,
                p.fabricante AS industria,
                'campanha' AS tipo_ean,
                p.marca AS marca_campanha,
                p.fabricante AS industria_campanha,
                CONCAT('1 - ', p.marca) AS marca_ordenada_graficos,
                c.nome as campanha,
                CASE WHEN cp.removido_em IS NULL THEN 'ativo' ELSE 'inativo' END AS status_produto_na_campanha
            FROM campanhas_campanhaproduto cp
            INNER JOIN campanhas_campanha c ON cp.campanha_id = c.id
            INNER JOIN produtos_produtov2 p ON cp.produto_id = p.id
            WHERE c.tipo_campanha = 'INDUSTRIA'
        ),
        produtos_concorrentes AS (
            SELECT DISTINCT
                pcc.id AS produto_id,
                vp.ean_buscado AS ean,
                pcc.nome AS nome_produto,
                pcc.marca AS marca,
                pcc.fabricante AS industria,
                'concorrente' AS tipo_ean,
                pc.marca AS marca_campanha,
                pc.fabricante AS industria_campanha,
                pcc.marca as marca_ordenada_graficos,
                cc.nome as campanha,
                CASE WHEN cp.removido_em IS NULL THEN 'ativo' ELSE 'inativo' END AS status_produto_na_campanha
            FROM v4_produtocampanhasubstituto vp
            INNER JOIN produtos_produtov2 pc ON vp.ean_substituto = pc.ean  -- campanha
            INNER JOIN produtos_produtov2 pcc ON vp.ean_buscado = pcc.ean   -- concorrente
            inner join campanhas_campanhaproduto cp on pc.id = cp.produto_id
            inner join campanhas_campanha cc on cp.campanha_id = cc.id
            WHERE cc.tipo_campanha = 'INDUSTRIA' AND vp.score = 1
        )
        SELECT
            produto_id,
            ean,
            nome_produto,
            status_produto_na_campanha,
            marca,
            industria,
            tipo_ean,
            marca_campanha,
            industria_campanha,
            marca_ordenada_graficos,
            campanha
        FROM produtos_campanha
        UNION ALL
        SELECT
            produto_id,
            ean,
            nome_produto,
            status_produto_na_campanha,
            marca,
            industria,
            tipo_ean,
            marca_campanha,
            industria_campanha,
            marca_ordenada_graficos,
            campanha
        FROM produtos_concorrentes
"""

df_produtos_campanha_concorrente = postgres.execute_query(query_produto)

# COMMAND ----------

df_produtos_campanha_concorrente = spark.createDataFrame(
    df_produtos_campanha_concorrente
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Criando produto_loja_campanha

# COMMAND ----------

df_produto_loja_campanha = (
    df_loja_campanha.alias("l")
    .join(df_produtos_campanha_concorrente.alias("p"), "campanha", "inner")
    .join(df_lojas.alias("lc"), "loja_id", "inner")
    .select(
        "p.*",
        "lc.*",
        "l.status_campanha",
        F.concat_ws(
            "|",  # loja_id|produto_id
            F.col("l.loja_id").cast("string"),
            F.col("p.produto_id").cast("string"),
        ).alias("loja_produto_id"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando delta table

# COMMAND ----------

df_produto_loja_campanha.write.mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable(delta_produto_loja_campanha)
