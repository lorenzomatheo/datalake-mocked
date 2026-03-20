# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# MAGIC %md
# MAGIC # Configurações e parâmetros

# COMMAND ----------

# Spark

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])
spark = SparkSession.builder.appName("sobe_vendas").config(conf=config).getOrCreate()

# Postgres

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter("prod", USER, PASSWORD)

# COMMAND ----------

# MAGIC %md
# MAGIC # Queries para criação das views

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Vendas

# COMMAND ----------

query_vendas = """
CREATE OR REPLACE VIEW view_vendas_item AS
SELECT
    vv.venda_concluida_em,
    vv.id AS venda_id,
    vv.codigo_externo_do_vendedor,
    vv.loja_id,
    vv.pre_venda_id,
    vv.vendedor_id,
    vv.status AS status_venda,
    vi.id AS venda_item_id,
    vi.item_da_pre_venda_id,
    vi.produto_id,
    vi.ean,
    vi.preco_venda,
    vi.preco_venda_desconto,
    vi.custo_compra,
    vi.desconto_total,
    vi.valor_final,
    vi.quantidade,
    vi.status AS status_venda_item,
    (vi.quantidade * vi.preco_venda_desconto) AS valor_venda,
    vv.atualizado_em AS atualizado_em,
    CURRENT_TIMESTAMP AS data_hora_execucao
FROM vendas_item vi
INNER JOIN vendas_venda vv ON vi.venda_id = vv.id
"""

# COMMAND ----------

postgres.execute_query(query_vendas)
