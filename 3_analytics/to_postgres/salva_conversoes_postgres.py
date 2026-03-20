# Databricks notebook source
import uuid

import pandas as pd
from pyspark.sql import functions as F

from maggulake.environment import DatabricksEnvironmentBuilder

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "salva-conversoes-postgres",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={"environment": "production"},
)

spark = env.spark
DEBUG = dbutils.widgets.get("debug") == "true"

bigquery_schema_postgres = env.get_bigquery_schema()
bigquery_schema_gold = env.get_bigquery_schema_gold()

# TODO colocar parâmetro para realizar testes em staging
# apontando para o banco de produção para fazer o insert
postgres = env.postgres_adapter

# COMMAND ----------

# MAGIC %md
# MAGIC # Data máxima salva na tabela de recomendações do postgres

# COMMAND ----------

# pega a data máxima da venda convertida salva
df_data_max = postgres.execute_query(
    "select max(data_venda) as max_date from recomendacoes_recomendacoesconversoes"
)

# COMMAND ----------

df_data_max.display()

# COMMAND ----------

val = df_data_max.iat[0, 0]

if pd.notna(val) and getattr(val, "tzinfo", None) is not None:
    val = val.tz_localize(None)

data_maxima = "" if pd.isna(val) else val.strftime("%Y-%m-%d %H:%M:%S")
print(
    f"A data máxima salva na tabela recomendacoes_recomendacoesconversoes é {data_maxima}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lendo as recomendações convertidas do Bigquery

# COMMAND ----------

# A tabela materializada abaixo é atualizada todos os dias usando uma consulta programada no Bigquery.
# Iremos salvar só recomendações convertidas
df_recomendacoes_bq = spark.sql(f"""
SELECT
    *
FROM
    {bigquery_schema_gold}.recomendacoes r
WHERE
    conversion_status = 'CONVERTIDA'
    and sale_time > '{data_maxima}'
""")

# COMMAND ----------

display(
    df_recomendacoes_bq.agg(
        F.min("sale_time").alias('min_data_venda'),
        F.max("sale_time").alias('max_data_venda'),
    )
)

# COMMAND ----------

# referência para entender o período que está sendo salvo e a quantidade
display(
    df_recomendacoes_bq.agg(
        F.min("recommendation_time").alias('min_data_rec'),
        F.max("recommendation_time").alias('max_data_rec'),
    )
)
print("Total de linhas:", df_recomendacoes_bq.count())

# COMMAND ----------

pandas_df = df_recomendacoes_bq.select(
    F.col("ref_ean"),
    F.col("rec_ean"),
    F.col("rec_tipo_venda").alias("tipo_venda"),
    F.col("rec_tipo_recomendacao").alias("tipo_recomendacao"),
    F.col("sale_time").alias("data_venda"),
).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tratamento dos dados para inserir no postgres

# COMMAND ----------

# MAGIC %md
# MAGIC ## Busca o id do produto

# COMMAND ----------

eans_list = pandas_df["ref_ean"].tolist() + pandas_df["rec_ean"].tolist()
unique_eans = list(set(eans_list))
eans_format = ", ".join([f"'{ean}'" for ean in unique_eans])

query_product_ids = f"""
SELECT id, ean FROM {bigquery_schema_postgres}.produtos_produtov2
WHERE ean IN ({eans_format})
"""

product_mapping_df = spark.sql(query_product_ids).toPandas()
ean_to_id = dict(zip(product_mapping_df["ean"], product_mapping_df["id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monta a query

# COMMAND ----------

valid_rows = []

for _, row in pandas_df.iterrows():
    if row['ref_ean'] not in ean_to_id or row['rec_ean'] not in ean_to_id:
        continue

    produto_buscado_id = ean_to_id[row["ref_ean"]]
    produto_recomendado_id = ean_to_id[row["rec_ean"]]
    new_id = str(uuid.uuid4())

    tipo_venda = row["tipo_venda"] if pd.notna(row["tipo_venda"]) else None
    tipo_recomendacao = (
        row["tipo_recomendacao"] if pd.notna(row["tipo_recomendacao"]) else None
    )
    data_venda = row["data_venda"] if pd.notna(row["data_venda"]) else None

    valid_rows.append(
        (
            new_id,
            produto_buscado_id,
            produto_recomendado_id,
            tipo_venda,
            tipo_recomendacao,
            data_venda,
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subindo para o Copilot

# COMMAND ----------

if DEBUG:
    dbutils.notebook.exit("Modo teste, não executar")

# COMMAND ----------

print(f"{len(valid_rows)} registros a serem salvos")
batch_size = 1000

for i in range(0, len(valid_rows), batch_size):
    batch = valid_rows[i : i + batch_size]

    valores_inserir = []
    for row_data in batch:
        (
            new_id,
            produto_buscado_id,
            produto_recomendado_id,
            tipo_venda,
            tipo_recomendacao,
            data_venda,
        ) = row_data

        tipo_venda_sql = f"'{tipo_venda}'" if tipo_venda is not None else "NULL"
        tipo_recomendacao_sql = (
            f"'{tipo_recomendacao}'" if tipo_recomendacao is not None else "NULL"
        )
        data_venda_sql = f"'{data_venda}'" if data_venda is not None else "NULL"

        valores_inserir.append(
            f"('{new_id}'::uuid, '{produto_buscado_id}'::uuid, "
            f"'{produto_recomendado_id}'::uuid, {tipo_venda_sql}, "
            f"{tipo_recomendacao_sql}, {data_venda_sql}::timestamp, 'CONVERTIDA')"
        )

    # TODO - implementar modo append ou upsert e para isso precisa acrescentar a coluna de venda_id
    query_valores = ", ".join(valores_inserir)
    batch_insert_query = f"""
    INSERT INTO recomendacoes_recomendacoesconversoes
    (id, produto_buscado_id, produto_recomendado_id, tipo_venda,
    tipo_recomendacao, data_venda, status_conversao)
    SELECT * FROM (VALUES {query_valores}) AS v(
        id, produto_buscado_id, produto_recomendado_id, tipo_venda,
        tipo_recomendacao, data_venda, status_conversao)
    """

    postgres.execute_query(batch_insert_query)
    print(f"Batch de {len(batch)} registros concluído")

print("Processo concluído.")

# COMMAND ----------

# verificação final
env.postgres_adapter.read_table(spark, "recomendacoes_recomendacoesconversoes").limit(
    10
).display()
