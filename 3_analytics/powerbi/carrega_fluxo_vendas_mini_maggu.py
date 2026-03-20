# Databricks notebook source
# MAGIC %md
# MAGIC Este notebook é provisório, pois o processo será realizado posteriormente no BigQuery.

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# MAGIC %pip install pandas gspread gspread-dataframe google PyDrive2 cryptography==41.0.7 pyOpenSSL==23.2.0 tiktoken
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime
from functools import reduce

import pandas as pd
import psycopg2
import pytz
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração do ambiente

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])
spark = SparkSession.builder.appName("sobe_vendas").config(conf=config).getOrCreate()

stage = "prod"
catalog = "production"
delta_conversao_recomendacao = "hive_metastore.pbi.conversao_recomendacao"

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, USER, PASSWORD, utilizar_read_replica=True)

# COMMAND ----------

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 1000)

# COMMAND ----------


def agora():
    return datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S")


# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando Recomendações - Postgres

# COMMAND ----------

try:
    conn = psycopg2.connect(
        database=postgres.DATABASE_NAME,
        user=postgres.USER,
        host=postgres.DATABASE_HOST,
        password=postgres.PASSWORD,
        port=postgres.DATABASE_PORT,
    )
    cursor = conn.cursor()

    query = """
        SELECT
            criado_em,
            atualizado_em,
            conversa_id,
            cesta_id,
            mostrar_apenas,
            ordinality,
            produto_visivel,
            ref_id,
            ref_ean,
            ref_nome,
            ref_tipo_venda,
            ref_tipo_recomendacao,
            produto_id              AS produto_id_recomendacao,
            produto_ean,
            produto_nome,
            produto_tipo_venda,
            produto_missao_produto_id,
            produto_missao_id,
            produto_missao_nome,
            produto_tipo_recomendacao
        FROM view_recomendacao_cesta r
        INNER JOIN cesta_de_compras_cesta c ON r.cesta_id = c.id
        WHERE DATE(c.aberta_em) >= '2025-04-14'                   -- primeira segunda após a inclusão da cesta_id na conversa
          AND origem = 'mini-maggu'
    """

    cursor.execute(f"SELECT COUNT(*) FROM ({query}) AS sub")
    total_rows = cursor.fetchone()[0]

    cursor.execute(query)

    chunk_size = 10_000
    processed_rows = 0
    dfs = []

    while True:
        data = cursor.fetchmany(chunk_size)
        if not data:
            break

        processed_rows += len(data)
        columns = [d[0] for d in cursor.description]
        pandas_df = pd.DataFrame(data, columns=columns)

        spark_df = spark.createDataFrame(pandas_df)
        dfs.append(spark_df)

    df_recomendacao = reduce(lambda d1, d2: d1.unionByName(d2), dfs)

except psycopg2.DatabaseError as err:
    print(f"Erro: {err}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando Cesta - Postgres

# COMMAND ----------

try:
    conn = psycopg2.connect(
        database=postgres.DATABASE_NAME,
        user=postgres.USER,
        host=postgres.DATABASE_HOST,
        password=postgres.PASSWORD,
        port=postgres.DATABASE_PORT,
    )
    cursor = conn.cursor()

    query = """
        SELECT
            c.aberta_em,
            c.id            AS cesta_id,
            c.loja_id       AS loja_id_cesta,
            c.user_id,
            c.origem,
            ci.produto_id   AS produto_id_cesta,
            ci.ean          AS ean_cesta,
            ci.quantidade   AS quantidade_cesta,
            ci.tipo_venda   AS tipo_venda_cesta
        FROM cesta_de_compras_cesta  c
        JOIN  cesta_de_compras_item ci ON c.id = ci.cesta_id
        WHERE DATE(c.aberta_em) >= '2025-04-14'                    -- primeira segunda após a inclusão da cesta_id na conversa
          AND c.origem = 'mini-maggu'
    """

    cursor.execute(f"SELECT COUNT(*) FROM ({query}) AS sub")
    total_rows = cursor.fetchone()[0]

    cursor.execute(query)

    chunk_size = 10_000
    processed_rows = 0
    dfs = []

    while True:
        data = cursor.fetchmany(chunk_size)
        if not data:
            break

        processed_rows += len(data)
        columns = [d[0] for d in cursor.description]
        pdf = pd.DataFrame(data, columns=columns)
        dfs.append(spark.createDataFrame(pdf))

    df_cesta = (
        reduce(lambda d1, d2: d1.unionByName(d2), dfs)
        if dfs
        else spark.createDataFrame([], schema=None)
    )

except psycopg2.DatabaseError as err:
    print(f"Erro: {err}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC # Carrega Vendas - Postgres

# COMMAND ----------

try:
    conn = psycopg2.connect(
        database=postgres.DATABASE_NAME,
        user=postgres.USER,
        host=postgres.DATABASE_HOST,
        password=postgres.PASSWORD,
        port=postgres.DATABASE_PORT,
    )
    cursor = conn.cursor()

    query = """
        SELECT
            v.realizada_em              AS venda_concluida_em,
            v.id                        AS venda_id,
            v.pre_venda_id              AS cesta_id,
            v.loja_id                   AS loja_id_venda,
            CAST(v.vendedor_id AS text) AS vendedor_id,
            v.status                    AS status_venda,
            vi.id                       AS venda_item_id,
            vi.produto_id               AS produto_id_venda,
            vi.ean                      AS ean_venda,
            vi.quantidade               AS quantidade_venda,
            vi.status                   AS status_item,
            vi.campanha_produto_id
        FROM vendas_venda v
        JOIN vendas_item vi ON v.id = vi.venda_id
        JOIN cesta_de_compras_cesta  c ON v.pre_venda_id  = c.id
        WHERE DATE(v.realizada_em) >= '2025-04-14'              -- -- primeira segunda após a inclusão da cesta_id na conversa
          AND c.origem             = 'mini-maggu'
    """

    cursor.execute(f"SELECT COUNT(*) FROM ({query}) AS sub")
    total_rows = cursor.fetchone()[0]

    cursor.execute(query)

    chunk_size = 10_000
    processed_rows = 0
    dfs = []

    schema = StructType(
        [
            StructField("venda_concluida_em", TimestampType(), True),
            StructField("venda_id", StringType(), True),
            StructField("cesta_id", StringType(), True),
            StructField("loja_id", StringType(), True),
            StructField("vendedor_id", StringType(), True),
            StructField("status_venda", StringType(), True),
            StructField("venda_item_id", StringType(), True),
            StructField("produto_id", StringType(), True),
            StructField("ean", StringType(), True),
            StructField("quantidade", DoubleType(), True),
            StructField("status_item", StringType(), True),
            StructField("campanha_produto_id", StringType(), True),
        ]
    )

    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break

        processed_rows += len(rows)
        pdf = pd.DataFrame(rows, columns=[d[0] for d in cursor.description])

        pdf["vendedor_id"] = (
            pd.to_numeric(pdf["vendedor_id"], errors="coerce")
            .fillna(0)
            .astype("Int64")
            .astype(str)
        )

        dfs.append(spark.createDataFrame(pdf, schema=schema))

    df_venda = (
        reduce(lambda d1, d2: d1.unionByName(d2), dfs)
        if dfs
        else spark.createDataFrame([], schema=schema)
    )

except psycopg2.Error as err:
    print(f"Erro: {err}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC # Criando o df_final consolidado

# COMMAND ----------

df_recomendacao_join = df_recomendacao.join(df_cesta, ["cesta_id"], "outer").join(
    df_venda, ["cesta_id"], "outer"
)

# COMMAND ----------

df_final = (
    df_recomendacao_join.withColumn(
        "semana", F.to_date(F.date_trunc("week", F.to_timestamp("aberta_em")))
    )
    .groupBy(
        "semana", "loja_id_cesta", "user_id", "produto_id_cesta", "tipo_venda_cesta"
    )
    .agg(
        F.countDistinct("cesta_id").alias("qtd_cesta"),
        F.countDistinct("venda_id").alias("qtd_venda"),
        # vendas em que HOUVE algum tipo de recomendação
        F.countDistinct(
            F.when(F.col("produto_tipo_recomendacao").isNotNull(), F.col("venda_id"))
        ).alias("vendas_com_recomendacao"),
        # vendas em que NÃO houve recomendação
        F.countDistinct(
            F.when(F.col("produto_tipo_recomendacao").isNull(), F.col("venda_id"))
        ).alias("vendas_sem_recomendacao"),
        # vendas em que a recomendação foi do tipo IA-Ads
        F.countDistinct(
            F.when(F.col("produto_tipo_recomendacao") == "ia_ads", F.col("venda_id"))
        ).alias("vendas_com_recomendacao_iaads"),
        # vendas em que a recomendação foi do tipo IA-Foco
        F.countDistinct(
            F.when(F.col("produto_tipo_recomendacao") == "ia_foco", F.col("venda_id"))
        ).alias("vendas_com_recomendacao_iafoco"),
        # vendas em que a recomendação foi do tipo IA_Ads e contém EAN de campanha
        F.countDistinct(
            F.when(
                (F.col("produto_tipo_recomendacao") == "ia_ads")
                & F.col("campanha_produto_id").isNotNull(),
                F.col("venda_id"),
            )
        ).alias("vendas_com_recomedacao_e_ean_campanha"),
    )
)

# COMMAND ----------

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando no delta o fluxo de conversão

# COMMAND ----------

df_final.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    delta_conversao_recomendacao
)
