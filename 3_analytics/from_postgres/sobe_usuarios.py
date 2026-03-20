# Databricks notebook source
# MAGIC %pip install pandas

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import pandas as pd
import psycopg2
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])

spark = SparkSession.builder.appName("Cesta Compras").config(conf=config).getOrCreate()

stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, USER, PASSWORD)

# COMMAND ----------

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 1000)

df = pd.DataFrame()

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
        id, UPPER(username)
    FROM
        auth_user
"""

    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(data, columns=columns)

    # pra garantir que não vai dar conflito entre colunas
    def rename_duplicates(old_columns):
        seen = {}
        for idx, column in enumerate(old_columns):
            if column in seen:
                seen[column] += 1
                old_columns[idx] = f"{column}_{seen[column]}"
            else:
                seen[column] = 0
        return old_columns

    df.columns = rename_duplicates(list(df.columns))

    display(df)
    conn.commit()
except psycopg2.Error as error:
    print(f"An error occurred: {error}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

# COMMAND ----------

if not df.empty:
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        f"{catalog}.analytics.view_usuarios"
    )
