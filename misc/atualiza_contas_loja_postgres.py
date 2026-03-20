# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# TODO: notebook legado... Deveria ser excluido ou atualizado. Idealmente deveria ir para o 3_analytics/to_postgres, mas requer manutencao primeiro

import pandas as pd
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("atualiza_info_lojas")
    .config(conf=config)
    .getOrCreate()
)

# ambiente e cliente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# postgres
USER = dbutils.secrets.get(scope='postgres', key='POSTGRES_USER')
PASSWORD = dbutils.secrets.get(scope='postgres', key='POSTGRES_PASSWORD')
contas_loja_table = "contas_loja"

# COMMAND ----------

postgres = PostgresAdapter(stage, USER, PASSWORD)

# COMMAND ----------

df_contas_loja = postgres.read_table(spark, table=contas_loja_table).drop(
    'cidade', 'estado', 'tamanho_loja'
)

# COMMAND ----------

# MAGIC %md
# MAGIC Selecionando a informação de `tamanho_loja`, `cidade` e `estado` para atualizar a tabela

# COMMAND ----------

df_tamanho_loja = (
    spark.read.table("production.analytics.clusterizacao_lojas")
    .withColumnRenamed('loja_id', 'id')
    .select('id', 'tamanho_loja', 'cidade', 'estado')
)

# COMMAND ----------

df_contas_atualizado = df_contas_loja.join(df_tamanho_loja, how='left', on=['id'])

# COMMAND ----------

# Transforma para pandas para atualizar
df = df_contas_atualizado.toPandas()

# COMMAND ----------


def update_contas_loja(df: pd.DataFrame, do_commit: bool = True):
    """
    If do_commit is False, the transaction will be rolled back and not updated.
    """
    # cria conexão
    conn = postgres.get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    sql = """
            UPDATE contas_loja
                SET cidade       = %s,
                    estado       = %s,
                    tamanho_loja = %s
                WHERE id = %s
    """

    try:
        for idx, row in df.iterrows():  # pylint: disable=unused-variable
            params = (
                row['cidade'] if pd.notna(row['cidade']) else None,
                row['estado'] if pd.notna(row['estado']) else None,
                row['tamanho_loja'] if pd.notna(row['tamanho_loja']) else None,
                row['id'],
            )
            cur.execute(sql, params)

        if do_commit:
            conn.commit()
            print("✅ Transaction committed.")
        else:
            conn.rollback()
            print("🔄 Transaction rolled back (test mode).")

    except Exception as e:
        # on any error, rollback
        conn.rollback()
        print("❌ Error occurred, rolled back. Details:", e)
        raise

    finally:
        cur.close()
        conn.close()


# COMMAND ----------
update_contas_loja(df=df, do_commit=True)
