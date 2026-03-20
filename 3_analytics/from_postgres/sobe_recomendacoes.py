# Databricks notebook source
# MAGIC %pip install pandas

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import pandas as pd
import psycopg2
import pyspark.sql.functions as F
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("Analisa Recomendacoes")
    .config(conf=config)
    .getOrCreate()
)

stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")

# COMMAND ----------

# MAGIC %md
# MAGIC Listando tabelas disponíveis

# COMMAND ----------

postgres = PostgresAdapter(stage, USER, PASSWORD)

postgres.list_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC Verificando recomendacoes

# COMMAND ----------

postgres.read_table(spark, "produtos_recomendacoesproduto").display()

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
      r.id AS recomendacao_id,
      r.loja_id,
      c.name AS loja,
      r.produto_id,
      p.ean as produto_ean,
      p.nome AS produto_nome,
      p.fabricante as produto_marca,
      r.genero as triagem_genero,
      r.idade as triagem_idade,
      r.indicacao as triagem_indicacao,
      prod.nome AS produto_recomendado_nome,
      prod.marca AS produto_recomendado_marca,
      prod.ean AS produto_recomendado_ean,
      prod.eh_medicamento AS produto_recomendado_eh_medicamento,
      prod.eh_tarjado AS produto_recomendado_eh_tarjado,
      prod.receita AS produto_recomendado_receita,
      prod.motivo AS produto_recomendado_motivo,
      r.finished_at,
      r.status
    FROM produtos_recomendacoesproduto r
    INNER JOIN produtos_produtov2 p ON r.produto_id = p.id
    LEFT JOIN contas_loja c ON r.loja_id = c.id
    CROSS JOIN LATERAL jsonb_array_elements(r.recomendacoes -> 'produtos') WITH ORDINALITY AS arr(prod, idx)
    CROSS JOIN LATERAL jsonb_to_record(prod) AS prod(
      nome text,
      marca text,
      ean text,
      eh_medicamento text,
      eh_tarjado text,
      receita text,
      motivo text
    )
    WHERE r.status LIKE 'succeeded'
    ORDER BY r.finished_at DESC, idx
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
        f"{catalog}.analytics.view_produtos_recomendados"
    )
else:
    dbutils.notebook.exit("Sem recomendações")

# COMMAND ----------

# Group by both product EAN and recommended product EAN, and count occurrences
recommendation_counts = spark_df.groupBy(
    "produto_ean",
    "produto_nome",
    "produto_marca",
    "produto_recomendado_ean",
    "produto_recomendado_nome",
    "produto_recomendado_marca",
).count()

# Show the result to understand the distribution
recommendation_counts.display()


# COMMAND ----------

recommendation_counts.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.analytics.view_produtos_recomendados_rank"
)

# COMMAND ----------

windowSpec = Window.partitionBy("produto_ean").orderBy(F.col("count").desc())

top_recommendations = (
    recommendation_counts.withColumn("rank", F.rank().over(windowSpec))
    .filter("rank = 1")
    .drop("rank")
)

top_recommendations.display()
