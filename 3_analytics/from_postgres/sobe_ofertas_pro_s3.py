# Databricks notebook source
# MAGIC %md
# MAGIC # Sobe Tabela de `Ofertas` para o S3
# MAGIC

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("somente_testes", "true", ["true", "false"])
dbutils.widgets.dropdown("debug", "true", ["false", "true"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from datetime import datetime

import pyspark.sql.functions as F
from pyspark.context import SparkConf
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurações

# COMMAND ----------

# Spark

spark = (
    SparkSession.builder.appName("sobe_ofertas_pro_s3")
    .config(conf=SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")]))
    .getOrCreate()
)

# STAGE

STAGE = dbutils.widgets.get("stage")
SOMENTE_TESTES = dbutils.widgets.get("somente_testes") == "true"
DEBUG = dbutils.widgets.get("debug") == "true"

# s3

TABELA_S3_FOLDER = (
    f"s3://maggu-datalake-{STAGE}/5-sharing-layer/copilot.maggu.ai/ofertas/"
)

# Postgres

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(STAGE, USER, PASSWORD)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler as tabelas de ofertas que estão no postgres

# COMMAND ----------

df_ofertas = postgres.read_table(spark, "v4_oferta")
df_produto_ofertado = postgres.read_table(spark, "v4_produtoofertado")

# COMMAND ----------

# colocar "po" em todas as colunas do dataframe de df_produto_ofertado
for col in df_produto_ofertado.columns:
    df_produto_ofertado = df_produto_ofertado.withColumnRenamed(col, f"po_{col}")


# COMMAND ----------

# Join dataframes

df_postgres = df_ofertas.join(
    df_produto_ofertado,
    on=df_ofertas.id == df_produto_ofertado.po_oferta_id,
    how='right',
)

# COMMAND ----------

if DEBUG:
    df_postgres.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler a tabela de ofertas que está salva no s3.

# COMMAND ----------


try:
    df_s3 = spark.read.parquet(TABELA_S3_FOLDER)
    ultimo_horario_extracao = df_s3.agg(F.max("horario_Extracao")).collect()[0][0]
except PySparkException:
    print("A tabela ainda não existe no S3!")
    # caso a tabela esteja vazia, define arbitrariamente como anos 2000
    ultimo_horario_extracao = datetime(2000, 1, 1)


print("Este foi o horário da última vez que salvamos no s3: ", ultimo_horario_extracao)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tratamento

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remover colunas desnecessárias

# COMMAND ----------

df_postgres = df_postgres.drop(
    "po_atualizado_em",
    "po_criado_em",
    "po_oferta_id",
    "po_motivo",
    "po_produto_substituido_id",
    "api_endpoint",
    "resultado_integracao_desconto_json",
    "cliente_id",
)

# COMMAND ----------

# Olhar no postgres somente aquelas ofertas geradas depois da data de última extração
df_postgres = df_postgres.filter(F.col("criado_em") > ultimo_horario_extracao)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Criar coluna "ano-mes" para fazer a partição no s3

# COMMAND ----------

df_postgres = df_postgres.withColumn("ano_mes", F.date_format("criado_em", "yyyy-MM"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define novo horario de corte pegando a data maxima da coluna "criado_em"

# COMMAND ----------

novo_horario_extracao = df_postgres.agg(F.max("criado_em")).collect()[0][0]

print("O novo horário de corte será: ", novo_horario_extracao)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Definir a coluna de horário de extração dos novos dados

# COMMAND ----------

df_postgres = df_postgres.withColumn(
    "horario_Extracao", F.lit(novo_horario_extracao).cast("timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ordenar por "criado_em" para garantir que os dados estão ordenados

# COMMAND ----------

df_postgres = df_postgres.orderBy("criado_em")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado final

# COMMAND ----------

df_final = df_postgres

if DEBUG:
    count_new = df_final.count()

    print(f"Serão criadas {count_new} linhas no S3")

    df_final.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar as ofertas no s3 no modo append, particionando por `ano_mes`
# MAGIC

# COMMAND ----------

if not SOMENTE_TESTES:
    # NOTE: "Use ZSTD for best balance of compression ratio and speed"
    df_final.write.mode("append").partitionBy("ano_mes").parquet(
        TABELA_S3_FOLDER, compression="zstd"
    )
    print(">>> Dados salvos no S3!")
