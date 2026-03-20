# Databricks notebook source
# MAGIC %md
# MAGIC # Atualiza o status dos atendentes baseado na atividade de vendas

# COMMAND ----------

from datetime import datetime, timedelta

from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

dbutils.widgets.dropdown("stage", "prod", ["dev", "prod"])
dbutils.widgets.text("periodo_analise_dias", "35", "Período de análise (dias)")
dbutils.widgets.dropdown("debug", "false", ["true", "false"], "Debug")

stage = dbutils.widgets.get("stage")
periodo_analise_dias = int(dbutils.widgets.get("periodo_analise_dias"))
debug = dbutils.widgets.get("debug") == "true"
catalog = "production" if stage == "prod" else "staging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])
spark = (
    SparkSession.builder.appName("atualiza_status_atendentes")
    .config(conf=config)
    .getOrCreate()
)

if stage == "prod":
    BIGQUERY_SCHEMA = "bigquery.postgres_public"
else:
    BIGQUERY_SCHEMA = "bigquery.postgres_staging_public"

VENDAS_VENDA_TABLE = f"{BIGQUERY_SCHEMA}.vendas_venda"
VENDAS_ITEM_TABLE = f"{BIGQUERY_SCHEMA}.vendas_item"

POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, POSTGRES_USER, POSTGRES_PASSWORD)

data_limite = (datetime.now() - timedelta(days=periodo_analise_dias)).strftime(
    '%Y-%m-%d'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos dados de vendas

# COMMAND ----------

print(f"Analisando vendas a partir de: {data_limite}")

vendedores_com_vendas = spark.sql(f"""
    SELECT DISTINCT v.vendedor_id
    FROM {VENDAS_VENDA_TABLE} v
    INNER JOIN {VENDAS_ITEM_TABLE} vi ON v.id = vi.venda_id
    WHERE v.vendedor_id IS NOT NULL
      AND v.status = 'venda-concluida'
      AND vi.status = 'vendido'
      AND DATE(v.venda_concluida_em) >= '{data_limite}'
""").cache()

if debug:
    print(
        f"Encontrados {vendedores_com_vendas.count()} vendedores "
        f"com vendas nos últimos {periodo_analise_dias} dias"
    )

# COMMAND ----------

if debug:
    vendedores_com_vendas.limit(500).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Busca todos os atendentes do Postgres

# COMMAND ----------

query_atendentes = """
SELECT
    aa.id,
    aa.usuario_django_id,
    aa.username,
    aa.is_active as is_active_atual
FROM
    atendentes_atendente aa
LEFT JOIN
    contas_conta cc
        ON
    cc.id = aa.conta_id
WHERE
    aa.usuario_django_id IS NOT NULL
        AND
    lower(cc.name) NOT IN ('maggu', 'toolspharma')  -- Exclui contas de teste
"""

atendentes_spark_df = postgres.read_query(spark, query_atendentes)

if debug:
    total_atendentes = atendentes_spark_df.count()
    print(f"Total de atendentes encontrados: {total_atendentes}")

# COMMAND ----------

if debug:
    atendentes_spark_df.limit(500).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determina o novo status dos atendentes

# COMMAND ----------

atendentes_com_status = atendentes_spark_df.join(
    vendedores_com_vendas,
    atendentes_spark_df.usuario_django_id == vendedores_com_vendas.vendedor_id,
    "left",
)
atendentes_atualizados = atendentes_com_status.withColumn(
    "is_active_novo",
    F.when(
        (F.col("vendedor_id").isNotNull()) & (~F.col("username").contains("obsoleto")),
        True,
    ).otherwise(False),
).select("id", "usuario_django_id", "username", "is_active_atual", "is_active_novo")

# COMMAND ----------

mudancas_status = atendentes_atualizados.filter(
    F.col("is_active_atual") != F.col("is_active_novo")
)

if debug:
    print("Resumo das mudanças de status:")
    atendentes_atualizados.groupBy("is_active_atual", "is_active_novo").count().orderBy(
        "is_active_atual", "is_active_novo"
    ).display()

# COMMAND ----------

print(f"Atendentes que terão status alterado: {mudancas_status.count()}")
mudancas_status.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos dados para atualização

# COMMAND ----------

dados_atualizacao = atendentes_atualizados.select(
    F.col("id").alias("atendente_id"), F.col("is_active_novo").alias("is_active")
)

if debug:
    print(f"Total de registros para atualização: {dados_atualizacao.count()}")

# COMMAND ----------

if debug:
    dados_atualizacao.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualização no Postgres

# COMMAND ----------

dados_pandas = dados_atualizacao.toPandas()

if dados_pandas.empty:
    dbutils.notebook.exit(
        "Processo finalizado com sucesso - nenhuma atualização necessária"
    )

# COMMAND ----------

atendente_ids = dados_pandas['atendente_id'].tolist()
is_active_values = dados_pandas['is_active'].tolist()

placeholders = ','.join(['%s'] * len(atendente_ids))
case_when_parts = []
params = []

for atendente_id, is_active in zip(atendente_ids, is_active_values):
    case_when_parts.append("WHEN %s THEN %s")
    params.extend([atendente_id, is_active])

params.extend(atendente_ids)

update_query = f"""
    UPDATE atendentes_atendente
    SET
        is_active = CASE id
            {' '.join(case_when_parts)}
            ELSE is_active
        END
    WHERE id IN ({placeholders})
"""

# COMMAND ----------

postgres.execute_query_with_params(update_query, params)
print("✓ Atualização concluída! Registros atualizados com sucesso!")
