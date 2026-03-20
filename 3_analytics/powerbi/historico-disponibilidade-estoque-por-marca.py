# Databricks notebook source

from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder, HiveMetastoreTable

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "historico-disponibilidade-estoque-por-marca",
    dbutils,
)

DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

periodo_analise_dias = 30
data_atual = datetime.now()
data_inicio_analise = data_atual - timedelta(days=periodo_analise_dias)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buscando lojas ativas

# COMMAND ----------

lojas_ativas_df = (
    env.postgres_replica_adapter.get_lojas(
        env.spark,
        ativo=True,
        extra_fields=["c.databricks_tenant_name as tenant", "codigo_loja"],
    )
    .select("id", "codigo_loja", "tenant")
    .cache()
)

lojas_ativas_ids = [str(row.id) for row in lojas_ativas_df.collect()]

if DEBUG:
    print(f"Encontradas {len(lojas_ativas_ids)} lojas ativas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtendo produtos em campanhas ativas

# COMMAND ----------

df_campanha = env.postgres_replica_adapter.get_produtos_em_campanha(env.spark)

# COMMAND ----------

produtos_campanha_df = df_campanha.toPandas()

eans_campanha: list[str] = list(produtos_campanha_df["ean"].unique())

eans_campanha_spark = df_campanha.select("ean").distinct()

if DEBUG:
    print(f"Encontrados {eans_campanha_spark.count()} EANs em campanha")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buscando dados de estoque via BigQuery (alivia PostgreSQL)

# COMMAND ----------

stock_df = env.bigquery_adapter.get_estoque_por_eans(eans=eans_campanha)

if DEBUG:
    print(
        f"Encontrados {stock_df.count()} registros de estoque para produtos em campanha"
    )
    stock_df.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buscando dados de produtos via BigQuery (alivia PostgreSQL)

# COMMAND ----------

products_df = env.bigquery_adapter.get_produtos_por_eans(
    eans=eans_campanha,
    colunas=["id as produto_id", "ean", "nome", "marca"],
)

# Filtrar apenas produtos com marca
products_df = products_df.filter(F.col("marca").isNotNull())

if DEBUG:
    print(f"Encontrados {products_df.count()} produtos com marca em campanha")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buscando dados de vendas via BigQuery (alivia PostgreSQL)

# COMMAND ----------

sales_df = env.bigquery_adapter.get_vendas_por_periodo(
    data_inicio=data_inicio_analise,
    loja_ids=lojas_ativas_ids,
    eans=eans_campanha,
    incluir_tenant=True,
)

if DEBUG:
    print(
        f"Encontradas {sales_df.count()} itens em campanha vendidos no período de {periodo_analise_dias} dias"
    )
    sales_df.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento dos dados

# COMMAND ----------

# Garantindo que estamos usando o estoque mais recente para cada produto e loja
windowSpec = Window.partitionBy("ean", "tenant", "codigo_loja").orderBy(
    F.desc("atualizado_em")
)
latest_stock_raw = (
    stock_df.withColumn("row_num", F.row_number().over(windowSpec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Filtrando estoque apenas para lojas ativas
latest_stock = latest_stock_raw.join(
    lojas_ativas_df,
    (latest_stock_raw.tenant == lojas_ativas_df.tenant)
    & (latest_stock_raw.codigo_loja == lojas_ativas_df.codigo_loja),
    "inner",
).select(latest_stock_raw["*"])

print("Filtro de estoque concluído")
if DEBUG:
    latest_stock.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculando média de vendas diárias por produto e loja

# COMMAND ----------

# Agrupando vendas por loja e EAN
sales_agg = (
    sales_df.groupBy("loja_id", "tenant", "codigo_loja", "ean")
    .agg(F.sum("quantidade").alias("quantidade_total_vendida"))
    .withColumn(
        "qtd_media_vendida_por_dia_ultimos_30d",
        F.col("quantidade_total_vendida") / periodo_analise_dias,
    )
)

if DEBUG:
    sales_agg.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Juntando dados de estoque, produtos e vendas

# COMMAND ----------

# Juntando estoque com dados de produtos
stock_with_products = latest_stock.join(
    products_df,
    latest_stock.ean == products_df.ean,
    "inner",  # Apenas produtos com marca
).select(
    F.current_timestamp().alias("created_at"),
    latest_stock.tenant,
    latest_stock.codigo_loja,
    latest_stock.ean,
    latest_stock.estoque_atual.cast("double").alias("estoque_atual"),
    products_df.marca,
    products_df.produto_id,
)

# COMMAND ----------

# Adicionando aliases para evitar ambiguidade de colunas
stock_products_alias = stock_with_products.alias("sp")
sales_agg_alias = sales_agg.alias("sa")
lojas_ativas_alias = lojas_ativas_df.alias("la")

stock_sales_combined = (
    stock_products_alias.join(
        sales_agg_alias,
        (F.col("sp.tenant") == F.col("sa.tenant"))
        & (F.col("sp.codigo_loja") == F.col("sa.codigo_loja"))
        & (F.col("sp.ean") == F.col("sa.ean")),
        "left",
    )
    .join(
        lojas_ativas_alias,
        (F.col("sp.tenant") == F.col("la.tenant"))
        & (F.col("sp.codigo_loja") == F.col("la.codigo_loja")),
        "left",
    )
    .select(
        F.col("sp.created_at"),
        F.col("la.id").alias("loja_id"),
        F.col("sp.tenant"),
        F.col("sp.codigo_loja"),
        F.col("sp.ean"),
        F.col("sp.produto_id"),
        F.col("sp.estoque_atual"),
        F.coalesce(F.col("sa.qtd_media_vendida_por_dia_ultimos_30d"), F.lit(0.0)).alias(
            "qtd_media_vendida_por_dia_ultimos_30d"
        ),
        F.col("sp.marca"),
    )
)

# COMMAND ----------

# Calculando cobertura de estoque em dias
cobertura_df = stock_sales_combined.withColumn(
    "cobertura_dias_estoque",
    F.when(
        (F.col("qtd_media_vendida_por_dia_ultimos_30d") > 0),
        F.col("estoque_atual") / F.col("qtd_media_vendida_por_dia_ultimos_30d"),
    )
    .when(
        (F.col("estoque_atual") > 0)
        & (F.col("qtd_media_vendida_por_dia_ultimos_30d") == 0),
        F.lit(None),  # NULL para representar cobertura infinita
    )
    .otherwise(0.0),  # Se o estoque é zero
).withColumn(
    "categoria_disponibilidade_ean",
    F.when(F.col("cobertura_dias_estoque").isNull(), "Sem vendas no período")
    .when(F.col("cobertura_dias_estoque") > 5, "Alta")
    .otherwise("Baixa"),
)

if DEBUG:
    cobertura_df.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculando disponibilidade por marca e loja

# COMMAND ----------

# Agrupando por marca e loja para calcular estoque total e vendas médias
brand_store_metrics = (
    stock_sales_combined.groupBy("marca", "loja_id")
    .agg(
        F.sum("estoque_atual").alias("estoque_total_marca"),
        F.sum("qtd_media_vendida_por_dia_ultimos_30d").alias(
            "qtd_media_vendida_por_dia_marca"
        ),
    )
    .withColumn(
        "cobertura_dias_estoque_marca",
        F.when(
            (F.col("qtd_media_vendida_por_dia_marca") > 0),
            F.col("estoque_total_marca") / F.col("qtd_media_vendida_por_dia_marca"),
        )
        .when(
            (F.col("estoque_total_marca") > 0)
            & (F.col("qtd_media_vendida_por_dia_marca") == 0),
            F.lit(None),  # NULL para representar cobertura infinita
        )
        .otherwise(0.0),  # Se o estoque é zero
    )
    .withColumn(
        "categoria_disponibilidade_marca",
        F.when(F.col("cobertura_dias_estoque_marca").isNull(), "Sem vendas no período")
        .when(F.col("cobertura_dias_estoque_marca") > 5, "Alta")
        .otherwise("Baixa"),
    )
)

# Juntando de volta ao resultado principal
cobertura_marca_df = cobertura_df.join(
    brand_store_metrics.select(
        "marca",
        "loja_id",
        "estoque_total_marca",
        "qtd_media_vendida_por_dia_marca",
        "cobertura_dias_estoque_marca",
        "categoria_disponibilidade_marca",
    ),
    ["marca", "loja_id"],
    "left",
).select(
    F.col("created_at"),
    F.col("loja_id"),
    F.col("ean"),
    F.col("produto_id"),
    F.col("estoque_atual"),
    F.col("qtd_media_vendida_por_dia_ultimos_30d"),
    F.col("cobertura_dias_estoque"),
    F.col("categoria_disponibilidade_ean"),
    F.col("marca"),
    F.col("estoque_total_marca"),
    F.col("qtd_media_vendida_por_dia_marca"),
    F.col("cobertura_dias_estoque_marca"),
    F.col("categoria_disponibilidade_marca"),
)

if DEBUG:
    cobertura_marca_df.orderBy(["loja_id", "marca"]).limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando a tabela final

# COMMAND ----------

cobertura_marca_df.write.mode("append").saveAsTable(
    HiveMetastoreTable.disponibilidade_estoque_por_marca.value
)
