# Databricks notebook source
import uuid

import pandas as pd
from pyspark.sql import functions as F

from maggulake.environment import DatabricksEnvironmentBuilder

env = DatabricksEnvironmentBuilder.build(
    "cria-tabela-conversoes",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "full_refresh": ["false", "true", "false"],
        "environment": "production",
    },
)

spark = env.spark
table_conversao_recomendacao = "hive_metastore.pbi.conversao_recomendacao_por_produto"
DEBUG = dbutils.widgets.get("debug") == "true"
full_refresh = dbutils.widgets.get("full_refresh") == "true"

bigquery_schema = env.get_bigquery_schema()

analysis_start_date = '2025-04-14'  # data de início de dados de cesta mini-maggu
write_mode = "overwrite"
existing_message_ids = set()

if not full_refresh:
    max_date_result = spark.sql(f"""
        SELECT MAX(DATE(recommendation_time)) as max_date
        FROM {table_conversao_recomendacao}
    """).collect()

    if max_date_result and max_date_result[0]['max_date']:
        max_date = max_date_result[0]['max_date']
        # pega dados de 1 dia antes para ter overlap e evitar perda de dados
        analysis_start_date = (max_date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        write_mode = "append"

        # Pega os ids de mensagens já processadas para evitar duplicação
        existing_df = spark.sql(f"""
            SELECT DISTINCT message_id
            FROM {table_conversao_recomendacao}
            WHERE DATE(recommendation_time) >= '{analysis_start_date}'
        """)
        existing_message_ids = {row.message_id for row in existing_df.collect()}

        print(f"Processando dados a partir de {analysis_start_date}")
    else:
        print("Tabela vazia, executando FULL REFRESH")

else:
    print("Executando FULL REFRESH")

# COMMAND ----------

# MAGIC %md
# MAGIC Recomendações mini-maggu
# MAGIC

# COMMAND ----------

print("Buscando recomendações...")

recommendations_df = spark.sql(f"""
SELECT
    m.id AS message_id,
    m.conversa_id,
    c.cesta_id,
    m.criado_em AS recommendation_time,
    m._data AS raw_data
FROM
    {bigquery_schema}.mini_maggu_v1_mensagem m
JOIN
    {bigquery_schema}.mini_maggu_v1_conversa c ON m.conversa_id = c.id
WHERE
    c.criado_em >= '{analysis_start_date}'
    AND c.cesta_id IS NOT NULL
    AND get_json_object(m._data, '$.referente_ao_produto.ean') IS NOT NULL
    AND get_json_object(m._data, '$.produtos') IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM {bigquery_schema}.cesta_de_compras_cesta ce
        WHERE ce.id = c.cesta_id AND DATE(ce.aberta_em) >= '{analysis_start_date}'
        AND ce.origem = 'mini-maggu'
    )
""")
if DEBUG:
    rec_count = recommendations_df.count()
    print(f"Encontradas {rec_count} recomendações")

# COMMAND ----------

# MAGIC %md
# MAGIC Cestas

# COMMAND ----------

print("Buscando cestas...")
cart_items_df = spark.sql(f"""
SELECT
    ce.id AS cesta_id,
    ci.id AS cart_item_id,
    ci.ean,
    ci.quantidade,
    ci.adicionado_em AS added_to_cart_time,
    ce.aberta_em
FROM
    {bigquery_schema}.cesta_de_compras_cesta ce
JOIN
    {bigquery_schema}.cesta_de_compras_item ci ON ce.id = ci.cesta_id
WHERE
    DATE(ce.aberta_em) >= '{analysis_start_date}'
    AND ce.origem = 'mini-maggu'
""")
if DEBUG:
    cart_count = cart_items_df.count()
    print(f"Encontradas {cart_count} cestas")

# COMMAND ----------

# MAGIC %md
# MAGIC Vendas

# COMMAND ----------

print("Buscando vendas...")
sales_df = spark.sql(f"""
SELECT
    v.pre_venda_id AS cesta_id,
    v.id AS venda_id,
    v.status,
    vi.id AS venda_item_id,
    vi.ean,
    vi.quantidade,
    vi.valor_final,
    vi.criado_em AS sale_time
FROM
    {bigquery_schema}.vendas_venda v
JOIN
    {bigquery_schema}.vendas_item vi ON v.id = vi.venda_id
WHERE
    v.status = 'venda-concluida'
    AND EXISTS (
        SELECT 1 FROM {bigquery_schema}.cesta_de_compras_cesta ce
        WHERE ce.id = v.pre_venda_id AND DATE(ce.aberta_em) >= '{analysis_start_date}'
        AND ce.origem = 'mini-maggu'
    )
""")
if DEBUG:
    sales_count = sales_df.count()
    print(f"Encontradas {sales_count} vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC Produtos recomendados

# COMMAND ----------

print("Processando dados de recomendações...")
# Extraindo info de produtos
recommendations_processed = (
    recommendations_df.withColumn(
        "ref_ean", F.get_json_object(F.col("raw_data"), "$.referente_ao_produto.ean")
    )
    .withColumn(
        "ref_nome", F.get_json_object(F.col("raw_data"), "$.referente_ao_produto.nome")
    )
    .withColumn(
        "produtos",
        F.explode(
            F.from_json(
                F.get_json_object(F.col("raw_data"), "$.produtos"),
                """array<struct<
                    ean:string,
                    nome:string,
                    tipo_venda:string,
                    tipo_recomendacao:string
                >>""",
            )
        ),
    )
    .withColumn("rec_ean", F.col("produtos").getItem("ean"))
    .withColumn("rec_nome", F.col("produtos").getItem("nome"))
    .withColumn("rec_tipo_venda", F.col("produtos").getItem("tipo_venda"))
    .withColumn(
        "rec_tipo_recomendacao",
        F.coalesce(
            F.col("produtos").getItem("tipo_recomendacao"),
            F.get_json_object(
                F.col("raw_data"), "$.referente_ao_produto.tipo_recomendacao"
            ),
        ),
    )
    .select(
        F.col("message_id"),
        F.col("conversa_id"),
        F.col("cesta_id"),
        F.col("recommendation_time"),
        F.col("ref_ean"),
        F.col("ref_nome"),
        F.col("rec_tipo_venda"),
        F.col("rec_ean"),
        F.col("rec_nome"),
        F.col("rec_tipo_recomendacao"),
    )
)

recommendations_processed = recommendations_processed.cache()
if DEBUG:
    rec_count = recommendations_processed.count()
    print(f"{rec_count} recomendações processadas")

# COMMAND ----------

# MAGIC %md
# MAGIC Funil de recomendações para cestas

# COMMAND ----------

print("Buscando cestas que contém o produto inicial...")
ref_in_cart = (
    cart_items_df.alias("ci")
    .join(
        recommendations_processed.alias("rp"),
        (F.col("ci.cesta_id") == F.col("rp.cesta_id"))
        & (F.col("ci.ean") == F.col("rp.ref_ean")),
        # & (F.col("ci.added_to_cart_time") > F.col("rp.recommendation_time"))
        "inner",
    )
    .withColumn("is_reference_product_in_cart", F.lit(True))
    .select(
        F.col("rp.message_id").alias("ref_message_id"),
        F.col("rp.conversa_id").alias("ref_conversa_id"),
        F.col("rp.cesta_id").alias("recommendation_cesta_id"),
        F.col("ci.cesta_id").alias("cart_cesta_id"),
        F.col("rp.ref_ean"),
        F.col("rp.ref_nome"),
        F.col("rp.rec_ean"),
        F.col("rp.rec_nome"),
        F.col("rp.rec_tipo_venda"),
        F.col("rp.rec_tipo_recomendacao"),
        F.col("ci.cart_item_id"),
        F.col("ci.added_to_cart_time"),
        F.col("is_reference_product_in_cart"),
    )
)

if DEBUG:
    ref_count = ref_in_cart.count()
    print(f"Encontradas {ref_count} cestas com o produto inicial")

# COMMAND ----------

print("Encontrando cestas com produtos recomendados...")

rec_in_cart = (
    cart_items_df.alias("ci")
    .join(
        recommendations_processed.alias("rp"),
        (F.col("ci.cesta_id") == F.col("rp.cesta_id"))
        & (F.col("ci.ean") == F.col("rp.rec_ean")),
        # & (F.col("ci.added_to_cart_time") > F.col("rp.recommendation_time"))
        "inner",
    )
    .withColumn("is_recommended_product_in_cart", F.lit(True))
    .select(
        F.col("rp.message_id").alias("rec_message_id"),
        F.col("rp.rec_ean"),
        F.col("ci.cart_item_id"),
        F.col("is_recommended_product_in_cart"),
        F.col("ci.quantidade"),
    )
)

if DEBUG:
    rec_count = rec_in_cart.count()
    print(f"Encontradas {rec_count} cestas com o produto recomendado")

# COMMAND ----------

print("Combinando dataframes de cestas...")
ref_cart_df = ref_in_cart.alias("ref")
rec_cart_df = rec_in_cart.alias("rec")

cart_conversions = ref_cart_df.join(
    rec_cart_df,
    (F.col("ref.ref_message_id") == F.col("rec.rec_message_id"))
    & (F.col("ref.rec_ean") == F.col("rec.rec_ean"))
    & (F.col("ref.cart_item_id") == F.col("rec.cart_item_id")),
    "full_outer",
).select(
    F.col("ref.ref_message_id").alias("message_id"),
    F.col("ref.ref_conversa_id").alias("conversa_id"),
    F.col("ref.recommendation_cesta_id"),
    F.col("ref.cart_cesta_id"),
    F.col("ref.ref_ean"),
    F.col("ref.ref_nome"),
    F.col("ref.rec_tipo_venda"),
    F.col("ref.rec_ean"),
    F.col("ref.rec_nome"),
    F.col("ref.rec_tipo_recomendacao"),
    F.col("ref.cart_item_id"),
    F.col("ref.added_to_cart_time"),
    F.col("ref.is_reference_product_in_cart"),
    F.col("rec.is_recommended_product_in_cart"),
    F.col("rec.quantidade"),
)

cart_conversions = cart_conversions.cache()
if DEBUG:
    cart_count = cart_conversions.count()
    print(f"Encontradas {cart_count} cestas com conversões de produtos recomendados")

# COMMAND ----------

# MAGIC %md
# MAGIC Funil de cestas para vendas

# COMMAND ----------

ref_sold = (
    sales_df.alias("sd")
    .join(
        cart_conversions.alias("cc"),
        (F.col("sd.cesta_id") == F.col("cc.cart_cesta_id"))
        & (F.col("sd.ean") == F.col("cc.ref_ean")),
        "inner",
    )
    .withColumn("is_reference_product_sold", F.lit(True))
    .select(
        F.col("cc.message_id").alias("ref_message_id"),
        F.col("cc.conversa_id"),
        F.col("cc.recommendation_cesta_id"),
        F.col("cc.cart_cesta_id"),
        F.col("sd.cesta_id").alias("sale_cesta_id"),
        F.col("cc.ref_ean"),
        F.col("cc.ref_nome"),
        F.col("cc.rec_tipo_venda"),
        F.col("cc.rec_ean"),
        F.col("cc.rec_nome"),
        F.col("cc.rec_tipo_recomendacao"),
        F.col("cc.cart_item_id"),
        F.col("sd.venda_id"),
        F.col("sd.sale_time"),
        F.col("cc.is_reference_product_in_cart"),
        F.col("cc.is_recommended_product_in_cart"),
        F.col("is_reference_product_sold"),
    )
)

if DEBUG:
    ref_sold_count = ref_sold.count()
    print(f"Encontrados {ref_sold_count} produtos iniciais vendidos")

print("Encontrando vendas com produtos recomendados...")
rec_sold = (
    sales_df.alias("sd")
    .join(
        cart_conversions.alias("cc"),
        (F.col("sd.cesta_id") == F.col("cc.cart_cesta_id"))
        & (F.col("sd.ean") == F.col("cc.rec_ean")),
        "inner",
    )
    .withColumn("is_recommended_product_sold", F.lit(True))
    .select(
        F.col("cc.message_id").alias("rec_message_id"),
        F.col("cc.rec_ean"),
        F.col("cc.cart_item_id"),
        F.col("sd.venda_id"),
        F.col("is_recommended_product_sold"),
        F.col("sd.quantidade"),
        F.col("sd.valor_final"),
    )
)

if DEBUG:
    rec_sold_count = rec_sold.count()
    print(f"Encontradas {rec_sold_count} vendas com produtos recomendados")

# COMMAND ----------

print("Combinando as tabelas...")
ref_sold_df = ref_sold.alias("rs")
rec_sold_df = rec_sold.alias("rcs")

sales_conversions = ref_sold_df.join(
    rec_sold_df,
    (F.col("rs.ref_message_id") == F.col("rcs.rec_message_id"))
    & (F.col("rs.rec_ean") == F.col("rcs.rec_ean"))
    & (F.col("rs.cart_item_id") == F.col("rcs.cart_item_id"))
    & (F.col("rs.venda_id") == F.col("rcs.venda_id")),
    "full_outer",
).select(
    F.col("rs.ref_message_id").alias("message_id"),
    F.col("rs.conversa_id"),
    F.col("rs.recommendation_cesta_id"),
    F.col("rs.cart_cesta_id"),
    F.col("rs.sale_cesta_id"),
    F.col("rs.venda_id"),
    F.col("rs.ref_ean"),
    F.col("rs.ref_nome"),
    F.col("rs.rec_tipo_venda"),
    F.col("rs.rec_ean"),
    F.col("rs.rec_nome"),
    F.col("rs.rec_tipo_recomendacao"),
    F.col("rs.cart_item_id"),
    F.col("rs.sale_time"),
    F.col("rs.is_reference_product_in_cart"),
    F.col("rs.is_recommended_product_in_cart"),
    F.col("rs.is_reference_product_sold"),
    F.col("rcs.is_recommended_product_sold"),
    F.col("rcs.quantidade"),
    F.col("rcs.valor_final"),
)

sales_conversions = sales_conversions.cache()
if DEBUG:
    sales_count = sales_conversions.count()
    print(f"Encontradas {sales_count} vendas com algum produto recomendado convertido")

# COMMAND ----------

# MAGIC %md
# MAGIC Tabela completa

# COMMAND ----------

print("Criando tabela final...")

sales_conv_df = sales_conversions.alias("sc")
recommendations_proc_df = recommendations_processed.alias("rp")

part1 = sales_conv_df.join(
    recommendations_proc_df,
    (F.col("sc.message_id") == F.col("rp.message_id"))
    & (F.col("sc.rec_ean") == F.col("rp.rec_ean")),
    "inner",
).select(
    F.col("sc.message_id"),
    F.col("sc.conversa_id"),
    F.col("sc.recommendation_cesta_id"),
    F.col("sc.cart_cesta_id"),
    F.col("sc.sale_cesta_id"),
    F.col("sc.venda_id"),
    F.col("sc.ref_ean"),
    F.col("sc.ref_nome"),
    F.col("sc.rec_tipo_venda"),
    F.col("sc.rec_ean"),
    F.col("sc.rec_nome"),
    F.col("sc.rec_tipo_recomendacao"),
    F.col("sc.cart_item_id"),
    F.col("rp.recommendation_time"),
    F.col("sc.sale_time"),
    F.col("sc.is_reference_product_in_cart"),
    F.col("sc.is_recommended_product_in_cart"),
    F.col("sc.is_reference_product_sold"),
    F.col("sc.is_recommended_product_sold"),
    F.col("sc.quantidade"),
    F.col("sc.valor_final"),
)

part1_df = part1.alias("p1")
cart_conv_df = cart_conversions.alias("cc")

part2 = part1_df.join(
    cart_conv_df,
    (F.col("p1.message_id") == F.col("cc.message_id"))
    & (F.col("p1.rec_ean") == F.col("cc.rec_ean"))
    & (F.col("p1.cart_item_id") == F.col("cc.cart_item_id")),
    "inner",
).select(
    F.col("p1.message_id"),
    F.col("p1.conversa_id"),
    F.col("p1.recommendation_cesta_id"),
    F.col("p1.cart_cesta_id"),
    F.col("p1.sale_cesta_id"),
    F.col("p1.venda_id"),
    F.col("p1.ref_ean"),
    F.col("p1.ref_nome"),
    F.col("p1.rec_tipo_venda"),
    F.col("p1.rec_ean"),
    F.col("p1.rec_nome"),
    F.col("p1.rec_tipo_recomendacao"),
    F.col("p1.cart_item_id"),
    F.col("p1.recommendation_time"),
    F.col("cc.added_to_cart_time"),
    F.col("p1.sale_time"),
    F.col("p1.is_reference_product_in_cart"),
    F.col("p1.is_recommended_product_in_cart"),
    F.col("p1.is_reference_product_sold"),
    F.col("p1.is_recommended_product_sold"),
    F.col("p1.quantidade"),
    F.col("p1.valor_final"),
)

product_recommendation_conversions = part2.withColumn(
    "conversion_status",
    F.when(F.col("is_recommended_product_sold"), "Convertida").otherwise(
        "Não Convertida"
    ),
).withColumn("valor_final", F.col("valor_final").cast("decimal(7,2)"))

product_recommendation_conversions = product_recommendation_conversions.filter(
    F.col("ref_ean") != F.col("rec_ean")
)

# Filtra para evitar duplicação
if existing_message_ids:
    product_recommendation_conversions = product_recommendation_conversions.filter(
        ~F.col("message_id").isin(list(existing_message_ids))
    )
    if DEBUG:
        print(f"{len(existing_message_ids)} ids filtrados pois já existem na tabela")

if DEBUG:
    final_count = product_recommendation_conversions.count()
    print(f"Tabela final criada com {final_count} linhas")

# COMMAND ----------

if not product_recommendation_conversions.take(1):
    dbutils.notebook.exit("Dataframe vazio, parando execução.")

if DEBUG:
    product_recommendation_conversions.limit(10).display()

# COMMAND ----------

print("Salvando tabela...")
product_recommendation_conversions.write.mode(write_mode).saveAsTable(
    table_conversao_recomendacao
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvando conversões no Postgres

# COMMAND ----------

# apontando para o banco de produção para fazer o insert
postgres = env.postgres_adapter

# COMMAND ----------

# Iremos salvar só recomendações convertidas
converted_recommendations = product_recommendation_conversions.filter(
    F.col("conversion_status") == "Convertida"
)

pandas_df = converted_recommendations.select(
    F.col("ref_ean"),
    F.col("rec_ean"),
    F.col("rec_tipo_venda").alias("tipo_venda"),
    F.col("rec_tipo_recomendacao").alias("tipo_recomendacao"),
    F.col("sale_time").alias("data_venda"),
).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Busca ids dos produtos

# COMMAND ----------

eans_list = pandas_df["ref_ean"].tolist() + pandas_df["rec_ean"].tolist()
unique_eans = list(set(eans_list))
eans_format = ", ".join([f"'{ean}'" for ean in unique_eans])

query_product_ids = f"""
SELECT id, ean FROM {bigquery_schema}.produtos_produtov2
WHERE ean IN ({eans_format})
"""

product_mapping_df = spark.sql(query_product_ids).toPandas()
ean_to_id = dict(zip(product_mapping_df["ean"], product_mapping_df["id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Monta a query

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
# MAGIC Subindo para o Copilot

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

    query_valores = ", ".join(valores_inserir)
    batch_insert_query = f"""
    INSERT INTO recomendacoes_recomendacoesconversoes
    (id, produto_buscado_id, produto_recomendado_id, tipo_venda,
     tipo_recomendacao, data_venda, status_conversao)
    SELECT * FROM (VALUES {query_valores}) AS v(
        id, produto_buscado_id, produto_recomendado_id, tipo_venda,
        tipo_recomendacao, data_venda, status_conversao)
    WHERE NOT EXISTS (
        SELECT 1 FROM recomendacoes_recomendacoesconversoes r
        WHERE r.data_venda = v.data_venda
        AND r.produto_buscado_id = v.produto_buscado_id
        AND r.produto_recomendado_id = v.produto_recomendado_id
    )
    """

    postgres.execute_query(batch_insert_query)
    print(f"Batch de {len(batch)} registros concluído")

print("Processo concluído.")

# COMMAND ----------

env.postgres_adapter.read_table(spark, "recomendacoes_recomendacoesconversoes").limit(
    10
).display()
