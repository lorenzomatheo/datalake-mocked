# Databricks notebook source
import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Table

env = DatabricksEnvironmentBuilder.build("atualiza_categorias_recomendacoes", dbutils)

# COMMAND ----------

MAX_CATEGORIAS_POR_PRODUTO = 5
MIN_NUM_REGRAS = 5
MIN_DATA_CALCULO = '90 days'

spark = env.spark

bigquery_schema = env.settings.databricks_views_schema
tabela_produtos = Table.produtos_em_processamento

# COMMAND ----------

# MAGIC   %md ### Agrega recomendacoes

# COMMAND ----------

recomendacoes_por_conversoes = spark.read.table(
    f"{bigquery_schema}.recomendacoes_por_conversoes",
).sort(F.col("total_conversoes").desc())

# COMMAND ----------

recomendacoes_por_associacoes = spark.sql(f"""
    WITH associacoes_categorias AS (
        SELECT
            produto_a_id AS produto_buscado_id,
            explode(p.categorias) AS categoria_recomendada,
            pc.confidence,
            pc.support
        FROM analytics.product_association_rules pc
        JOIN refined.produtos_refined p ON pc.produto_b_id = p.id
        WHERE data_calculo >= NOW() - INTERVAL '{MIN_DATA_CALCULO}'
    )
    SELECT
    produto_buscado_id,
    categoria_recomendada,
    AVG(confidence) AS confidence_media,
    AVG(support) AS support_medio,
    COUNT(*) AS num_regras
    FROM associacoes_categorias
    GROUP BY produto_buscado_id, categoria_recomendada
    HAVING num_regras >= {MIN_NUM_REGRAS}
    AND categoria_recomendada is not null
    -- acima do 1o quartil
    AND support_medio >= 0.000120
    ORDER BY confidence_media DESC
""")

# COMMAND ----------

tags = [
    'tags_complementares',
    'tags_potencializam_uso',
    'tags_atenuam_efeitos',
]

queries = [
    f"""
        SELECT
            id AS produto_buscado_id,
            explode(split({t}, '[|]')) AS categoria_recomendada
        FROM {tabela_produtos.value}
    """
    for t in tags
]

recomendacoes_por_tags = spark.sql(" UNION ".join(queries))

# COMMAND ----------

recomendacoes = (
    recomendacoes_por_conversoes.unionByName(
        recomendacoes_por_associacoes, allowMissingColumns=True
    )
    .unionByName(recomendacoes_por_tags, allowMissingColumns=True)
    .dropDuplicates(["produto_buscado_id", "categoria_recomendada"])
    .cache()
)

# COMMAND ----------

# MAGIC %md ### Ordena por associações de categorias

# COMMAND ----------

associacoes_de_categorias = spark.sql(f"""
    WITH associacoes_categorias AS (
        SELECT
            explode(p1.categorias) AS categoria_buscada,
            explode(p2.categorias) AS categoria_recomendada,
            pc.confidence
        FROM analytics.product_association_rules pc
        JOIN refined.produtos_refined p1 ON pc.produto_a_id = p1.id
        JOIN refined.produtos_refined p2 ON pc.produto_b_id = p2.id
        WHERE data_calculo >= NOW() - INTERVAL '{MIN_DATA_CALCULO}'
    )
    SELECT
    categoria_buscada,
    categoria_recomendada,
    AVG(confidence) AS confidence_media,
    COUNT(*) AS num_regras
    FROM associacoes_categorias
    GROUP BY categoria_buscada, categoria_recomendada
    HAVING num_regras >= {MIN_NUM_REGRAS}
    AND categoria_buscada is not null
    AND categoria_recomendada is not null
    AND categoria_buscada != categoria_recomendada
    ORDER BY confidence_media DESC
""").cache()

# COMMAND ----------

produtos = env.table(tabela_produtos)

associacoes_de_categorias_por_produto = (
    produtos.selectExpr(
        "id AS produto_buscado_id",
        "explode(categorias) AS categoria",
    )
    .alias("p")
    .join(
        F.broadcast(associacoes_de_categorias).alias("a"),
        F.col("categoria") == F.col("categoria_buscada"),
    )
    .groupBy("produto_buscado_id", "categoria_recomendada")
    .agg(F.avg("confidence_media").alias("confidence_media"))
).cache()

# COMMAND ----------

somente_boas_recomendacoes = (
    recomendacoes.alias("r")
    .join(
        associacoes_de_categorias_por_produto.alias("a"),
        ["produto_buscado_id", "categoria_recomendada"],
        "left",
    )
    .sort(F.col("a.confidence_media").desc())
    .filter("a.confidence_media is not null or total_conversoes is not null")
    .select("r.produto_buscado_id", "r.categoria_recomendada", "a.confidence_media")
).cache()

# COMMAND ----------


@F.udf("array<string>")
def limita_recomendacoes(recomendacoes: list[str]):
    return recomendacoes[:MAX_CATEGORIAS_POR_PRODUTO]


somente_boas_recomendacoes_agrupadas = (
    somente_boas_recomendacoes.groupBy("produto_buscado_id")
    .agg(F.collect_list("categoria_recomendada").alias("categorias_recomendadas"))
    .select(
        F.col("produto_buscado_id").alias("id"),
        limita_recomendacoes("categorias_recomendadas").alias(
            "categorias_recomendadas"
        ),
    )
).cache()

# COMMAND ----------

# MAGIC %md ### Salva resultados nos produtos

# COMMAND ----------

# TODO: Duvida: A gente deveria usar o id do produto mesmo? Nao seria melhor o EAN?
produtos_novos = (
    produtos.drop("categorias_recomendadas")
    .alias("p")
    .join(somente_boas_recomendacoes_agrupadas.alias("r"), "id", "left")
    .select("p.*", "r.categorias_recomendadas")
)

# COMMAND ----------

produtos_novos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    tabela_produtos.value
)
