# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------


from pyspark.context import SparkConf
from pyspark.sql import SparkSession

# import tiktoken

# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("enriquecimento_produtos")
    .config(conf=config)
    .getOrCreate()
)

# ambiente e cliente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"
# tenant = dbutils.widgets.get("tenant")

# pastas s3
s3_extra_info_folder = (
    f"s3://maggu-datalake-{stage}/3-refined-layer/gpt_extract_product_info"
)
s3_tags_folder = f"s3://maggu-datalake-{stage}/3-refined-layer/gpt_tags_produtos"

# info para coluna que vai gerar os embeddings
embedding_encoding = "cl100k_base"
max_tokens = (
    4000  # the maximum for text-embedding-3-large is 8191 but databricks limits to 4096
)

# COMMAND ----------

extra_info_prod = spark.read.parquet(
    "s3://maggu-datalake-prod/3-refined-layer/gpt_extract_product_info"
).cache()
# extra_info_prod = spark.read.parquet(f"s3://maggu-datalake-prod/3-refined-layer/gpt_tags_produtos").cache()

# COMMAND ----------

ids_produtos_prod = spark.read.table('production.utils.ids_produtos')

# COMMAND ----------

extra_info_prod_com_ean = (
    extra_info_prod.alias('e')
    .join(ids_produtos_prod.alias('i'), 'id')
    .select('i.ean', 'e.*')
    .drop('id')
)

display(extra_info_prod_com_ean)

# COMMAND ----------

ids_produtos_staging = spark.read.table('staging.utils.ids_produtos')

# COMMAND ----------

extra_info_staging = (
    extra_info_prod_com_ean.alias('e')
    .join(ids_produtos_staging.alias('i'), 'ean')
    .select('i.id', 'e.*')
    .drop('ean')
)

display(extra_info_staging)

# COMMAND ----------

extra_info_staging.write.mode('overwrite').parquet(s3_extra_info_folder)

# COMMAND ----------


# COMMAND ----------

tags_prod = spark.read.parquet(
    "s3://maggu-datalake-prod/3-refined-layer/gpt_tags_produtos"
).cache()
# extra_info_prod = spark.read.parquet(f"s3://maggu-datalake-prod/3-refined-layer/gpt_tags_produtos").cache()

# COMMAND ----------

tags_prod_com_ean = (
    tags_prod.alias('e')
    .join(ids_produtos_prod.alias('i'), 'id')
    .select('i.ean', 'e.*')
    .drop('id')
)

display(tags_prod_com_ean)

# COMMAND ----------

# tags_prod_com_ean.filter("ean = '94100008981'").show()
# ids_produtos_staging.filter("ean = '94100008981'").show(truncate=False)
# tags_staging.filter("id = '29ae78bd-b826-42cc-9f02-3dc5ba16df46'").show(truncate=False)

# COMMAND ----------

tags_staging = (
    tags_prod_com_ean.alias('e')
    .join(ids_produtos_staging.alias('i'), 'ean')
    .select('i.id', 'e.*')
    .drop('ean')
)

display(tags_staging)

# COMMAND ----------

tags_staging.write.mode('overwrite').parquet(s3_tags_folder)

# COMMAND ----------
