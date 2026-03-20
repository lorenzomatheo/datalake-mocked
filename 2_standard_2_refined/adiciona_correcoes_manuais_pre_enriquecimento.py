# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %pip install pandas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.enums import DescricaoTipoReceita
from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("adiciona_correcoes_manuais_pre_enriquecimento")
    .config(conf=config)
    .getOrCreate()
)

# ambiente e cliente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"
s3_bucket_name = f"maggu-datalake-{stage}"

# pastas s3
s3_refined_folder = f"s3://{s3_bucket_name}/3-refined-layer/produtos_enriquecidos"

# tabelas delta
tabela_produtos = produtos = f"{catalog}.standard.produtos_standard"

# postgres
USER = dbutils.secrets.get(scope='postgres', key='POSTGRES_USER')
PASSWORD = dbutils.secrets.get(scope='postgres', key='POSTGRES_PASSWORD')
postgres_correcao_manual_table = "produtos_produtocorrecaomanual"

# COMMAND ----------

produtos = spark.read.table(tabela_produtos)
# COMMAND ----------

postgres = PostgresAdapter(stage, USER, PASSWORD)
# COMMAND ----------

correcoes_df = postgres.read_table(spark, table=postgres_correcao_manual_table)
correcoes_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Padroniza os dados no formato da tabela e aplica correções

# COMMAND ----------

colunas_para_atualizar = [
    "ean",
    "nome",
    "nomes_alternativos",
    "eh_tarjado",
    "eh_medicamento",
    "tipo_medicamento",
    "fabricante",
    "marca",
    "descricao",
    "categorias",
    "imagem_url",
    "tipo_de_receita_completo",
    "eh_controlado",
    "principio_ativo",
]

array_columns = ["categorias"]

info_para_atualizar = correcoes_df.select(*colunas_para_atualizar)

for array_col in array_columns:
    info_para_atualizar = info_para_atualizar.withColumn(
        array_col, F.from_json(array_col, 'array<string>')
    )

# Cria mapping de receita completa a partir do Enum DescricaoTipoReceita
receita_enum_mapping = F.expr(
    "CASE "
    + " ".join(
        [
            f"WHEN tipo_de_receita_completo = '{desc.name}' THEN '{desc.value}'"
            for desc in DescricaoTipoReceita
        ]
    )
    + " ELSE tipo_de_receita_completo END"
)

info_para_atualizar = info_para_atualizar.withColumn(
    "tipo_de_receita_completo", receita_enum_mapping
)

info_para_mergear = info_para_atualizar.select(
    *[F.col(c).alias(f"new_{c}") for c in info_para_atualizar.columns]
)

joined_df = produtos.join(
    info_para_mergear, on=F.col("ean") == F.col("new_ean"), how="left"
)

for column in colunas_para_atualizar:
    if column != "ean":
        joined_df = joined_df.withColumn(
            column,
            F.coalesce(
                F.col(f"new_{column}"), F.col(column)
            ),  # colunas que não forem preenchidas serão mantidas
        )

final_df = joined_df.select(produtos.columns)

# COMMAND ----------

ean_list = [row.ean for row in correcoes_df.select("ean").collect()]
final_df.filter(F.col("ean").isin(ean_list)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Salva delta table

# COMMAND ----------

final_df = final_df.dropDuplicates(["ean"])

# COMMAND ----------

final_df = final_df.withColumn(
    "gerado_em", F.coalesce("gerado_em", F.current_timestamp())
)

# COMMAND ----------


final_df.write.mode('overwrite').saveAsTable(tabela_produtos)
