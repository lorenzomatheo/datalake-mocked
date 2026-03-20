# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %pip install pandas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.enums import DescricaoTipoReceita, TiposReceita
from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# TODO: precisa usar o DatabricksNotebook Environment

# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("adiciona_correcoes_manuais")
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
produtos_em_processamento_path = f"{catalog}.refined._produtos_em_processamento"

# postgres
USER = dbutils.secrets.get(scope='postgres', key='POSTGRES_USER')
PASSWORD = dbutils.secrets.get(scope='postgres', key='POSTGRES_PASSWORD')
postgres_correcao_manual_table = "produtos_produtocorrecaomanual"


# COMMAND ----------

produtos = spark.read.table(produtos_em_processamento_path)

# COMMAND ----------

postgres = PostgresAdapter(stage, USER, PASSWORD)

# COMMAND ----------

correcoes_df = postgres.read_table(spark, table=postgres_correcao_manual_table)
correcoes_df = correcoes_df.withColumn(
    "tipo_receita", F.col("tipo_de_receita_completo")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Padroniza os dados no formato da tabela e aplica correções

# COMMAND ----------


def padroniza_tags(column_name: str):
    return F.when(F.col(column_name).isNull(), None).otherwise(
        F.concat_ws(
            "|",
            F.array_remove(
                F.transform(
                    F.split(F.regexp_replace(F.col(column_name), r"[|]+", "|"), r"\|"),
                    lambda x: F.trim(x),
                ),
                "",
            ),
        )
    )


def padroniza_array(column_name):
    cleaned = F.regexp_replace(
        F.regexp_replace(
            F.regexp_replace(F.col(column_name), r"^\[|\]$", ""),
            r'\\?["]',
            "",
        ),
        r"[{}]",
        "",
    )
    return F.transform(F.split(cleaned, r",\s*"), lambda x: F.trim(F.lower(x))).cast(
        T.ArrayType(T.StringType())
    )


# COMMAND ----------

colunas_para_atualizar = [
    "ean",
    "nome",
    "eh_tarjado",
    "eh_medicamento",
    "eh_otc",
    "tipo_medicamento",
    "nome_comercial",
    "fabricante",
    "marca",
    "descricao",
    "instrucoes_de_uso",
    "indicacao",
    "categorias",  # TODO: precisa atualizar essa planilha pra usar as categorias da nova árvore de categorias
    "tags_substitutos",
    "tags_complementares",
    "tags_atenuam_efeitos",
    "tags_potencializam_uso",
    "imagem_url",
    "tipo_receita",
    "tipo_de_receita_completo",
    "power_phrase",
    "principio_ativo",
    "via_administracao",
]

tag_columns = [
    "tags_substitutos",
    "tags_complementares",
    "tags_atenuam_efeitos",
    "tags_potencializam_uso",
]

array_columns = ["categorias"]

info_para_atualizar = correcoes_df.select(*colunas_para_atualizar)

for tag_col in tag_columns:
    info_para_atualizar = info_para_atualizar.withColumn(
        tag_col, padroniza_tags(tag_col)
    )

for array_col in array_columns:
    info_para_atualizar = info_para_atualizar.withColumn(
        array_col, padroniza_array(array_col)
    )

# Cria mapping de receita completa a partir do Enum DescricaoTipoReceita
receita_enum_mapping = F.expr(
    "CASE "
    + " ".join(
        [
            f"WHEN tipo_receita = '{desc.name}' THEN '{desc.value}'"
            for desc in TiposReceita
        ]
    )
    + " ELSE tipo_receita END"
)

descricao_receita_enum_mapping = F.expr(
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
    "tipo_receita", receita_enum_mapping
).withColumn("tipo_de_receita_completo", descricao_receita_enum_mapping)

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
# MAGIC Salva no S3 e no delta table

# COMMAND ----------

final_df = final_df.dropDuplicates(["id"])

# COMMAND ----------

final_df = final_df.withColumn(
    "gerado_em", F.coalesce("gerado_em", F.current_timestamp())
)

# COMMAND ----------


final_df.write.mode('overwrite').saveAsTable(produtos_em_processamento_path)

# COMMAND ----------

# final_df.filter("tags_potencializam_uso is not null").count()
