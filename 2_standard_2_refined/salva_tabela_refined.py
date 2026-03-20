# Databricks notebook source
# MAGIC %pip install tiktoken
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
import tiktoken

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_produtos_refined
from maggulake.utils.pyspark import to_schema
from maggulake.utils.strings import to_list_or_null
from maggulake.utils.text_similarity import filtrar_nomes_por_semelhanca
from maggulake.utils.valores_invalidos import nullifica_valores_invalidos

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "sync_vectorsearch_idx",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark

# ambiente
catalog = env.settings.catalog
s3_bucket_name = env.settings.bucket

# pastas s3
s3_refined_folder = env.full_s3_path("3-refined-layer/produtos_enriquecidos")

# embeddings
ENCODING = "cl100k_base"
MAX_TOKENS = 8000  # the maximum for text-embedding-3-large is 8191

# vectorsearch
DATABRICKS_TOKEN = dbutils.secrets.get(
    scope='databricks', key='DATABRICKS_ACCESS_TOKEN'
)
DATABRICKS_WORKSPACE_URL = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
DATABRICKS_VECTOR_SEARCH_INDEX_NAME = f"{catalog}.refined.produtos_refined_idx"

# COMMAND ----------

# MAGIC %md
# MAGIC Agrupa novos dados na coluna `informacoes_para_embeddings` para gerar os embeddings

# COMMAND ----------

df = env.table(Table.produtos_em_processamento)

# COMMAND ----------


def concatenar_condicional(prefixo, coluna):
    if coluna == "nome":
        valor = F.coalesce(F.col("nome_comercial"), F.col("nome"))
    else:
        valor = F.col(coluna)

    return F.when(valor.isNotNull() & (valor != ""), F.concat(F.lit(prefixo), valor))


df = df.withColumn("nome_concat", concatenar_condicional("NOME: ", "nome"))
df = df.withColumn(
    "principio_ativo_concat",
    concatenar_condicional("PRINCIPIO ATIVO: ", "principio_ativo"),
)
df = df.withColumn(
    "indicacao_concat", concatenar_condicional("DESCRIÇÃO: ", "indicacao")
)
df = df.withColumn(
    "pra_quem_concat", concatenar_condicional("PRA QUEM: ", "sexo_recomendado")
)
df = df.withColumn(
    "pra_qual_idade_concat",
    concatenar_condicional("PRA QUAL IDADE: ", "idade_recomendada"),
)

# Parse categorias_de_complementares_por_indicacao
df = df.withColumn(
    "indicacoes_parsed",
    F.from_json(
        F.col("categorias_de_complementares_por_indicacao"),
        "array<struct<indicacao:string,categorias:array<string>>>",
    ),
)

# Extrai indicacoes
df = df.withColumn(
    "indicacoes_extracted", F.transform("indicacoes_parsed", lambda x: x.indicacao)
)

# Combina categorias e indicacoes
df = df.withColumn(
    "all_categories",
    F.array_union(
        F.coalesce(F.col("categorias"), F.array()), F.col("indicacoes_extracted")
    ),
)

# Processa categories
df = df.withColumn(
    "processed_categories",
    F.array_distinct(F.transform("all_categories", lambda x: F.lower(F.trim(x)))),
)

# Cria INDICAÇÕES
df = df.withColumn(
    "indicacoes_concat",
    F.when(
        F.size("processed_categories") > 0,
        F.concat(F.lit("INDICAÇÕES: "), F.array_join("processed_categories", ", ")),
    ),
)

# Processa tags_substitutos
df = df.withColumn(
    "processed_substitutos",
    F.array_distinct(
        F.transform(F.split("tags_substitutos", "\\|"), lambda x: F.lower(F.trim(x)))
    ),
)

# Cria SUBSTITUTOS
df = df.withColumn(
    "substitutos_concat",
    F.when(
        (F.col("tags_substitutos").isNotNull()) & (F.col("tags_substitutos") != ""),
        F.concat(F.lit("SUBSTITUTOS: "), F.array_join("processed_substitutos", ", ")),
    ),
)


# Passo 1: Remove duplicatas exatas
# Aplica lowercase e compara com o nome principal
df = df.withColumn(
    "nomes_alt_sem_duplicatas",
    F.expr("""
        filter(
            nomes_alternativos,
            x -> lower(trim(x)) != lower(trim(nome))
        )
    """),
)

# Passo 2: Filtra por semelhança semântica (usando função especializada)
# Remove se: idêntico (>95% similar) OU sem semelhança (<20% similar)
# Mantém se: parcialmente similar (20-95%)
filtrar_nomes_udf = F.udf(
    lambda nome, alts: filtrar_nomes_por_semelhanca(
        nome,
        alts,
        threshold_minimo=0.40,  # 40% semelhança mínima
        threshold_maximo=0.95,  # 95% considera duplicata
    ),
    T.ArrayType(T.StringType()),
)

df = df.withColumn(
    "nomes_alternativos_filtrados",
    filtrar_nomes_udf(F.col("nome"), F.col("nomes_alt_sem_duplicatas")),
)

# Cria concatenação de nomes alternativos filtrados
df = df.withColumn(
    "nomes_alternativos_concat",
    F.when(
        (F.col("nomes_alternativos_filtrados").isNotNull())
        & (F.size("nomes_alternativos_filtrados") > 0),
        F.concat(
            F.lit("NOMES ALTERNATIVOS: "),
            F.array_join("nomes_alternativos_filtrados", ", "),
        ),
    ),
)

final_df = df.withColumn(
    "informacoes_para_embeddings",
    F.concat_ws(
        "\n\n",
        "nome_concat",
        "nomes_alternativos_concat",
        "principio_ativo_concat",
        "indicacao_concat",
        "pra_quem_concat",
        "pra_qual_idade_concat",
        "indicacoes_concat",
        "substitutos_concat",
    ),
)

columns_to_drop = [
    "nome_concat",
    "nomes_alternativos_concat",
    "nomes_alt_sem_duplicatas",
    "principio_ativo_concat",
    "indicacao_concat",
    "pra_quem_concat",
    "pra_qual_idade_concat",
    "indicacoes_parsed",
    "indicacoes_extracted",
    "all_categories",
    "processed_categories",
    "indicacoes_concat",
    "processed_substitutos",
    "substitutos_concat",
    "nomes_alternativos_filtrados",
]

final_df = final_df.drop(*columns_to_drop)


# deixa dentro do limite de tokens para não dar problema nos embeddings
@F.udf(T.StringType())
def adjust_text_size_udf(text):
    encoding = tiktoken.get_encoding(ENCODING)
    tokens = encoding.encode(text)
    if len(tokens) > MAX_TOKENS:
        return encoding.decode(tokens[:MAX_TOKENS])
    else:
        return text


final_df = final_df.withColumn(
    "informacoes_para_embeddings",
    adjust_text_size_udf(final_df["informacoes_para_embeddings"]),
)

final_df = final_df.withColumn("nome", F.initcap(F.col("nome"))).withColumn(
    "nome_comercial", F.initcap(F.col("nome_comercial"))
)

final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Salva no delta table

# COMMAND ----------

final_df = final_df.dropDuplicates(["id"])


# COMMAND ----------


_to_list_or_null_udf = F.udf(to_list_or_null, "ARRAY<STRING>")

# COMMAND ----------

# Transformações estruturais
final_df2 = (
    final_df.withColumn(
        "categorias_de_complementares_por_indicacao",
        F.from_json(
            "categorias_de_complementares_por_indicacao",
            "array<struct<indicacao string, categorias array<string>>>",
        ),
    )
    .withColumn("tags_complementares", _to_list_or_null_udf("tags_complementares"))
    .withColumn("tags_substitutos", _to_list_or_null_udf("tags_substitutos"))
    .withColumn(
        "tags_potencializam_uso", _to_list_or_null_udf("tags_potencializam_uso")
    )
    .withColumn("tags_atenuam_efeitos", _to_list_or_null_udf("tags_atenuam_efeitos"))
    .withColumn("tags_agregadas", _to_list_or_null_udf("tags_agregadas"))
    .withColumn('gerado_em', F.coalesce('gerado_em', F.current_timestamp()))
    .withColumn('atualizado_em', F.coalesce('atualizado_em', 'gerado_em'))
)

# Nullifica valores inválidos em todas as colunas STRING e ARRAY<STRING>
final_df2 = nullifica_valores_invalidos(final_df2)

# Transformações de formatação específicas
final_df2 = (
    final_df2.withColumn(
        "eans_alternativos", F.coalesce("eans_alternativos", F.lit([]))
    )
    .withColumn('marca', F.initcap(F.col('marca')))
    .withColumn('fabricante', F.upper(F.col('fabricante')))
    .withColumn('principio_ativo', F.upper(F.col('principio_ativo')))
    .dropna(subset="ean")
).cache()

# COMMAND ----------

# MAGIC %md ### Define atualizado_em se houve mudancas

# COMMAND ----------

# "<=>": null-safe equal operator
# null = null => unknown
# null <=> null => true


def cols_equal(column: str) -> str:
    return f"(s.{column} <=> p.{column})"


def all_cols_equal(columns: list[str]) -> str:
    return " and ".join([cols_equal(col) for col in columns])


# COMMAND ----------

produtos_refined = env.table(Table.produtos_refined).cache()
sem_atualizado_em = final_df2.drop('atualizado_em')

# TODO: gerado_em e mais atualizado do que deveria
colunas_update = [col for col in sem_atualizado_em.columns if col not in ['gerado_em']]

final_df3 = (
    sem_atualizado_em.alias('s')
    .join(produtos_refined.alias('p'), "ean", 'left')
    .select(
        "s.*",
        F.when(F.expr(all_cols_equal(colunas_update)), F.col('p.atualizado_em'))
        .otherwise(F.current_timestamp())
        .alias('atualizado_em'),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score de qualidade por ean

# COMMAND ----------

# Score de qualidade por ean mais recentes
table_latest_name = f"{Table.score_qualidade_produto.value}_latest"
df_score = spark.table(table_latest_name).select('ean', 'quality_score')

# Join via EAN
df_com_score = final_df3.join(df_score, on="ean", how="left").dropDuplicates(['ean'])

# COMMAND ----------

# garante o schema da tabela refined
df_final = to_schema(df_com_score, schema_produtos_refined)

# COMMAND ----------

df_final.write.mode('overwrite').saveAsTable(Table.produtos_refined.value)
