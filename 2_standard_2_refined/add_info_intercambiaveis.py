# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("clear_cache", "false", ["true", "false"])
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"])

# COMMAND ----------

import json

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from maggulake.enums import TipoMedicamento

# TODO: migrar para DatabricksEnvironmentBuilder e adicionar spark.sql.caseSensitive = true.
spark = SparkSession.builder.appName("Add info intercambiáveis").getOrCreate()
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# Tabelas
produtos_em_processamento_path = f"{catalog}.refined._produtos_em_processamento"

# Não queremos duplicar o guia da farmacia em staging, hardcoded pra prod
guia_equivalentes_folder = (
    "s3://maggu-datalake-prod/1-raw-layer/maggu/guiadafarmacia_intercambialidade/"
)

MAX_LENGTH_REFERENCIAS_E_SIMILARES = 10000  # máximo de caracteres na coluna
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

clear_cache = dbutils.widgets.get("clear_cache")

if clear_cache == "true":
    spark.catalog.clearCache()
    print("Cache limpo")

# COMMAND ----------

# MAGIC %md
# MAGIC Criando tabela de intercambiáveis

# COMMAND ----------

intercambiaveis = spark.read.table(f"{catalog}.refined.intercambiaveis").cache()

print(intercambiaveis.count())
intercambiaveis.display()

# COMMAND ----------

produtos_refined = spark.read.table(produtos_em_processamento_path).cache()

produtos_refined = produtos_refined.drop(
    "medicamentos_referencia", "medicamentos_similares"
)

join_referencia = produtos_refined.join(
    intercambiaveis,
    produtos_refined["ean"] == intercambiaveis["ean_referencia"],
    "left",
)

join_referencia = join_referencia.withColumn(
    "medicamentos_similares",
    F.when(
        join_referencia["ean_referencia"].isNotNull(),
        F.struct(
            F.col("marca_intercambiavel").alias("nome"),
            F.col("tipo_intercambiavel").alias("tipo_medicamento"),
        ),
    ),
)

join_similar = produtos_refined.join(
    intercambiaveis,
    produtos_refined["ean"] == intercambiaveis["ean_intercambiavel"],
    "left",
)

join_similar = join_similar.withColumn(
    "medicamentos_referencia",
    F.when(
        join_similar["ean_intercambiavel"].isNotNull(),
        F.struct(
            F.col("marca_referencia").alias("nome"),
            F.lit(TipoMedicamento.REFERENCIA.value).alias("tipo_medicamento"),
        ),
    ),
)

join_referencia = join_referencia.select(
    produtos_refined["*"], join_referencia["medicamentos_similares"]
)

join_similar = join_similar.select(
    produtos_refined["*"], join_similar["medicamentos_referencia"]
)

union_df = join_referencia.unionByName(join_similar, allowMissingColumns=True)

grouped_df = union_df.groupBy(produtos_refined.columns).agg(
    F.collect_set("medicamentos_referencia").alias("medicamentos_referencia_list"),
    F.collect_set("medicamentos_similares").alias("medicamentos_similares_list"),
)

final_df = (
    grouped_df.withColumn(
        "medicamentos_referencia", F.to_json("medicamentos_referencia_list")
    )
    .withColumn("medicamentos_similares", F.to_json("medicamentos_similares_list"))
    .drop("medicamentos_referencia_list", "medicamentos_similares_list")
)

# COMMAND ----------


# Cria coluna `eh_generico`, deixa nulo onde não temos certeza
final_df = final_df.withColumn(
    'eh_generico',
    F.when(F.col('tipo_medicamento') == TipoMedicamento.GENERICO.value, True)
    .when(F.col('tipo_medicamento') == TipoMedicamento.SIMILAR.value, False)
    .when(F.col('tipo_medicamento') == 'Medicamento Novo', False)
    .when(
        F.col('tipo_medicamento') == TipoMedicamento.SIMILAR_INTERCAMBIAVEL.value, False
    )
    .when(F.col('tipo_medicamento') == TipoMedicamento.REFERENCIA.value, False),
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona info do scraper Guia da Farmácia

# COMMAND ----------

info_guia_farmacia = spark.read.parquet(guia_equivalentes_folder)
info_guia_farmacia = info_guia_farmacia.select(
    [F.col(c).alias("info_" + c) for c in info_guia_farmacia.columns]
)
info_guia_farmacia.display()

# COMMAND ----------

json_schema = T.ArrayType(
    T.StructType(
        [
            T.StructField("nome", T.StringType(), True),
            T.StructField("tipo_medicamento", T.StringType(), True),
        ]
    )
)


# Caso já existam dados na coluna, adiciona mais medicamentos
def merge_arrays(current_array, new_array):
    if current_array is None or FULL_REFRESH:
        return new_array if new_array is not None else []
    if new_array is None:
        return current_array
    return current_array + new_array


merge_arrays_udf = F.udf(merge_arrays, json_schema)

# Pega info dos medicamentos referencia e similares
grouped_info = info_guia_farmacia.groupBy("info_ean").agg(
    F.collect_list(
        F.struct(
            F.when(
                F.col("info_referencia").isNotNull(),
                F.concat_ws(
                    " ",
                    F.col("info_referencia"),
                    F.col("info_laboratorio"),
                    F.col("info_concentracao_forma"),
                ),
            ).alias("referencia"),
            F.when(
                F.col("info_similar_equivalente").isNotNull(),
                F.concat_ws(
                    " ",
                    F.col("info_similar_equivalente"),
                    F.col("info_laboratorio"),
                    F.col("info_concentracao_forma"),
                ),
            ).alias("similar"),
        )
    ).alias("info_list")
)

# Deixa no mesmo formato da base de produtos
grouped_info = grouped_info.withColumn(
    "new_referencia",
    F.array_distinct(
        F.filter(
            F.transform(
                "info_list",
                lambda x: F.struct(
                    x.referencia.alias("nome"),
                    F.lit(TipoMedicamento.REFERENCIA.value).alias("tipo_medicamento"),
                ),
            ),
            lambda x: x.nome.isNotNull(),
        )
    ),
)

grouped_info = grouped_info.withColumn(
    "new_similares",
    F.array_distinct(
        F.filter(
            F.transform(
                "info_list",
                lambda x: F.struct(
                    x.similar.alias("nome"),
                    F.lit(TipoMedicamento.SIMILAR.value).alias("tipo_medicamento"),
                ),
            ),
            lambda x: x.nome.isNotNull(),
        )
    ),
)


final_df = final_df.join(grouped_info, final_df.ean == grouped_info.info_ean, "left")

# COMMAND ----------


# TODO colocar isso em algum utils
@F.udf(T.StringType())
def limit_json_array_by_chars(array_col):
    if array_col is None or len(array_col) == 0:
        return "[]"

    full_json = json.dumps(array_col, ensure_ascii=False)

    if len(full_json) <= MAX_LENGTH_REFERENCIAS_E_SIMILARES:
        return full_json

    # Aplica binary search
    left, right = 1, len(array_col)
    best = 1

    while left <= right:
        mid = (left + right) // 2
        truncated = array_col[:mid]
        json_str = json.dumps(truncated)

        if len(json_str) <= MAX_LENGTH_REFERENCIAS_E_SIMILARES:
            best = mid
            left = mid + 1
        else:
            right = mid - 1

    return json.dumps(array_col[:best])


final_df = final_df.withColumn(
    "medicamentos_referencia_array",
    merge_arrays_udf(
        F.from_json(F.col("medicamentos_referencia"), json_schema),
        F.col("new_referencia"),
    ),
)

final_df = final_df.withColumn(
    "medicamentos_similares_array",
    merge_arrays_udf(
        F.from_json(F.col("medicamentos_similares"), json_schema),
        F.col("new_similares"),
    ),
)

# Aplica limite de caracteres
final_df = final_df.withColumn(
    "medicamentos_referencia",
    limit_json_array_by_chars("medicamentos_referencia_array"),
).withColumn(
    "medicamentos_similares", limit_json_array_by_chars("medicamentos_similares_array")
)

final_df = final_df.drop(
    "medicamentos_referencia_array",
    "medicamentos_similares_array",
    "info_list",
    "new_referencia",
    "new_similares",
    "info_ean",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva resultado

# COMMAND ----------

final_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    produtos_em_processamento_path
)
