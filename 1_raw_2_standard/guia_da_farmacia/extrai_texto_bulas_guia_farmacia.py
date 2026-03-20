# Databricks notebook source
# MAGIC %md
# MAGIC # Extrai texto de bulas do Guia da Farmácia
# MAGIC
# MAGIC Lê dados de bula do Guia da Farmácia (parquet com colunas por seção)
# MAGIC e salva como texto plano concatenado na `bulas_medicamentos`.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Row

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_bulas_medicamentos
from maggulake.utils.iters import batched
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "extrai_texto_bulas_guia_farmacia",
    dbutils,
    widgets={
        "full_refresh": ["false", "true"],
        "batch_size": "500",
    },
)
spark = env.spark
s3_bucket_name = env.settings.bucket

FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
DEBUG = dbutils.widgets.get("debug") == "true"

TARGET_TABLE = Table.bulas_medicamentos
FONTE = "guia_da_farmacia"

guia_farmacia_folder = f"s3://{s3_bucket_name}/1-raw-layer/maggu/guiadafarmacia/"

# COMMAND ----------

COLUNAS_BULA = [
    "composicao",
    "quando_nao_devo_usar_este_medicamento",
    "como_devo_usar_este_medicamento",
    "o_que_devo_saber_antes_de_usar_este_medicamento",
    "para_que_esse_medicamento_e_indicado",
    "dizeres_legais",
    "quais_os_males_que_este_medicamento_pode_me_causar",
    "o_que_fazer_se_alguem_usar_uma_quantidade_maior_do_que_a_indicada_deste_medicamento",
    "como_esse_medicamento_funciona",
    "onde_como_e_por_quanto_tempo_posso_guardar_esse_medicamento",
    "o_que_devo_fazer_quando_eu_me_esquecer_de_usar_este_medicamento",
]

# COMMAND ----------

info_guia_farmacia = spark.read.parquet(guia_farmacia_folder)
info_guia_farmacia = info_guia_farmacia.filter(
    F.col("product_url").isNotNull() & (F.col("product_url") != "")
)

available_columns = set(info_guia_farmacia.columns)
colunas_presentes = [c for c in COLUNAS_BULA if c in available_columns]

if DEBUG:
    print(f"Colunas de bula encontradas: {colunas_presentes}")
    print(f"Total de registros no Guia da Farmácia: {info_guia_farmacia.count()}")

# COMMAND ----------

bula_concat = F.concat_ws(
    "\n\n",
    *[
        F.when(
            F.col(c).isNotNull() & (F.trim(F.col(c)) != ""),
            F.concat(F.lit(f"{c}: "), F.col(c)),
        )
        for c in colunas_presentes
    ],
)

df_bulas_gf = (
    info_guia_farmacia.withColumn("bula_texto", bula_concat)
    .filter(F.col("bula_texto").isNotNull() & (F.trim(F.col("bula_texto")) != ""))
    .select("ean", "bula_texto")
    .dropDuplicates(["ean"])
    .cache()
)

if DEBUG:
    print(f"Total de EANs com bula no Guia da Farmácia: {df_bulas_gf.count()}")
    df_bulas_gf.limit(5).display()

# COMMAND ----------


def get_processed_eans() -> set[str]:
    if not spark.catalog.tableExists(TARGET_TABLE.value) or FULL_REFRESH:
        return set()

    return set(
        row.ean
        for row in spark.table(TARGET_TABLE.value)
        .filter(F.col("fonte") == FONTE)
        .select("ean")
        .collect()
    )


def save_batch(data: list[Row]) -> None:
    if not data:
        return

    df = spark.createDataFrame(data, schema=schema_bulas_medicamentos)
    env.create_table_if_not_exists(TARGET_TABLE, schema_bulas_medicamentos)
    df.write.format("delta").mode("append").saveAsTable(TARGET_TABLE.value)
    print(f"Saved batch of {len(data)} records.")


# COMMAND ----------


def run_extraction() -> None:
    env.create_table_if_not_exists(TARGET_TABLE, schema_bulas_medicamentos)

    processed_eans = get_processed_eans()
    print(f"Found {len(processed_eans)} already processed EANs for fonte={FONTE}.")

    rows_to_process = [
        row for row in df_bulas_gf.collect() if row.ean not in processed_eans
    ]
    total_rows = len(rows_to_process)
    print(f"Found {total_rows} EANs to process.")

    if total_rows == 0:
        print("Nothing to process.")
        return

    current_time = agora_em_sao_paulo()
    total_success = 0

    total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_num, batch in enumerate(batched(rows_to_process, BATCH_SIZE), 1):
        print(f"Processing batch {batch_num}/{total_batches}...")

        results: list[Row] = []
        for row in batch:
            if not row.bula_texto or not row.bula_texto.strip():
                continue

            results.append(
                Row(
                    ean=row.ean,
                    bula_profissional=None,
                    bula_paciente=row.bula_texto,
                    fonte=FONTE,
                    atualizado_em=current_time,
                )
            )

        save_batch(results)
        total_success += len(results)

    print(f"Extraction completed. Total records saved: {total_success}")


# COMMAND ----------

run_extraction()
