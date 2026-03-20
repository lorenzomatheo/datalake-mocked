# Databricks notebook source
# MAGIC %md
# MAGIC # Extrai texto de bulas do Consulta Remédios
# MAGIC
# MAGIC Lê bulas no formato `ARRAY<STRUCT<id, title, content>>` da `produtos_raw`
# MAGIC (fonte CR) e salva como texto plano na `bulas_medicamentos`.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Row

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.produtos.bula import get_text_from_bula
from maggulake.schemas import schema_bulas_medicamentos
from maggulake.utils.iters import batched
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "extrai_texto_bulas_cr",
    dbutils,
    widgets={
        "full_refresh": ["false", "true"],
        "batch_size": "500",
    },
)
spark = env.spark

FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
DEBUG = dbutils.widgets.get("debug") == "true"

TARGET_TABLE = Table.bulas_medicamentos
FONTE = "consulta_remedios"

# COMMAND ----------

df_cr_bulas = (
    spark.table(Table.produtos_raw.value)
    .filter(F.col("bula").isNotNull())
    .filter(F.size(F.col("bula")) > 0)
    .select("ean", "bula")
    .dropDuplicates(["ean"])
    .cache()
)

if DEBUG:
    print(f"Total de EANs com bula na raw (fonte CR): {df_cr_bulas.count()}")

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
        row for row in df_cr_bulas.collect() if row.ean not in processed_eans
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
            bula_text = get_text_from_bula(row.bula)
            if not bula_text:
                continue

            results.append(
                Row(
                    ean=row.ean,
                    bula_profissional=None,
                    bula_paciente=bula_text,
                    fonte=FONTE,
                    atualizado_em=current_time,
                )
            )

        save_batch(results)
        total_success += len(results)

    print(f"Extraction completed. Total records saved: {total_success}")


# COMMAND ----------

run_extraction()
