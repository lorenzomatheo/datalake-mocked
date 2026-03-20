# Databricks notebook source
# MAGIC %pip install 'markitdown[pdf]'
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from dataclasses import asdict, dataclass
from datetime import datetime

import pyspark.sql.functions as F
from markitdown import FileConversionException, MarkItDown
from pyspark.sql import Row

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_bulas_medicamentos
from maggulake.tables.raw.enum import Raw
from maggulake.utils.iters import batched
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "extrai_texto_bulas_sara",
    dbutils,
    widgets={
        "full_refresh": ["false", "true"],
        "batch_size": "50",
    },
)
spark = env.spark
stage = env.settings.name_short
catalog = env.settings.catalog

FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
DEBUG = dbutils.widgets.get("debug") == "true"

TARGET_TABLE = Table.bulas_medicamentos
FONTE = "scraper_sara"

df_sara = (
    env.table(Raw.sara)
    .spark_df.select("ean", "professional_leaflet_url", "patient_leaflet_url")
    .filter("professional_leaflet_url IS NOT NULL OR patient_leaflet_url IS NOT NULL")
    .cache()
)


@dataclass
class BulaResult:
    ean: str
    bula_profissional: str | None
    bula_paciente: str | None
    fonte: str
    atualizado_em: datetime


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


def extract_pdf_text(url: str | None) -> tuple[str | None, str | None]:
    if not url:
        return None, None

    try:
        md = MarkItDown()
        result = md.convert(url)
        return result.text_content, None
    except (OSError, ValueError, RuntimeError, FileConversionException) as e:
        return None, f"{type(e).__name__}: {e}"


def log_error(
    error_list: list[dict], ean: str, url: str | None, error: str | None
) -> None:
    if error and url:
        error_list.append({"ean": ean, "url": url, "error": error})


def process_bulas(
    rows: list[Row], error_list: list[dict]
) -> tuple[list[BulaResult], int, int]:
    results: list[BulaResult] = []
    current_time = agora_em_sao_paulo()
    batch_success = 0
    batch_fail = 0

    for row in rows:
        print(f"Processing EAN: {row.ean}")

        bula_prof_text, prof_error = extract_pdf_text(row.professional_leaflet_url)
        bula_pac_text, pac_error = extract_pdf_text(row.patient_leaflet_url)

        log_error(error_list, row.ean, row.professional_leaflet_url, prof_error)
        log_error(error_list, row.ean, row.patient_leaflet_url, pac_error)

        has_text = bula_prof_text or bula_pac_text

        if has_text:
            results.append(
                BulaResult(
                    ean=row.ean,
                    bula_profissional=bula_prof_text,
                    bula_paciente=bula_pac_text,
                    fonte="scraper_sara",
                    atualizado_em=current_time,
                )
            )
            batch_success += 1
        elif prof_error or pac_error:
            batch_fail += 1

    return results, batch_success, batch_fail


def save_batch(table: Table, data: list[BulaResult]) -> None:
    if not data:
        return

    df = spark.createDataFrame(
        [asdict(item) for item in data], schema=schema_bulas_medicamentos
    )

    env.create_table_if_not_exists(table, schema_bulas_medicamentos)
    df.write.format("delta").mode("append").saveAsTable(table.value)
    print(f"Saved batch of {len(data)} records.")


def run_extraction() -> None:
    env.create_table_if_not_exists(TARGET_TABLE, schema_bulas_medicamentos)

    processed_eans = get_processed_eans()
    print(f"Found {len(processed_eans)} already processed EANs for fonte={FONTE}.")

    rows_to_process = [
        row for row in df_sara.collect() if row.ean not in processed_eans
    ]
    total_rows = len(rows_to_process)
    print(f"Found {total_rows} EANs to process.")

    total_success = 0
    total_fail = 0
    errors = []

    total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_num, batch in enumerate(batched(rows_to_process, BATCH_SIZE), 1):
        print(f"Processing batch {batch_num}/{total_batches}...")

        batch_data, b_success, b_fail = process_bulas(batch, errors)
        total_success += b_success
        total_fail += b_fail

        save_batch(TARGET_TABLE, batch_data)

    print("\n" + "=" * 50)
    print("EXTRACTION COMPLETED")
    print(f"Total Successful Records: {total_success}")
    print(f"Total Failed Records: {total_fail}")
    print(f"Total Errors Encountered: {len(errors)}")

    if errors:
        print("\n=== ERROR REPORT ===")
        for err in errors:
            print(f"EAN: {err['ean']} | URL: {err['url']} | Error: {err['error']}")
    print("=" * 50)


# COMMAND ----------

run_extraction()
