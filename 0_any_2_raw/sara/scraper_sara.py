# Databricks notebook source
# MAGIC %pip install requests beautifulsoup4 ftfy
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from typing import Any

import requests
from delta import DeltaTable
from pyspark.sql import Row
from utils_sara import (
    create_session,
    extract_product_links,
    get_total_pages,
    parse_product_data,
)

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_eans_processados_sara
from maggulake.tables.base_table import BaseTable
from maggulake.tables.raw.enum import Raw
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "scraper_sara",
    dbutils,
    widgets={
        "max_products": "10000",
        "full_refresh": ["false", "true"],
    },
)
spark = env.spark
stage = env.settings.name_short
catalog = env.settings.catalog

BATCH_SIZE = 100
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
DEBUG = dbutils.widgets.get("debug") == "true"

PROCESSED_EANS_TABLE = Table.eans_processados_sara


def register_processed_eans(eans_list: list[str]) -> None:
    if not eans_list:
        return

    current_time = agora_em_sao_paulo()
    rows = [Row(ean=ean, scraped_at=current_time) for ean in eans_list]
    source_df = spark.createDataFrame(rows, schema_eans_processados_sara)

    delta_table = DeltaTable.forName(spark, PROCESSED_EANS_TABLE.value)

    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), "target.ean = source.ean")
        .whenMatchedUpdate(set={"scraped_at": "source.scraped_at"})
        .whenNotMatchedInsert(
            values={"ean": "source.ean", "scraped_at": "source.scraped_at"}
        )
        .execute()
    )


def get_processed_eans() -> set[str]:
    if not spark.catalog.tableExists(PROCESSED_EANS_TABLE.value) or FULL_REFRESH:
        return set()

    return set(
        row.ean for row in env.table(PROCESSED_EANS_TABLE).select("ean").collect()
    )


# COMMAND ----------


class ScraperState:
    def __init__(self, processed_eans: set[str]):
        self.processed_eans = processed_eans
        self.products_buffer: list[dict[str, Any]] = []
        self.products_scraped_count = 0
        self.skipped_count = 0
        self.failed_count = 0


def _should_skip_url(url: str, processed_eans: set[str]) -> bool:
    try:
        ean_candidate = url.split("-")[-1]
        return ean_candidate in processed_eans
    except (IndexError, AttributeError) as e:
        print(f"Error extracting EAN from URL {url}: {e}")
        return False


def _process_product(
    session: requests.Session,
    url: str,
    state: ScraperState,
) -> bool:
    product_data = parse_product_data(session, url)
    if not product_data:
        state.failed_count += 1
        return False

    if product_data["ean"] in state.processed_eans:
        state.skipped_count += 1
        return False

    if DEBUG:
        name = (
            product_data["product_name"][:50] if product_data["product_name"] else "N/A"
        )
        print(f"[DEBUG] Scraped: {product_data['ean']} - {name}...")

    state.products_buffer.append(product_data)
    state.processed_eans.add(product_data["ean"])
    state.products_scraped_count += 1
    return True


def _flush_buffer_if_needed(
    state: ScraperState, target_table: BaseTable, force: bool = False
) -> None:
    if not state.products_buffer:
        return
    if force or len(state.products_buffer) >= BATCH_SIZE:
        save_batch(target_table, state.products_buffer)
        if DEBUG:
            print(
                f"[DEBUG] Progress: {state.products_scraped_count}/{MAX_PRODUCTS} "
                f"(skipped: {state.skipped_count}, failed: {state.failed_count})"
            )
        state.products_buffer = []


def _print_debug_summary(state: ScraperState, target_table: BaseTable) -> None:
    if not DEBUG:
        return
    print("\n[DEBUG] === FINAL SUMMARY ===")
    print(f"[DEBUG] Total scraped: {state.products_scraped_count}")
    print(f"[DEBUG] Total skipped (already processed): {state.skipped_count}")
    print(f"[DEBUG] Total failed: {state.failed_count}")
    print("\n[DEBUG] === LAST 100 PRODUCTS FROM TABLE ===")
    last_100_df = (
        spark.read.table(target_table.table_name)
        .orderBy("scraped_at", ascending=False)
        .limit(100)
    )
    display(last_100_df)


def _initialize_scraper() -> tuple[requests.Session, int, set[str]] | None:
    env.create_table_if_not_exists(PROCESSED_EANS_TABLE, schema_eans_processados_sara)
    processed_eans = get_processed_eans()

    if DEBUG:
        print(f"[DEBUG] Already processed EANs count: {len(processed_eans)}")

    print("Initializing session with Imperva CDN...")
    session = create_session()
    print("Session initialized successfully.")

    total_pages = get_total_pages(session)
    print(f"Total pages to scrape: {total_pages}")

    if total_pages == 0:
        print("No pages found to scrape. Exiting.")
        return None

    first_page_urls = extract_product_links(session, 1)
    if DEBUG:
        unprocessed = [
            u for u in first_page_urls if u.split("-")[-1] not in processed_eans
        ]
        print(f"[DEBUG] First page URLs found: {len(first_page_urls)}")
        print(f"[DEBUG] Unprocessed on first page: {len(unprocessed)}")

    if not first_page_urls:
        print("No product URLs found. Exiting.")
        return None

    unprocessed_first_page = [
        url for url in first_page_urls if url.split("-")[-1] not in processed_eans
    ]
    if not unprocessed_first_page and total_pages == 1:
        print("All EANs already processed. Exiting.")
        return None

    return session, total_pages, processed_eans


def run_scraper() -> None:
    init_result = _initialize_scraper()
    if init_result is None:
        return

    session, total_pages, processed_eans = init_result
    target_table = env.table(Raw.sara)
    state = ScraperState(processed_eans)

    for page in range(1, total_pages + 1):
        if state.products_scraped_count >= MAX_PRODUCTS:
            break

        print(f"Scraping page {page}/{total_pages}...")
        product_urls = extract_product_links(session, page)

        if DEBUG:
            print(f"[DEBUG] Page {page}: Found {len(product_urls)} product URLs")

        for url in product_urls:
            if state.products_scraped_count >= MAX_PRODUCTS:
                break
            if _should_skip_url(url, state.processed_eans):
                state.skipped_count += 1
                continue
            _process_product(session, url, state)
            _flush_buffer_if_needed(state, target_table)

    _flush_buffer_if_needed(state, target_table, force=True)
    print(f"Finished. Total products scraped: {state.products_scraped_count}")
    _print_debug_summary(state, target_table)


def save_batch(table_obj: BaseTable, data: list[dict[str, Any]]) -> None:
    if not data:
        return

    df = spark.createDataFrame(data, schema=table_obj.schema)
    table_obj.write(df, mode="append")

    eans = [d["ean"] for d in data]
    register_processed_eans(eans)
    print(f"Saved batch of {len(data)} products.")


# COMMAND ----------

run_scraper()
