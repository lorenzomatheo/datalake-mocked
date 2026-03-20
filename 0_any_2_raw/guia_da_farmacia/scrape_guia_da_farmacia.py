# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])
dbutils.widgets.text("max_products", "10000")
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"])

# COMMAND ----------

# MAGIC %pip install requests beautifulsoup4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import random
import re
import time
import unicodedata
from datetime import datetime, timedelta
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

# TODO: migrar para DatabricksEnvironmentBuilder e adicionar spark.sql.caseSensitive = true.
spark = SparkSession.builder.appName("scrape_guia_da_farmacia").getOrCreate()

countAccumulator = spark.sparkContext.accumulator(0)
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

BATCH_SIZE = 100
RATE_LIMIT = 0.2  # Tempo mínimo de espera entre requisições (em segundos)
MAX_RETRIES = 2
SCRAPING_TTL = datetime.now() - timedelta(days=90)
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))

produtos_table = f"{catalog}.refined.produtos_refined"
tabela_eans_processados = f"{catalog}.utils.eans_processados_guia_da_farmacia_bula"
guia_farmacia_folder = f"s3://maggu-datalake-{stage}/1-raw-layer/maggu/guiadafarmacia/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê a tabela de produtos e filtra por medicamentos

# COMMAND ----------

produtos_df = spark.read.table(produtos_table)
produtos_df = produtos_df.filter(F.col("eh_medicamento")).filter(F.col("bula").isNull())
print(f"Total de produtos que são medicamentos e estão sem bula: {produtos_df.count()}")
produtos_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria funções auxiliares

# COMMAND ----------


def strip_html(text: str) -> str:
    return BeautifulSoup(text, "html.parser").get_text()


def clean_column_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("ASCII")
    name = name.lower().replace(" ", "_")
    name = re.sub(r"[^a-z_]", "", name)
    name = name.lstrip("_")
    if not name:
        name = "column"
    if name[0].isdigit():
        name = "col" + name
    return name


# COMMAND ----------

# MAGIC %md
# MAGIC Função para criar tabela de controle e salvar eans já processados

# COMMAND ----------


def cria_tabela_eans_processados_se_nao_existir() -> None:
    if not spark.catalog.tableExists(tabela_eans_processados):
        spark.sql(f"""
            CREATE TABLE {tabela_eans_processados} (
                ean STRING NOT NULL,
                data_scraping TIMESTAMP NOT NULL
            )
            USING DELTA
        """)
        print(f"Tabela {tabela_eans_processados} criada com sucesso.")


def registra_eans_processados(eans_list: List[str]) -> None:
    current_time = datetime.now()

    rows = [Row(ean=ean, data_scraping=current_time) for ean in eans_list]

    schema = T.StructType(
        [
            T.StructField("ean", T.StringType(), False),
            T.StructField("data_scraping", T.TimestampType(), False),
        ]
    )

    source_df = spark.createDataFrame(rows, schema)

    source_df.createOrReplaceTempView("source_updates")

    merge_query = f"""
        MERGE INTO {tabela_eans_processados} target
        USING source_updates source
        ON target.ean = source.ean
        WHEN MATCHED THEN
            UPDATE SET data_scraping = source.data_scraping
        WHEN NOT MATCHED THEN
            INSERT (ean, data_scraping)
            VALUES (source.ean, source.data_scraping)
    """

    try:
        spark.sql(merge_query)
    except Exception as e:
        raise RuntimeError(f"Erro ao executar o merge, volte e corrija: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria a função de scrape usando a API que eu encontrei no HTML interno deles

# COMMAND ----------


def scrape_ean(ean: str) -> Tuple[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }
    search_api_url = (
        "https://guiadafarmaciadigital.com.br/wp-json/guiadigital/v1/busca-produto"
    )
    product_api_url = "https://guiadafarmaciadigital.com.br/wp-json/guiadigital/v1/produto-relacionado"
    product_base_url = (
        "https://guiadafarmacia.com.br/consulta-medicamentos/medicamento/?item="
    )
    search_params = {
        "qtdeitens": 1,
        "pagina": 1,
        "estado": "SP",  # setei por padrão
        "termo": ean,
    }
    product_params = {
        "qtdeitens": 1,
        "pagina": 1,
        "estado": "SP",  # setei por padrão
    }

    for attempt in range(MAX_RETRIES):
        try:
            # Adiciona delay randomico para evitar bloqueio
            time.sleep(RATE_LIMIT + random.uniform(0, 1))

            # busca a url do produto
            search_response = requests.get(
                search_api_url, headers=headers, params=search_params
            )
            search_data = search_response.json()

            if "data" not in search_data or len(search_data["data"]) == 0:
                return ("", json.dumps({}))

            # como estamos buscando por ean, eu pego o primeiro
            product = search_data["data"][0]
            id_apresentacao = product["id_apresentacao"]
            product_params["id_apresentacao"] = id_apresentacao

            # acessa a url e pega as informações do produto
            product_response = requests.get(
                product_api_url, headers=headers, params=product_params
            )
            product_data = product_response.json()

            if "data" not in product_data or not product_data["data"]:
                return ("", json.dumps({}))

            detailed_product = product_data["data"][0]
            bula_info = detailed_product.get("bula", {})

            bula_items = {}
            for item in bula_info.get("bula_itens", []):
                key = clean_column_name(item["titulo"])
                value = strip_html(item["texto"])
                bula_items[key] = value

            product_url = f"{product_base_url}{id_apresentacao}"

            return (product_url, json.dumps(bula_items))

        except (requests.exceptions.RequestException, KeyError) as e:
            if attempt < MAX_RETRIES - 1:
                print(f"Erro ao buscar o EAN {ean}: {str(e)}. Retentando...")
                time.sleep(2**attempt)  # Estratégia de exponential backoff
            else:
                print(
                    f"Erro ao buscar o EAN {ean}: {str(e)}. Máximo de retentativas alcançado."
                )
                return ("", json.dumps({}))

    # Não deve chegar aqui, adicionando para evitar erro do lint
    return ("", json.dumps({}))


# Schema de retorno
result_schema = T.StructType(
    [
        T.StructField("product_url", T.StringType(), True),
        T.StructField("bula_info", T.StringType(), True),
    ]
)

# Cria a UDF
scrape_ean_udf = F.udf(scrape_ean, result_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria a função para processar os eans

# COMMAND ----------


def filtra_a_fazer(produtos: DataFrame, feitos: DataFrame) -> DataFrame:
    return (
        produtos.alias("p")
        .join(
            feitos,
            produtos.ean == feitos.ean,
            "anti",
        )
        .selectExpr("p.*")
    )


def processa_eans(produtos_df: DataFrame) -> None:
    global countAccumulator

    cria_tabela_eans_processados_se_nao_existir()

    if FULL_REFRESH:
        produtos_pendentes = produtos_df.collect()
    else:
        produtos_feitos = spark.read.table(tabela_eans_processados).filter(
            F.col("data_scraping") >= F.lit(SCRAPING_TTL)
        )
        produtos_pendentes = (
            filtra_a_fazer(produtos_df, produtos_feitos).limit(MAX_PRODUCTS).collect()
        )
    print("Quantidade de produtos a serem enriquecidos: ", len(produtos_pendentes))

    results = []
    for i, row in enumerate(produtos_pendentes):
        ean = row["ean"]
        result = scrape_ean(ean)
        results.append((ean,) + result)
        countAccumulator.add(1)
        print(
            agora_em_sao_paulo_str()
            + f" -> {countAccumulator.value} produtos já processados, de um total de {len(produtos_pendentes)}"
        )

        # salva o batch de eans para não perder informação caso dê algum erro
        if (i + 1) % BATCH_SIZE == 0:
            salva_resultados(results, guia_farmacia_folder)
            results = []

    if results:
        salva_resultados(results, guia_farmacia_folder)


def salva_resultados(results: List[Tuple[str, str, str]], folder: str) -> None:
    schema = T.StructType(
        [
            T.StructField("ean", T.StringType(), True),
            T.StructField("product_url", T.StringType(), True),
            T.StructField("bula_info", T.StringType(), True),
        ]
    )

    df_results = spark.createDataFrame(results, schema=schema)
    df_results = df_results.withColumn(
        "bula_map", F.from_json("bula_info", T.MapType(T.StringType(), T.StringType()))
    )

    keys_df = df_results.select(F.explode(F.map_keys("bula_map"))).distinct()
    bula_keys = [row[0] for row in keys_df.collect()]

    for key in bula_keys:
        clean_key = clean_column_name(key)
        df_results = df_results.withColumn(clean_key, F.col(f"bula_map.`{key}`"))

    df_results = df_results.drop("bula_info", "bula_map")
    df_results = df_results.withColumn("gerado_em", F.current_timestamp())

    df_results.write.mode("append").parquet(folder)
    print(f"Nessa rodada foram salvos {df_results.count()} produtos na pasta {folder}")

    eans = list(map(lambda x: x[0], results))
    registra_eans_processados(eans)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Executa o scraping

# COMMAND ----------

processa_eans(produtos_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mostra o resultado

# COMMAND ----------

spark.read.parquet(guia_farmacia_folder).limit(10).display()
