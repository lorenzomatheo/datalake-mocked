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
import time
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
spark = SparkSession.builder.appName(
    "scrape_guia_da_farmacia_intercambiaveis"
).getOrCreate()

countAccumulator = spark.sparkContext.accumulator(0)
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

BATCH_SIZE = 10  # Batch de EANs para ir salvando no S3
RATE_LIMIT = 1  # Tempo mínimo de espera entre requisições (em segundos)
MAX_RETRIES = 3  # Número máximo de tentativas para uma requisição
MAX_PAGES = 50  # Limite máximo de páginas para evitar loop infinito
# Tenta scraping novamente se tiver sido feito antes dessa data
SCRAPING_TTL = datetime.now() - timedelta(days=90)
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))


produtos_table = f"{catalog}.refined.produtos_refined"
tabela_eans_processados = f"{catalog}.utils.eans_processados_guia_da_farmacia"
guia_equivalentes_folder = (
    f"s3://maggu-datalake-{stage}/1-raw-layer/maggu/guiadafarmacia_intercambialidade/"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê a tabela de produtos e filtra por medicamentos com marca, principio ativo e sem info de intercambialidade

# COMMAND ----------

produtos_df = spark.read.table(produtos_table)
produtos_df = produtos_df.filter(
    F.col("eh_medicamento")
    & F.col("marca").isNotNull()
    & F.col("principio_ativo").isNotNull()
    & (
        F.col("medicamentos_referencia").like("[]")
        | F.col("medicamentos_similares").like("[]")
        | FULL_REFRESH
    )
)
produtos_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria a função de scraping

# COMMAND ----------


def scrape_intercambiaveis(marca: str, principio_ativo: str) -> str:  # pylint: disable=too-many-nested-blocks
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,en;q=0.5",
    }

    all_results = []
    page = 1

    while page <= MAX_PAGES:  # pylint: disable=too-many-nested-blocks
        for attempt in range(MAX_RETRIES):
            try:
                # Adiciona delay randomico para evitar bloqueio
                time.sleep(RATE_LIMIT + random.uniform(0, 1))

                search_url = (
                    f"https://guiadesimilares.com.br/search/{marca.replace(' ', '+')}/"
                )
                if page > 1:
                    search_url += f"page/{page}/"

                response = requests.get(search_url, headers=headers)
                soup = BeautifulSoup(response.content, "html.parser")

                # Encontra tabela com intercambiaveis
                table = soup.find("div", id="resultados")
                if not table:
                    return json.dumps(all_results)

                table = table.find("table")
                if not table:
                    return json.dumps(all_results)

                rows = table.find_all("tr")[1:]  # Pula header
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) == 5:
                        result = {
                            "product_name": marca,
                            "principio_ativo": cells[1].text.strip(),
                            "referencia": cells[0].text.strip(),
                            "similar_equivalente": cells[2].text.strip(),
                            "laboratorio": cells[3].text.strip(),
                            "concentracao_forma": cells[4].text.strip(),
                        }
                        if principio_ativo.lower() == result["principio_ativo"].lower():
                            all_results.append(result)

                # Verifica se existe outra página
                next_page = soup.find("link", rel="next")
                if not next_page:
                    return json.dumps(all_results)

                page += 1
                break

            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    print(
                        f"Erro ao buscar o {marca} na página {page}: {str(e)}. Retentando..."
                    )
                    time.sleep(2**attempt)  # Estratégia de exponential backoff
                else:
                    print(
                        f"Erro ao buscar o {marca} na página {page}: {str(e)}. Máximo de retries alcançado."
                    )
                    return json.dumps(all_results)

    # Se atingir o limite de páginas, retorna o que foi coletado
    print(f"Limite de páginas ({MAX_PAGES}) atingido para marca {marca}.")
    return json.dumps(all_results)


# Cria a UDF
scrape_intercambiaveis_udf = F.udf(scrape_intercambiaveis, T.StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Função para salvar eans já processados

# COMMAND ----------


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
        marca = row["marca"]
        principio_ativo = row["principio_ativo"]
        result = scrape_intercambiaveis(marca, principio_ativo)
        results.append((ean, marca, result))
        countAccumulator.add(1)
        print(
            agora_em_sao_paulo_str()
            + f" -> {countAccumulator.value} produtos já processados, de um total de {len(produtos_pendentes)}"
        )

        # salva o batch para não perder informação caso dê algum erro
        if (i + 1) % BATCH_SIZE == 0:
            salva_resultados(results, guia_equivalentes_folder)
            results = []

    if results:
        salva_resultados(results, guia_equivalentes_folder)


def salva_resultados(results: List[Tuple[str, str, str]], folder: str) -> None:
    schema = T.StructType(
        [
            T.StructField("ean", T.StringType(), True),
            T.StructField("marca", T.StringType(), True),
            T.StructField("scrape_results", T.StringType(), True),
        ]
    )

    df_results = spark.createDataFrame(results, schema=schema)
    df_results = df_results.withColumn(
        "scrape_array",
        F.from_json(
            "scrape_results", T.ArrayType(T.MapType(T.StringType(), T.StringType()))
        ),
    )

    df_exploded = df_results.withColumn("scrape_item", F.explode("scrape_array"))

    for col in [
        "principio_ativo",
        "referencia",
        "similar_equivalente",
        "laboratorio",
        "concentracao_forma",
    ]:
        df_exploded = df_exploded.withColumn(col, F.col(f"scrape_item.{col}"))

    df_exploded = df_exploded.drop("scrape_results", "scrape_array", "scrape_item")
    df_exploded = df_exploded.withColumn("gerado_em", F.current_timestamp())

    df_exploded.write.mode("append").parquet(folder)
    print(f"Nessa rodada foram salvos {df_exploded.count()} produtos na pasta {folder}")

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

spark.read.parquet(guia_equivalentes_folder).limit(100).display()
