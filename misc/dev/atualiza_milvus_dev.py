# Databricks notebook source
get_ipython().system("pip install unidecode")
dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F
from langchain_core.documents import Document
from openai import OpenAI
from pydantic import BaseModel  # pylint: disable=unused-import # noqa: F401
from pymilvus import AnnSearchRequest, WeightedRanker
from unidecode import unidecode

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.io.milvus import Milvus
from maggulake.produtos.cria_indice_milvus import create_collection

env = DatabricksEnvironmentBuilder.build(
    "atualiza_milvus_dev",
    dbutils,
    widgets={"environment": ["production", "production"]},
)

# COMMAND ----------

# tabelas delta
eans_table = "production.utils.eans_dev"
produtos_table = "production.refined.produtos_com_embeddings"
ID_LOJA_MAGGU = 'eae52186-3700-4de4-9a14-d9c6829ec138'

# milvus
MILVUS_URI = dbutils.secrets.get(scope="databricks", key="MILVUS_URI_DEV")
MILVUS_TOKEN = dbutils.secrets.get(scope="databricks", key="MILVUS_TOKEN_DEV")

OPENAI_ORGANIZATION = dbutils.secrets.get(scope='openai', key='OPENAI_ORGANIZATION')
OPENAI_API_KEY = dbutils.secrets.get(scope='openai', key="CHAT_OPENAI_API_KEY_DEV")

# collection
collection = "produtos"
MODEL = "text-embedding-3-large"
DIMENSION = 1536

# COMMAND ----------

eans = spark.read.table(eans_table)

produtos = (
    env.table(Table.produtos_refined)
    .filter("status = 'ativo'")
    .withColumn("informacoes_para_embeddings", F.trim("informacoes_para_embeddings"))
    .cache()
)

embeddings = (
    env.table(Table.embeddings).filter(f"modelo = '{MODEL}-{DIMENSION}'").cache()
)

# COMMAND ----------

produtos_com_embeddings = (
    produtos.alias("p")
    .join(F.broadcast(eans), "ean", "semi")
    .join(
        embeddings.alias("e"),
        F.col("p.informacoes_para_embeddings") == F.col("e.texto"),
        "inner",
    )
    .selectExpr("p.*", "e.embedding AS vector")
    .withColumn("ids_lojas_com_produto", F.lit([ID_LOJA_MAGGU]))
    .cache()
)

# COMMAND ----------

if not produtos_com_embeddings.take(1):
    dbutils.notebook.exit("DataFrame de embeddings está vazio e não será salvo.")

# COMMAND ----------

milvus = Milvus(MILVUS_URI, MILVUS_TOKEN)

# COMMAND ----------

create_collection(milvus.client, collection, apaga_antiga=True)

# COMMAND ----------

produtos_formatados = milvus.formata_produtos(produtos_com_embeddings)

# COMMAND ----------

milvus.upsert(produtos_formatados, collection)

# COMMAND ----------

# MAGIC %md
# MAGIC Teste full-text-search

# COMMAND ----------

search_params = {
    "params": {
        "drop_ratio_search": 0.2
    },  # the smallest 20% of values in the query vector will be ignored during the search
}

milvus.client.search(
    collection_name="produtos",
    data=["fluconazol"],
    anns_field="sparse_bm25",
    limit=3,
    search_params=search_params,
    output_fields=["informacoes_para_embeddings"],
)

# COMMAND ----------

openai_client = OpenAI(
    organization=OPENAI_ORGANIZATION,
    api_key=OPENAI_API_KEY,
)

output_fields = ["informacoes_para_embeddings", "ean"]
filter_expr = "eh_nao_controlado == true && lojas_com_produto like \"%|maggu:maggu|%\" && eh_nao_tarjado == true"
query = "Seringa descartável"

max_results = 5
score_threshold = 0.4


def get_embeddings(text: str) -> list[float]:
    text = text.replace("\n", " ")
    response = openai_client.embeddings.create(
        input=[text],
        model="text-embedding-3-large",
        dimensions=1536,
    )
    return response.data[0].embedding


def search_milvus(query: str, mode: str = "hybrid"):
    """
    Realiza a busca no Milvus utilizando 3 modos:
        - "dense": utiliza busca por vetor denso (usando embeddings do OpenAI)
        - "sparse": utiliza busca textual completa (usando o texto original da consulta)
        - "hybrid": combina a busca densa e textual
    """
    output_fields = ["informacoes_para_embeddings", "ean"]

    if mode == "sparse":
        results = milvus.client.search(
            collection_name=collection,
            anns_field="sparse_bm25",
            data=[query],
            limit=max_results,
            output_fields=output_fields,
            filter=unidecode(filter_expr),
            search_params={
                "params": {
                    "drop_ratio_search": 0.2,  # Config sugerida pela documentação
                },
            },
        )
    elif mode == "dense":
        dense_vec = get_embeddings(query)
        results = milvus.client.search(
            collection_name=collection,
            anns_field="vector",
            data=[dense_vec],
            limit=max_results,
            output_fields=output_fields,
            filter=unidecode(filter_expr),
            search_params={"params": {"radius": score_threshold}},
        )
    elif mode == "hybrid":
        dense_vec = get_embeddings(query)

        sparse_req = AnnSearchRequest(
            data=[query],
            anns_field="sparse_bm25",
            param={
                "metric_type": "BM25",
                "radius": score_threshold,
                "drop_ratio_search": 0.2,
            },
            limit=max_results,
            expr=unidecode(filter_expr),
        )

        dense_req = AnnSearchRequest(
            data=[dense_vec],
            anns_field="vector",
            param={"metric_type": "COSINE", "radius": score_threshold},
            limit=max_results,
            expr=unidecode(filter_expr),
        )

        results = milvus.client.hybrid_search(
            collection_name=collection,
            reqs=[sparse_req, dense_req],
            ranker=WeightedRanker(
                0.2,
                0.8,
            ),  # usei o weighted para termos mais controle sobre os resultados
            limit=max_results,
            output_fields=output_fields,
            timeout=30,
        )
    else:
        raise ValueError(f"Modo de busca inválido: {mode}")

    print(f"Tipo do retorno: {type(results[0])}")
    return [
        Document(
            page_content=doc["entity"]["informacoes_para_embeddings"],
            metadata={"id": doc["id"], "ean": doc["entity"]["ean"]},
        )
        for doc in results[0]
    ]


search_milvus(query)
