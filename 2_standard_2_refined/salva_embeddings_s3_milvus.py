# Databricks notebook source
get_ipython().system("pip install unidecode")
dbutils.library.restartPython()

# COMMAND ----------

import json
import uuid
from datetime import datetime

import boto3
import pyspark.sql.functions as F
from botocore.exceptions import ClientError
from unidecode import unidecode

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.produtos.cria_indice_milvus import create_collection

MODEL = "text-embedding-3-large"
DIMENSION = 1536

env = DatabricksEnvironmentBuilder.build(
    "gera_embeddings_produtos",
    dbutils,
    widgets={"full_refresh": ["false", "false", "true"]},
)

spark = env.spark
s3_bucket_name = env.settings.bucket

# COMMAND ----------

temp_uuid = str(uuid.uuid4())
temp_path = f"/tmp/arrumado_{temp_uuid}"

# pastas s3
embeddings_folder = f"s3://{s3_bucket_name}/3-refined-layer/produtos_refined_embeddings"

# checkpoint
checkpoint_key = "checkpoints/ultimo_update_milvus.txt"
full_refresh = dbutils.widgets.get("full_refresh") == "true"

# collection
collection = "produtos"
new_collection = f"{collection}_new"
old_collection = f"{collection}_old"

# COMMAND ----------

# MAGIC %md
# MAGIC Lê e processa tabela de produtos

# COMMAND ----------

produtos = env.table(Table.produtos_refined).withColumn(
    "informacoes_para_embeddings", F.trim("informacoes_para_embeddings")
)

embeddings = env.table(Table.embeddings).filter(f"modelo = '{MODEL}-{DIMENSION}'")

# COMMAND ----------

s3_client = boto3.client("s3")


def save_last_updated_at(timestamp):
    raw = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f").encode()
    s3_client.put_object(Body=raw, Bucket=s3_bucket_name, Key=checkpoint_key)


def get_last_updated_at():
    try:
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=checkpoint_key)
        raw = response["Body"].read()
        return datetime.strptime(raw.decode(), "%Y-%m-%d %H:%M:%S.%f")
    except (ClientError, ValueError):
        return None


# COMMAND ----------

last_updated_at = (
    get_last_updated_at() or datetime.fromtimestamp(0)
    if not full_refresh
    else datetime.fromtimestamp(0)
)

novos_produtos = (
    produtos.filter(F.col("atualizado_em") > last_updated_at)
    .dropDuplicates(["id"])
    .withColumn("texto", F.col("informacoes_para_embeddings"))
)

last_updated_at

# COMMAND ----------

milvus = env.vector_search.milvus_adapter

# COMMAND ----------

arrumado = milvus.formata_produtos(novos_produtos).cache()

# COMMAND ----------


def tamanho_campo_dinamico(produto):
    campos_dinamicos = {
        k: v
        for k, v in produto.asDict().items()
        if k
        not in (
            "id",
            "ean",
            "lojas_com_produto",
            "ids_lojas_com_produto",
            "principio_ativo",
            "via_administracao",
            "eh_nao_tarjado",
            "eh_nao_controlado",
            "categorias",
            "informacoes_para_embeddings",
            "texto",
        )
    }

    return len(json.dumps(campos_dinamicos))


eans_produtos_muito_grandes = [
    p.ean for p in arrumado.toLocalIterator() if tamanho_campo_dinamico(p) > 65536
]

produtos_prontos = arrumado.filter(~F.col("ean").isin(eans_produtos_muito_grandes))

# COMMAND ----------

produtos_prontos.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Valida colecao

# COMMAND ----------

if full_refresh and milvus.client.has_collection(collection):
    stats = milvus.client.get_collection_stats(collection_name=collection)

    assert produtos_prontos.count() >= stats["row_count"]
    assert produtos_prontos.filter("length(principio_ativo) > 0").count() > 100
    assert produtos_prontos.filter("eh_nao_tarjado").count() > 100
    assert produtos_prontos.filter("length(lojas_com_produto) > 0").count() > 100

# COMMAND ----------

# MAGIC %md Junta com embeddings

# COMMAND ----------

embeddings_df = (
    produtos_prontos.alias("p")
    .join(
        embeddings.alias("e"),
        "texto",
        "inner",
    )
    .selectExpr("p.*", "e.embedding AS vector")
    .drop("texto")
)

# COMMAND ----------

if not embeddings_df.take(1):
    dbutils.notebook.exit("DataFrame de embeddings está vazio e não será salvo.")

# COMMAND ----------

# MAGIC %md Atualiza Milvus

# COMMAND ----------

collection_para_atualizar = collection

if full_refresh:
    create_collection(
        milvus.client, new_collection, apaga_antiga=True, dimension=DIMENSION
    )
    collection_para_atualizar = new_collection

# COMMAND ----------


def sanitize_informacoes_para_embeddings(payload):
    """Remove caracteres nao UTF-8 e limita o campo."""
    texto = payload.get('informacoes_para_embeddings')
    if isinstance(texto, str):
        payload['informacoes_para_embeddings'] = unidecode(texto)[:65530]


# COMMAND ----------

milvus.upsert(
    embeddings_df,
    collection_para_atualizar,
    batch_size=100,
    process_row_function=sanitize_informacoes_para_embeddings,
)

# COMMAND ----------

if full_refresh:
    if milvus.client.has_collection(old_collection):
        milvus.client.drop_collection(collection_name=collection)

    if milvus.client.has_collection(collection):
        milvus.client.rename_collection(old_name=collection, new_name=old_collection)
        milvus.client.release_collection(collection_name=old_collection)

    milvus.client.rename_collection(old_name=new_collection, new_name=collection)

# COMMAND ----------

if eans_produtos_muito_grandes:
    arrumado.filter(F.col("ean").isin(eans_produtos_muito_grandes)).drop(
        "lojas_com_produto",
        "ids_lojas_com_produto",
        "principio_ativo",
        "via_administracao",
        "eh_nao_tarjado",
        "eh_nao_controlado",
        "categorias",
        "informacoes_para_embeddings",
    ).display()

    raise RuntimeError("Atualização incompleta, produtos muito grandes.")

# COMMAND ----------

new_last_updated_at = novos_produtos.agg({"atualizado_em": "max"}).head()[0]

save_last_updated_at(new_last_updated_at)
