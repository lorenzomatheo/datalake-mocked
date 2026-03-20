# Databricks notebook source
get_ipython().system("pip install openai==1.35.7 tiktoken langchain-google-genai")
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md # Utiliza o GPT para gerar Sintomas e Categorias de produtos
# MAGIC
# MAGIC Agora utilizando o Bulk mode do openai.
# MAGIC Utilizar este método de solicitações pode reduzir nosso custo total!
# MAGIC
# MAGIC Leia mais sobre: https://platform.openai.com/docs/guides/batch/overview
# MAGIC NOTE: Hoje, 03/Out/2025, esse notebook não está sendo usado. Só mantive aqui para caso um dia precisemos usar o bulk mode em algum lugar.

# COMMAND ----------

# MAGIC %md ## Task Inputs

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["prod", "dev"])

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import warnings

import boto3
import pytz
from openai import OpenAI
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.llm.batch_utils import (
    BatchGpt,
    lista_todos_os_batches_abertos_no_openai,
)
from maggulake.llm.models import get_model_name
from maggulake.utils.iters import batched
from maggulake.utils.strings import truncate_text_to_tokens

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configurações Spark Session

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("gpt_pega_categorias_dos_produtos")
    .config(conf=config)
    .getOrCreate()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configurações OpenAI

# COMMAND ----------

STAGE = dbutils.widgets.get("stage")
openai_client = OpenAI(
    api_key=dbutils.secrets.get(scope="openai", key=f"OPENAI_API_KEY_{STAGE.upper()}"),
    organization=dbutils.secrets.get(scope="openai", key="OPENAI_ORGANIZATION"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variáveis globais

# COMMAND ----------


CATALOG = "production.refined" if STAGE == "prod" else "staging.refined"
GPT_MODEL = get_model_name(provider="openai", size="SMALL")
SAO_PAULO_TZ = pytz.timezone("America/Sao_Paulo")
BUCKET_NAME = f"maggu-datalake-{STAGE}"

# TODO: ngm usa mais o bulk_mode, porém seria legal já migrar para delta table aqui
s3_produtos_folder = f"s3://{BUCKET_NAME}/2-optimized-layer/produtos"
s3_categorias_folder = f"s3://{BUCKET_NAME}/3-refined-layer/categorias_dos_produtos_gpt"
s3_openai_batch_files_folder = "3-refined-layer/openai_batch_files/"

s3_client = boto3.client("s3")

print("STAGE:                          ", STAGE)
print("GPT_MODEL:                      ", GPT_MODEL)
print("s3_produtos_folder:             ", s3_produtos_folder)
print("s3_categorias_folder:           ", s3_categorias_folder)
print("s3_openai_batch_files_folder:   ", s3_openai_batch_files_folder)

# COMMAND ----------

# MAGIC %md ## Leitura de Produtos
# MAGIC

# COMMAND ----------

produtos = spark.read.table(f"{CATALOG}.produtos_refined").cache()

# COMMAND ----------

produtos.count()

# COMMAND ----------

# MAGIC %md ## Apaga respostas de sintomas (somente para gerar tudo de novo)

# COMMAND ----------

# try:
#     rs = spark.read.parquet(s3_sintomas_folder)
#     print(rs.count())
#     spark.createDataFrame([], rs.schema).write.mode('overwrite').parquet(s3_sintomas_folder)
# except Exception as e:
#     print(e)

# COMMAND ----------

# MAGIC %md ## Processa os sintomas

# COMMAND ----------


EMBEDDING_ENCODING = "cl100k_base"
MAX_TOKENS = 13000


def formata_info_produto(produto):
    descricao = produto.indicacao or produto.descricao
    return truncate_text_to_tokens(
        "\n\n".join(
            [
                f"Nome: {produto.nome}",
                f"Indicação: {descricao}" if descricao else "",
            ]
        ),
        MAX_TOKENS,
        EMBEDDING_ENCODING,
    )


# COMMAND ----------

PROMPT_TEMPLATE = """Act as a pharmacist and use the information about the product in the passage below to generate synthetic data, according to the specified schema. Use general knowledge to provide a plausible response for each property as per the schema's descriptions mentioned in the 'information_extraction' function.

Passage:
< {input} >

Gere tags (palavras-chave) para o produto descrito no texto. Inclua sempre o tipo de produto na tag. Não inclua a marca e a dosagem. Evite tags sinônimas. Responda sempre no plural. Exemplo 1: Se o produto descrito fosse "Esfoliante Facial Oxyeau Limpeza Suave Todos Tipos De Pele 60gr", a resposta seria: "Esfoliantes|Esfoliantes faciais|Esfoliantes suaves". Exemplo 2: Se o produto descrito fosse "Hidratante Rosa Mosqueta Epile 200ml", a resposta seria: "Hidratantes|Hidratantes para pele seca|Hidratantes para pele oleosa". Exemplo 3: Se o produto descrito fosse "Rubia 0,075mg, caixa com 28 comprimidos revestidos", a resposta seria: "Contraceptivo|Contraceptivo oral".
"""

# COMMAND ----------

help(openai_client.completions)

# COMMAND ----------

produto = produtos.rdd.takeSample(False, 1)[0]
# produto = produtos.filter("nome = 'Kit Super Band Extensor Elástico Forte e Extra Forte Yangfit'").first()
print(produto.nome)

prompt = PROMPT_TEMPLATE.format(input=formata_info_produto(produto))
print(prompt)

# a = tagging_chain.invoke({"input": prompt})

# a.content

# COMMAND ----------


def filtra_a_fazer_produtos(produtos):
    pedidos_abertos = lista_todos_os_batches_abertos_no_openai(openai_client)
    if len(pedidos_abertos) > 0:
        warnings.warn(
            f"Ainda existem batches abertos sendo processados no OpenAI. "
            f"É recomendado  aguardar a conclusão de todos os requests antes de continuar. "
            f"Caso contrário, você pode acabar enviar requests duplicados para o OpenAI. "
            f"A lista de pedidos abertos é: {pedidos_abertos}"
        )
    rs = spark.read.parquet(s3_categorias_folder).cache()
    return (
        produtos.alias("p").join(rs, produtos.ean == rs.ean, "anti").selectExpr("p.*")
    )


produtos_a_processar = filtra_a_fazer_produtos(produtos).collect()
print(len(produtos_a_processar), " produtos ainda precisam de sintomas.")

# COMMAND ----------

tasks = [
    PROMPT_TEMPLATE.format(input=formata_info_produto(p)) for p in produtos_a_processar
]
eans = [p.ean for p in produtos_a_processar]

# COMMAND ----------

len(tasks), len(eans)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salva o dataframe

# COMMAND ----------


def salva_categorias(eans: list, categorias: list):
    rows = [
        {"ean": ean, "resposta": s}
        for ean, s in zip(eans, categorias)
        if isinstance(s, str)
    ]

    if len(rows) == 0:
        return

    df = spark.createDataFrame(rows)
    df.write.mode("append").parquet(s3_categorias_folder)


# COMMAND ----------

# MAGIC %md ### Pergunta para o GPT em batches

# COMMAND ----------


if len(produtos_a_processar) > 0:
    batch_gpt = BatchGpt(spark, openai_client, CATALOG, "batch_tasks_sintomas")
    results = batch_gpt.get_all_results(tasks)

    for b in batched(zip(eans, results), 10000):
        eans = [i[0] for i in b]
        categorias = [i[1] for i in b]

        salva_categorias(eans, categorias)
