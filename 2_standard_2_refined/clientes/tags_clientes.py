# Databricks notebook source
get_ipython().system(
    "pip install langchain==1.0.0 langchain-openai langchain-core==1.2.2 langchain-community==0.4.1 langchainhub langchain_google_genai pymilvus s3fs pydantic langchain-google-vertexai langsmith"
)
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Geração de tags clientes

# COMMAND ----------

import datetime
import warnings
from operator import itemgetter

import pyspark.sql.functions as F
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from pyspark.context import SparkConf
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from maggulake.llm.models import get_model_name
from maggulake.llm.tagging_processor import TaggingProcessor
from maggulake.llm.vertex import setup_langsmith, setup_vertex_ai_credentials
from maggulake.pipelines.schemas import EnriquecimentoTagsCliente
from maggulake.utils.iters import create_batches, remove_duplicatas_sem_perder_ordem
from maggulake.utils.spark_udfs import array_to_string_udf
from maggulake.utils.strings import truncate_text_to_tokens
from maggulake.vector_search_retriever import get_data_api_retriever

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

# NOTE: enriquecemos os clientes somente em produção. Não há vendas em staging

# COMMAND ----------

dbutils.widgets.dropdown("LLM_PROVIDER", "google", ["openai", "google"])

# COMMAND ----------

STAGE = "prod"
CATALOG = "production" if STAGE == "prod" else "staging"
LLM_PROVIDER = dbutils.widgets.get("LLM_PROVIDER")

tabela_clientes_conta_refined = f"{CATALOG}.refined.clientes_conta_refined"
tabela_produtos_refined = f"{CATALOG}.refined.produtos_refined"
# TODO: migrar a tabela abaixo pra um delta table
s3_clientes_tags_folder = "s3://maggu-datalake-prod/3-refined-layer/gpt_tags_clientes"

# COMMAND ----------

spark = (
    SparkSession.builder.appName("tags_clientes")
    .config(conf=SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")]))
    .getOrCreate()
)
spark.conf.set("maggu.app.name", "tags_clientes")

# COMMAND ----------

setup_langsmith(dbutils, spark, stage=STAGE)

# COMMAND ----------

# COMMAND ----------

setup_langsmith(dbutils, spark, stage=STAGE)

# COMMAND ----------

# accumulator initialization
countAccumulator = spark.sparkContext.accumulator(0)

# LLM parameters
EMBEDDING_ENCODING = "cl100k_base"
MAX_TOKENS = 13000

match LLM_PROVIDER:
    case "openai":
        LLM_MODEL = get_model_name(provider="openai", size="SMALL")
        LLM_ORGANIZATION = dbutils.secrets.get(
            scope="openai", key="OPENAI_ORGANIZATION"
        )
        LLM_API_KEY = dbutils.secrets.get(
            scope="openai", key=f"OPENAI_API_KEY_{STAGE.upper()}"
        )
        BATCH_SIZE = 200
    case "google":
        # TODO: reduzir para SMALL depois que os créditos do Google acabarem
        LLM_MODEL = get_model_name(provider="gemini_vertex", size="LARGE")
        LLM_ORGANIZATION = None
        LLM_API_KEY = None  # Não precisa no vertex
        BATCH_SIZE = 2500  # Janela de contexto maior permite maior batch

PROJECT_ID, LOCATION = setup_vertex_ai_credentials(spark)

# retriever
DATA_API_URL = dbutils.secrets.get(
    scope="data_api", key=f"DATA_API_URL_{CATALOG.upper()}"
)
DATA_API_KEY = dbutils.secrets.get(
    scope="data_api", key=f"DATA_API_KEY_{CATALOG.upper()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura tabela refined

# COMMAND ----------

clientes_conta_refined_df = spark.read.table(tabela_clientes_conta_refined)

# Retirar as linhas em que "medicamentos_tarjados" são nulas ou arrays vazios
clientes_conta_refined_df = clientes_conta_refined_df.filter(
    F.col("medicamentos_tarjados").isNotNull()
    & (F.size(F.col("medicamentos_tarjados")) > 0)
)

# Retirar linhas em que "total_de_compras" é nulo
clientes_conta_refined_df = clientes_conta_refined_df.filter(
    F.col("total_de_compras").isNotNull()
)

print("Quantidade de linhas a serem processadas: ", clientes_conta_refined_df.count())

# COMMAND ----------

# clientes_conta_refined_df.display()

# COMMAND ----------

clientes_conta_refined_df = clientes_conta_refined_df.withColumn(
    "medicamentos_tarjados_str", array_to_string_udf(F.col("medicamentos_tarjados"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Junta com tags calculadas anteriormente

# COMMAND ----------

tags_df = spark.read.parquet(s3_clientes_tags_folder)
tags_df = tags_df.dropDuplicates(["id"])

# COMMAND ----------

if tags_df is not None:
    a_fazer_df = clientes_conta_refined_df.join(tags_df, on=["id"], how="leftanti")
else:
    a_fazer_df = clientes_conta_refined_df

# COMMAND ----------

n = a_fazer_df.count()
print(f"Quantidade de clientes que ainda não foram refinados: {n}")

if n < 1:
    dbutils.notebook.exit("Todos os clientes já foram refinados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriquecimento

# COMMAND ----------

# MAGIC %md
# MAGIC Nesse notebook vamos usar LLM para preencher os seguintes campos:
# MAGIC
# MAGIC - doencas_cronicas ARRAY<STRING>,
# MAGIC - doencas_agudas ARRAY<STRING>,
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define o vector search retriever

# COMMAND ----------


def get_vector_search_retriever():
    return get_data_api_retriever(
        data_api_url=DATA_API_URL,
        data_api_key=DATA_API_KEY,
        filtro="eh_medicamento == true",
        max_results=5,
        score_threshold=0.4,
        campos_output=[
            "id",
            "ean",
            "nome_comercial",
            "marca",
            "eh_medicamento",
            "tipo_medicamento",
            "eh_otc",
            "eh_tarjado",
            "forma_farmaceutica",
            "informacoes_para_embeddings",
            "via_administracao",
            "instrucoes_de_uso",
            "idade_recomendada",
            "sexo_recomendado",
            "indicacao",
            "contraindicacoes",
            "efeitos_colaterais",
            "interacoes_medicamentosas",
            "advertencias_e_precaucoes",
            "medicamentos_referencia",
            "medicamentos_similares",
            "tipo_receita",
            "tipo_de_receita_completo",
            "tarja",
        ],
    )


vs_retriever = get_vector_search_retriever()

# COMMAND ----------

# MAGIC %md
# MAGIC testa o retriever:

# COMMAND ----------

# vs_retriever.invoke("dor de garganta")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define enums de respostas

# COMMAND ----------

# fonte: http://www2.datasus.gov.br/cid10/V2008/cid10.htm

# TODO: Escrever esse enum num código python para dar maior visibilidade
# TODO: Por algum motivo retiraram esse enum do Prompt, precisa verificar o porquê ou adicionar novamente

doencas_df = (
    spark.read.option("encoding", "WINDOWS-1252")
    .option("sep", ";")
    .option("header", "true")
    .csv("s3://maggu-datalake-prod/1-raw-layer/maggu/cid10/CID-10-CATEGORIAS.CSV")
)

doencas_df = doencas_df.select(
    F.col("CAT"),
    F.col("DESCRICAO"),
)

doencas = [row.DESCRICAO for row in doencas_df.collect()]
doencas = list(set(doencas))

print("Quantidade de doenças possíveis: ", len(doencas))
print(doencas[:10])

# NOTE: Se esse número for muito grande, o request pode falhar por excesso de tokens

# COMMAND ----------

# TODO: para um futuro distante:
# - Separar doenças que são dependentes do sexo
# por exemplo, ejaculação precoce e cólica menstrual). E variar o enum
# de doenças de acordo com o sexo do cliente

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define cadeia de perguntas

# COMMAND ----------


def format_retrieved_documents(docs: list[dict]) -> str:
    """Format and adjust size of retrieved documents."""

    def format_document(doc):
        try:
            doc_content: dict = doc["entity"]
        except KeyError as e:
            warnings.warn(
                f"Returned VS document does not contain 'entity' key. Using empty document instead. Error: {e}"
            )
            return "## Metadata\n\n"

        listed_items = [
            f"{key}: {value}" for key, value in doc_content.items() if value
        ]
        formatted_metadata = "\n".join(listed_items)
        return f"## Metadata\n{formatted_metadata}\n"

    combined_docs = "\n\n".join([format_document(doc) for doc in docs])
    truncated_text = truncate_text_to_tokens(
        combined_docs, MAX_TOKENS, EMBEDDING_ENCODING
    )
    return truncated_text


# COMMAND ----------

PROMPT_TEMPLATE = """Act as a pharmacist (female) and use the information about the customer and products in the passage below to generate synthetic data, according to the specified schema.

Use general knowledge to provide a plausible response for each property as per the schema's descriptions mentioned in the 'information_extraction' function.

Passage:
< {informacoes_cliente} >

I'm providing you with the following details about the products the customer has purchased:

< {informacoes_medicamentos} >

------

Further instructions:
- Please provide a response for each property. Do not include more properties.
- The response must be in Brazilian Portuguese.
- If the answer is not applicable, please leave it blank.
- If the answer is given in English, please translate it into Brazilian Portuguese.
"""

# COMMAND ----------

gpt_caller = TaggingProcessor(
    provider="gemini_vertex" if LLM_PROVIDER == "google" else "openai",
    api_key=LLM_API_KEY,
    organization=LLM_ORGANIZATION,
    project_id=PROJECT_ID if LLM_PROVIDER == "google" else None,
    location=LOCATION if LLM_PROVIDER == "google" else None,
    model=LLM_MODEL,
    prompt_template=PROMPT_TEMPLATE,
    output_schema=EnriquecimentoTagsCliente,
)

# COMMAND ----------

chain_retriever = RunnablePassthrough() | {
    "informacoes_cliente": itemgetter("informacoes_cliente"),
    "informacoes_medicamentos": itemgetter("informacoes_cliente")
    | vs_retriever
    | RunnableLambda(format_retrieved_documents),
}


prompt_pergunta_tags = PromptTemplate(
    input_variables=["informacoes_cliente", "informacoes_medicamentos"],
    template=PROMPT_TEMPLATE,
)

# Altera a cadeia de perguntas
gpt_caller.chain = chain_retriever | prompt_pergunta_tags | gpt_caller.structured_llm

# COMMAND ----------


def formata_info_cliente(cliente):
    # TODO: testar se tem como incluir a data de nascimento (ou idade) do cliente aqui pra melhorar as respostas
    return truncate_text_to_tokens(
        "\n\n".join(
            [
                f"ID do cliente: {cliente.id}",
                f"CPF do cliente: {cliente.cpf_cnpj}",
                f"Nome: {cliente.nome_completo}",
                f"medicamentos_tarjados: {cliente.medicamentos_tarjados_str}",
            ]
        ),
        MAX_TOKENS,
        EMBEDDING_ENCODING,
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Testando resultado para 1 cliente

# COMMAND ----------

# seleciona aleatoriamente um item do df
exemplo = a_fazer_df.orderBy(F.rand()).limit(1).collect()[0]

query = formata_info_cliente(exemplo)


print("medicamentos_tarjados:  ", exemplo.medicamentos_tarjados_str, "\n\n")
print(query)

# COMMAND ----------

# Testa a chamada para 1 query

gpt_caller.executa_tagging_chain(
    {
        "informacoes_cliente": query,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Executa enriquecimento

# COMMAND ----------

DATAFRAME_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("cpf_cnpj", StringType(), True),
        StructField("doencas_cronicas", ArrayType(StringType()), True),
        StructField("doencas_agudas", ArrayType(StringType()), True),
        StructField("atualizado_em", TimestampType(), True),
    ]
)

SCHEMA_FIELDS = [field.name for field in DATAFRAME_SCHEMA.fields]

# COMMAND ----------


async def extrai_info_e_salva(collected_list: list[Row]):
    global countAccumulator

    n_clientes = a_fazer_df.count()

    for b in create_batches(iterable=collected_list, n=BATCH_SIZE):
        rs = await extrair_info(b)
        if rs:
            salva_info_produto(rs)
        else:
            print(f"Erro ao extrair informações desse batch. Batch: {b}\n")

        countAccumulator.add(len(b))
        print(
            f"Quantidade de clientes pendentes: {n_clientes - countAccumulator.value} de {n_clientes}"
        )


async def extrair_info(clientes: list[Row]):
    payloads = [
        {
            "informacoes_cliente": formata_info_cliente(cl),
        }
        for cl in clientes
    ]
    return await gpt_caller.executa_tagging_chain_async_batch(payloads, timeout=600)


def salva_info_produto(info):
    global countAccumulator

    validated_rows = []
    for res in info:
        if isinstance(res, dict):
            complete_res = {field: res.get(field, None) for field in SCHEMA_FIELDS}

            complete_res["doencas_agudas"] = remove_duplicatas_sem_perder_ordem(
                complete_res.get("doencas_agudas", [])
            )
            complete_res["doencas_cronicas"] = remove_duplicatas_sem_perder_ordem(
                complete_res.get("doencas_cronicas", [])
            )

            complete_res["atualizado_em"] = datetime.datetime.now()
            validated_rows.append(Row(**complete_res))
        else:
            print(f"Erro ao processar uma linha: {repr(res)}")

    if validated_rows:
        df_results = spark.createDataFrame(validated_rows, DATAFRAME_SCHEMA)
        df_results.write.mode("append").parquet(s3_clientes_tags_folder)
        print(f"Salvamos {len(validated_rows)} tags")


# COMMAND ----------

await extrai_info_e_salva(a_fazer_df.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualiza resultado

# COMMAND ----------

df_teste = spark.read.parquet(s3_clientes_tags_folder)

df_teste = df_teste.dropDuplicates(["id"])

print("Quantidade de clientes refinados com tags: ", df_teste.count())

df_teste.limit(50).display()

# COMMAND ----------

df_ids = df_teste.select("id").collect()
df_ids = list(set(row.id for row in df_ids))

df_ids = df_ids[:-1]

print(len(df_ids))

# COMMAND ----------


def remove_tag_do_id(id_a_apagar: str):
    if not isinstance(id_a_apagar, str):
        raise TypeError("id must be a string")

    antes = spark.read.parquet(s3_clientes_tags_folder)
    antes_count = antes.count()

    depois = antes.filter(antes.id != id_a_apagar)
    depois_count = depois.count()

    if antes_count - depois_count > 1:
        raise ValueError("Algo de errado está acontecendo")

    # TODO: migrar para delta table
    depois.write.mode("overwrite").parquet(s3_clientes_tags_folder)
    print(f"id removido com sucesso: {id_a_apagar}")


# TODO: de tempos em tempos (definir isso depois) temos que refazer essas tags
