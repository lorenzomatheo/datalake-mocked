# Databricks notebook source
get_ipython().system(
    "pip install openai langchain==1.0.0 langchain-community==0.4.1 langchain-core==1.2.2"
)
dbutils.library.restartPython()

# COMMAND ----------

import json
from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta import DeltaTable
from langchain_community.chat_models import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from openai import OpenAI
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("gera_embeddings_conversas_chat")
    .config(conf=config)
    .getOrCreate()
)

# TODO: nomes de tabelas, modelo de endpoint e chat_history_days hardcoded. Parametrizar via widgets ou config.
mensagens_table = "bigquery.postgres_public.mini_maggu_v1_mensagem"
chat_agno_payload_table = "production.raw.chat_agno_production_payload"
chat_payload_table = "production.raw.chat_milvus_production_payload"
chat_embeddings_table = "production.raw.chat_milvus_production_payload_with_embeddings"
chat_history_days = 90

OPENAI_ORGANIZATION = dbutils.secrets.get(scope="openai", key="OPENAI_ORGANIZATION")
OPENAI_API_KEY = dbutils.secrets.get(scope="openai", key="OPENAI_API_KEY_PROD")
CHAT = ChatDatabricks(endpoint='vertex-gemini-flash', temperature=0.2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Busca perguntas e respostas do chat

# COMMAND ----------

conversas = spark.sql(
    f"""WITH message_details AS (
    SELECT
        c.databricks_request_id,
        c.client_request_id,
        c.date as chat_date,
        m.content,
        m.role,
        ROW_NUMBER() OVER (PARTITION BY c.databricks_request_id ORDER BY monotonically_increasing_id() DESC) as rn
    FROM {chat_payload_table} c
    LATERAL VIEW explode(from_json(request, 'struct<messages:array<struct<content:string, role:string>>>').messages) AS m
    WHERE m.role = 'user'
)
SELECT
    m.databricks_request_id,
    m.client_request_id,
    m.chat_date,
    m.content AS last_user_question,
    a.answer.answer AS chat_answer
FROM message_details m
JOIN (
    SELECT
        databricks_request_id,
        explode(from_json(response, 'array<struct<answer:string>>')) AS answer
    FROM {chat_payload_table}
    WHERE status_code = 200
    AND response NOT LIKE '%Sou uma atendente virtual%'
    AND request NOT LIKE '%VANNA%'
    AND date >= date_add(current_date(), -{chat_history_days})
) a ON m.databricks_request_id = a.databricks_request_id
WHERE m.rn = 1"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Busca perguntas e respostas do chat Agno

# COMMAND ----------

df_chat_agno = spark.read.table(chat_agno_payload_table)

# COMMAND ----------

# Formata mensagens
messages_df = (
    df_chat_agno.withColumn(
        "parsed_messages",
        F.from_json(
            "request",
            'struct<messages:array<struct<content:string, role:string, timestamp_ms:long>>>',
        ),
    )
    .withColumn("m", F.explode("parsed_messages.messages"))
    .filter(F.col("m.role") == "user")
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("databricks_request_id").orderBy(
                F.col('timestamp_ms').desc()
            )
        ),
    )
    .select(
        "databricks_request_id",
        "client_request_id",
        F.col("date").alias("chat_date"),
        F.col("m.content").alias("content"),
        F.col("m.role").alias("role"),
        "rn",
    )
)

# Formata respostas
answers_df = (
    df_chat_agno.filter(
        (F.col("status_code") == 200)
        & (~F.col("response").contains("Sou uma atendente virtual"))
        & (~F.col("request").contains("VANNA"))
    )
    .withColumn(
        "answer",
        F.explode(
            F.from_json("response", 'array<struct<answer:string, timestamp_ms:long>>')
        ),
    )
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("databricks_request_id").orderBy(
                F.col('timestamp_ms').desc()
            )
        ),
    )
    .select(
        "databricks_request_id",
        F.col("answer.answer").alias("chat_answer"),
        F.col('rn'),
    )
)
# dataframe final
conversas_agno = (
    messages_df.filter(F.col("rn") == 1)  # somente a ultima resposta
    .join(answers_df, on=["databricks_request_id", "rn"], how="inner")
    .select(
        "databricks_request_id",
        "client_request_id",
        "chat_date",
        F.col("content").alias("last_user_question"),
        "chat_answer",
    )
)

# COMMAND ----------

conversas_final = conversas.union(conversas_agno)

# COMMAND ----------

conversas_unicas = (
    conversas_final.orderBy(F.desc("chat_date"))
    .dropDuplicates(subset=["last_user_question"])
    .drop("client_request_id")
    .withColumn("informacoes_para_embeddings", F.lit(""))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê mensagens com avaliação NEGATIVA

# COMMAND ----------

mensagens_com_avalicao_negativa = spark.sql(
    f""" SELECT * FROM {mensagens_table} m WHERE m.avaliacao like 'negativa' """
).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove respostas negativadas para não serem salvas no cache

# COMMAND ----------

respostas_negativadas = [
    json.loads(r["_data"])["conteudo"]
    for r in mensagens_com_avalicao_negativa.to_dict('records')
]
conversas_unicas = conversas_unicas.filter(
    ~F.col("chat_answer").isin(respostas_negativadas)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria nova tabela para salvar conversas e embeddings

# COMMAND ----------

schema = conversas_unicas.schema
schema.add("embedding", T.ArrayType(T.DoubleType()))
schema

# COMMAND ----------

dt_embeddings = (
    DeltaTable.createIfNotExists(spark)
    .tableName(chat_embeddings_table)
    .addColumns(conversas_unicas.schema)
    .execute()
)

# COMMAND ----------

chats_com_embeddings = spark.read.table(chat_embeddings_table)

row_ultimo_update = chats_com_embeddings.selectExpr(
    "max(chat_date) as chat_date"
).head()
ultimo_update = row_ultimo_update.chat_date or datetime.fromtimestamp(0)

# COMMAND ----------

conversas_novas = conversas_unicas.filter(F.col("chat_date") > ultimo_update)
conversas_novas.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Padroniza perguntas para embedding

# COMMAND ----------

GERA_NOVA_PERGUNTA = """
    A partir do pergunta atual, crie uma nova pergunta que seja semanticamente similar.

    Além disso, considere essa como uma possível resposta para a nova pergunta:
    <resposta fornecida>{chat_answer}</resposta fornecida>

    Aqui estão as instruções que você deve seguir:
    1. Formule a nova pergunta de forma clara e concisa, utilizando as informações coletadas e seguindo as orientações dadas.
    2. Se a pergunta atual for sobre um produto específico, mantenha o nome o produto informado na nova pergunta.
    3. Responda apenas com a nova pergunta.

    <exemplos>
    Pergunta atual: Posso tomar fluimiciu 2x ao dia?
    Nova pergunta: Qual a posologia do fluimiciu?

    Pergunta atual: posologia diclofenaco sodico
    Nova pergunta: Qual a posologia do Diclofenaco Sódico?

    Pergunta atual: bromoprida
    Nova pergunta: Para que serve a Bromoprida?

    Pergunta atual: resfriado
    Nova pergunta: O que indicar para resfriado?

    Pergunta atual: xarelto similar
    Nova pergunta: Qual medicamento é similar ao Xarelto?
    </exemplos>

    Aqui está a pergunta atual:
    <pergunta atual>{last_user_question}</pergunta atual>

    Nova pergunta:
"""

prompt_gera_pergunta = PromptTemplate(
    input_variables=[
        "chat_answer",
        "last_user_question",
    ],
    template=GERA_NOVA_PERGUNTA,
)

chain = prompt_gera_pergunta | CHAT | StrOutputParser()


# COMMAND ----------


# TODO: adicionar type hints em processa_perguntas_chat e get_embeddings.
def processa_perguntas_chat(question, answer):
    chat_payload = {"chat_answer": answer, "last_user_question": question}
    return chain.invoke(input=chat_payload)


spark.udf.register("processa_perguntas_chat", processa_perguntas_chat, T.StringType())

# COMMAND ----------

pra_embeddar = conversas_novas.join(
    chats_com_embeddings,
    ["databricks_request_id", "informacoes_para_embeddings"],
    "leftanti",
)
sem_embeddar = conversas_novas.join(
    chats_com_embeddings,
    ["databricks_request_id", "informacoes_para_embeddings"],
    "leftsemi",
)

pra_embeddar.count()

# COMMAND ----------

pra_embeddar = pra_embeddar.withColumn(
    "informacoes_para_embeddings",
    F.expr("processa_perguntas_chat(last_user_question, chat_answer)"),
).cache()

# COMMAND ----------

pra_embeddar.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gera embeddings

# COMMAND ----------


def get_embeddings(text):
    model = "text-embedding-3-large"
    client_openai = OpenAI(organization=OPENAI_ORGANIZATION, api_key=OPENAI_API_KEY)

    text = text.replace("\n", " ")
    return client_openai.embeddings.create(input=[text], model=model).data[0].embedding


spark.udf.register("get_embeddings", get_embeddings, "array<float>")

# COMMAND ----------

com_embeddings = pra_embeddar.withColumn(
    "embedding", F.expr("get_embeddings(informacoes_para_embeddings)")
).cache()

# COMMAND ----------

com_embeddings.limit(5).display()

# COMMAND ----------

(
    dt_embeddings.alias("dt")
    .merge(
        com_embeddings.alias("p"), "p.databricks_request_id = dt.databricks_request_id"
    )
    .whenNotMatchedInsert(values={c: c for c in com_embeddings.columns})
    .whenMatchedUpdate(set={c: f"p.{c}" for c in com_embeddings.columns})
    .execute()
)

# COMMAND ----------

(
    dt_embeddings.alias("dt")
    .merge(
        sem_embeddar.alias("p"), "p.databricks_request_id = dt.databricks_request_id"
    )
    .whenMatchedUpdate(set={c: f"p.{c}" for c in sem_embeddar.columns})
    .execute()
)
