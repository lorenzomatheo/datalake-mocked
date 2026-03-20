# Databricks notebook source
# MAGIC %pip install pandas mlflow==2.12.1 langchain==1.0.0 langchain-core==1.2.2 langchain-community==0.4.1 fsspec s3fs openpyxl xlrd langchain-google-genai langchain-google-vertexai langsmith
# MAGIC dbutils.library.restartPython()

# COMMAND ----------


import pandas as pd
from langchain_community.chat_models import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.llm.models import get_model_name
from maggulake.llm.vertex import setup_langsmith

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("Categoriza Chat").config(conf=config).getOrCreate()
)
spark.conf.set("maggu.app.name", "categoriza_perguntas_chat")

# ambiente
stage = "prod"
s3_bucket_name = f"maggu-datalake-{stage}"

setup_langsmith(dbutils, spark, stage=stage)

# pastas s3
s3_folder_chat = f"s3://{s3_bucket_name}/1-raw-layer/maggu/respostas-chat/"
s3_file_chat = s3_folder_chat + "pergunta_e_resposta_chat_endpoint_2024_08_20.xlsx"

# TODO: Trocar para SMALL quando os créditos do google acabarem
CHAT_MODEL = get_model_name(provider="openai", size="LARGE")

# COMMAND ----------

dados_chat = pd.read_excel(s3_file_chat, sheet_name="result", header=0)
chat_df = spark.createDataFrame(dados_chat)

print(f"Total de linhas na tabela de dados do chat: {chat_df.count()}")
chat_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Categorizando perguntas com avaliação do GPT

# COMMAND ----------

chat_gpt = ChatDatabricks(endpoint=CHAT_MODEL, temperature=0)

# COMMAND ----------

CATEGORIZA_PERGUNTAS = """
    Você deve categorizar objetivamente a interação de pergunta e resposta em uma categoria de atendimento.

    A pergunta feita foi: <pergunta>{pergunta}</pergunta>.

    A resposta dada foi: <resposta>{resposta}</resposta>.

    Avalie a interação e categorize em uma das categorias:

    - Posologia
    - Recomendação
    - Complementares
    - Intercambialidade
    - Comparação
    - Tipo de Receita
    - Outros

    Responda somente com a categoria, sem explicações ou outras palavras adicionais.
"""

prompt_categoriza = PromptTemplate(
    input_variables=["pergunta", "resposta"], template=CATEGORIZA_PERGUNTAS
)

chain = prompt_categoriza | chat_gpt | StrOutputParser()

for index, row in dados_chat.iterrows():
    interacao = {
        "pergunta": row["last_user_content"],
        "resposta": row["response_answer"],
    }
    response = chain.invoke(input=interacao)
    dados_chat.at[index, "categoria"] = response.lower()
    print("--------------------- ")
    print(f"Interacao {interacao} obteve a categoria: {response}")


# COMMAND ----------

chat_df = spark.createDataFrame(dados_chat)
chat_df.display()

# COMMAND ----------

chat_df.write.mode("overwrite").saveAsTable(
    "production.analytics.view_categorias_respostas_chat_paguemenos"
)
