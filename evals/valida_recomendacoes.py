# Databricks notebook source
# MAGIC %pip install pandas mlflow==2.12.1 langchain==1.0.0 langchain-core==1.2.2 langchain-community==0.4.1 langsmith

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import pandas as pd
import psycopg2
from langchain_community.chat_models import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter
from maggulake.llm.vertex import setup_langsmith

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("Valida Recomendacoes")
    .config(conf=config)
    .getOrCreate()
)
spark.conf.set("maggu.app.name", "valida_recomendacoes")

stage = dbutils.widgets.get("stage")

setup_langsmith(dbutils, spark, stage=stage)

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")

# COMMAND ----------

# MAGIC %md
# MAGIC Listando tabelas disponíveis

# COMMAND ----------

postgres = PostgresAdapter(stage, USER, PASSWORD)

postgres.list_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC Verificando recomendacoes

# COMMAND ----------

postgres.read_table(spark, "cesta_de_compras_recomendacaofeitamanualmente").display()

# COMMAND ----------


pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 1000)

df = pd.DataFrame()

try:
    conn = psycopg2.connect(
        database=postgres.DATABASE_NAME,
        user=postgres.USER,
        host=postgres.DATABASE_HOST,
        password=postgres.PASSWORD,
        port=postgres.DATABASE_PORT,
    )
    cursor = conn.cursor()

    query = """
    SELECT
      c.id, p.nome as produto_cesta, COALESCE(p.indicacao, p.descricao) as descricao_produto_cesta, p2.nome as produto_recomendado, COALESCE(p2.indicacao,p2.descricao) as descricao_recomendado
      from cesta_de_compras_recomendacaofeitamanualmente c
      inner join produtos_produtov2 p on c.produto_recomendacao_id = p.id
      inner join produtos_produtov2 p2 on c.produto_recomendado_id = p2.id
  """

    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(data, columns=columns)

    # pra garantir que não vai dar conflito entre colunas
    def rename_duplicates(old_columns):
        seen = {}
        for idx, column in enumerate(old_columns):
            if column in seen:
                seen[column] += 1
                old_columns[idx] = f"{column}_{seen[column]}"
            else:
                seen[column] = 0
        return old_columns

    df.columns = rename_duplicates(list(df.columns))

    display(df)
    conn.commit()
except psycopg2.DatabaseError as error:
    print(f"An error occurred: {error}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC Atualizando as recomendações de acordo com avaliação do GPT

# COMMAND ----------

CHAT_MODEL_GPT4 = "gpt-4o"
chat_gpt4 = ChatDatabricks(endpoint=CHAT_MODEL_GPT4, temperature=0)

# COMMAND ----------


def update_recomendacao_valida(id):
    try:
        conn = psycopg2.connect(
            database=postgres.DATABASE_NAME,
            user=postgres.USER,
            host=postgres.DATABASE_HOST,
            password=postgres.PASSWORD,
            port=postgres.DATABASE_PORT,
        )

        cursor = conn.cursor()
        update_query = f"""
            UPDATE cesta_de_compras_recomendacaofeitamanualmente
            SET recomendacao_valida = TRUE
            WHERE id = '{id}'
        """
        cursor.execute(update_query)
        conn.commit()
        cursor.close()
    except psycopg2.DatabaseError as error:
        print(f"Failed to update record {id}, error: {error}")
        conn.rollback()


# COMMAND ----------

VALIDA_RECOMENDACOES_MANUAIS = """
    Você deve avaliar objetivamente se o produto recomendado é uma recomendação válida para complementar a compra do cliente.

    O cliente já está comprando o produto {produto_cesta}. Sabemos que {descricao_produto_cesta}.

    O produto {produto_recomendado} está sendo recomendado para complementar sua compra.

    Considere que {descricao_recomendado}.

    A recomendação faz sentido? Os produtos são complementares no tratamento do cliente?

    Responda apenas "sim" ou "não"
"""

prompt_validacao = PromptTemplate(
    input_variables=[
        "produto_cesta",
        "descricao_produto_cesta",
        "produto_recomendado",
        "descricao_recomendado",
    ],
    template=VALIDA_RECOMENDACOES_MANUAIS,
)

chain = prompt_validacao | chat_gpt4 | StrOutputParser()

for index, row in df.iterrows():
    recomendacao = {
        "produto_cesta": row["produto_cesta"],
        "descricao_produto_cesta": row["descricao_produto_cesta"],
        "produto_recomendado": row["produto_recomendado"],
        "descricao_recomendado": row["descricao_recomendado"],
    }
    response = chain.invoke(input=recomendacao)
    if "sim" in response.lower():
        update_recomendacao_valida(row["id"])
        print(f"Updated ID {row['id']} as valid recommendation.")


# COMMAND ----------

postgres.read_table(spark, "cesta_de_compras_recomendacaofeitamanualmente").display()
