# Databricks notebook source
# MAGIC %md
# MAGIC # Atualiza `ProdutoLojaExtraInfo` no Postgres
# MAGIC
# MAGIC A partir da tabela `refined.produtos_loja_refined`, vamos atualizar o postgres!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment

# COMMAND ----------

import math
from textwrap import dedent

import pandas as pd
import psycopg2
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from maggulake.io.postgres import PostgresAdapter
from maggulake.utils.df_utils import escape_string

# COMMAND ----------

# Configuração Spark
config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])
spark = (
    SparkSession.builder.appName("atualiza_produto_loja_extra_info")
    .config(conf=config)
    .getOrCreate()
)

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# Parâmetros e conexões
STAGE = dbutils.widgets.get("stage")
CATALOG = "production" if STAGE == "prod" else "staging"
USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")

postgres = PostgresAdapter(STAGE, USER, PASSWORD)

tabela_refined_path = f"{CATALOG}.refined.produtos_loja_refined"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê dados refinados do Spark

# COMMAND ----------

# Leitura da tabela refined
produtos_loja_refined = spark.read.table(tabela_refined_path)

# Seleciona apenas os campos necessários
campos_extras = [
    "id",
    "margem_media",
    "demanda_media_diaria",
    "dias_de_estoque",
    "classificacao_demanda_abc",
    "classificacao_margem_abc",
]
refined_extras = produtos_loja_refined.select(*campos_extras)

# COMMAND ----------

# Fazer rounding de "demanda_media_diaria" e "dias_de_estoque" para 3 casas decimais:
refined_extras = refined_extras.withColumn(
    "demanda_media_diaria",
    F.round(refined_extras.demanda_media_diaria, 3),
)
refined_extras = refined_extras.withColumn(
    "dias_de_estoque",
    F.round(refined_extras.dias_de_estoque, 3),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualiza ProdutoLojaExtraInfo no Postgres
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepara funções

# COMMAND ----------


def create_value_string(row: dict) -> str:
    """
    Cria a string de valores para uma linha do DataFrame para usar na query de update.
    """
    return f"""(
        '{escape_string(row["id"])}',
        {row["margem_media"] if pd.notna(row["margem_media"]) else 'NULL'},
        {row["demanda_media_diaria"] if pd.notna(row["demanda_media_diaria"]) else 'NULL'},
        {row["dias_de_estoque"] if pd.notna(row["dias_de_estoque"]) else 'NULL'},
        {f"'{escape_string(row['classificacao_demanda_abc'])}'" if pd.notna(row["classificacao_demanda_abc"]) else 'NULL'},
        {f"'{escape_string(row['classificacao_margem_abc'])}'" if pd.notna(row["classificacao_margem_abc"]) else 'NULL'}
    )"""


# COMMAND ----------


def update_produto_loja_extra_info_in_batches(
    df, postgres: PostgresAdapter, batch_size: int = 1000
):
    """
    Atualiza os campos extras de ProdutoLojaExtraInfo no Postgres usando ean_conta_loja como chave.
    Utiliza o método execute_query do PostgresAdapter seguindo o padrão do projeto.

    Returns:
        dict: Estatísticas do processamento com sucessos, erros e exceções
    """
    df_pandas = df.toPandas()
    # drop duplicates (in case of any)
    df_pandas = df_pandas.drop_duplicates(subset=["id"])
    total_rows = len(df_pandas)
    num_batches = math.ceil(total_rows / batch_size)

    # Contadores para estatísticas
    sucessos = 0
    erros = 0
    excecoes = 0

    update_base_query = dedent(
        """
        INSERT INTO produtos_produtolojaextrainfo (
            produto_loja_id,
            margem_media,
            demanda_media_diaria,
            dias_de_estoque,
            classificacao_demanda_abc,
            classificacao_margem_abc
        )
        SELECT
            uv.produto_loja_id::uuid,
            uv.margem_media::numeric,
            uv.demanda_media_diaria::numeric,
            uv.dias_de_estoque::numeric,
            uv.classificacao_demanda_abc,
            uv.classificacao_margem_abc
        FROM
            (VALUES {values}) AS uv (
                produto_loja_id,
                margem_media,
                demanda_media_diaria,
                dias_de_estoque,
                classificacao_demanda_abc,
                classificacao_margem_abc
            )
        -- Juntar com produtos_produtoloja para garantir que o produto_loja_id é válido.
        INNER JOIN
            produtos_produtoloja AS pl
                ON
            pl.id = uv.produto_loja_id::uuid
        ON CONFLICT (produto_loja_id) DO UPDATE SET
            margem_media              = EXCLUDED.margem_media,
            demanda_media_diaria      = EXCLUDED.demanda_media_diaria,
            dias_de_estoque           = EXCLUDED.dias_de_estoque,
            classificacao_demanda_abc = EXCLUDED.classificacao_demanda_abc,
            classificacao_margem_abc  = EXCLUDED.classificacao_margem_abc;
    """
    )

    postgres_connection = postgres.get_connection()

    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_rows)
        batch_df = df_pandas.iloc[start_idx:end_idx]

        try:
            values_list = [create_value_string(row) for _, row in batch_df.iterrows()]
            update_query = update_base_query.format(values=",".join(values_list))
            result = postgres.execute_query(
                update_query, connection=postgres_connection
            )

            # Verificação do resultado da query
            if result.loc[0].status == "error":
                erros += 1
                print(f"❌ ERRO no batch {batch_num + 1}/{num_batches}:")
                print(f"   Mensagem de erro: {result.loc[0].message}")
                print(f"   Linhas afetadas no batch: {len(values_list)}")
                print("   Primeiras linhas desse batch que deu erro:")
                for i, (_, row) in enumerate(batch_df.head(3).iterrows()):
                    print(f"     Linha {start_idx + i + 1}: id={row['id']}")
                print()
            else:
                sucessos += 1
                print(
                    f"✅ Processado batch {batch_num + 1:>4}/{num_batches} ({len(values_list)} linhas) - {result.loc[0].status}"
                )
        except psycopg2.Error as e:
            excecoes += 1
            print(f"❌ EXCEÇÃO no batch {batch_num + 1}/{num_batches}:")
            print(f"   Erro: {str(e)}")
            print(f"   Linhas afetadas no batch: {len(batch_df)}")
            print("   Primeiras linhas desse batch que deu erro:")
            for i, (_, row) in enumerate(batch_df.head(3).iterrows()):
                print(f"     Linha {start_idx + i + 1}: id={row['id']}")
            print()
        except KeyboardInterrupt:
            print("❌ PROCESSO INTERROMPIDO PELO USUÁRIO")
            break

    # Fechar conexão após o loop
    postgres_connection.close()

    # Resumo final
    print("\n📊 RESUMO DO PROCESSAMENTO:")
    print(f"   Total de batches: {num_batches}")
    print(f"   Total de linhas: {total_rows}")
    print(f"   ✅ Sucessos: {sucessos}")
    print(f"   ❌ Erros: {erros}")
    print(f"   ⚠️  Exceções: {excecoes}")

    return {
        "total_batches_a_serem_executados": num_batches,
        "total_rows_a_serem_executados": total_rows,
        "linhas_sucessos": sucessos,
        "linhas_erros": erros,
        "linhas_excecoes": excecoes,
        "batches_nao_foram_processados": num_batches - sucessos - erros - excecoes,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ### Executa batches

# COMMAND ----------

BATCH_SIZE = 10000

print(
    f"Serão atualizados {refined_extras.count()} objetos da tabela produtos_produtolojaextrainfo."
)
print(f"Serão processados {math.ceil(refined_extras.count() / BATCH_SIZE)} batches.")

# COMMAND ----------

# Executa o update em lote
resultado = update_produto_loja_extra_info_in_batches(
    refined_extras, postgres, batch_size=BATCH_SIZE
)

# COMMAND ----------

resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação: consulta alguns registros atualizados

# COMMAND ----------

df_check_db = postgres.read_table(spark, "produtos_produtolojaextrainfo")
df_check_dl = spark.read.table(tabela_refined_path)

# COMMAND ----------

id_exemplo = "edbef847-9718-4a34-9342-5624230fb979"
if STAGE == "dev":
    id_exemplo = "5a140b61-226e-4ff3-a468-214f88cffb79"

# Banco (Pandas)
display(df_check_db[df_check_db.produto_loja_id == id_exemplo])

# Delta (spark)
df_check_dl.select(*campos_extras).filter(f"id = '{id_exemplo}'").display()


# COMMAND ----------

# Visualizar o df final
df_check_db.limit(100).display()
