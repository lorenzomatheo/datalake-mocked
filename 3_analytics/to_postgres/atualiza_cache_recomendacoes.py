# Databricks notebook source
# MAGIC %md
# MAGIC # Atualiza Cache Recomendacoes
# MAGIC - Esse script tem como objetivo atualizar o cache de recomendações baseado nas tags (atenuam_efeitos, potencializam_uso, complementares) de produtos através do vectorsearch
# MAGIC - Consulta recomendações no cache para a tag atual, caso não encontre, salva a nova recomendação e atualiza a tabela `recomendacoes_vectorsearch`

# COMMAND ----------

# MAGIC %pip install langchain_google_genai langchain-google-vertexai psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiente

# COMMAND ----------

import base64
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from psycopg2.extras import Json
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.io.postgres import PostgresAdapter
from maggulake.utils.time import agora_em_sao_paulo_str
from maggulake.vector_search_retriever import (
    get_data_api_retriever,
)

env = DatabricksEnvironmentBuilder.build("atualiza_cache_recomendacoes", dbutils)
STAGE = env.settings.name_short
DEBUG = dbutils.widgets.get("debug") == "true"
CATALOG = env.settings.catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

# postgres
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(STAGE, POSTGRES_USER, POSTGRES_PASSWORD)

# vector search
DATA_API_KEY = env.settings.data_api_key
DATA_API_URL = env.settings.data_api_url
DEFAULT_MAX_RESULTS = 20
DEFAULT_SCORE_THRESHOLD = 0.5
DEFAULT_RERANK = False
DEFAULT_PERMITE_TARJADO = False

# spark
spark = env.spark

# delta tables
bigquery_schema = env.get_bigquery_schema()

# delta table
RECOMENDACOES_CACHE_TABLE = f"{bigquery_schema}.recomendacoes_vectorsearchcache"

# COMMAND ----------

# Query com os produtos mais relevantes em vendas na última semana
query_classificacao_abc = f"""
    WITH base_vendas AS (
      SELECT
        p.id,
       ARRAY_DISTINCT(
        ARRAY_UNION(
            ARRAY_UNION(
                from_json(p.tags_complementares, 'ARRAY<STRING>'),
                from_json(p.tags_potencializam_uso, 'ARRAY<STRING>')
            ),
            from_json(p.tags_atenuam_efeitos, 'ARRAY<STRING>')
        )
    ) AS todas_tags,
        SUM(vi.quantidade) AS quantidade_vendas,
        SUM(vi.valor_final) AS valor_vendas
    FROM {bigquery_schema}.vendas_item AS vi
    JOIN {bigquery_schema}.vendas_venda AS v ON vi.venda_id = v.id
    INNER JOIN {bigquery_schema}.contas_loja AS cl ON v.loja_id = cl.id
    INNER JOIN {bigquery_schema}.produtos_produtov2 AS p ON vi.produto_id = p.id
    WHERE v.venda_concluida_em >= DATE_SUB(CURRENT_DATE(), 7)
      AND cl.status IN ('ATIVA', 'EM_ATIVACAO')

    GROUP BY p.id, todas_tags
    ),

  ordenado AS (
    SELECT
      *,
      RANK() OVER (ORDER BY valor_vendas DESC) AS rank_vendas,
      SUM(valor_vendas) OVER () AS total_geral,
      SUM(valor_vendas) OVER (ORDER BY valor_vendas DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado
    FROM base_vendas
  ),

  com_classificacao AS (
    SELECT
      *,
      ROUND(acumulado / total_geral, 4) AS perc_acumulado,
      CASE
        WHEN acumulado / total_geral <= 0.80 THEN 'A'
        WHEN acumulado / total_geral <= 0.95 THEN 'B'
        ELSE 'C'
      END AS classificacao_abc
    FROM ordenado
  )

  SELECT
    id,
    todas_tags
  FROM com_classificacao
  WHERE classificacao_abc = 'A'
  ORDER BY quantidade_vendas DESC
"""


lista_tags_produtos_mais_vendidos = (
    spark.sql(query_classificacao_abc)
    .withColumn("todas_tags", F.explode(F.col("todas_tags")))
    .select('todas_tags')
    .distinct()
    .rdd.map(lambda x: x[0])
    .collect()
)

# COMMAND ----------

# Query que busca os produtos que estão em campanha ATIVA
query_produtos_campanha = f"""
    SELECT DISTINCT
    p.id,
    ARRAY_DISTINCT(
        ARRAY_UNION(
            ARRAY_UNION(
                from_json(p.tags_complementares, 'ARRAY<STRING>'),
                from_json(p.tags_potencializam_uso, 'ARRAY<STRING>')
            ),
            from_json(p.tags_atenuam_efeitos, 'ARRAY<STRING>')
        )
    ) AS todas_tags
        FROM {bigquery_schema}.produtos_produtov2 p
        INNER JOIN {bigquery_schema}.campanhas_campanhaproduto cp ON p.id = cp.produto_id AND cp.removido_em IS NULL
        INNER JOIN {bigquery_schema}.campanhas_campanha c ON c.id = cp.campanha_id
    WHERE c.status = 'ATIVA'
"""


lista_tags_produtos_campanha = (
    spark.sql(query_produtos_campanha)
    .withColumn("todas_tags", F.explode(F.col("todas_tags")))
    .select('todas_tags')
    .distinct()
    .rdd.map(lambda x: x[0])
    .collect()
)

# COMMAND ----------

# Pega as queries únicas entre ambas
tags = list(set(lista_tags_produtos_mais_vendidos + lista_tags_produtos_campanha))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define vector search

# COMMAND ----------

vs = get_data_api_retriever(
    data_api_url=DATA_API_URL,
    data_api_key=DATA_API_KEY,
    format_return=True,  # importante para o retorno das recomendacoes para o cache
)

# COMMAND ----------


@dataclass
class VectorSearchParameters:
    max_results: int = DEFAULT_MAX_RESULTS
    score_threshold: Optional[float] = DEFAULT_SCORE_THRESHOLD
    eans_subset: Optional[list[str]] = None
    rerank: Optional[bool] = DEFAULT_RERANK
    permite_tarjado: Optional[bool] = DEFAULT_PERMITE_TARJADO
    principio_ativo: Optional[str] = None
    categorias: Optional[list[str]] = None
    via_administracao: Optional[str] = None
    idade_recomendada: Optional[str] = None
    sexo_recomendado: Optional[str] = None
    sem_medicamentos: Optional[bool] = None

    def asdict(self) -> dict:
        return {
            'max_results': self.max_results,
            'score_threshold': self.score_threshold,
            'eans_subset': self.eans_subset,
            'rerank': self.rerank,
            'permite_tarjado': self.permite_tarjado,
            'principio_ativo': self.principio_ativo,
            'categorias': self.categorias,
            'via_administracao': self.via_administracao,
            'idade_recomendada': self.idade_recomendada,
            'sexo_recomendado': self.sexo_recomendado,
            'sem_medicamentos': self.sem_medicamentos,
        }


default_params = json.dumps(VectorSearchParameters().asdict(), ensure_ascii=False)

# COMMAND ----------

# Funcoes de apoio para comprimir as respostas e gerar os hashes


def get_hash(query: Optional[str] = None, parameters: Optional[dict] = None) -> str:
    if parameters is not None:
        raw = json.dumps(parameters, sort_keys=True).encode()
    elif query is not None:
        raw = query.encode()
    else:
        raise ValueError("É necessário passar 'query' ou 'parameters'.")

    hasher = hashlib.sha256()
    hasher.update(raw)
    return base64.b64encode(hasher.digest()).decode()


hash_udf = F.udf(get_hash, T.StringType())

# COMMAND ----------

# Tabela de cache
df_vector_search_cache = spark.read.table(RECOMENDACOES_CACHE_TABLE)

# COMMAND ----------

df_tags = spark.createDataFrame([(tag,) for tag in tags], schema='query STRING')
df_tags_com_params = (
    df_tags.withColumn('params', F.lit(default_params))
    .withColumn('parameters_hash', hash_udf(F.col("params")))
    .withColumn('query_hash', hash_udf(F.col("query")))
)

# COMMAND ----------


def verificar_cache_de_tags(df_tags_a_verificar, df_cache_delta):
    print(f"{agora_em_sao_paulo_str()} - Iniciando a verificação de cache...")

    # --- Passo 1: Definir o limite de tempo para considerar um cache "velho" (stale) ---
    dias_antes_stale = 7
    limite_tempo = datetime.now() - timedelta(days=dias_antes_stale)

    print(
        f"Considerando entradas de cache geradas após {limite_tempo.strftime('%Y-%m-%d')} como válidas."
    )

    # --- Passo 2: Filtrar a tabela de cache para manter apenas as entradas recentes ---

    df_cache_recente = df_cache_delta.filter(F.col("gerado_em") >= limite_tempo)

    # --- Passo 3: Verfica se query já existe na tabela de cache ---

    df_resultado_join = df_tags_a_verificar.join(
        df_cache_recente.withColumnRenamed("query", "cache_query"),
        on=[df_tags_a_verificar.query_hash == F.col("hash")],
        how="left",
    )

    # --- Passo 4: Determinar o status do cache (HIT ou MISS) ---
    df_com_status = df_resultado_join.withColumn(
        "cache_status",
        F.when(F.col("id").isNotNull(), F.lit("HIT")).otherwise(F.lit("MISS")),
    )

    # --- Passo 5: Selecionar apenas cache mais recente ---
    janela_cache = Window.partitionBy("query").orderBy(F.desc("gerado_em"))
    df_com_status = (
        df_com_status.withColumn("row", F.row_number().over(janela_cache))
        .filter(F.col("row") == 1)
        .drop("row")
    )
    # --- Passo 6: Selecionar colunas de interesse ---
    df_final = df_com_status.select(
        df_tags_a_verificar.query,
        df_tags_a_verificar.params,
        df_tags_a_verificar.parameters_hash,
        "cache_status",
        F.col("id").alias("cache_id"),
        F.col("gerado_em").alias("cache_gerado_em"),
        F.col('hash').alias('hash_cache'),
    )

    len_total = df_final.count()
    len_hit = df_final.filter(F.col("cache_status") == "HIT").count()
    len_miss = df_final.filter(F.col("cache_status") == "MISS").count()

    print(
        f"{agora_em_sao_paulo_str()} - Cache HIT: {len_hit} / {len_total} ({len_hit / len_total * 100:.2f}%)"
    )
    print(
        f"{agora_em_sao_paulo_str()} - Cache MISS: {len_miss} / {len_total} ({len_miss / len_total * 100:.2f}%)"
    )
    print(f"{agora_em_sao_paulo_str()} - Verificação de cache finalizada.")

    return df_final


# COMMAND ----------

df_check_cache = verificar_cache_de_tags(df_tags_com_params, df_vector_search_cache)

# COMMAND ----------

# Define o schema do DataFrame que será retornado pela UDF.
schema_retorno = T.StructType(
    [
        T.StructField("id", T.StringType(), False),
        T.StructField("hash", T.BinaryType(), False),
        T.StructField("query", T.StringType(), False),
        T.StructField("parameters_hash", T.StringType(), False),
        T.StructField("parameters", T.StringType(), False),
        T.StructField("response", T.StringType(), True),
    ]
)


def processa_recomendacoes_cache(iterator):
    """
    Processa um iterador de DataFrames pandas (partições)
    """
    vs = get_data_api_retriever(
        data_api_url=DATA_API_URL,
        data_api_key=DATA_API_KEY,
        format_return=True,
    )

    for pdf in iterator:
        results = []
        total_linhas = len(pdf)
        print(f"Iniciando processamento de uma partição com {total_linhas} linhas.")

        # Itera sobre as linhas do DataFrame pandas
        for idx, row in pdf.iterrows():
            print(f"  [Partição] Processando linha {idx + 1} de {total_linhas}...")

            query = row['query']
            params_str = row['params']
            param_hash_b64 = row['parameters_hash']
            hash_cache = row['hash_cache']
            cache_id = row['cache_id']
            cache_status = row['cache_status']

            # 1. Busca as recomendações
            recommendations_docs = vs.iterate(query)

            # Converte para lista de dicts
            recommendations_list = [doc.model_dump() for doc in recommendations_docs]

            # Converte para JSON string
            response_str = json.dumps(recommendations_list, default=str)

            # 2.Gera hash
            main_hash = get_hash(query)

            # 3. Se o cache existir, reaproveitar o hash e id já salvos
            if cache_status == "HIT":
                results.append(
                    {
                        "id": cache_id,
                        "hash": hash_cache,
                        "query": query,
                        "parameters_hash": param_hash_b64,
                        "parameters": params_str,
                        "response": response_str,
                    }
                )
            else:
                results.append(
                    {
                        "id": str(uuid.uuid4()),
                        "hash": base64.b64decode(main_hash),
                        "query": query,
                        "parameters_hash": param_hash_b64,
                        "parameters": params_str,
                        "response": response_str,
                    }
                )

        yield pd.DataFrame(results)


# COMMAND ----------

# Aplicar o processamento
df_cache_recomendacoes = (
    df_check_cache.mapInPandas(processa_recomendacoes_cache, schema=schema_retorno)
    .withColumn("gerado_em", F.current_timestamp())
    .withColumn("loja_id", F.lit(None).cast(T.StringType()))
    .withColumn("produto_id", F.lit(None).cast(T.StringType()))
    .withColumn(
        'response_gzip', F.lit(None).cast(T.StringType())
    )  # Nao salvar gzip no cache
    .withColumn(
        'response_json',
        F.from_json(
            F.col('response'),
            'ARRAY<STRUCT<id: STRING,metadata: STRUCT<id: STRING,score: DOUBLE,ean: STRING,nome: STRING>,page_content: STRING,type: STRING>>',
        ),
    )
    .filter(F.size('response_json') > 0)  # elimina queries que não retornam resultados
    .drop('response_json')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Salvar resultados no postgres

# COMMAND ----------

df = df_cache_recomendacoes.toPandas()


def update_recomendacoes_cache(df: pd.DataFrame, do_commit: bool = True):
    conn = postgres.get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    columns = df.columns.tolist()
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)

    update_cols = ["response", "response_gzip", "gerado_em"]
    update_clause = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = f"""
        INSERT INTO recomendacoes_vectorsearchcache ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (hash) DO UPDATE
        SET {update_clause};
    """

    try:
        for _, row in df.iterrows():
            params = list(row)

            # Garante formato jsonb
            if "response" in columns:
                params[columns.index("response")] = Json(
                    json.loads(json.dumps(row["response"], default=str))
                )
            if "response_gzip" in columns:
                params[columns.index("response_gzip")] = Json(
                    json.loads(json.dumps(row["response_gzip"], default=str))
                )

            cur.execute(sql, params)

        if do_commit:
            conn.commit()
            print("✅ Transaction committed.")
        else:
            conn.rollback()
            print("🔄 Transaction rolled back (test mode).")

    except Exception as e:
        conn.rollback()
        print("❌ Error occurred, rolled back. Details:", e)
        raise

    finally:
        cur.close()
        conn.close()


# COMMAND ----------

update_recomendacoes_cache(df, do_commit=True)
