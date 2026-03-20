# Databricks notebook source
# MAGIC %md
# MAGIC # Eval de Recomendações Convertidas
# MAGIC - Usa como fonte da verdade as cestas convertidas
# MAGIC - Usa o pareto dos produtos mais vendidos (produtos que representam 80% das vendas)
# MAGIC - Usa os 100 produtos mais vendidos (pareto)
# MAGIC - Verifica se na jornada `complementares`se o produto agregado na cesta está presente na consulta do vectorsearch
# MAGIC - Retorna um score com o percentual de quantos produtos recomendados que tiveram uma venda convertida está sendo retornada nos primeiro 10 registros do vectorsearch. Quanto maior esse número, maior a chance de estar recomendando um produto que é convertido
# MAGIC - `Alternativos` não foi considerado pois tem muitas regras de negócio e olhar semânticamento não faz muito sentido.
# MAGIC - TODO: Quando o `data-api` receber todas as regras de negócio do copilot, migrar esse notebook lá para avaliar nossas recomendações

# COMMAND ----------


from dataclasses import dataclass
from typing import List

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.environment.tables import BigqueryGold, CopilotTable
from maggulake.vector_search_retriever import (
    get_data_api_retriever,
)

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "eval_recomendacoes", dbutils, spark_config={"spark.sql.caseSensitive": "true"}
)

spark = env.spark

bigquery_schema = env.get_bigquery_schema()

# COMMAND ----------

DEBUG = dbutils.widgets.get("debug") == "true"

# retriever
catalog = env.settings.catalog
DATA_API_KEY = env.settings.data_api_key
DATA_API_URL = env.settings.data_api_url

# params vectorsearch
TEXT_COLUMNS = [
    "nome",
    "ean",
    'categorias',
]
# mesmos params que está no COPILOT
SCORE_THRESHOLD = 0.5
MAX_RESULTS = 10


# COMMAND ----------

# Leitura de tabelas
df_produtos_refined = env.table(Table.produtos_refined)
df_cesta_item = env.table(CopilotTable.cesta_de_compras_item)
df_recomendacoes = env.table(BigqueryGold.view_recomendacoes)

# COMMAND ----------

# MAGIC %md
# MAGIC Pega os produtos mais vendidos - Curva A

# COMMAND ----------

# Query que retorna os 100 produtos mais relevantes em venda dos últimos 6 meses
df_produtos_mais_vendidos = spark.sql(f"""
WITH base_vendas AS (
  SELECT
    p.nome,
    p.ean,
    SUM(vi.quantidade) AS quantidade_vendas,
    SUM(vi.valor_final) AS valor_vendas
  FROM {bigquery_schema}.vendas_item AS vi
  JOIN {bigquery_schema}.vendas_venda AS v ON vi.venda_id = v.id
  INNER JOIN {bigquery_schema}.contas_loja AS cl ON v.loja_id = cl.id
  INNER JOIN {bigquery_schema}.produtos_produtov2 AS p ON vi.produto_id = p.id
  WHERE v.venda_concluida_em >= add_months(CURRENT_DATE(), -6)
    AND cl.status IN ('ATIVA', 'EM_ATIVACAO')

  GROUP BY p.nome, p.ean
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
  quantidade_vendas,
  classificacao_abc,
  ean,
  nome
FROM com_classificacao
WHERE classificacao_abc = 'A' AND rank_vendas <= 100
ORDER BY quantidade_vendas DESC
""")

produtos_mais_vendidos = (
    df_produtos_mais_vendidos.select('ean').rdd.flatMap(lambda x: x).collect()
)

# COMMAND ----------

df_info_params_recomendacao = (
    df_produtos_refined.withColumn(
        'tags',
        F.array_distinct(
            F.array_union(
                F.array_union(
                    F.col('tags_complementares'), F.col('tags_potencializam_uso')
                ),
                F.col('tags_atenuam_efeitos'),
            )
        ),
    )
    .filter((F.col('tags').isNotNull()) | (F.size(F.col('tags')) > 0))
    .select(F.col('id').alias('ref_produto_id'), F.col("tags"))
)

# COMMAND ----------

# puxar infos addicionais para as consultas no vectorsearch

df_info_adicionais = df_produtos_refined.select(
    F.col('id'), F.col('ean'), F.col("categorias")
).cache()

df_info_prod_ref = df_info_adicionais.select(
    F.col('id').alias("ref_produto_id"),
    F.col('ean').alias("ref_ean"),
    F.col('categorias').alias("ref_categoria"),
)

df_info_prod_rec = df_info_adicionais.select(
    F.col('id').alias("rec_produto_id"),
    F.col('ean').alias("rec_ean"),
    F.col('categorias').alias("rec_categoria"),
)

# COMMAND ----------

df_recomendacoes = (
    df_recomendacoes.withColumn(
        'recommendation_time', F.to_date(F.col('recommendation_time'))
    )
    .filter(F.col("conversion_status") == 'CONVERTIDA')  # somente vendas convertidas
    .filter(F.col('recommendation_time') == (F.date_sub(F.current_date(), 1)))
    .filter(
        F.col('ref_ean').isin(produtos_mais_vendidos)
    )  # somente os produtos de curva A
    .join(
        df_info_prod_ref, on=['ref_ean'], how='inner'
    )  # pega categoria produto bipado
    .join(
        df_info_prod_rec, on=['rec_ean'], how='inner'
    )  # pega categoria produto recomendado
    .join(
        df_info_params_recomendacao, on=['ref_produto_id'], how='inner'
    )  # query do produto bipado
    .groupBy('ref_produto_id', 'ref_ean', 'ref_nome', 'ref_categoria', 'rec_tipo_venda')
    .agg(
        F.collect_set(F.col('rec_ean')).alias('rec_ean'),
        F.collect_set(F.col('rec_nome')).alias('rec_nome'),
        F.array_distinct(F.flatten(F.collect_list('rec_categoria'))).alias(
            'rec_categoria'
        ),
    )
    .join(df_info_params_recomendacao, on=['ref_produto_id'], how='inner')
)


df_recomendacoes_complementares = (
    df_recomendacoes.filter(F.col('rec_tipo_venda') == 'agregada').filter(
        F.size(F.col('rec_categoria')) >= 1
    )  # remove cestas com produtos recomendados sem categoria
).drop('informacoes_para_embeddings')

# COMMAND ----------


if not df_recomendacoes_complementares.take(1):
    dbutils.notebook.exit(
        "Não tem recomendações convertidas de complementares para avaliar"
    )
else:
    print(
        f"Vao ser analisadas {df_recomendacoes_complementares.count()} recomendações convertidas de complementares"
    )

# COMMAND ----------


@dataclass
class ComplementaresRecomendacaoResponse:
    nome: str
    ean: str
    categorias: list[str]

    def to_tuple(self) -> tuple:
        return (
            self.nome,
            self.ean,
            self.categorias,
        )


def process_retriever_response_data_api(
    raw_response: List[dict],
) -> List[ComplementaresRecomendacaoResponse]:
    results: List[ComplementaresRecomendacaoResponse] = []

    if not raw_response:
        return results

    for doc in raw_response:
        entity = doc.get('entity')
        if not entity:
            continue

        response_item = ComplementaresRecomendacaoResponse(
            nome=entity.get('nome'),
            ean=entity.get('ean'),
            categorias=entity.get('categorias', []),
        )
        results.append(response_item)

    return results


# COMMAND ----------

retriever = get_data_api_retriever(
    data_api_url=DATA_API_URL,
    data_api_key=DATA_API_KEY,
    max_results=MAX_RESULTS,
    score_threshold=SCORE_THRESHOLD,
    campos_output=TEXT_COLUMNS,
)

# COMMAND ----------

if DEBUG:
    print(retriever.invoke('Melatonina'))

# COMMAND ----------

OUTPUT_SCHEMA = StructType(
    [
        StructField("recomendacoes_vectorsearch_ean", ArrayType(StringType()), True),
        StructField("recomendacoes_vectorsearch_nome", ArrayType(StringType()), True),
        StructField("extra_info_categorias", ArrayType(StringType()), True),
    ]
)


# optei por udf para distribuir o processamento nos workers no cluster dos jobs
@F.pandas_udf(OUTPUT_SCHEMA)
def get_recomendacoes_udf(
    query_series: pd.Series, ref_ean_series: pd.Series
) -> pd.DataFrame:
    results = []
    eans_rec = []
    nome_rec = []
    extra_info_cat = []
    for query, ref_ean in zip(query_series, ref_ean_series):
        for tag in query:
            similar_docs_raw = retriever.invoke(tag)
            standard_data = process_retriever_response_data_api(similar_docs_raw)
            for item in standard_data:
                if item.ean and item.ean != ref_ean:
                    eans_rec.append(item.ean)
                    nome_rec.append(item.nome)
                    categorias = item.categorias or []
                    extra_info_cat.extend(categorias)

            eans_rec = list(set(eans_rec))
            nome_rec = list(set(nome_rec))
            extra_info_cat = list(set(extra_info_cat))

        results.append((eans_rec, nome_rec, extra_info_cat))

    return pd.DataFrame(
        results,
        columns=[
            "recomendacoes_vectorsearch_ean",
            "recomendacoes_vectorsearch_nome",
            "extra_info_categorias",
        ],
    )


# COMMAND ----------

# MAGIC %md
# MAGIC Complementares

# COMMAND ----------

teste = df_recomendacoes_complementares

df_eval_complementares = teste.withColumn(
    "recomendacoes", get_recomendacoes_udf(F.col("tags"), F.col("ref_ean"))
).withColumn(
    "recomendacao_check",
    F.when(
        F.size(
            F.array_intersect(
                F.col("rec_categoria"), F.col("recomendacoes.extra_info_categorias")
            )
        )
        > 0,
        1,
    ).otherwise(0),
)

# COMMAND ----------

# Hoje segui com esse caminho mais simplificado para termos pelo menos alguma métrica de como está nossa recomendação sob a ótica de informação
# TODO: Depois da migração das regras pro data-api, devemos mirar lá e salvar o histórico desse eval para acompanhar de perto e verificar a evolução

total_sum = df_eval_complementares.agg(F.sum('recomendacao_check')).collect()[0][0]
total_count = df_eval_complementares.count()
total_percentage = total_sum / total_count * 100
print(
    f"De {total_count} recomendações convertidas de complementares, {total_percentage:.2f}% dos produtos recomendados convertidos foram encontrados na busca do vectorsearch"
)
