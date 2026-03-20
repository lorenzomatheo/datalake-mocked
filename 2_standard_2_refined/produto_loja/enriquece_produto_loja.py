# Databricks notebook source
# MAGIC %md # Enriquece Produtos Loja
# MAGIC
# MAGIC Script para enriquecer a tabela produtos_loja com métricas de margem, demanda, dias de estoque e classificações ABC.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from maggulake.enums import ClassificacaoABC
from maggulake.environment import CopilotTable, DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_produtos_loja_refined
from maggulake.utils.pyspark import to_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inicialização do ambiente Databricks

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "enriquece_produto_loja",
    dbutils,
)
DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura das tabelas de entrada

# COMMAND ----------

produtos_loja = env.table(CopilotTable.produtos_loja).cache()

# NOTE: nem era pra essa coluna existir... Tirei pois está formatada como string e isso quebra o código
produtos_loja = produtos_loja.drop("eans_alternativos")

if DEBUG:
    produtos_loja.limit(100).display()


# COMMAND ----------

# TODO: substituir essa view_vendas por uma view equivalente do bigquery (ou postgres)
view_vendas = env.table(Table.view_vendas).cache()


# criar coluna "data_realizada_em" que troca de timestamp para data (sem horas)
view_vendas = view_vendas.withColumn("data_realizada_em", F.to_date("realizada_em"))

if DEBUG:
    view_vendas.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo das métricas agregadas de vendas por produto/conta/loja

# COMMAND ----------


def calcula_metricas_vendas(vendas_df):
    # Calcular a primeira e última data de venda de todo o dataset
    min_data_venda = vendas_df.agg(F.min("data_realizada_em")).collect()[0][0]
    max_data_venda = vendas_df.agg(F.max("data_realizada_em")).collect()[0][0]
    total_dias_periodo = (max_data_venda - min_data_venda).days + 1

    return (
        vendas_df.groupBy("produto_ean", "conta_loja", "loja_id")
        .agg(
            F.sum("quantidade").alias("qtd_vendida"),
            # O preco_venda_desconto ja representa o valor total, ver notebook `sobe_vendas`
            F.sum("preco_venda_desconto").alias("valor_total_vendas"),
            F.sum("custo_compra").alias("custo_compra_vendas"),
            F.countDistinct("data_realizada_em").alias("dias_com_venda"),
            F.first("nome_loja").alias("nome_loja"),
            # TODO: depois passar um conversor de EAN para EAN principal, senao nao pega eans alternativos
            F.first("produto_id").alias("produto_id"),
            F.min("data_realizada_em").alias("data_primeira_venda"),
            F.max("data_realizada_em").alias("data_ultima_venda"),
        )
        .withColumn(
            "margem_media",
            (
                (F.col("valor_total_vendas") - F.col("custo_compra_vendas"))
                / F.when(F.col("qtd_vendida") > 0, F.col("qtd_vendida")).otherwise(
                    F.lit(1)
                )
            ),
        )
        .withColumn(
            "demanda_media_diaria", F.col("qtd_vendida") / F.lit(total_dias_periodo)
        )
    )


vendas_produto = calcula_metricas_vendas(view_vendas)

if DEBUG:
    vendas_produto.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join das 2 tabelas

# COMMAND ----------

produtos_loja_enriquecido = produtos_loja.join(
    vendas_produto,
    on=(produtos_loja.produto_id == vendas_produto.produto_id)
    & (produtos_loja.loja_id == vendas_produto.loja_id),
    how="left",
).drop(vendas_produto.produto_id, vendas_produto.loja_id)

if DEBUG:
    produtos_loja_enriquecido.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de dias de estoque (DOI)

# COMMAND ----------

produtos_loja_enriquecido = produtos_loja_enriquecido.withColumn(
    "dias_de_estoque",
    F.when(
        F.col("demanda_media_diaria") > 0,
        F.col("estoque_unid") / F.col("demanda_media_diaria"),
    ).otherwise(None),
)

if DEBUG:
    produtos_loja_enriquecido.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classificação ABC de demanda e margem

# COMMAND ----------

window_demanda = Window.partitionBy("tenant", "loja_id").orderBy(
    F.col("qtd_vendida").desc_nulls_last()
)
window_margem = Window.partitionBy("tenant", "loja_id").orderBy(
    F.col("margem_media").desc_nulls_last()
)

produtos_loja_enriquecido = produtos_loja_enriquecido.withColumn(
    "rank_demanda", F.row_number().over(window_demanda)
).withColumn("rank_margem", F.row_number().over(window_margem))

# COMMAND ----------

# Para classificação ABC, é necessário saber o total de produtos por conta/loja
window_count = Window.partitionBy("tenant", "loja_id")

produtos_loja_enriquecido = produtos_loja_enriquecido.withColumn(
    "total_produtos", F.count("produto_id").over(window_count)
)

# Usando ceil para garantir arredondamento correto dos percentuais
produtos_loja_enriquecido = (
    produtos_loja_enriquecido.withColumn(
        "classificacao_demanda_abc",
        F.when(F.col("qtd_vendida").isNull() | (F.col("qtd_vendida") == 0), F.lit(None))
        .when(
            F.col("rank_demanda") <= F.ceil(F.col("total_produtos") * 0.2),
            ClassificacaoABC.A.value,
        )
        .when(
            F.col("rank_demanda") <= F.ceil(F.col("total_produtos") * 0.5),
            ClassificacaoABC.B.value,
        )
        .otherwise(ClassificacaoABC.C.value),
    )
    .withColumn(
        "classificacao_margem_abc",
        F.when(F.col("rank_margem").isNull(), F.lit(None))
        .when(
            F.col("rank_margem") <= F.ceil(F.col("total_produtos") * 0.2),
            ClassificacaoABC.A.value,
        )
        .when(
            F.col("rank_margem") <= F.ceil(F.col("total_produtos") * 0.5),
            ClassificacaoABC.B.value,
        )
        .otherwise(ClassificacaoABC.C.value),
    )
    .cache()
)

if DEBUG:
    produtos_loja_enriquecido.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação de schema

# COMMAND ----------

if DEBUG:
    produtos_loja_enriquecido.printSchema()


# COMMAND ----------

produtos_loja_enriquecido = to_schema(
    df=produtos_loja_enriquecido,
    schema=schema_produtos_loja_refined,
)

if DEBUG:
    produtos_loja_enriquecido.limit(100).display()

# COMMAND ----------

if DEBUG:
    produtos_loja_enriquecido.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar

# COMMAND ----------

if DEBUG:
    print("Contagem de classificações ABC de DEMANDA:")
    demanda_count = produtos_loja_enriquecido.groupBy(
        "classificacao_demanda_abc"
    ).count()
    demanda_total = demanda_count.agg(F.sum("count")).collect()[0][0]
    demanda_count = demanda_count.withColumn(
        "percentual", F.round((F.col("count") / demanda_total) * 100, 1)
    )
    display(demanda_count)

    print("Contagem de classificações ABC de MARGEM:")
    margem_count = produtos_loja_enriquecido.groupBy("classificacao_margem_abc").count()
    margem_total = margem_count.agg(F.sum("count")).collect()[0][0]
    margem_count = margem_count.withColumn(
        "percentual", F.round((F.col("count") / margem_total) * 100, 1)
    )
    display(margem_count)


# COMMAND ----------

produtos_loja_enriquecido.write.mode("overwrite").partitionBy(
    "conta", "loja"
).saveAsTable(Table.produtos_loja_refined.value)
