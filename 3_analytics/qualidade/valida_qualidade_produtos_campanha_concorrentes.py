# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %pip install pydeequ==1.4.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os

os.environ["SPARK_VERSION"] = "3.5"  # necessário definir antes dos outros imports

# COMMAND ----------

import pydeequ  # https://github.com/awslabs/python-deequ?tab=readme-ov-file
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pydeequ.analyzers import (
    AnalysisRunner,
    AnalyzerContext,
    Completeness,
    Uniqueness,
)
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter

spark = (
    SparkSession.builder.appName(
        "valida_qualidade_dados_produtos_campanha_concorrentes"
    )
    .config(
        "spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5"
    )  # instalado via maven no cluster
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate()
)

# ambiente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# postgres
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, POSTGRES_USER, POSTGRES_PASSWORD)
postgres_correcao_manual_table = "produtos_produtocorrecaomanual"

# delta
enum_principio_ativo_medicamentos = f"{catalog}.utils.enum_principio_ativo_medicamentos"

# COMMAND ----------


# TODO: DRY — padroniza_title_case e convert_array_to_json duplicados aqui e em valida_qualidade_dados_produtos.py.
# TODO: mover para maggulake/utils/.
def padroniza_title_case_array(column_name):
    return F.transform(column_name, lambda x: F.initcap(x)).cast(
        T.ArrayType(T.StringType())
    )


def padroniza_title_case(column_name):
    return F.initcap(column_name).cast(T.StringType())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Planilha de Correções Manuais Copilot

# COMMAND ----------

correcoes_df = postgres.read_table(spark, table=postgres_correcao_manual_table)

correcoes_df = correcoes_df.withColumn(
    "categorias", F.from_json(F.col("categorias"), T.ArrayType(T.StringType()))
)
correcoes_df.limit(10).display()

# COMMAND ----------

# Ajuste de formatação algumas colunas

colunas_atualizar_sem_array = ['nome', 'nome_comercial']
colunas_atualizar_com_array = ['categorias']

for col in colunas_atualizar_sem_array:
    correcoes_df = correcoes_df.withColumn(col, padroniza_title_case(col))
for col in colunas_atualizar_com_array:
    correcoes_df = correcoes_df.withColumn(col, padroniza_title_case_array(col))

# COMMAND ----------

df_campanhas_concorrentes = spark.sql(
    """
    WITH competitor_eans AS (
    SELECT
        p.ean_em_campanha,
        t.ean_data.ean as ean_concorrente,
        t.ean_data.score as ean_concorrente_score,
        t.ean_data.frase as ean_concorrente_frase
    FROM production.standard.produtos_campanha_substitutos p
    LATERAL VIEW EXPLODE(p.eans_recomendar) t AS ean_data
    )
    SELECT
    ean_concorrente,
    pr.nome as nome_concorrente,
    pr.marca as marca_concorrente,
    ean_em_campanha,
    pr2.nome as nome_produto_em_campanha,
    pr2.marca as marca_campanha,
    ean_concorrente_frase as frase_melhor_opcao
    FROM competitor_eans
    INNER JOIN production.refined.produtos_refined pr ON pr.ean = ean_concorrente
    INNER JOIN production.refined.produtos_refined pr2 ON pr2.ean = ean_em_campanha
    --WHERE ean_concorrente_score = 1
    ORDER BY nome_produto_em_campanha;
"""
)

# COMMAND ----------

ean_concorrente = (
    df_campanhas_concorrentes.select("ean_concorrente")
    .distinct()
    .rdd.map(lambda x: x.ean_concorrente)
    .collect()
)
ean_campanha = (
    df_campanhas_concorrentes.select("ean_em_campanha")
    .distinct()
    .rdd.map(lambda x: x.ean_em_campanha)
    .collect()
)

# COMMAND ----------

df_refined = spark.read.table(f"{catalog}.refined.produtos_refined")

# COMMAND ----------

# Ajuste de formatação algumas colunas da refined

for col in colunas_atualizar_sem_array:
    df_refined = df_refined.withColumn(col, padroniza_title_case(col))
for col in colunas_atualizar_com_array:
    df_refined = df_refined.withColumn(col, padroniza_title_case_array(col))

# COMMAND ----------

nao_analisar = ["arquivo_s3", "apagado_em"]

df_refined = df_refined.drop(
    *[col for col in nao_analisar if col in df_refined.columns]
)


# COMMAND ----------

df_concorrentes = (df_refined.filter(F.col("ean").isin(ean_concorrente))).cache()

df_campanha = (df_refined.filter(F.col("ean").isin(ean_campanha))).cache()


print(f"Concorrentes: {df_concorrentes.count()}")
print(f"Campanhas: {df_campanha.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Validação Produtos Campanhas

# COMMAND ----------


def add_analyzers(runner, df):
    # Métricas de completude
    for col in df.columns:
        runner = runner.addAnalyzer(Completeness(col))

    # Métricas de unicidade
    for col in ["id", "ean"]:
        runner = runner.addAnalyzer(Uniqueness([col]))

    return runner


def _run_analysis(spark, df):
    try:
        runner = AnalysisRunner(spark).onData(df)
        runner = add_analyzers(runner, df)
        analysis_result = runner.run()

        return AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result).drop(
            "entity"
        )
    except TypeError:
        raise TypeError(
            "Não foi possível criar o runner, verifique se você instalou o spark.jars.packages maven no cluster."
        )


def process_validation_analysis(spark, df):
    result_dfs = []

    if df.count() > 0:
        result_df = _run_analysis(spark, df)
        result_dfs.append(result_df)

    return (
        result_dfs[0]
        if len(result_dfs) == 1
        else result_dfs[0].unionAll(result_dfs[1])
        if result_dfs
        else None
    )


# COMMAND ----------

result_df_campanha = process_validation_analysis(spark, df_campanha)
result_df_campanha.display()

# COMMAND ----------

# Adicionando colunas de filtros
result_df_campanha = result_df_campanha.withColumn("tipo_produto", F.lit('campanha'))


# COMMAND ----------

# MAGIC %md
# MAGIC # Validação Produtos Concorrentes

# COMMAND ----------

result_df_concorrente = process_validation_analysis(spark, df_concorrentes)
result_df_concorrente.display()

# COMMAND ----------

result_df_concorrente = result_df_concorrente.withColumn(
    "tipo_produto", F.lit('concorrente')
)

# COMMAND ----------

final_df = result_df_campanha.union(result_df_concorrente)

final_df = final_df.withColumn("gerado_em", F.lit(F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Análise Detalhada
# MAGIC
# MAGIC - Análise nível ean das colunas selecionadas abaixo para ver se o que está cadastrado na planilhan reflete no datalake
# MAGIC
# MAGIC - Analisando algumas colunas principais:  `categorias`, `marca`, `fabricante`, `imagem_url`, `nome_comercial`, `indicacao`, `instrucao_de_uso`

# COMMAND ----------

# Categorias cadastradas na planilha
categorias_list_refined = (
    correcoes_df.select(F.explode("categorias"))
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect()
)

# Condição para filtro comparativo entre string e array
condition = " OR ".join(
    [f"array_contains(ad.categorias, '{value}')" for value in categorias_list_refined]
)

# COMMAND ----------

# Analise das outras colunas selecionadas
agg_df = correcoes_df.agg(
    F.collect_set("marca").alias("marca_list_refined"),
    F.collect_set("fabricante").alias("fabricante_list_refined"),
    F.collect_set("nome_comercial").alias("nome_comercial_list_refined"),
    F.collect_set("indicacao").alias("indicacao_list_refined"),
    F.collect_set("instrucoes_de_uso").alias("instrucoes_de_uso_list_refined"),
    F.collect_set("imagem_url").alias("imagem_url_list_refined"),
    F.collect_set("principio_ativo").alias("principio_ativo_list_refined"),
    F.collect_set("via_administracao").alias("via_administracao_list_refined"),
)

# Retornando em uma lista todas as colunas selecionadas
valores_planilha_correcoes = agg_df.collect()[0]

# COMMAND ----------

produtos_total = ean_campanha + ean_concorrente

# COMMAND ----------

# Analise Compliance
df_analise_detalhada = (
    df_refined.alias('ad')
    .join(correcoes_df.alias("cor"), how='left', on='ean')
    .filter(F.col('ean').isin(produtos_total))
    .withColumn(
        "invalid_columns",
        F.concat_ws(
            ", ",
            F.when(
                (F.col("cor.marca").isNotNull())
                & (
                    ~F.col("ad.marca").isin(
                        valores_planilha_correcoes.marca_list_refined
                    )
                ),
                F.lit("marca"),
            ),
            F.when(
                (F.col("cor.fabricante").isNotNull())
                & (
                    ~F.col("ad.fabricante").isin(
                        valores_planilha_correcoes.fabricante_list_refined
                    )
                ),
                F.lit("fabricante"),
            ),
            F.when(
                (F.col("cor.nome_comercial").isNotNull())
                & (
                    ~F.col("ad.nome_comercial").isin(
                        valores_planilha_correcoes.nome_comercial_list_refined
                    )
                ),
                F.lit("nome_comercial"),
            ),
            F.when(
                (F.col("cor.indicacao").isNotNull())
                & (
                    ~F.col("ad.indicacao").isin(
                        valores_planilha_correcoes.indicacao_list_refined
                    )
                ),
                F.lit("indicacao"),
            ),
            F.when(
                (F.col("cor.instrucoes_de_uso").isNotNull())
                & (
                    ~F.col("ad.instrucoes_de_uso").isin(
                        valores_planilha_correcoes.instrucoes_de_uso_list_refined
                    )
                ),
                F.lit("instrucoes_de_uso"),
            ),
            F.when(
                (F.col("cor.imagem_url").isNotNull())
                & (
                    ~F.col("ad.imagem_url").isin(
                        valores_planilha_correcoes.imagem_url_list_refined
                    )
                ),
                F.lit("imagem_url"),
            ),
            F.when(
                (F.col("cor.categorias").isNotNull()) & (~F.expr(condition)),
                F.lit("categorias"),
            ),
            F.when(
                (F.col("cor.principio_ativo").isNotNull())
                & (
                    ~F.col("ad.principio_ativo").isin(
                        valores_planilha_correcoes.principio_ativo_list_refined
                    )
                ),
                F.lit("principio_ativo"),
            ),
            F.when(
                (F.col("cor.via_administracao").isNotNull())
                & (
                    ~F.col("ad.via_administracao").isin(
                        valores_planilha_correcoes.via_administracao_list_refined
                    )
                ),
                F.lit("via_administracao"),
            ),
        ),
    )
    .selectExpr(
        "ad.ean",
        "invalid_columns",
        "ad.nome_comercial as nome_comercial_datalake",
        "cor.nome_comercial as nome_comercial_planilha",
        "ad.fabricante as fabricante_datalake",
        "cor.fabricante as fabricante_planilha",
        "ad.marca as marca_datalake",
        "cor.marca as marca_planilha",
        "ad.indicacao as indicacao_datalake",
        "cor.indicacao as indicacao_planilha",
        "ad.instrucoes_de_uso as instrucoes_de_uso_datalake",
        "cor.instrucoes_de_uso as instrucoes_de_uso_planilha",
        "ad.categorias as categorias_datalake",
        "cor.categorias as categorias_planilha",
        "ad.imagem_url as imagem_url_datalake",
        "cor.imagem_url as imagem_url_planilha",
        "ad.tags_complementares as tags_complementares_datalake",
        "cor.tags_complementares as tags_complementares_planilha",
        "ad.tags_atenuam_efeitos as tags_atenuam_efeitos_datalake",
        "cor.tags_atenuam_efeitos as tags_atenuam_efeitos_planilha",
        "ad.principio_ativo as principio_ativo_datalake",
        "cor.principio_ativo as principio_ativo_planilha",
        "ad.via_administracao as via_administracao_datalake",
        "cor.via_administracao as via_administracao_planilha",
    )
)

# COMMAND ----------

final_analise_detalhada_df = df_analise_detalhada.withColumn(
    'tipo_produto',
    F.when(F.col('ean').isin(ean_campanha), F.lit('campanha')).when(
        F.col('ean').isin(ean_concorrente), F.lit('concorrente')
    ),
)

final_analise_detalhada_df = final_analise_detalhada_df.withColumn(
    "gerado_em", F.lit(F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Salva no delta table

# COMMAND ----------

final_df.write.option("mergeSchema", "true").mode("append").saveAsTable(
    f"{catalog}.analytics.view_data_quality_produtos_campanha_concorrente"
)

# COMMAND ----------

final_analise_detalhada_df.write.option("mergeSchema", "true").mode(
    "append"
).saveAsTable(
    f"{catalog}.analytics.view_data_quality_produtos_campanha_concorrente_analise_detalhada"
)
