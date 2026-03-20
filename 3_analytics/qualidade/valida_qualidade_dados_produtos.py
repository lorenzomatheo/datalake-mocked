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
    Compliance,
    Uniqueness,
)
from pyspark.sql import SparkSession

from maggulake.enums import (
    FaixaEtaria,
    FormaFarmaceutica,
    SexoRecomendado,
    TiposReceita,
    ViaAdministracao,
)
from maggulake.io.postgres import PostgresAdapter
from maggulake.produtos.dosagem import normaliza_dosagem_udf
from maggulake.utils.df_utils import normalize_column_accent

spark = (
    SparkSession.builder.appName("valida_qualidade_dados_produtos")
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

# delta
enum_principio_ativo_medicamentos = f"{catalog}.utils.enum_principio_ativo_medicamentos"

# COMMAND ----------

df = spark.read.table(f"{catalog}.refined.produtos_refined")


df = normalize_column_accent(df, "principio_ativo")

# COMMAND ----------

produtos_em_campanha_list = postgres.get_eans_em_campanha(spark)
print(
    f"Temos {len(produtos_em_campanha_list)} produtos em campanha: {produtos_em_campanha_list}"
)

# COMMAND ----------

nao_analisar = ["arquivo_s3", "apagado_em", "fonte"]

df = df.drop(*[col for col in nao_analisar if col in df.columns])

# COMMAND ----------


# TODO: definir na maggulake.utils
def convert_array_to_json(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, T.ArrayType):
            df = df.withColumn(field.name, F.to_json(F.col(field.name)))
    return df


df = convert_array_to_json(df)  # a lib não aceita arrays

# COMMAND ----------

colunas_medicamentos = [
    "eh_tarjado",
    "eh_controlado",
    "dosagem",
    "classes_terapeuticas",
    "bula",
    "variantes",
    "especialidades",
    "contraindicacoes",
    "efeitos_colaterais",
    "interacoes_medicamentosas",
    "advertencias_e_precaucoes",
    "tipo_receita",
    "tipo_de_receita_completo",
    "forma_farmaceutica",
    "via_administracao",
    "principio_ativo",
    "tarja",
    "validade_receita_dias",
    "medicamentos_referencia",
    "medicamentos_similares",
    "idade_recomendada",
    "sexo_recomendado",
    "tamanho_produto",
    'instrucoes_de_uso',
    'condicoes_de_armazenamento',
    'validade_apos_abertura',
    'volume_quantidade',
]

df_medicamentos = df.filter(F.col("eh_medicamento") == True).withColumn(
    "produto_em_campanha",
    F.when(F.col("ean").isin(produtos_em_campanha_list), F.lit(True)).otherwise(
        F.lit(False)
    ),
)

df_outros = (
    df.filter((F.col("eh_medicamento") == False) | (F.col("eh_medicamento").isNull()))
    .drop(*colunas_medicamentos)
    .withColumn(
        "produto_em_campanha",
        F.when(F.col("ean").isin(produtos_em_campanha_list), F.lit(True)).otherwise(
            F.lit(False)
        ),
    )
)


print(f"Medicamentos: {df_medicamentos.count()}")
print(f"Outros: {df_outros.count()}")

# COMMAND ----------

# Valores retirados da tabela production.refined.produtos_refined em 2024.Dec.08
df_p_ativos_enum = spark.read.table(enum_principio_ativo_medicamentos)

p_ativos_enum = df_p_ativos_enum.select("principio_ativo").distinct().collect()
p_ativos_list = [(row.principio_ativo) for row in p_ativos_enum]
p_ativos_tuple = tuple(row.principio_ativo for row in p_ativos_enum)

print(f"Um total de {len(p_ativos_enum)} princípios ativos estão cadastrados no Enum.")


# COMMAND ----------

# MAGIC %md
# MAGIC # Validação de medicamentos

# COMMAND ----------

enums = {
    "tipo_receita": TiposReceita.tuple(),
    "forma_farmaceutica": FormaFarmaceutica.tuple(),
    "via_administracao": ViaAdministracao.tuple(),
    "idade_recomendada": FaixaEtaria.tuple(),
    "sexo_recomendado": SexoRecomendado.tuple(),
    "principio_ativo": p_ativos_tuple,
}


def add_analyzers(runner, df, enums):
    # Métricas de completude
    for col in df.columns:
        runner = runner.addAnalyzer(Completeness(col))

    # Métricas de unicidade
    for col in ["id", "ean"]:
        runner = runner.addAnalyzer(Uniqueness([col]))

    # Métricas compliance
    compliance_rules = [
        ("nome_comercial_70caracteres", "len(nome_comercial) <= 70"),
        ("medicamentos_referencia_preenchido", "len(medicamentos_referencia) > 5"),
        ("medicamentos_similares_preenchido", "len(medicamentos_similares) > 5"),
    ]

    compliance_rules.extend(
        [(f"{key}_enum", f"{key} in {value}") for key, value in enums.items()]
    )

    for name, rule in compliance_rules:
        runner = runner.addAnalyzer(Compliance(name, rule))

    return runner


def _run_analysis(spark, df, enums, is_em_campanha):
    try:
        runner = AnalysisRunner(spark).onData(df)
        runner = add_analyzers(runner, df, enums)
        analysis_result = runner.run()

        return (
            AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)
            .drop("entity")
            .withColumn("tipo_produto", F.lit("medicamentos"))
            .withColumn("produto_em_campanha", F.lit(is_em_campanha))
        )
    except TypeError:
        raise TypeError(
            "Não foi possível criar o runner, verifique se você instalou o spark.jars.packages maven no cluster."
        )


def process_medicamentos_analysis(spark, df_medicamentos, enums):
    result_dfs = []

    for is_em_campanha in [True, False]:
        df_filtered = df_medicamentos.filter(
            F.col("produto_em_campanha") == is_em_campanha
        )

        if df_filtered.count() > 0:
            result_df = _run_analysis(spark, df_filtered, enums, is_em_campanha)
            result_dfs.append(result_df)

    return (
        result_dfs[0]
        if len(result_dfs) == 1
        else result_dfs[0].unionAll(result_dfs[1])
        if result_dfs
        else None
    )


# COMMAND ----------

result_df_medicamentos = process_medicamentos_analysis(spark, df_medicamentos, enums)
result_df_medicamentos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Validação outros produtos

# COMMAND ----------


def add_analyzers_outros(runner, df):
    # Métricas de unicidade
    for col in ["id", "ean"]:
        runner = runner.addAnalyzer(Uniqueness([col]))

    # Métricas de completude
    for col in df.columns:
        runner = runner.addAnalyzer(Completeness(col))

    # Métricas compliance
    runner = runner.addAnalyzer(
        Compliance("nome_comercial_70caracteres", "len(nome_comercial) <= 70")
    )

    return runner


def _run_analysis_outros(spark, df, is_em_campanha):
    try:
        runner = AnalysisRunner(spark).onData(df)
        runner = add_analyzers_outros(runner, df)
        analysis_result = runner.run()

        return (
            AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)
            .drop("entity")
            .withColumn("tipo_produto", F.lit("outros"))
            .withColumn("produto_em_campanha", F.lit(is_em_campanha))
        )
    except TypeError:
        raise TypeError(
            "Não foi possível criar o runner, verifique se você instalou o spark.jars.packages maven no cluster."
        )


def process_outros_analysis(spark, df_outros):
    result_dfs = []

    for is_em_campanha in [True, False]:
        df_filtered = df_outros.filter(F.col("produto_em_campanha") == is_em_campanha)

        if df_filtered.count() > 0:
            result_df = _run_analysis_outros(spark, df_filtered, is_em_campanha)
            result_dfs.append(result_df)

    return (
        result_dfs[0]
        if len(result_dfs) == 1
        else result_dfs[0].unionAll(result_dfs[1])
        if result_dfs
        else None
    )


# COMMAND ----------

result_df_outros = process_outros_analysis(spark, df_outros)
result_df_outros.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Análise de normalização de dosagem

# COMMAND ----------

_total_med = df_medicamentos.count()
_com_dosagem = df_medicamentos.filter(F.col("dosagem").isNotNull()).count()
_pct = round(_com_dosagem / _total_med * 100, 1) if _total_med else 0.0

_dosagem_distintas_antes = (
    df_medicamentos.filter(F.col("dosagem").isNotNull())
    .select("dosagem")
    .distinct()
    .count()
)

print(f"Fill rate dosagem (medicamentos): {_com_dosagem}/{_total_med} ({_pct}%)")
print(f"Valores distintos de dosagem: {_dosagem_distintas_antes}")

# COMMAND ----------

_df_normalizado = df_medicamentos.filter(F.col("dosagem").isNotNull()).withColumn(
    "dosagem_normalizada", normaliza_dosagem_udf(F.col("dosagem"))
)

_df_muda = _df_normalizado.filter(
    F.col("dosagem_normalizada").isNull()
    | (F.col("dosagem") != F.col("dosagem_normalizada"))
)

print(f"Valores que seriam alterados pela normalização: {_df_muda.count()}")
_df_muda.select("dosagem", "dosagem_normalizada").distinct().orderBy(
    "dosagem"
).display()

# COMMAND ----------

_dosagem_distintas_depois = (
    _df_normalizado.filter(F.col("dosagem_normalizada").isNotNull())
    .select("dosagem_normalizada")
    .distinct()
    .count()
)

_reducao = _dosagem_distintas_antes - _dosagem_distintas_depois

print(f"Valores distintos ANTES da normalização: {_dosagem_distintas_antes}")
print(f"Valores distintos DEPOIS da normalização: {_dosagem_distintas_depois}")
print(f"Redução de valores distintos: {_reducao} consolidados")

# COMMAND ----------

# MAGIC %md
# MAGIC Salva no delta table

# COMMAND ----------

final_df = result_df_medicamentos.union(result_df_outros)

final_df = final_df.withColumn("gerado_em", F.lit(F.current_timestamp()))

# COMMAND ----------

final_df.write.option("mergeSchema", "true").mode("append").saveAsTable(
    f"{catalog}.analytics.view_data_quality_produtos"
)
