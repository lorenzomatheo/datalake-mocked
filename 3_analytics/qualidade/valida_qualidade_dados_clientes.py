# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %pip install pydeequ==1.4.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os

os.environ["SPARK_VERSION"] = "3.5"  # necessário definir antes dos outros imports

# COMMAND ----------

from typing import Literal

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
from pyspark.sql import DataFrame, SparkSession

from maggulake.enums import FaixaEtaria, RFMCategories, TamanhoProduto

# COMMAND ----------

spark = (
    SparkSession.builder.appName("valida_qualidade_dados_clientes_conta")
    .config(
        "spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5"
    )  # instalado via maven no cluster
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate()
)

# ambiente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# COMMAND ----------

clientes_conta_df = spark.read.table(f"{catalog}.refined.clientes_conta_refined")

# COMMAND ----------


def convert_array_to_json(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, T.ArrayType):
            df = df.withColumn(field.name, F.to_json(F.col(field.name)))
    return df


# Converte todas as colunas de array para JSON uma vez no início
clientes_conta_json = convert_array_to_json(clientes_conta_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição de colunas e divisão dos DataFrames

# COMMAND ----------

# Colunas que só fazem sentido para clientes que realizaram ao menos 1 compra
colunas_compras = [
    'data_primeira_compra',
    'data_ultima_compra',
    'total_de_compras',  # A própria coluna usada para o filtro
    'compras',
    'ticket_medio',
    'media_itens_por_cesta',
    'pref_dia_semana',
    'pref_dia_mes',
    'pref_hora',
    'intervalo_medio_entre_compras',
    'cluster_rfm',
    'loja_mais_frequente',
    'pct_canal_loja_fisica',
    'pct_canal_tele_entrega',
    'pct_canal_e_commerce',
    'pct_canal_outros_canais',
    'canal_preferido',
    'pct_pgto_cartao',
    'pct_pgto_pix',
    'pct_pgto_dinheiro',
    'pct_pgto_crediario',
    'metodo_pgto_preferido',
    'pct_produtos_impulsionados',
    'pct_itens_medicamentos',
    'pct_itens_medicamentos_genericos',
    'pct_itens_medicamentos_referencia',
    'pct_itens_medicamentos_similar',
    'pct_itens_medicamentos_similar_intercambiavel',
    'formas_farmaceuticas_preferidas',
    'pct_venda_agregada',
    'pct_venda_organica',
    'pct_venda_substituicao',
    'pct_pequeno',
    'pct_medio',
    'pct_grande',
    'tamanho_produto_preferido',
    'total_de_cestas',
]

# Colunas que só fazem sentido para clientes que compraram ao menos 1 medicamento
colunas_comprar_medicamentos = [
    'pct_itens_medicamentos',
    'pct_itens_medicamentos_genericos',
    'pct_itens_medicamentos_referencia',
    'pct_itens_medicamentos_similar',
    'pct_itens_medicamentos_similar_intercambiavel',
    'formas_farmaceuticas_preferidas',
    'pct_pequeno',
    'pct_medio',
    'pct_grande',
    'tamanho_produto_preferido',
]


# COMMAND ----------

# Drop colunas que não são relevantes para a análise de qualidade
clientes_conta_json = clientes_conta_json.drop(
    "nome_social",  # Nome social é opcional
    "telefone_alt",  # Coluna opcional
)

# COMMAND ----------

# Clientes que compraram medicamentos: manter todas as colunas para análise completa
df_comprado = clientes_conta_json.filter(F.col("total_de_compras") > 0)

# Clientes que nao compraram medicamentos, mas compraram outros produtos:
df_comprado_sem_medicamentos = df_comprado.filter(
    (F.col("ja_comprou_medicamento") == False)
    | (F.col("ja_comprou_medicamento").isNull())
).drop(*colunas_comprar_medicamentos)

# Clientes que não compraram: Remover colunas relacionadas a compras
df_nao_comprado = clientes_conta_json.filter(
    (F.col("total_de_compras") <= 0) | (F.col("total_de_compras").isNull())
).drop(*colunas_compras)


print(f"Clientes que compraram: {df_comprado.count()}")
print(f"Clientes que não compraram: {df_nao_comprado.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria avaliadores (Analyzers)

# COMMAND ----------

ENUMS_MAP = {
    "faixa_etaria": FaixaEtaria.tuple(),
    "cluster_rfm": RFMCategories.tuple(),
    "tamanho_produto_preferido": TamanhoProduto.tuple(),
    "sexo": ("masculino", "feminino"),
}


def __add_uniqueness(runner: AnalysisRunner) -> AnalysisRunner:
    # Unicidade coluna 'id'
    runner = runner.addAnalyzer(Uniqueness(["id"]))
    # TODO: adicionar unicidade da chave "conta-cpf_cnpj"
    return runner


def __add_completeness(
    runner: AnalysisRunner, df_columns_for_completeness: list[str]
) -> AnalysisRunner:
    for col_name in df_columns_for_completeness:
        runner = runner.addAnalyzer(Completeness(col_name))
    return runner


def __add_compliance(
    runner: AnalysisRunner, current_df_columns: list[str]
) -> AnalysisRunner:
    # Regras de conformidade são adicionadas apenas se a coluna relevante existir no DataFrame atual

    if "idade" in current_df_columns:
        runner = runner.addAnalyzer(
            Compliance("idade_positiva", "idade IS NULL OR idade >= 0")
        )
        runner = runner.addAnalyzer(
            Compliance("idade_menor_120", "idade IS NULL OR idade <= 120")
        )

    if "cpf_cnpj" in current_df_columns:
        runner = runner.addAnalyzer(
            Compliance(
                "cpf_cnpj_tamanho", "cpf_cnpj IS NULL OR length(cpf_cnpj) in (11, 14)"
            )
        )

    if "data_nascimento" in current_df_columns:
        runner = runner.addAnalyzer(
            Compliance(
                "data_nascimento_menor_hoje",
                "data_nascimento IS NULL OR data_nascimento <= current_date()",
            )
        )

    if "email" in current_df_columns:
        runner = runner.addAnalyzer(
            Compliance(
                "email_valido",
                "email IS NULL OR (email LIKE '%@%' AND email LIKE '%.%')",
            )
        )

    if "celular" in current_df_columns:
        runner = runner.addAnalyzer(
            Compliance(
                "celular_valido",
                "celular IS NULL OR length(regexp_replace(celular, '\\\\D', '')) in (10, 11)",
            )
        )

    # Conformidade com Enums
    enum_compliance_rules = []
    for key, value_tuple in ENUMS_MAP.items():
        # Adiciona regra apenas se a coluna do enum existir
        if key in current_df_columns:
            # Garante que o tuple de valores seja formatado corretamente para a query SQL do Deequ
            formatted_value_tuple = str(value_tuple).replace("[", "(").replace("]", ")")
            enum_compliance_rules.append(
                (
                    f"{key}_enum",
                    f"{key} IS NULL OR {key} IN {formatted_value_tuple}",
                )
            )

    for name, rule_expression in enum_compliance_rules:
        runner = runner.addAnalyzer(Compliance(name, rule_expression))

    return runner


# COMMAND ----------


def add_analyzers(runner: AnalysisRunner, df_columns: list[str]) -> AnalysisRunner:
    runner = __add_uniqueness(runner)
    runner = __add_completeness(runner, df_columns)
    runner = __add_compliance(runner, df_columns)
    return runner


# COMMAND ----------


def run_analysis(
    spark_session: SparkSession,
    df_to_analyze: DataFrame,
    df_segment_name: Literal['comprador', 'nao_comprador'],
) -> DataFrame | None:
    if df_to_analyze.count() == 0:
        print(
            f"Nenhum dado para analisar no segmento: {df_segment_name}. Pulando análise."
        )
        return None

    print(
        f"Iniciando análise para o segmento: {df_segment_name} ({df_to_analyze.count()} registros)"
    )
    try:
        analysis_runner = AnalysisRunner(spark_session).onData(df_to_analyze)
        analysis_runner_with_analyzers = add_analyzers(
            analysis_runner, df_to_analyze.columns
        )
        analysis_result = analysis_runner_with_analyzers.run()

        result_df = AnalyzerContext.successMetricsAsDataFrame(
            spark_session, analysis_result
        ).drop("entity")
        result_df = result_df.withColumn("segmento_cliente", F.lit(df_segment_name))
        return result_df
    except TypeError as e:
        print(f"Erro de TypeError durante a análise do segmento {df_segment_name}: {e}")
        raise TypeError(
            "Não foi possível criar o runner do Deequ. Verifique se 'spark.jars.packages' "
            "para o Deequ está corretamente configurado no cluster."
        )
    # NOTE: removendo broad exception catch para evitar mascarar erros, deixei comentado caso fique insustentavel
    # except Exception as e:
    #     print(f"Erro inesperado durante a análise do segmento {df_segment_name}: {e}")
    #     return None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução da Análise por Segmento

# COMMAND ----------

results_list = []

segments_to_analyze = {
    "comprador": df_comprado,
    "comprador_sem_medicamentos": df_comprado_sem_medicamentos,
    "nao_comprador": df_nao_comprado,
}

# Executa análise para cada segmento
for segment_name, segment_df in segments_to_analyze.items():
    print(f"Processando segmento: {segment_name}")

    result_df = run_analysis(spark, segment_df, segment_name)
    if result_df is not None:
        results_list.append(result_df)
        print(f"✓ Análise concluída para {segment_name}")
    else:
        print(f"⚠ Nenhum resultado para {segment_name}")

print(f"\nTotal de segmentos analisados: {len(results_list)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consolidação e Salvamento dos Resultados

# COMMAND ----------

final_results_df = None

if not results_list:
    print("Nenhum resultado de análise foi gerado para nenhum segmento.")
    dbutils.notebook.exit("Nenhum resultado de qualidade para salvar.")
elif len(results_list) == 1:
    final_results_df = results_list[0]
else:
    # UnionAll pode falhar se os schemas não forem exatamente os mesmos.
    # Asseguramos que 'segmento_cliente' é adicionado em run_analysis
    final_results_df = results_list[0]
    for i in range(1, len(results_list)):
        final_results_df = final_results_df.unionAll(results_list[i])


# COMMAND ----------

if final_results_df is not None and final_results_df.take(1):
    final_df_to_save = final_results_df.withColumn("gerado_em", F.current_timestamp())

    print(f"Salvando {final_df_to_save.count()} registros de métricas de qualidade...")
    # final_df_to_save.display()  # Visualizar antes de salvar

    final_df_to_save.write.option("mergeSchema", "true").mode("append").saveAsTable(
        f"{catalog}.analytics.view_data_quality_clientes_conta"
    )
    print("Resultados salvos com sucesso.")
else:
    print("Nenhum DataFrame final de resultados para salvar.")
