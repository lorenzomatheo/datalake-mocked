# Databricks notebook source
# MAGIC %md # Market Basket Analysis - Categories
# MAGIC
# MAGIC Este notebook implementa análise de cesta usando algoritmo FP-Growth
# MAGIC para identificar categorias frequentemente compradas juntas e gerar regras de associação.
# MAGIC
# MAGIC Para mais dúvidas consulte o README: datalake/3_analytics/README_market_basket.md

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.ml.fpm import FPGrowth

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas.market_basket_analysis import (
    schema_category_association_rules,
)
from maggulake.utils.pyspark import to_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inicialização do ambiente Databricks

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "market_basket_analysis_categories",
    dbutils,
    widgets={
        "min_support": "0.01",
        "min_confidence": "0.05",
        "dias_analise": "180",
        "auto_ajuste": ["ligado", "desligado", "ligado"],
    },
)
spark = env.spark

DEBUG = dbutils.widgets.get("debug") == "true"
MIN_SUPPORT = float(dbutils.widgets.get("min_support"))
MIN_CONFIDENCE = float(dbutils.widgets.get("min_confidence"))
DIAS_ANALISE = int(dbutils.widgets.get("dias_analise"))
AUTO_AJUSTE = dbutils.widgets.get("auto_ajuste") == "ligado"

tabela_regras_associacao = (
    f"{env.settings.stage.value}.analytics.category_association_rules"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura e preparação dos dados

# COMMAND ----------

data_limite = F.current_date() - F.expr(f"INTERVAL {DIAS_ANALISE} DAYS")

vendas_raw = (
    env.table(Table.view_vendas)
    .filter(F.col("realizada_em") >= data_limite)
    .filter(F.col("quantidade") > 0)
    .cache()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação para formato de transações de categorias

# COMMAND ----------


def preparar_dados_categorias(vendas_df):
    """
    Transforma dados de vendas em formato adequado para market basket analysis de categorias.
    Cada transação é representada como uma lista de categorias únicas.
    """
    produtos_categorias = (
        env.table(Table.produtos_refined)
        .select("id", "categorias")
        .filter(F.col("categorias").isNotNull())
        .filter(F.size(F.col("categorias")) > 0)
    )

    vendas_com_categorias = (
        vendas_df.select("venda_id", "produto_id")
        .join(produtos_categorias, vendas_df.produto_id == produtos_categorias.id)
        .select("venda_id", "produto_id", "categorias")
    )

    # Explodir categorias para ter uma linha por categoria por produto por transação
    categorias_por_transacao = (
        vendas_com_categorias.select(
            "venda_id", F.explode("categorias").alias("categoria")
        ).distinct()  # Remover duplicatas de categoria por transação
    )

    # Agrupar por transação para criar listas de categorias
    transacoes_categorias = (
        categorias_por_transacao.groupBy("venda_id")
        .agg(
            F.collect_list("categoria").alias("categorias_transacao"),
            F.count("categoria").alias("num_categorias"),
        )
        .filter(F.col("num_categorias") >= 2)  # Só transações com 2+ categorias
    )

    return transacoes_categorias


print("Preparando dados de categorias...")
transacoes_df = preparar_dados_categorias(vendas_raw)
total_transacoes = transacoes_df.count()

print(f"Total de transações com 2+ categorias: {total_transacoes:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-ajuste de parâmetros

# COMMAND ----------

MIN_SUPPORT_ORIGINAL = MIN_SUPPORT
MIN_CONFIDENCE_ORIGINAL = MIN_CONFIDENCE

if AUTO_AJUSTE:
    categorias_unicas = (
        transacoes_df.select(F.explode("categorias_transacao").alias("categoria"))
        .distinct()
        .count()
    )

    total_items_categorias = transacoes_df.agg(F.sum("num_categorias")).collect()[0][0]

    if (
        total_items_categorias is None
        or total_transacoes == 0
        or categorias_unicas == 0
    ):
        densidade_categorias = 0.0
    else:
        densidade_categorias = total_items_categorias / (
            total_transacoes * categorias_unicas
        )

    if densidade_categorias > 0.1:
        modo_ajuste = "conservador"
    elif densidade_categorias > 0.01:
        modo_ajuste = "moderado"
    else:
        modo_ajuste = "agressivo"

    parametros_modo = {
        "conservador": {
            "support_multiplier": 0.1,
            "confidence_multiplier": 0.2,
            "min_support": 0.001,
            "min_confidence": 0.005,
        },
        "moderado": {
            "support_multiplier": 0.05,
            "confidence_multiplier": 0.1,
            "min_support": 0.0005,
            "min_confidence": 0.002,
        },
        "agressivo": {
            "support_multiplier": 0.01,
            "confidence_multiplier": 0.05,
            "min_support": 0.0001,
            "min_confidence": 0.001,
        },
    }

    config = parametros_modo[modo_ajuste]

    novo_min_support = max(
        config["min_support"],
        2.0 / total_transacoes,
        densidade_categorias * config["support_multiplier"],
    )

    novo_min_confidence = max(
        config["min_confidence"], densidade_categorias * config["confidence_multiplier"]
    )

    MIN_SUPPORT = novo_min_support
    MIN_CONFIDENCE = novo_min_confidence

    if DEBUG:
        print(
            f"Densidade das categorias: {densidade_categorias:.6f} "
            f"({densidade_categorias * 100:.4f}%)"
        )
        print(f"Auto-ajuste ativado - Modo: {modo_ajuste.upper()}")
        print(
            f"Parâmetros ajustados: support={novo_min_support:.6f}, "
            f"confidence={novo_min_confidence:.6f}"
        )
        print("\nExemplos de transações:")
        transacoes_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do FP-Growth

# COMMAND ----------


def executar_fp_growth_categorias(transacoes_df, min_support, min_confidence):
    """
    Executa FP-Growth para análise de categorias.
    """
    fp_growth = FPGrowth(
        itemsCol="categorias_transacao",
        minSupport=min_support,
        minConfidence=min_confidence,
    )

    modelo = fp_growth.fit(transacoes_df)
    itemsets_frequentes = modelo.freqItemsets
    regras_associacao = modelo.associationRules

    itemsets_count = itemsets_frequentes.count()
    regras_count = regras_associacao.count()

    print(f"Itemsets frequentes (categorias): {itemsets_count:,}")
    print(f"Regras de associação (categorias): {regras_count:,}")

    if regras_count == 0 and AUTO_AJUSTE:
        print("🔄 Testando parâmetros alternativos para categorias...")

        if modo_ajuste == "agressivo":
            parametros_teste = [
                (min_support * 0.1, min_confidence * 0.1),
                (min_support * 0.01, min_confidence * 0.05),
                (0.0001, 0.0005),
                (0.00005, 0.0001),
            ]
        elif modo_ajuste == "moderado":
            parametros_teste = [
                (min_support * 0.2, min_confidence * 0.2),
                (min_support * 0.05, min_confidence * 0.1),
                (0.0005, 0.001),
            ]
        else:  # conservador
            parametros_teste = [
                (min_support * 0.5, min_confidence * 0.5),
                (min_support * 0.2, min_confidence * 0.2),
                (0.001, 0.002),
            ]

        for test_support, test_confidence in parametros_teste:
            test_fp = FPGrowth(
                itemsCol="categorias_transacao",
                minSupport=test_support,
                minConfidence=test_confidence,
            )
            test_model = test_fp.fit(transacoes_df)
            test_rules = test_model.associationRules

            rule_count = test_rules.count()

            if rule_count > 0:
                print(
                    f"✅ {rule_count} regras de categorias encontradas "
                    f"com parâmetros alternativos"
                )
                if DEBUG:
                    print("Top 10 regras de categorias:")
                    test_rules.orderBy(F.desc("confidence")).limit(10).display()
                return test_model.freqItemsets, test_rules

    if DEBUG and regras_count > 0:
        print("Top 10 regras de associação (categorias):")
        regras_associacao.orderBy(F.desc("confidence")).limit(10).display()

    return itemsets_frequentes, regras_associacao


itemsets_frequentes, regras_associacao = executar_fp_growth_categorias(
    transacoes_df, MIN_SUPPORT, MIN_CONFIDENCE
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento das regras de associação

# COMMAND ----------


def processar_regras_categorias(regras_df, total_transacoes):
    """
    Processa regras de associação para categorias.
    """
    regras_pares = regras_df.filter(
        (F.size(F.col("antecedent")) == 1) & (F.size(F.col("consequent")) == 1)
    )

    regras_processadas = regras_pares.select(
        F.col("antecedent")[0].alias("categoria_a"),
        F.col("consequent")[0].alias("categoria_b"),
        F.col("confidence"),
        F.col("lift"),
        F.col("support"),
    )

    regras_associacao_categorias = (
        regras_processadas.withColumn("total_transacoes", F.lit(total_transacoes))
        .withColumn(
            "transacoes_ambas_categorias",
            (F.col("support") * F.lit(total_transacoes)).cast("long"),
        )
        .withColumn(
            "transacoes_categoria_a",
            (F.col("transacoes_ambas_categorias") / F.col("confidence")).cast("long"),
        )
        .withColumn(
            "transacoes_categoria_b", F.lit(None).cast("long")
        )  # Será calculado depois
        .withColumn(
            "conviction",
            F.when(
                F.col("confidence") < 1.0,
                (1.0 - F.col("support")) / (1.0 - F.col("confidence")),
            ).otherwise(F.lit(None)),
        )
        .withColumn("algoritmo_usado", F.lit("fp_growth"))
        .withColumn("periodo_analise_dias", F.lit(DIAS_ANALISE))
        .withColumn("data_calculo", F.current_timestamp())
    )

    return regras_associacao_categorias


regras_associacao_categorias = processar_regras_categorias(
    regras_associacao, total_transacoes
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de métricas complementares

# COMMAND ----------


def calcular_metricas_categorias(regras_associacao_df, transacoes_df):
    """
    Calcula métricas complementares para categorias.
    """
    freq_categorias = (
        transacoes_df.select(F.explode("categorias_transacao").alias("categoria"))
        .groupBy("categoria")
        .agg(F.count("*").alias("freq_categoria"))
    )

    regras_associacao_completas = (
        regras_associacao_df.join(
            freq_categorias.alias("fb"),
            F.col("categoria_b") == F.col("fb.categoria"),
            "left",
        )
        .withColumn("transacoes_categoria_b", F.col("fb.freq_categoria"))
        .drop("fb.categoria", "fb.freq_categoria")
    )

    return regras_associacao_completas


regras_associacao_categorias = calcular_metricas_categorias(
    regras_associacao_categorias, transacoes_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtros e validações

# COMMAND ----------


def aplicar_filtros_qualidade_categorias(
    regras_associacao_df, min_support, min_confidence
):
    """
    Aplica filtros de qualidade para regras de associação de categorias.
    """
    regras_associacao_filtradas = regras_associacao_df.filter(
        (F.col("confidence") >= min_confidence)
        & (F.col("support") >= min_support)
        & (F.col("lift") > 1.0)  # Só regras de associação positivas
        & (F.col("transacoes_ambas_categorias") >= 2)  # Mínimo 2 co-ocorrências
        & (F.col("categoria_a") != F.col("categoria_b"))  # Categorias diferentes
    )

    return regras_associacao_filtradas


regras_associacao_filtradas = aplicar_filtros_qualidade_categorias(
    regras_associacao_categorias, MIN_SUPPORT, MIN_CONFIDENCE
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação do schema e validação

# COMMAND ----------

regras_associacao_final = to_schema(
    df=regras_associacao_filtradas, schema=schema_category_association_rules
).dropDuplicates()

print(f"Regras de associação de categorias finais: {regras_associacao_final.count():,}")

# COMMAND ----------

if DEBUG:
    print("Top 20 regras de associação de categorias por confiança:")
    regras_associacao_final.select(
        "categoria_a",
        "categoria_b",
        "confidence",
        "lift",
        "support",
        "transacoes_ambas_categorias",
    ).orderBy(F.desc("confidence")).limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar resultados

# COMMAND ----------

regras_associacao_final.write.mode("overwrite").saveAsTable(tabela_regras_associacao)

if DEBUG:
    print(
        f"✅ {regras_associacao_final.count():,} regras de associação de categorias salvas na tabela: "
        f"{tabela_regras_associacao}"
    )
