# Databricks notebook source
# MAGIC %md # Market Basket Analysis
# MAGIC
# MAGIC Este notebook implementa análise de cesta usando algoritmo FP-Growth
# MAGIC para identificar produtos frequentemente comprados juntos e gerar regras de associação.
# MAGIC
# MAGIC Para mais dúvidas consulte o README: datalake/3_analytics/README_market_basket.md

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.ml.fpm import FPGrowth

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas.market_basket_analysis import (
    schema_product_association_rules,
)
from maggulake.utils.pyspark import to_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inicialização do ambiente Databricks

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "market_basket_analysis",
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
    f"{env.settings.stage.value}.analytics.product_association_rules"
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
# MAGIC ## Transformação para formato de transações

# COMMAND ----------


def preparar_dados_transacoes(vendas_df):
    """
    Transforma dados de vendas em formato adequado para market basket analysis.
    Cada transação (venda_id) é representada como uma lista de produto_ids.
    """

    produtos_unicos_por_transacao = (
        vendas_df.select("venda_id", "produto_id", "produto_ean", "nome_produto")
        .groupBy("venda_id", "produto_id")
        .agg(
            F.first("produto_ean").alias("produto_ean"),
            F.first("nome_produto").alias("nome_produto"),
        )
    )

    transacoes = (
        produtos_unicos_por_transacao.groupBy("venda_id")
        .agg(
            F.collect_list("produto_id").alias("produtos"),
            F.count("produto_id").alias("num_produtos"),
        )
        .filter(F.col("num_produtos") >= 2)  # Só considerar transações com 2+ produtos
    )

    return transacoes


transacoes_df = preparar_dados_transacoes(vendas_raw)
total_transacoes = transacoes_df.count()

print(f"Total de transações analisadas: {total_transacoes:,}")

MIN_SUPPORT_ORIGINAL = MIN_SUPPORT
MIN_CONFIDENCE_ORIGINAL = MIN_CONFIDENCE

if AUTO_AJUSTE:
    produtos_unicos = (
        transacoes_df.select(F.explode("produtos").alias("produto")).distinct().count()
    )
    total_items_nas_transacoes = transacoes_df.agg(F.sum("num_produtos")).collect()[0][
        0
    ]
    densidade_dados = total_items_nas_transacoes / (total_transacoes * produtos_unicos)

    if densidade_dados > 0.1:
        modo_ajuste = "conservador"
    elif densidade_dados > 0.01:
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
        densidade_dados * config["support_multiplier"],
    )

    novo_min_confidence = max(
        config["min_confidence"], densidade_dados * config["confidence_multiplier"]
    )

    MIN_SUPPORT = novo_min_support
    MIN_CONFIDENCE = novo_min_confidence

if DEBUG and AUTO_AJUSTE:
    print(f"Densidade dos dados: {densidade_dados:.6f} ({densidade_dados * 100:.4f}%)")
    print(f"Auto-ajuste ativado - Modo: {modo_ajuste.upper()}")
    print(
        f"Parâmetros ajustados: support={novo_min_support:.6f}, confidence={novo_min_confidence:.6f}"
    )
    print("\nExemplos de transações:")
    transacoes_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do FP-Growth

# COMMAND ----------


def executar_fp_growth(transacoes_df, min_support, min_confidence):
    fp_growth = FPGrowth(
        itemsCol="produtos", minSupport=min_support, minConfidence=min_confidence
    )

    modelo = fp_growth.fit(transacoes_df)

    itemsets_frequentes = modelo.freqItemsets
    regras_associacao = modelo.associationRules

    itemsets_count = itemsets_frequentes.count()
    regras_count = regras_associacao.count()

    print(f"Itemsets frequentes: {itemsets_count:,}")
    print(f"Regras de associação: {regras_count:,}")

    if regras_count == 0 and AUTO_AJUSTE:
        print("🔄 Testando parâmetros alternativos...")

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
                itemsCol="produtos",
                minSupport=test_support,
                minConfidence=test_confidence,
            )
            test_model = test_fp.fit(transacoes_df)
            test_rules = test_model.associationRules

            rule_count = test_rules.count()

            if rule_count > 0:
                print(f"✅ {rule_count} regras encontradas com parâmetros alternativos")
                if DEBUG:
                    print("Top 10 regras encontradas:")
                    test_rules.orderBy(F.desc("confidence")).limit(10).display()
                return test_model.freqItemsets, test_rules

    if DEBUG and regras_count > 0:
        print("Top 10 regras de associação:")
        regras_associacao.orderBy(F.desc("confidence")).limit(10).display()

    return itemsets_frequentes, regras_associacao


itemsets_frequentes, regras_associacao = executar_fp_growth(
    transacoes_df, MIN_SUPPORT, MIN_CONFIDENCE
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento das regras de associação

# COMMAND ----------


def processar_regras_associacao(regras_df, vendas_df, total_transacoes):
    regras_pares = regras_df.filter(
        (F.size(F.col("antecedent")) == 1) & (F.size(F.col("consequent")) == 1)
    )

    regras_processadas = regras_pares.select(
        F.col("antecedent")[0].alias("produto_a_id"),
        F.col("consequent")[0].alias("produto_b_id"),
        F.col("confidence"),
        F.col("lift"),
        F.col("support"),
    )

    produtos_info = vendas_df.select(
        "produto_id", "produto_ean", "nome_produto"
    ).distinct()

    regras_enriquecidas = (
        regras_processadas.join(
            produtos_info.alias("pa"), F.col("produto_a_id") == F.col("pa.produto_id")
        )
        .select(
            F.col("produto_a_id"),
            F.col("produto_b_id"),
            F.col("pa.produto_ean").alias("produto_a_ean"),
            F.col("pa.nome_produto").alias("produto_a_nome"),
            F.col("confidence"),
            F.col("lift"),
            F.col("support"),
        )
        .join(
            produtos_info.alias("pb"), F.col("produto_b_id") == F.col("pb.produto_id")
        )
        .select(
            F.col("produto_a_id"),
            F.col("produto_b_id"),
            F.col("produto_a_ean"),
            F.col("produto_a_nome"),
            F.col("pb.produto_ean").alias("produto_b_ean"),
            F.col("pb.nome_produto").alias("produto_b_nome"),
            F.col("confidence"),
            F.col("lift"),
            F.col("support"),
        )
    )

    regras_associacao_finais = (
        regras_enriquecidas.withColumn("total_transacoes", F.lit(total_transacoes))
        .withColumn(
            "transacoes_ambos_produtos",
            (F.col("support") * F.lit(total_transacoes)).cast("long"),
        )
        .withColumn(
            "transacoes_produto_a",
            (F.col("transacoes_ambos_produtos") / F.col("confidence")).cast("long"),
        )
        .withColumn(
            "transacoes_produto_b", F.lit(None).cast("long")
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

    return regras_associacao_finais


regras_associacao_produtos = processar_regras_associacao(
    regras_associacao, vendas_raw, total_transacoes
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de métricas complementares

# COMMAND ----------


def calcular_metricas_complementares(regras_associacao_df, vendas_df):
    freq_produtos = (
        vendas_df.select("venda_id", "produto_id")
        .distinct()
        .groupBy("produto_id")
        .agg(F.countDistinct("venda_id").alias("freq_produto"))
    )

    regras_associacao_completas = (
        regras_associacao_df.join(
            freq_produtos.alias("fp"),
            F.col("produto_b_id") == F.col("fp.produto_id"),
            "left",
        )
        .withColumn("transacoes_produto_b", F.col("fp.freq_produto"))
        .drop("fp.produto_id", "fp.freq_produto")
    )

    return regras_associacao_completas


regras_associacao_produtos = calcular_metricas_complementares(
    regras_associacao_produtos, vendas_raw
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtros e validações

# COMMAND ----------


def aplicar_filtros_qualidade(regras_associacao_df, min_support, min_confidence):
    regras_associacao_filtradas = regras_associacao_df.filter(
        (F.col("confidence") >= min_confidence)
        & (F.col("support") >= min_support)
        & (F.col("lift") > 1.0)  # Só regras de associação positivas
        & (F.col("transacoes_ambos_produtos") >= 2)  # Mínimo 2 co-ocorrências
        & (F.col("produto_a_ean") != F.col("produto_b_ean"))  # Produtos diferentes
        & (F.col("produto_a_nome") != F.col("produto_b_nome"))
    )

    return regras_associacao_filtradas


regras_associacao_filtradas = aplicar_filtros_qualidade(
    regras_associacao_produtos, MIN_SUPPORT, MIN_CONFIDENCE
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação do schema e validação

# COMMAND ----------

regras_associacao_final = (
    to_schema(df=regras_associacao_filtradas, schema=schema_product_association_rules)
    .filter(
        ~F.col("produto_a_nome").like("%Taxa Tele Entrega%")
        & ~F.col("produto_b_nome").like("%Taxa Tele Entrega%")
    )
    .dropDuplicates()
)

print(
    f"Regras de associação de produtos finais processadas: {regras_associacao_final.count():,}"
)

# COMMAND ----------

if DEBUG:
    print("Top 50 regras de associação de produtos por confiança:")
    regras_associacao_final.select(
        "produto_a_ean",
        "produto_a_nome",
        "produto_b_ean",
        "produto_b_nome",
        "confidence",
        "lift",
        "support",
        "transacoes_ambos_produtos",
    ).orderBy(F.desc("confidence")).limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar resultados

# COMMAND ----------

regras_associacao_final.write.mode("overwrite").saveAsTable(tabela_regras_associacao)


if DEBUG:
    print(
        f"✅ {regras_associacao_final.count():,} regras de associação de produtos salvas na tabela: {tabela_regras_associacao}"
    )
