# Databricks notebook source
# MAGIC %md
# MAGIC # Score de Aderência de Produtos por Loja
# MAGIC
# MAGIC ## Objetivo
# MAGIC Calcular o **score de aderência** que mede a compatibilidade entre um produto e o perfil de consumo de uma loja.
# MAGIC
# MAGIC O score combina 4 dimensões complementares:
# MAGIC - **Disponibilidade** (Estoque): O produto está presente na rede?
# MAGIC - **Desempenho** (Volume): A loja vende bem este produto?
# MAGIC - **Atratividade** (Preço): O produto tem preço competitivo para o público da loja?
# MAGIC - **Rentabilidade** (Margem): O produto é lucrativo comparado ao mix da loja?
# MAGIC
# MAGIC ## Critérios
# MAGIC 1. **Estoque**: Disponibilidade física - % de lojas (top 100) que possuem o produto
# MAGIC 2. **Volume**: Concentração de vendas - % das vendas totais do produto que vêm desta loja
# MAGIC 3. **Preço**: Atratividade - ratio preço_produto/ticket_médio_loja (quanto menor, mais atrativo)
# MAGIC 4. **Margem**: Rentabilidade - ratio margem_produto/margem_média_loja (quanto maior, mais rentável)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Imports

# COMMAND ----------

from datetime import timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from maggulake.environment import CopilotTable, DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_score_aderencia
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurações e Parâmetros

# COMMAND ----------

# Configurações do Score
TOP_N_LOJAS = 100  # Top lojas por faturamento
CURVA_PRODUTO = "A"  # Curva A de produtos (80% do faturamento)
PERIODO_DIAS = 180  # Período de análise para vendas

# Pesos dos critérios (devem somar 1.0)
PESO_ESTOQUE = 0.4
PESO_VOLUME = 0.2
PESO_PRECO = 0.2
PESO_MARGEM = 0.2

# Definir faixa de impacto no score_preço (prioriza produtos próximos do ticket_medio loja)
RATIO_MIN = 0.5  # Produto muito barato (score = 1.0)
RATIO_MAX = 2.0  # Produto muito caro (score = 0.0)

# Data de referência
DATA_FIM = agora_em_sao_paulo()
DATA_INICIO = DATA_FIM - timedelta(days=PERIODO_DIAS)

print(f"Período de análise: {DATA_INICIO.date()} a {DATA_FIM.date()}")
print(f"Escopo: Top {TOP_N_LOJAS} lojas + Produtos Curva {CURVA_PRODUTO}")
print(
    f"Pesos: Estoque={PESO_ESTOQUE}, Volume={PESO_VOLUME}, Preço={PESO_PRECO}, Margem={PESO_MARGEM}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inicialização do Ambiente e Criação da Tabela

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "score-aderencia",
    dbutils,
)

DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

# Criar tabela se não existir

env.create_table_if_not_exists(Table.score_aderencia, schema_score_aderencia)

print(f"✓ Tabela {Table.score_aderencia.value} criada/validada com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extração de Escopo - Top 100 Lojas e Produtos Curva A

# COMMAND ----------

# Lojas mais relevantes por faturamento
df_lojas_top = env.bigquery_adapter.get_lojas_mais_vendas(
    data_inicio=DATA_INICIO, apenas_lojas_ativas=True
).filter(F.col("rank_vendas") <= TOP_N_LOJAS)

# Extrair lista de IDs de lojas
lojas_top_ids = [row.id for row in df_lojas_top.select("id").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extração de Dados Transacionais

# COMMAND ----------

# vendas para lojas relevantes
df_vendas = env.bigquery_adapter.get_vendas_por_periodo(
    data_inicio=DATA_INICIO,
    loja_ids=lojas_top_ids,
    incluir_tenant=True,
).cache()

# Extrair lista de EANs
produtos_eans = [row.ean for row in df_vendas.select("ean").distinct().collect()]

# COMMAND ----------

# Posicao de estoque mais recente para lojas relevantes

df_estoque = produtos_loja_copilot = (
    env.table(CopilotTable.produtos_loja)
    .filter(F.col("loja_id").isin(lojas_top_ids))
    .cache()
)

windowSpec = Window.partitionBy("ean", "loja_id").orderBy(F.desc("atualizado_em"))
latest_stock_raw = (
    df_estoque.withColumn("row_num", F.row_number().over(windowSpec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cálculos Agregados via PySpark

# COMMAND ----------

# 6.1. Métricas agregadas por Loja
df_metricas_loja = (
    df_vendas.groupBy("loja_id")
    .agg(
        F.sum(F.col("valor_final")).alias("faturamento_total_loja"),
        F.count_distinct("venda_id").alias("num_vendas_loja"),
        F.avg(
            F.when(
                F.col("valor_final") > 0,
                (F.col("valor_final") - F.col("custo_compra")) / F.col("valor_final"),
            ).otherwise(F.lit(0))
        ).alias("margem_media_loja"),
    )
    .withColumn(
        "ticket_medio_loja",
        F.when(
            F.col("num_vendas_loja") > 0,
            F.round(F.col("faturamento_total_loja") / F.col("num_vendas_loja"), 2),
        ).otherwise(F.lit(0)),
    )
)

# COMMAND ----------

# 6.2. Métricas agregadas por Produto
df_metricas_produto = df_vendas.groupBy("ean").agg(
    F.sum("quantidade").alias("volume_total_produto"),
    F.avg("preco_venda_desconto").alias("preco_medio_produto"),
    F.count("*").alias("num_vendas_produto"),
)


# COMMAND ----------

# 6.3. Métricas por EAN x Loja
df_ean_loja = (
    df_vendas.groupBy("ean", "loja_id")
    .agg(
        F.sum("quantidade").alias("volume_loja_produto"),
        F.sum(F.col("valor_final")).alias("faturamento_produto_loja"),
        F.count_distinct("venda_id").alias("num_vendas_produto_loja"),
        F.avg(
            F.when(
                F.col("valor_final") > 0,
                (F.col("valor_final") - F.col("custo_compra")) / F.col("valor_final"),
            ).otherwise(F.lit(0))
        ).alias("margem_produto_loja"),
    )
    .withColumn(
        "preco_produto_loja",
        F.when(
            F.col("volume_loja_produto") > 0,
            F.round(
                F.col("faturamento_produto_loja") / F.col("volume_loja_produto"), 2
            ),
        ).otherwise(F.lit(0)),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cálculo dos Scores Individuais

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1. Score Estoque - Disponibilidade Física
# MAGIC
# MAGIC **O que mede**: Presença do produto na rede de lojas (distribuição geográfica)
# MAGIC
# MAGIC **Como funciona**:
# MAGIC - Flag binário: 1 se estoque > 0 na loja, 0 caso contrário
# MAGIC - Score por EAN: COUNT(lojas com estoque) / COUNT(total de lojas do escopo)
# MAGIC
# MAGIC **Interpretação**:
# MAGIC - Score = 1.0 → Produto presente em 100% das lojas (distribuição total)
# MAGIC - Score = 0.5 → Produto presente em 50% das lojas (distribuição média)
# MAGIC - Score = 0.0 → Produto não está em nenhuma loja (sem distribuição)

# COMMAND ----------

# Criar flag de estoque
df_estoque_flag = latest_stock_raw.withColumn(
    "tem_estoque", F.when(F.col("estoque_unid") > 0, 1).otherwise(F.lit(0))
)

# Total de lojas no escopo
total_lojas_escopo = len(lojas_top_ids)

# Agregar por EAN
df_score_estoque = df_estoque_flag.groupBy("ean").agg(
    (F.sum("tem_estoque") / F.lit(total_lojas_escopo)).alias("score_estoque"),
    F.sum("tem_estoque").alias("num_lojas_com_estoque"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1.5. Fator de Confiabilidade - Penalização por Baixa Distribuição
# MAGIC
# MAGIC **O que mede**: Confiabilidade dos dados baseada na distribuição do produto nas lojas
# MAGIC **Interpretação**:
# MAGIC - Produto em 1 loja de 100 → fator = 0.01 (penalização de 99%)
# MAGIC - Produto em 10 lojas de 100 → fator = 0.10 (penalização de 90%)
# MAGIC - Produto em 50 lojas de 100 → fator = 0.50 (penalização de 50%)
# MAGIC - Produto em 100 lojas → fator = 1.00 (sem penalização)

# COMMAND ----------

# Calcular fator de confiabilidade por produto
df_fator_confiabilidade = (
    df_vendas.groupBy("ean")
    .agg(F.count_distinct("loja_id").alias("qtde_lojas_vendendo"))
    .withColumn(
        "fator_confiabilidade", F.col("qtde_lojas_vendendo") / F.lit(total_lojas_escopo)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2. Score Volume - Densidade de Vendas
# MAGIC
# MAGIC **O que mede**: Performance de vendas do produto normalizada pelo mercado e ajustada pela distribuição
# MAGIC **Interpretação**:
# MAGIC - Score alto → Produto vende bem **por loja** E está bem distribuído
# MAGIC - Score baixo → Produto vende pouco ou está presente em poucas lojas
# MAGIC
# MAGIC **Exemplos práticos** (assumindo percentil_90 = 100 unid/loja):
# MAGIC - Produto A: 150 unid em 1 loja → densidade = 150, fator = 0.01 → score = min(1.0, 150/100) × 0.01 = **0.01**
# MAGIC - Produto B: 500 unid em 10 lojas → densidade = 50, fator = 0.10 → score = min(1.0, 50/100) × 0.10 = **0.05**
# MAGIC - Produto C: 6000 unid em 50 lojas → densidade = 120, fator = 0.50 → score = min(1.0, 120/100) × 0.50 = **0.50**

# COMMAND ----------

# Passo 1: Calcular densidade de vendas por produto
df_densidade_produto = (
    df_metricas_produto.select("ean", "volume_total_produto")
    .join(df_fator_confiabilidade, "ean")
    .withColumn(
        "densidade_vendas",
        F.when(
            F.col("qtde_lojas_vendendo") > 0,
            F.col("volume_total_produto") / F.col("qtde_lojas_vendendo"),
        ).otherwise(F.lit(0)),
    )
)

# Passo 2: Calcular percentil 90 global de densidade
percentil_90_densidade = df_densidade_produto.approxQuantile(
    "densidade_vendas", [0.9], 0.01
)[0]


# Passo 3: Normalizar densidade e aplicar fator de confiabilidade
df_score_volume = (
    df_densidade_produto.withColumn(
        "score_volume_base",
        F.when(
            F.lit(percentil_90_densidade) > 0,
            F.least(
                F.lit(1.0), F.col("densidade_vendas") / F.lit(percentil_90_densidade)
            ),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "score_volume", F.col("score_volume_base") * F.col("fator_confiabilidade")
    )
    .select("ean", "score_volume")
    # Replicar score para todas as lojas que vendem o produto
    .join(df_ean_loja.select("ean", "loja_id").distinct(), "ean")
    .select("ean", "loja_id", "score_volume")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3. Score Preço - Atratividade de Preço com Fator de Confiabilidade
# MAGIC **O que mede**: Se o produto tem preço atrativo para o perfil de consumo da loja, ajustado pela confiabilidade dos dados
# MAGIC **Fórmula (Interpolação Linear Inversa + Confiabilidade)**:
# MAGIC 1. `ratio = preço_produto / ticket_médio_loja`
# MAGIC 2. Calcular score base:
# MAGIC    - Se ratio ≤ 0.5 → score_base = 1.0
# MAGIC    - Se ratio ≥ 2.0 → score_base = 0.0
# MAGIC    - Se 0.5 < ratio < 2.0 → score_base = interpolação linear
# MAGIC 3. `score_preco_final = score_base × fator_confiabilidade`
# MAGIC
# MAGIC **Exemplos práticos** (ticket_médio_loja = R$ 100):
# MAGIC - Produto R$ 50 em 1 loja → ratio = 0.50, score_base = 1.0, fator = 0.01 → **score = 0.01**
# MAGIC - Produto R$ 50 em 50 lojas → ratio = 0.50, score_base = 1.0, fator = 0.50 → **score = 0.50**
# MAGIC - Produto R$ 100 em 20 lojas → ratio = 1.00, score_base = 0.67, fator = 0.20 → **score = 0.13**
# MAGIC - Produto R$ 75 em 80 lojas → ratio = 0.75, score_base = 0.83, fator = 0.80 → **score = 0.66**

# COMMAND ----------

df_score_preco = (
    df_ean_loja.select("ean", "loja_id", "preco_produto_loja")
    .join(df_metricas_loja.select("loja_id", "ticket_medio_loja"), "loja_id")
    .withColumn("ratio_preco", F.col("preco_produto_loja") / F.col("ticket_medio_loja"))
    .withColumn(
        "score_preco_base",
        F.when(F.col("ratio_preco") <= RATIO_MIN, 1.0)
        .when(F.col("ratio_preco") >= RATIO_MAX, 0.0)
        .otherwise(
            # Interpolação linear entre RATIO_MIN e RATIO_MAX
            (RATIO_MAX - F.col("ratio_preco")) / (RATIO_MAX - RATIO_MIN)
        ),
    )
    # Aplicar fator de confiabilidade
    .join(df_fator_confiabilidade.select("ean", "fator_confiabilidade"), "ean")
    .withColumn(
        "score_preco", F.col("score_preco_base") * F.col("fator_confiabilidade")
    )
    .select("ean", "loja_id", "score_preco")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4. Score Margem - Rentabilidade Relativa com Fator de Confiabilidade
# MAGIC
# MAGIC **O que mede**: Se o produto é mais ou menos lucrativo comparado ao mix médio da loja, ajustado pela confiabilidade dos dados
# MAGIC **Interpretação**:
# MAGIC - ratio > 1.0 → Produto é **mais rentável** que a média → score base alto
# MAGIC - ratio < 1.0 → Produto é **menos rentável** que a média → score base baixo
# MAGIC - Fator de confiabilidade penaliza produtos em poucas lojas
# MAGIC
# MAGIC **Exemplos práticos** (margem_média_loja = 30%):
# MAGIC - Margem 45% em 1 loja → ratio = 1.50, score_base = 1.0, fator = 0.01 → **score = 0.01**
# MAGIC - Margem 30% em 50 lojas → ratio = 1.00, score_base = 1.0, fator = 0.50 → **score = 0.50**
# MAGIC - Margem 15% em 20 lojas → ratio = 0.50, score_base = 0.5, fator = 0.20 → **score = 0.10**
# MAGIC - Margem 40% em 80 lojas → ratio = 1.33, score_base = 1.0, fator = 0.80 → **score = 0.80**

# COMMAND ----------

df_score_margem = (
    df_ean_loja.select("ean", "loja_id", "margem_produto_loja")
    .join(df_metricas_loja.select("loja_id", "margem_media_loja"), "loja_id")
    .withColumn(
        "ratio_margem",
        F.when(
            F.col("margem_media_loja") > 0,
            F.col("margem_produto_loja") / F.col("margem_media_loja"),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn(
        "score_margem_base",
        F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.col("ratio_margem"))),
    )
    # Aplicar fator de confiabilidade
    .join(df_fator_confiabilidade.select("ean", "fator_confiabilidade"), "ean")
    .withColumn(
        "score_margem", F.col("score_margem_base") * F.col("fator_confiabilidade")
    )
    .select("ean", "loja_id", "score_margem")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Consolidação do Score de Aderência

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1. Score por EAN x Loja (Soma Ponderada)

# COMMAND ----------

# Join de todos os scores
df_score_ean_loja = (
    df_score_volume.select("ean", "loja_id", "score_volume")
    .join(
        df_score_preco.select("ean", "loja_id", "score_preco"),
        ["ean", "loja_id"],
    )
    .join(
        df_score_margem.select("ean", "loja_id", "score_margem"),
        ["ean", "loja_id"],
    )
)

# Join com score de estoque
df_score_ean_loja = df_score_ean_loja.join(
    df_score_estoque.select("ean", "score_estoque"), "ean"
)

# Calcular score final ponderado
df_score_ean_loja = df_score_ean_loja.withColumn(
    "score_aderencia",
    (
        F.col("score_estoque") * PESO_ESTOQUE
        + F.col("score_volume") * PESO_VOLUME
        + F.col("score_preco") * PESO_PRECO
        + F.col("score_margem") * PESO_MARGEM
    ),
)

# Arredondar para 4 casas decimais
df_score_ean_loja = (
    df_score_ean_loja.withColumn(
        "score_aderencia", F.round(F.col("score_aderencia"), 4)
    )
    .withColumn("score_estoque", F.round(F.col("score_estoque"), 4))
    .withColumn("score_volume", F.round(F.col("score_volume"), 4))
    .withColumn("score_preco", F.round(F.col("score_preco"), 4))
    .withColumn("score_margem", F.round(F.col("score_margem"), 4))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2. Agregação Final por EAN

# COMMAND ----------

df_score_por_ean = (
    df_score_ean_loja.groupBy("ean")
    .agg(
        F.round(F.avg("score_aderencia"), 2).alias("score_aderencia"),
        F.count_distinct("loja_id").alias("qtde_lojas"),
        F.round(F.avg("score_estoque"), 2).alias("score_estoque"),
        F.round(F.avg("score_volume"), 2).alias("score_volume"),
        F.round(F.avg("score_preco"), 2).alias("score_preco"),
        F.round(F.avg("score_margem"), 2).alias("score_margem"),
    )
    .withColumn("atualizado_em", F.lit(agora_em_sao_paulo()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2. Salvar Resultados

# COMMAND ----------

# Salvar tabela
df_score_por_ean.write.mode("overwrite").format("delta").saveAsTable(
    Table.score_aderencia.value
)
