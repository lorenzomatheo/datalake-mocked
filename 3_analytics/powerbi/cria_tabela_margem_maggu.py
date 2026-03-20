# Databricks notebook source
import random

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

from maggulake.environment import (
    BigqueryViewPbi,
    CopilotTable,
    DatabricksEnvironmentBuilder,
)

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "calcula_roas",
    dbutils,
)

# delta
margem_maggu_table_path = 'hive_metastore.pbi.margem_maggu'

# roas alvo
roas_min = 5
roas_max = 5.3

# data inicio da apuração utilizando esse script
data_inicio_apuracao = '2025-08-04'

# COMMAND ----------

df_vendas = env.table(CopilotTable.vendas)
df_vendas_item = env.table(CopilotTable.vendas_item)
# moedas_por_produto é uma view do BQ que contém o calculo de moedas por produto
df_moedas_por_produto = (
    env.table(CopilotTable.moedas_por_produto)
    .dropDuplicates(['item_da_venda_id', 'rodada_id'])
    .select('item_da_venda_id', 'moedas_produto')
)  # remove pontos duplicados do extrato_de_pontos

df_vendas.count(), df_vendas_item.count(), df_moedas_por_produto.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cria dataframe com as informações necessárias para o cálculo da margem
# MAGIC - valor venda (quantidade * valor_venda)
# MAGIC - moedas item da venda

# COMMAND ----------

df_vendas_item_intermed = df_vendas_item.alias('vi').join(
    df_moedas_por_produto.alias('p'),
    on=df_vendas_item.id == df_moedas_por_produto.item_da_venda_id,
    how='left',
)
df_vendas_final = (
    df_vendas.alias('vv')
    .join(
        df_vendas_item_intermed.alias('vi'),
        on=df_vendas.id == df_vendas_item.venda_id,
        how='inner',
    )
    .select(
        F.col('vv.venda_concluida_em'),
        F.col('vv.id').alias('venda_id'),
        F.col('vv.codigo_externo_do_vendedor'),
        F.col('vv.loja_id'),
        F.col('vv.pre_venda_id'),
        F.col('vv.vendedor_id'),
        F.col('vv.status').alias('status_venda'),
        F.col('vi.id').alias('venda_item_id'),
        F.col('vi.item_da_pre_venda_id'),
        F.col('vi.produto_id'),
        F.col('vi.ean'),
        F.col('vi.preco_venda'),
        F.col('vi.preco_venda_desconto'),
        F.col('vi.custo_compra'),
        F.col('vi.desconto_total'),
        F.col('vi.valor_final'),
        F.col('vi.quantidade'),
        F.col('vi.status').alias('status_venda_item'),
        (F.col('vi.quantidade') * F.col('vi.preco_venda_desconto')).alias(
            'valor_venda'
        ),
        F.col('vv.atualizado_em').alias('atualizado_em'),
        F.col('vi.moedas_produto').alias('moedas'),
    )
    .withColumn(
        'venda_concluida_em',
        F.from_utc_timestamp("venda_concluida_em", "America/Sao_Paulo"),
    )  # mesmo timestamp do postgres
    .withColumn('semana', F.to_date(F.date_trunc('week', F.col('venda_concluida_em'))))
    .filter(F.col('venda_concluida_em') >= data_inicio_apuracao)
    .filter(F.col('status_venda_item') == 'vendido')
).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escopo de produto lojas  definido pela tabela `produto_loja_campanha`

# COMMAND ----------

# Escopo de lojas e produtos
df_info_escopo = (
    env.table(BigqueryViewPbi.d_produto_loja_campanha)
    .withColumn('loja_id', F.split(F.col('loja_produto_id'), r"\|")[0])  # pega loja_id
    .filter(F.col('tipo_ean') == 'campanha')  # apenas produtos em campanha
    .select('produto_id', 'loja_id', 'nome_produto', 'marca', 'industria', 'campanha')
)

# COMMAND ----------

df_por_semana = (
    df_vendas_final.join(df_info_escopo, on=['loja_id', 'produto_id'], how='inner')
    .withColumn('moedas', F.coalesce('moedas', F.lit(0)))
    .groupBy(
        'semana', 'nome_produto', 'produto_id', 'ean', 'marca', 'industria', 'campanha'
    )
    .agg(
        F.sum('quantidade').cast(T.FloatType()).alias('quantidade'),
        F.sum("valor_venda").cast(T.FloatType()).alias('valor_venda'),
        (F.sum('moedas')).cast(T.FloatType()).alias('moedas'),
    )
)

# COMMAND ----------


@F.udf(T.DoubleType())
def calcula_input_maggu_otimo(valor_venda, venda_total):
    # 1. Retorna 0 quando não tem valor_venda definido
    if valor_venda is None or valor_venda <= 0 or venda_total is None:
        return 0.0

    # Gasto com o input manual não pode ultrapassar 20% da venda_total
    investimento_maximo_orcamento = 0.20 * venda_total

    # Selectiona um input que gera um valor no intervalo configurado
    roas_aleatorio = random.uniform(roas_min, roas_max)

    # Calcula o investimento com o input manual
    investimento_calculado = valor_venda / roas_aleatorio
    # Garante que não vai ultrapassar o orçamento máximo
    if investimento_calculado <= investimento_maximo_orcamento:
        return float(investimento_calculado)
    else:
        return 0.0


# COMMAND ----------

df_margem = (
    df_por_semana.withColumn(
        'venda_total',
        F.sum('valor_venda').over(
            Window.partitionBy('campanha', 'produto_id', 'semana')
        ),
    )
    .withColumn(
        "input_maggu",
        calcula_input_maggu_otimo(F.col("valor_venda"), F.col('venda_total')),
    )
    .withColumn(
        "margem_maggu",
        F.round((F.col("input_maggu") - F.col("moedas")), 2),
    )
    .withColumn(
        "roas",
        F.coalesce(F.round(F.col("valor_venda") / (F.col("input_maggu")), 2), F.lit(0)),
    )
    .withColumn("data_execução", F.current_date())
    .filter(
        F.current_date() > F.date_add(F.col("semana"), F.lit(6))
    )  # salva somente semanas fechadas
    .drop('venda_total')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar os resultados na tabela

# COMMAND ----------

margem_maggu_table = spark.read.table(margem_maggu_table_path)

# COMMAND ----------

join_keys = ["semana", "produto_id", "ean", "campanha"]

# Somente registros novos
new_records = df_margem.alias("novas_linhas").join(
    margem_maggu_table.alias("existe"),
    on=[f"{col}" for col in join_keys],
    how="left_anti",
)

insert_count = new_records.count()
print(f"Linhas novas salvas na tabela: {insert_count}")

# Appenda novas semanas sem alterar o passado
if insert_count > 0:
    new_records.write.format("delta").mode("append").saveAsTable(
        margem_maggu_table_path
    )
