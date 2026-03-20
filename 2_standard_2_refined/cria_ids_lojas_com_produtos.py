# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cria a coluna `ids_lojas_com_produtos` baseado em 3 regras :
# MAGIC - Produtos em campanha ativas pq são produtos importantes para o negócio com missões, análises
# MAGIC - Produtos com venda nos últimos 3 meses
# MAGIC - Produtos novos que não tiveram vendas. Esse grupo é importante para garantir que produtos novos tenham recomendações e caso fiquem sem vendas por um longo periodo, basta criar uma campanha para recomendar novamente

# COMMAND ----------

import pyspark.sql.functions as F

from maggulake.environment import CopilotTable, DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_ids_lojas_com_produto

# COMMAND ----------

# Inicializa environment e widgets
env = DatabricksEnvironmentBuilder.build(
    "cria_ids_lojas_com_produtos",
    dbutils,
)

HORIZONTE_VENDAS_MESES = 3
DATA_CRIACAO_PRODUTO_MESES = 1

spark = env.spark

# COMMAND ----------

# Garante existência da tabela de saída
env.create_table_if_not_exists(
    Table.ids_lojas_com_produto,
    schema_ids_lojas_com_produto,
)


bq = env.bigquery_adapter
# COMMAND ----------


produtos_loja_copilot = (
    env.table(CopilotTable.produtos_loja)
    .filter(F.col("estoque_unid") > 0)  # somente com estoque>0
    .cache()
)

# NOTE: materializa o cache()
produtos_loja_copilot.limit(10).display()

# COMMAND ----------

# 1) Produtos em campanha ativa
produtos_campanha_ativa = bq.get_produtos_campanha_ativa()

# 2) Produtos vendidos nos últimos meses
produtos_com_venda = bq.get_produtos_com_vendas_recentes(
    horizonte_meses=HORIZONTE_VENDAS_MESES
)

# 3) Produtos novos sem vendas
produtos_novos_sem_vendas = bq.get_produtos_novos_sem_vendas(
    janela_meses=DATA_CRIACAO_PRODUTO_MESES
)

# Escopo final
if env.settings.catalog == "production":
    # Produção: Campanha + Vendas + Novos
    ids_produtos_escopo = (
        produtos_com_venda.unionByName(produtos_campanha_ativa)
        .unionByName(produtos_novos_sem_vendas)
        .distinct()
    )
else:
    # Staging/Dev: Campanha + Tudo com Estoque (ignora vendas)
    ids_produtos_estoque = produtos_loja_copilot.select("ean").distinct()

    ids_produtos_escopo = ids_produtos_estoque.unionByName(
        produtos_campanha_ativa
    ).distinct()

# COMMAND ----------

print(
    f"✅ produtos_campanha_ativa: {produtos_campanha_ativa.count()} produtos\n",
    f"✅ produtos_com_venda: {produtos_com_venda.count()} produtos\n",
    f"✅ produtos_novos_sem_vendas: {produtos_novos_sem_vendas.count()} produtos\n",
)

# COMMAND ----------

# seleciona mix com produtos e lojas definidos
produtos_loja_filtrado = produtos_loja_copilot.where(
    "ean IS NOT NULL AND loja_id IS NOT NULL"
).join(ids_produtos_escopo, on=["ean"], how="inner")


# COMMAND ----------

# Agrega saída final com unicidade por EAN
ids_lojas_df = (
    produtos_loja_filtrado.groupBy("ean")
    .agg(
        # Coleta identificadores tenant:codigo_loja únicos
        F.collect_set(
            F.concat(F.col("tenant"), F.lit(":"), F.col("codigo_loja"))
        ).alias("tenant_lojas"),
        # Coleta IDs de lojas únicos
        F.collect_set(F.col("loja_id").cast("string")).alias("ids_lojas_com_produto"),
    )
    # Formata tenant_lojas como string delimitada por pipes: |tenant1:loja1|tenant2:loja2|
    .withColumn(
        "lojas_com_produto",
        F.concat(F.lit("|"), F.concat_ws("|", F.col("tenant_lojas")), F.lit("|")),
    )
    .drop("tenant_lojas")  # Remove coluna intermediária
    # Adiciona timestamps
    .withColumn("gerado_em", F.current_timestamp())
    .withColumn("atualizado_em", F.current_timestamp())
)

# COMMAND ----------

# Salva na tabela destino
ids_lojas_df.write.mode("overwrite").saveAsTable(Table.ids_lojas_com_produto.value)
