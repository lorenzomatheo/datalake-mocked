# Databricks notebook source
# MAGIC %pip install pandas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import pandas as pd
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

from maggulake.integrations.discord import enviar_mensagem_discord

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])
spark = (
    SparkSession.builder.appName("avisa_vendas_ads_discord")
    .config(conf=config)
    .getOrCreate()
)

stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

tabela_vendas = f"{catalog}.analytics.view_vendas"

path_info_produtos_ads = dbutils.secrets.get(
    scope="databricks", key="SHEETS_PRODUTOS_ADS"
)

# TODO: Discord channel ID hardcoded — centralizar em config junto com os demais IDs do projeto.
id_canal_vendas_ads = 1218342086739492955

# COMMAND ----------

info_produtos_ads = spark.createDataFrame(pd.read_csv(path_info_produtos_ads))
eans_foco = info_produtos_ads.select("ean").distinct()
eans_foco_list = [row.ean for row in eans_foco.collect()]
vendas = spark.read.table(tabela_vendas)

vendas_df = (
    vendas.filter(
        (col("pre_venda_id").isNotNull())
        & (col("conta_loja").isin("adifarma", "farmagui", "maxfarma"))
        & (col("realizada_em") >= expr("CURRENT_TIMESTAMP() - INTERVAL 2 HOURS"))
        & (col("produto_ean").isin(eans_foco_list))
    )
    .select(
        (col("realizada_em") - expr("INTERVAL 2 HOURS")).alias("data_hora_venda"),
        col("conta_loja").alias("conta"),
        col("codigo_loja").alias("loja"),
        expr("INITCAP(username_vendedor)").alias("atendente"),
        col("quantidade"),
        expr("INITCAP(nome_produto)").alias("produto"),
    )
    .orderBy(col("data_hora_venda"))
)

vendas_df = vendas_df.toPandas()

# COMMAND ----------


def cria_mensagem(df: pd.DataFrame) -> str:
    if df.empty:
        print("Sem vendas de ads no período")
        return ""

    message = "🚀🚀🤑🤑 Novas vendas de produtos ADS 🚀🚀🤑🤑:\n\n"
    for _, row in df.iterrows():
        message += f"*Atendente*: **{row['atendente']}**\n"
        message += f"> `Data e Hora da Venda`: {row['data_hora_venda']}\n"
        message += f"> `Conta`: {row['conta']}\n"
        message += f"> `Loja`: {row['loja']}\n"
        message += f"> `Quantidade`: {row['quantidade']}\n"
        message += f"> `Produto`: **{row['produto']}**\n"
        message += "\n-------------------------\n\n"

    return message


# COMMAND ----------

enviar_mensagem_discord(id_canal_vendas_ads, cria_mensagem(vendas_df))
