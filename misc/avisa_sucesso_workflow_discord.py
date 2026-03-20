# Databricks notebook source
dbutils.widgets.dropdown('stage', 'dev', ['dev', 'prod'])

# COMMAND ----------

from datetime import datetime

import pytz
from pyspark.sql import SparkSession

from maggulake.integrations.discord import enviar_mensagem_discord

# COMMAND ----------

spark = SparkSession.builder.appName("avisa_vendas_ads_discord").getOrCreate()
stage = dbutils.widgets.get("stage")

# TODO: Discord channel ID hardcoded — centralizar em config junto com os demais IDs do projeto.
id_canal_databricks = '1337132977859072020'  # canal de ativação

# COMMAND ----------


if stage == "prod":

    def agora():
        return datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S")

    enviar_mensagem_discord(
        id_canal_databricks,
        f"➡️ Pipeline de dados executado com sucesso às {agora()} ✅.\n\n> Para mais informações acesse: https://stats.uptimerobot.com/q9l0er03jr \n\n---------------------",
    )
