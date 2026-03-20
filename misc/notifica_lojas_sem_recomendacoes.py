# Databricks notebook source
# MAGIC %md
# MAGIC # Notifica Lojas Sem Recomendações
# MAGIC
# MAGIC Este script identifica lojas que não receberam recomendações nos últimos 7 dias e envia um alerta para o canal do Discord.
# MAGIC O alerta agrupa as lojas por conta e exibe o status de cada loja.

# COMMAND ----------

from collections import defaultdict
from datetime import datetime, timedelta

import pyspark.sql.functions as F

from maggulake.environment import (
    BigqueryGold,
    CopilotTable,
    DatabricksEnvironmentBuilder,
)
from maggulake.integrations.discord import enviar_mensagem_discord

# COMMAND ----------

# TODO: Discord channel ID hardcoded — centralizar em config junto com os demais IDs do projeto.
ID_CANAL_DISCORD = "1398338582888058980"

env = DatabricksEnvironmentBuilder.build(
    "notifica_lojas_sem_recomendacoes",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark

# COMMAND ----------

agora = datetime.now()
semana_passada = agora - timedelta(days=7)

# COMMAND ----------

# TODO: usar o PATTERN do PostgresAdapter
# Lojas teste
lojas_teste = [
    'asasys_loja_teste',
    'Loja Teste Automação',
    'teste',
    'hos_loja_teste',
    'automatiza_loja_teste',
    'MobileEcommerce_Loja_Prototipo',
    'HOSMobileEcommerce_Loja_Prototipo',
    'interativa_loja_prototipo',
    'interativapos:interativa_loja_prototipo',
    'bizpik_loja_prototipo',
    'Toolspharma',
    'Maggu',
    'Consys',
    'dream_loja_desenv',
    'asasys_loja_teste_emerson',
    'asasys_loja_homolog',
]

# COMMAND ----------

lojas = (
    env.table(CopilotTable.contas_loja)
    .select(
        F.col('id').alias('loja_id'),
        'name',
        'status',
        'erp',
        F.col('databricks_tenant_name').alias('conta'),
    )
    .filter(~F.col('name').isin(lojas_teste))
    .filter(~F.lower(F.col('name')).like('%hml%'))
    .filter(~F.lower(F.col('name')).like('%teste%'))
    .filter(~F.lower(F.col('name')).like('%não contém%'))
    .cache()
)

# COMMAND ----------

recomendacoes_loja = (
    env.table(BigqueryGold.view_saude_recomendacoes)
    .filter(F.col('data_criacao_mensagem') >= semana_passada)
    .groupby('loja_id')
    .agg(F.sum('qtd_ean_rec').alias('qtd_ean_rec'))
    .cache()
)

# COMMAND ----------

# vendas após a primeira sala aberta na mini maggu
# tem muitas lojas com vendas que não tem mini maggu, então esse filtro de "ter sala" é importante para entender as lojas que deveriam ter recomendações da mini maggu
vendas_loja = (
    env.table(BigqueryGold.view_vendas_apos_primeira_sala_maggu)
    .alias("v")
    .filter(F.col('data_venda') >= semana_passada)
    .groupby('loja_id')
    .agg(F.sum("qtd_produto_vendido").alias("qtd_produto_vendido"))
)

# COMMAND ----------

# Join para pegar informações da loja e das lojas com vendas
df_sem_recomendacoes = (
    vendas_loja.join(recomendacoes_loja, how="left", on=["loja_id"])
    .join(lojas, how='left', on=['loja_id'])
    .filter((F.col('qtd_ean_rec') == 0) | (F.col('qtd_ean_rec').isNull()))
    .filter(F.col('name').isNotNull())
    .filter(
        F.col('status').isin(['ATIVA', 'EM_ATIVACAO'])
    )  # somente mandar msg para lojas nesses status
    .distinct()
).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construindo mensagem e notificando no canal

# COMMAND ----------

# Agrupamento por ERP -> Conta
lojas_por_erp_conta = defaultdict(lambda: defaultdict(list))
for row in df_sem_recomendacoes:
    if row['conta']:  # Garante que tem conta associada
        erp = row['erp'] if row['erp'] else "Desconhecido"
        lojas_por_erp_conta[erp][row['conta']].append((row['name'], row['status']))

# Construção da mensagem
if lojas_por_erp_conta:
    mensagem = "**🚨 Lojas sem recomendações (últimos 7 dias)**\n\n"
    mensagem += "| ERP | Conta | Loja | Status |\n"
    mensagem += "| :--- | :--- | :--- | :--- |\n"

    for erp, contas in sorted(lojas_por_erp_conta.items()):
        for conta, lojas_info in sorted(contas.items()):
            for nome_loja, status in sorted(lojas_info):
                mensagem += f"| {erp} | {conta} | {nome_loja} | {status} |\n"

    # Envio para o Discord
    print(mensagem)
    enviar_mensagem_discord(spark, ID_CANAL_DISCORD, mensagem)
else:
    print("Nenhuma loja sem recomendações encontrada.")
