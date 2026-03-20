# Databricks notebook source
# MAGIC %md
# MAGIC # Envio de "Virou Lenda Viva" para Atendentes
# MAGIC
# MAGIC - IMPORTANTE - Este notebook foi sustituido pelo notebook de subiu de nível, que agora inclui o envio de mensagens para lendas vivas.
# MAGIC - Este notebook envia mensagens para atendentes que acabaram de virar "Lenda Viva" (nível-5) em qualquer missão.
# MAGIC - A mensagem é enviada imediatamente quando o atendente atinge o nível máximo em uma missão.
# MAGIC - Calcula o prêmio total da missão específica onde virou lenda viva.
# MAGIC - Não enviará mais de uma mensagem por atendente por dia.
# MAGIC - Se o atendente virar lenda viva em múltiplas missões, escolhe a de maior prêmio.
# MAGIC - O envio é realizado via Hyperflow.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql.window import Window

from maggulake.ativacao.whatsapp.enums import TipoMensagemReguaAtendente
from maggulake.ativacao.whatsapp.regua_atendente.utils import (
    calcular_rodadas,
    enviar_mensagens_e_salvar,
    filtrar_atendentes_sem_mensagem_hoje,
    ler_dados_basicos,
    ler_pontos_rodada_atual,
    prepara_valor_premiacao,
    print_relatorio_erros,
    setup_environment_and_widgets,
    verificar_limite_mensagens_diarias,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment

# COMMAND ----------

(
    env,
    TABELA_MENSAGENS_ENVIADAS_PATH,
    TABELA_ERROS_ENVIO_PATH,
    SOMENTE_TESTES,
    DEBUG,
    postgres,
) = setup_environment_and_widgets(dbutils, "regua_atendente_virou_lenda_viva")


# COMMAND ----------

print("Tabela mensagens enviadas: ", TABELA_MENSAGENS_ENVIADAS_PATH)
print("Tabela erros de envio: ", TABELA_ERROS_ENVIO_PATH)
print("Somente testes: ", SOMENTE_TESTES)
print("Debug: ", DEBUG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar limite de mensagens

# COMMAND ----------

disponiveis_para_envio = verificar_limite_mensagens_diarias(
    spark, TABELA_MENSAGENS_ENVIADAS_PATH, DEBUG
)

if disponiveis_para_envio <= 0:
    dbutils.notebook.exit(
        "Limite de mensagens diárias atingido. Nenhum envio será feito."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de dados básicos

# COMMAND ----------

df_rodadas_pg, df_atendentes_pg, df_missoes_pg = ler_dados_basicos(spark, postgres)

rodada_atual = calcular_rodadas(df_rodadas_pg, incluir_anterior=False)
rodada_atual_id = rodada_atual.id

if DEBUG:
    print(f"Rodada atual: {rodada_atual_id}")
    print(f"Atendentes carregados: {df_atendentes_pg.count()}")
    print(f"Missões ativas: {df_missoes_pg.count()}")

# COMMAND ----------

if df_missoes_pg.isEmpty():
    dbutils.notebook.exit("Não há missões ativas para enviar mensagens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de pontos da rodada atual

# COMMAND ----------

df_pontos_pg = ler_pontos_rodada_atual(spark, postgres, rodada_atual_id)

if df_pontos_pg.isEmpty():
    dbutils.notebook.exit(
        "Não há pontuações para a rodada atual. Provavelmente a rodada acabou de começar."
    )

if DEBUG:
    print("Pontuações filtradas para a rodada atual:")
    df_pontos_pg.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtrar apenas atendentes "Lenda Viva" (nível-5)

# COMMAND ----------

# Filtrar apenas atendentes que atingiram nível-5 em pelo menos uma missão
df_lendas_vivas = df_pontos_pg.filter(F.col("nivel_atual") == "nivel-5")

if df_lendas_vivas.isEmpty():
    dbutils.notebook.exit("Não há atendentes que viraram lenda viva recentemente.")

if DEBUG:
    print(f"Atendentes que são lenda viva: {df_lendas_vivas.count()}")
    df_lendas_vivas.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join com dados de atendentes e missões

# COMMAND ----------

df_lendas_completo = (
    df_lendas_vivas.alias("pontos")
    .join(
        df_atendentes_pg.alias("atendentes"),
        F.col("pontos.atendente_id") == F.col("atendentes.atendente_id"),
        "inner",
    )
    .join(
        df_missoes_pg.alias("missoes"),
        F.col("pontos.missao_id") == F.col("missoes.id"),
        "inner",
    )
    .select(
        # Colunas do Atendente
        F.col("atendentes.atendente_id").alias("atendente_id"),
        "atendentes.username",
        "atendentes.vocativo",
        "atendentes.telefone",
        # Colunas da Missão
        F.col("missoes.id").alias("missao_id"),
        F.col("missoes.nome").alias("missao_nome"),
        "missoes.moedas_desbloqueadas_nivel_1",
        "missoes.moedas_desbloqueadas_nivel_2",
        "missoes.moedas_desbloqueadas_nivel_3",
        "missoes.moedas_desbloqueadas_nivel_4",
        "missoes.moedas_desbloqueadas_nivel_5",
        # Colunas de Pontuação/Status
        "pontos.pontos_totais",
        "pontos.nivel_atual",
    )
)

if DEBUG:
    print("DataFrame com atendentes lenda viva e suas missões:")
    df_lendas_completo.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calcular prêmio total por missão

# COMMAND ----------

# Para cada atendente+missão, calcular o prêmio total (soma de todos os níveis)
df_lendas_com_premio = df_lendas_completo.withColumn(
    "premio_total_missao",
    F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0))
    + F.coalesce(F.col("moedas_desbloqueadas_nivel_2"), F.lit(0))
    + F.coalesce(F.col("moedas_desbloqueadas_nivel_3"), F.lit(0))
    + F.coalesce(F.col("moedas_desbloqueadas_nivel_4"), F.lit(0))
    + F.coalesce(F.col("moedas_desbloqueadas_nivel_5"), F.lit(0)),
)

if DEBUG:
    print("DataFrame com prêmios calculados:")
    df_lendas_com_premio.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecionar uma missão por atendente (maior prêmio)

# COMMAND ----------

# Se atendente virou lenda viva em múltiplas missões, pegar a de maior prêmio


window_spec = Window.partitionBy("atendente_id").orderBy(
    F.col("premio_total_missao").desc()
)

df_uma_missao_por_atendente = (
    df_lendas_com_premio.withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") == 1)
    .drop("rank")
)

if DEBUG:
    print("DataFrame com uma missão por atendente (maior prêmio):")
    df_uma_missao_por_atendente.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtrar atendentes que já receberam mensagem hoje

# COMMAND ----------

# Ler histórico de mensagens enviadas
mensagens_enviadas_dt = DeltaTable.forName(spark, TABELA_MENSAGENS_ENVIADAS_PATH).toDF()

# TODO: Se o atendente virar lenda viva em 2 missões no mesmo dia, atualmente enviamos só 1 mensagem.
# No dia seguinte, ele volta a ser elegível para receber a mensagem novamente. Deveria permitir múltiplas
# mensagens no mesmo dia, desde que de missões diferentes.
# Filtrar atendentes que ainda não receberam mensagem do tipo "virou_lenda_viva_agora" hoje
df_final = filtrar_atendentes_sem_mensagem_hoje(
    df_uma_missao_por_atendente,
    mensagens_enviadas_dt,
    tipo_mensagem=TipoMensagemReguaAtendente.VIROU_LENDA_VIVA_AGORA.value,
)

if df_final.isEmpty():
    dbutils.notebook.exit(
        "Não há atendentes para enviar mensagens. Provavelmente todos já receberam mensagem de 'virou lenda viva' hoje."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparar dados finais para envio

# COMMAND ----------

# Selecionar apenas colunas necessárias e limpar dados
df_final = df_final.select(
    "atendente_id",
    "username",
    "vocativo",
    "telefone",
    "missao_nome",
    "premio_total_missao",
)

# Remover registros com campos obrigatórios nulos
df_final = df_final.filter(
    (F.col("premio_total_missao").isNotNull())
    & (F.col("telefone").isNotNull())
    & (F.col("vocativo").isNotNull())
    & (F.col("missao_nome").isNotNull())
)

# COMMAND ----------

# Garantir que não há duplicatas
# Exibir duplicatas de telefone para investigação
df_duplicatas_telefone = df_final.groupBy("telefone").count().filter(F.col("count") > 1)
if df_duplicatas_telefone.count() > 0:
    print("Telefones duplicados encontrados:")
    df_final.join(df_duplicatas_telefone, "telefone", "inner").display()
else:
    print("Nenhum telefone duplicado encontrado.")

df_final = df_final.dropDuplicates(["telefone"])

# COMMAND ----------

# Limitar à quantidade disponível de envios
df_final = df_final.limit(disponiveis_para_envio)

total_atendentes_devem_receber_msg = df_final.count()

print(
    f"Total de atendentes que devem receber mensagem 'virou lenda viva': {total_atendentes_devem_receber_msg}"
)

if DEBUG:
    df_final.limit(1000).display()

# COMMAND ----------

if total_atendentes_devem_receber_msg < 1:
    dbutils.notebook.exit("Não há registros válidos para enviar mensagens.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envio de mensagens

# COMMAND ----------


def gerar_payload_virou_lenda_viva(row, telefone_formatado: str) -> dict:
    """
    Gera payload específico para mensagem de 'virou lenda viva'.
    """
    premio_formatado = prepara_valor_premiacao(
        row.premio_total_missao, valor_potencial=None, converter_reais=True
    )

    return {
        "telefone_destinatario": telefone_formatado,
        "tipo_regua": "atendente",
        "tipo_mensagem": TipoMensagemReguaAtendente.VIROU_LENDA_VIVA_AGORA.value,
        "nome_atendente": row.vocativo,
        "nome_missao": row.missao_nome,
        "valor_premio": premio_formatado,
    }


# COMMAND ----------

# Coletar dados e enviar mensagens
rows_to_send = df_final.collect()

mensagens_enviadas, erros = enviar_mensagens_e_salvar(
    spark=spark,
    rows_to_send=rows_to_send,
    tipo_mensagem=TipoMensagemReguaAtendente.VIROU_LENDA_VIVA_AGORA,
    payload_generator_func=gerar_payload_virou_lenda_viva,
    tabela_mensagens_path=TABELA_MENSAGENS_ENVIADAS_PATH,
    tabela_erros_path=TABELA_ERROS_ENVIO_PATH,
    eh_somente_teste=SOMENTE_TESTES,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório de erros

# COMMAND ----------

print_relatorio_erros(erros)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizar tabelas

# COMMAND ----------

# %sql
# SELECT * FROM production.raw.regua_atendente_mensagens_enviadas
# WHERE tipo_mensagem = 'virou_lenda_viva_agora'
# ORDER BY data_hora_envio DESC
# LIMIT 100;

# COMMAND ----------

# %sql
# SELECT * FROM production.raw.regua_atendente_mensagens_enviadas_erros
# ORDER BY data_hora_erro DESC
# LIMIT 100;
