# Databricks notebook source
# MAGIC %md
# MAGIC # Resumo de Fim de Rodada (semanal)
# MAGIC
# MAGIC - Roda toda segunda-feira.
# MAGIC - Rodadas são segunda a domingo.
# MAGIC - Envia uma mensagem por atendente por rodada, com dois casos:
# MAGIC   - **com ganho**: atendente desbloqueou moedas na rodada anterior → `resumo_rodada_com_ganho`
# MAGIC   - **sem ganho**: atendente **não** desbloqueou nada na rodada anterior → `resumo_rodada_sem_ganho`
# MAGIC - Ordena envios priorizando quem faz mais tempo que não recebe nenhuma mensagem.
# MAGIC - Respeita o limite de MAX_MENSAGENS_24_HORAS.

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
    calcular_moedas_desbloqueadas_por_nivel,
    calcular_potencial_total_moedas,
    calcular_rodadas,
    enviar_mensagens_e_salvar,
    ler_dados_basicos,
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
) = setup_environment_and_widgets(dbutils, "regua_atendente_resumo_fim_rodada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar limite de mensagens

# COMMAND ----------

disponiveis_para_envio = verificar_limite_mensagens_diarias(
    spark, TABELA_MENSAGENS_ENVIADAS_PATH, DEBUG
)

if disponiveis_para_envio < 1:
    dbutils.notebook.exit(
        "Limite de mensagens diárias atingido. Nenhum envio será feito."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler dados básicos (atendentes, missões)

# COMMAND ----------

df_rodadas_pg, df_atendentes_pg, df_missoes_pg = ler_dados_basicos(spark, postgres)

# COMMAND ----------

if DEBUG:
    print(f"Atendentes carregados: {df_atendentes_pg.count()}")
    print(f"Missões ativas: {df_missoes_pg.count()}")

if df_missoes_pg.isEmpty():
    dbutils.notebook.exit("Não há missões ativas para enviar mensagens.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determinar as rodadas atual e anterior

# COMMAND ----------

rodada_atual, rodada_anterior = calcular_rodadas(df_rodadas_pg)

rodada_atual_id = rodada_atual.id
rodada_anterior_id = rodada_anterior.id
rodada_anterior_inicio = rodada_anterior.inicio
rodada_anterior_termino = rodada_anterior.termino

if DEBUG:
    print(
        f"Rodada atual: {rodada_atual.id} ({rodada_atual.inicio} → {rodada_atual.termino})"
    )
    print(
        f"Rodada anterior: {rodada_anterior.id} ({rodada_anterior.inicio} → {rodada_anterior.termino})"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê gameficacao_saldodepontos para uma rodada e mantém o registro mais recente

# COMMAND ----------


def ler_pontos_por_rodada(spark, postgres, rodada_id: str):
    """
    Lê gameficacao_saldodepontos para uma rodada e mantém o registro mais recente
    por (atendente_id, missao_id).
    """
    df = (
        postgres.read_table(spark, table="gameficacao_saldodepontos")
        .select(
            "id",
            "atendente_id",
            "missao_id",
            "rodada_id",
            "pontos_totais",
            "nivel_atual",
            "atualizado_em",
        )
        .filter((F.col("rodada_id") == rodada_id) & F.col("atendente_id").isNotNull())
    )

    w = Window.partitionBy("atendente_id", "missao_id").orderBy(
        F.col("atualizado_em").desc_nulls_last(), F.col("id").desc()
    )
    return (
        df.withColumn("row_num", F.row_number().over(w))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler pontos: rodada anterior e rodada atual

# COMMAND ----------

df_pontos_rodada_anterior = ler_pontos_por_rodada(spark, postgres, rodada_anterior_id)
if df_pontos_rodada_anterior.isEmpty():
    dbutils.notebook.exit("Não há pontuações na rodada anterior — nada a notificar.")

df_pontos_rodada_atual = ler_pontos_por_rodada(spark, postgres, rodada_atual_id)

# Pegar atendente e missão onde houve participação e missão está ativa
participacoes_ativas = (
    df_pontos_rodada_anterior.select("atendente_id", "missao_id")
    .unionByName(df_pontos_rodada_atual.select("atendente_id", "missao_id"))
    .dropDuplicates(["atendente_id", "missao_id"])
    .join(
        df_missoes_pg.selectExpr("id as missao_id").dropDuplicates(["missao_id"]),
        "missao_id",
        "inner",
    )
)

# Adicionar quem ainda não pontuou na rodada atual
df_pontos_rodada_atual = participacoes_ativas.join(
    df_pontos_rodada_atual.select(
        "atendente_id", "missao_id", "pontos_totais", "nivel_atual"
    ),
    ["atendente_id", "missao_id"],
    "left",
).na.fill({"pontos_totais": 0, "nivel_atual": 0})

if DEBUG:
    print("Exemplo pontos (rodada anterior):")
    df_pontos_rodada_anterior.limit(100).display()
# COMMAND ----------

# MAGIC %md
# MAGIC ## Montar nível
# MAGIC

# COMMAND ----------


def montar_nivel(df_pontos):
    return (
        df_pontos.alias("p")
        .join(
            df_atendentes_pg.alias("a"),
            F.col("p.atendente_id") == F.col("a.atendente_id"),
            "inner",
        )
        .join(df_missoes_pg.alias("m"), F.col("p.missao_id") == F.col("m.id"), "inner")
        .select(
            F.col("a.atendente_id").alias("atendente_id"),
            F.col("a.username").alias("username"),
            F.col("a.vocativo").alias("vocativo"),
            F.col("a.telefone").alias("telefone"),
            F.col("m.id").alias("missao_id"),
            F.col("m.nome").alias("missao_nome"),
            "m.moedas_desbloqueadas_nivel_1",
            "m.moedas_desbloqueadas_nivel_2",
            "m.moedas_desbloqueadas_nivel_3",
            "m.moedas_desbloqueadas_nivel_4",
            "m.moedas_desbloqueadas_nivel_5",
            "m.pontos_necessarios_nivel_1",
            "m.pontos_necessarios_nivel_2",
            "m.pontos_necessarios_nivel_3",
            "m.pontos_necessarios_nivel_4",
            "m.pontos_necessarios_nivel_5",
            F.col("p.pontos_totais").alias("pontos_totais"),
            F.col("p.nivel_atual").alias("nivel_atual"),
        )
    )


df_nivel_rodada_anterior = montar_nivel(df_pontos_rodada_anterior)
df_nivel_rodada_atual = montar_nivel(df_pontos_rodada_atual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ganho desbloqueado (rodada anterior) e potencial (rodada atual)

# COMMAND ----------

df_ganho_desbloqueado = calcular_moedas_desbloqueadas_por_nivel(
    df_nivel_rodada_anterior
)

df_ganho_potencial = calcular_potencial_total_moedas(df_nivel_rodada_atual)

if DEBUG:
    print("Ganho desbloqueado (rodada anterior):")
    df_ganho_desbloqueado.limit(100).display()
    print("Potencial total (rodada atual):")
    df_ganho_potencial.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de atendentes elegíveis + classificação com/sem ganho

# COMMAND ----------

df_base = (
    df_atendentes_pg.select("atendente_id", "username", "vocativo", "telefone")
    .dropDuplicates(["atendente_id"])
    .join(df_ganho_desbloqueado, "atendente_id", "left")
    .join(df_ganho_potencial, "atendente_id", "left")
    .withColumn(
        "com_ganho",
        (F.coalesce(F.col("total_moedas_desbloqueadas"), F.lit(0)) > F.lit(0)),
    )
    .filter((F.col("telefone").isNotNull()) & (F.col("vocativo").isNotNull()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Garantir uma mensagem por atendente por **rodada** (evita reenvio na mesma rodada)

# COMMAND ----------

mensagens_enviadas_df = DeltaTable.forName(spark, TABELA_MENSAGENS_ENVIADAS_PATH).toDF()

tipos_resumo = [
    TipoMensagemReguaAtendente.RESUMO_RODADA_COM_GANHO.value,
    TipoMensagemReguaAtendente.RESUMO_RODADA_SEM_GANHO.value,
]

ja_enviou_resumo_da_rodada_anterior = (
    mensagens_enviadas_df.filter(F.col("eh_somente_teste") == False)
    .filter(F.col("tipo_mensagem").isin(tipos_resumo))
    .filter(
        (F.to_date(F.col("data_hora_envio")) >= F.lit(rodada_anterior_inicio))
        & (F.to_date(F.col("data_hora_envio")) <= F.lit(rodada_anterior_termino))
    )
    .select(F.col("id_atendente").alias("atendente_id"))
    .distinct()
)

df_elegiveis = df_base.join(
    ja_enviou_resumo_da_rodada_anterior, "atendente_id", "left_anti"
)

if df_elegiveis.isEmpty():
    dbutils.notebook.exit("Todos os atendentes já receberam o resumo desta rodada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ordenação por quem recebeu há mais tempo

# COMMAND ----------

ultima_por_atendente = (
    mensagens_enviadas_df.filter(F.col("eh_somente_teste") == False)
    .groupBy("id_atendente")
    .agg(F.max("data_hora_envio").alias("ultima_msg"))
    .withColumnRenamed("id_atendente", "atendente_id")
)

df_elegiveis = df_elegiveis.join(ultima_por_atendente, "atendente_id", "left").orderBy(
    F.col("ultima_msg").asc_nulls_first()
)

if DEBUG:
    print("Elegíveis (ordenados por última mensagem mais antiga):")
    df_elegiveis.limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restringir à janela de 24h

# COMMAND ----------

df_elegiveis = df_elegiveis.limit(disponiveis_para_envio)
if df_elegiveis.isEmpty():
    dbutils.notebook.exit("Sem capacidade de envio disponível (janela 24h).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payloads e pipeline de envio

# COMMAND ----------

TIPO_COM_GANHO = TipoMensagemReguaAtendente.RESUMO_RODADA_COM_GANHO
TIPO_SEM_GANHO = TipoMensagemReguaAtendente.RESUMO_RODADA_SEM_GANHO


def gerar_payload_resumo_com_ganho(row, telefone_formatado: str) -> dict:
    premio = prepara_valor_premiacao(
        int(row.total_moedas_desbloqueadas or 0), None, converter_reais=True
    )
    return {
        "telefone_destinatario": telefone_formatado,
        "tipo_regua": "atendente",
        "tipo_mensagem": TIPO_COM_GANHO.value,
        "nome_atendente": row.vocativo,
        "valor_premio": premio,
    }


def gerar_payload_resumo_sem_ganho(row, telefone_formatado: str) -> dict:
    potencial = prepara_valor_premiacao(
        0, int(row.potencial_total_moedas or 0), converter_reais=True
    )
    valor_potencial = potencial[1] if isinstance(potencial, tuple) else potencial
    return {
        "telefone_destinatario": telefone_formatado,
        "tipo_regua": "atendente",
        "tipo_mensagem": TIPO_SEM_GANHO.value,
        "nome_atendente": row.vocativo,
        "valor_potencial": valor_potencial,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Quebrar em 2 listas (com/sem ganho) e enviar

# COMMAND ----------

df_com_ganho = df_elegiveis.filter(F.col("com_ganho") == True)
df_sem_ganho = df_elegiveis.filter(F.col("com_ganho") == False)

restantes = df_elegiveis.count()

if DEBUG:
    print("Resumo — COM ganho (qtde):", df_com_ganho.count())
    print("Resumo — SEM ganho (qtde):", df_sem_ganho.count())
    print("Mensagens restantes:", restantes)

mensagens_enviadas_total, erros_total = [], []

# Enviar COM ganho primeiro
if restantes > 0 and not df_com_ganho.isEmpty():
    rows = df_com_ganho.limit(restantes).collect()
    enviados, erros = enviar_mensagens_e_salvar(
        spark=spark,
        rows_to_send=rows,
        tipo_mensagem=TIPO_COM_GANHO,
        payload_generator_func=gerar_payload_resumo_com_ganho,
        tabela_mensagens_path=TABELA_MENSAGENS_ENVIADAS_PATH,
        tabela_erros_path=TABELA_ERROS_ENVIO_PATH,
        eh_somente_teste=SOMENTE_TESTES,
    )
    mensagens_enviadas_total += enviados
    erros_total += erros
    restantes -= len(rows)

# Enviar SEM ganho, se ainda houverem mensagens disponíveis
if restantes > 0 and not df_sem_ganho.isEmpty():
    rows = df_sem_ganho.limit(restantes).collect()
    enviados, erros = enviar_mensagens_e_salvar(
        spark=spark,
        rows_to_send=rows,
        tipo_mensagem=TIPO_SEM_GANHO,
        payload_generator_func=gerar_payload_resumo_sem_ganho,
        tabela_mensagens_path=TABELA_MENSAGENS_ENVIADAS_PATH,
        tabela_erros_path=TABELA_ERROS_ENVIO_PATH,
        eh_somente_teste=SOMENTE_TESTES,
    )
    mensagens_enviadas_total += enviados
    erros_total += erros
    restantes -= len(rows)

print(f"Mensagens enviadas nesta execução: {len(mensagens_enviadas_total)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório de erros

# COMMAND ----------

print_relatorio_erros(erros_total)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizar tabelas

# COMMAND ----------

# %sql
# SELECT * FROM production.raw.regua_atendente_mensagens_enviadas
# WHERE tipo_mensagem IN ('resumo_rodada_com_ganho','resumo_rodada_sem_ganho')
# ORDER BY data_hora_envio DESC
# LIMIT 100;

# COMMAND ----------

# %sql
# SELECT * FROM production.raw.regua_atendente_mensagens_enviadas_erros
# ORDER BY data_hora_erro DESC
# LIMIT 100;
