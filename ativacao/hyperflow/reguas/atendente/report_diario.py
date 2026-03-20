# Databricks notebook source
# MAGIC %md
# MAGIC # Envio de "Report Diário" para Atendentes
# MAGIC
# MAGIC - Este notebook envia mensagens para o número pessoal dos atendentes com o status atualizado de suas missões.
# MAGIC - A mensagem informa a pontuação naquele dia e o valor já garantido, caso o atendente tenha atingido um novo nível no dia anterior.
# MAGIC - Não enviaremos mensagens para atendentes que não atingiram nenhum nível.
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

from maggulake.ativacao.whatsapp.enums import TipoMensagemReguaAtendente
from maggulake.ativacao.whatsapp.regua_atendente.utils import (
    calcular_moedas_desbloqueadas_por_nivel,
    calcular_potencial_total_moedas,
    calcular_rodadas,
    enviar_mensagens_e_salvar,
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
) = setup_environment_and_widgets(dbutils, "regua_atendente_resumo_diario")


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
# MAGIC ## Filtrar atendentes que já receberam mensagem hoje

# COMMAND ----------

# Ler histórico de mensagens enviadas
mensagens_enviadas_dt = DeltaTable.forName(spark, TABELA_MENSAGENS_ENVIADAS_PATH).toDF()

# Remover atendentes que já receberam mensagem hoje (qualquer tipo)
atendentes_ja_receberam = (
    mensagens_enviadas_dt.filter(F.col("eh_somente_teste") == F.lit(False))
    .filter(F.to_date(F.col("data_hora_envio")) == F.current_date())
    .select("id_atendente")
    .distinct()
)
df_atendentes_pg = df_atendentes_pg.join(
    atendentes_ja_receberam,
    df_atendentes_pg["atendente_id"] == atendentes_ja_receberam["id_atendente"],
    how="left_anti",
)

if df_atendentes_pg.isEmpty():
    dbutils.notebook.exit(
        "Não há atendentes para enviar mensagens. Provavelmente todos já receberam alguma mensagem hoje."
    )

if DEBUG:
    print(
        f"Atendentes que ainda não receberam mensagem hoje: {df_atendentes_pg.count()}"
    )
    df_atendentes_pg.limit(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Join de dados e cálculos

# COMMAND ----------

# Join para pegar o nível atual de cada atendente em cada missão
df_nivel_atual = (
    df_pontos_pg.alias("pontos")
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
        "missoes.pontos_necessarios_nivel_1",
        "missoes.pontos_necessarios_nivel_2",
        "missoes.pontos_necessarios_nivel_3",
        "missoes.pontos_necessarios_nivel_4",
        "missoes.pontos_necessarios_nivel_5",
        # Colunas de Pontuação/Status
        "pontos.pontos_totais",
        "pontos.nivel_atual",
    )
)

# Não enviar mensagem para quem não atingiu nenhum nível ainda
df_nivel_atual = df_nivel_atual.filter(F.col("nivel_atual") != "nenhum")

if df_nivel_atual.isEmpty():
    dbutils.notebook.exit(
        "Não há atendentes que atingiram algum nível para enviar mensagens. Provavelmente a rodada começou há pouco tempo."
    )

if DEBUG:
    print("DataFrame com o nível atual de cada atendente em cada missão:")
    df_nivel_atual.limit(100).display()

# COMMAND ----------

# Calcular moedas desbloqueadas e potencial total usando funções utilitárias
df_moedas_desbloqueadas = calcular_moedas_desbloqueadas_por_nivel(df_nivel_atual)
df_potencial_moedas = calcular_potencial_total_moedas(df_nivel_atual)

if DEBUG:
    print("DataFrame com o total de moedas desbloqueadas de cada atendente:")
    df_moedas_desbloqueadas.limit(100).display()
    print("DataFrame com o potencial total de moedas de cada atendente:")
    df_potencial_moedas.limit(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Remover atendentes que são "Lenda Viva" em tudo

# COMMAND ----------

# Remover atendentes que já conquistaram o nível máximo (lenda viva) em todas as missões que estão concorrendo
# Passo 1 e 2: Contar o total de missões e o total de missões em nível 5 para cada atendente
df_contagem_niveis = df_nivel_atual.groupBy("atendente_id").agg(
    F.count("missao_id").alias("total_missoes"),
    F.sum(F.when(F.col("nivel_atual") == "nivel-5", 1).otherwise(0)).alias(
        "missoes_nivel_5"
    ),
)

# Passo 3: Identificar os atendentes onde as duas contagens são iguais
lendas_vivas_df = df_contagem_niveis.filter(
    F.col("total_missoes") == F.col("missoes_nivel_5")
)

# COMMAND ----------

if DEBUG:
    print("Atendentes que são 'Lenda Viva' em TODAS as suas missões:")
    print("Total: ", lendas_vivas_df.count())
    lendas_vivas_df.display()

# Remover os atendentes "100% Lenda Viva"
df_nivel_atual = df_nivel_atual.join(
    lendas_vivas_df.select("atendente_id"), on="atendente_id", how="left_anti"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparar dados finais para envio

# COMMAND ----------

df_final = df_nivel_atual.join(
    df_moedas_desbloqueadas, on="atendente_id", how="left"
).join(df_potencial_moedas, on="atendente_id", how="left")

# Selecionar apenas colunas necessárias e limpar dados
df_final = df_final.select(
    "atendente_id",
    "username",
    "vocativo",
    "telefone",
    "total_moedas_desbloqueadas",
    "potencial_total_moedas",
)

# Remover registros com campos obrigatórios nulos
df_final = df_final.filter(
    (F.col("total_moedas_desbloqueadas").isNotNull())
    & (F.col("potencial_total_moedas").isNotNull())
    & (F.col("telefone").isNotNull())
    & (F.col("vocativo").isNotNull())
)

# Garantir que não há duplicatas
df_final = df_final.dropDuplicates(["atendente_id"])
df_final = df_final.dropDuplicates(["telefone"])

# Limitar à quantidade disponível de envios
df_final = df_final.limit(disponiveis_para_envio)

total_atendentes_devem_receber_msg = df_final.count()

print(
    f"Total de atendentes que devem receber report diário: {total_atendentes_devem_receber_msg}"
)

if DEBUG:
    df_final.limit(1000).display()

# COMMAND ----------

if df_final.count() < 1:
    dbutils.notebook.exit("Não há registros válidos para enviar mensagens.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envio de mensagens

# COMMAND ----------


def gerar_payload_report_diario(row, telefone_formatado: str) -> dict:
    """
    Gera payload específico para mensagem de 'report diário'.
    """
    premio, potencial = prepara_valor_premiacao(
        row.total_moedas_desbloqueadas, row.potencial_total_moedas
    )

    return {
        "telefone_destinatario": telefone_formatado,
        "tipo_regua": "atendente",
        "tipo_mensagem": TipoMensagemReguaAtendente.REPORTE_DIARIO.value,
        "nome_atendente": row.vocativo,
        "valor_premio": premio,
        "valor_potencial": potencial,
    }


# COMMAND ----------

# Coletar dados e enviar mensagens
rows_to_send = df_final.collect()

mensagens_enviadas, erros = enviar_mensagens_e_salvar(
    spark=spark,
    rows_to_send=rows_to_send,
    tipo_mensagem=TipoMensagemReguaAtendente.REPORTE_DIARIO,
    payload_generator_func=gerar_payload_report_diario,
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
# WHERE tipo_mensagem = 'reporte_diario'
# ORDER BY data_hora_envio DESC
# LIMIT 100;

# COMMAND ----------

# %sql
# SELECT * FROM production.raw.regua_atendente_mensagens_enviadas_erros
# ORDER BY data_hora_erro DESC
# LIMIT 100;
