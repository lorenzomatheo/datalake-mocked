# Databricks notebook source
# MAGIC %md
# MAGIC # Envio de Progressão de Nível para Atendentes
# MAGIC
# MAGIC - Este notebook envia mensagens para atendentes que subiram de nível (1 ao 5) em qualquer missão.
# MAGIC - Caso o atendente passe dois níveis desde a última execução, apenas o nível mais alto será informado.
# MAGIC - Garante que o atendente receba apenas uma mensagem de progressão de nível por dia, para o mesmo nível.
# MAGIC - Há possibilidade de enviar mensagens de níveis diferentes no mesmo dia, desde que janela de 24h esteja aberta no Hyperflow.

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
    calcular_moedas_desbloqueadas_por_nivel_por_missao,
    calcular_potencial_total_moedas_por_missao,
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
) = setup_environment_and_widgets(dbutils, "regua_atendente_progressao_nivel")

# COMMAND ----------

if DEBUG:
    print("Tabela mensagens enviadas: ", TABELA_MENSAGENS_ENVIADAS_PATH)
    print("Tabela erros de envio: ", TABELA_ERROS_ENVIO_PATH)
    print("Somente testes: ", SOMENTE_TESTES)

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
rodada_atual_inicio = rodada_atual.inicio

if df_missoes_pg.isEmpty():
    dbutils.notebook.exit("Não há missões ativas para enviar mensagens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de pontos da rodada atual

# COMMAND ----------

df_pontos_pg = ler_pontos_rodada_atual(spark, postgres, rodada_atual_id)

if df_pontos_pg.isEmpty():
    dbutils.notebook.exit("Não há pontuações para a rodada atual.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos Dados

# COMMAND ----------

CONFIG_NIVEIS = [
    {
        "nivel": "nivel-1",
        "tipo_mensagem": TipoMensagemReguaAtendente.PROGRESSAO_NIVEL_1,
    },
    {
        "nivel": "nivel-2",
        "tipo_mensagem": TipoMensagemReguaAtendente.PROGRESSAO_NIVEL_2,
    },
    {
        "nivel": "nivel-3",
        "tipo_mensagem": TipoMensagemReguaAtendente.PROGRESSAO_NIVEL_3,
    },
    {
        "nivel": "nivel-4",
        "tipo_mensagem": TipoMensagemReguaAtendente.PROGRESSAO_NIVEL_4,
    },
    {
        "nivel": "nivel-5",
        "tipo_mensagem": TipoMensagemReguaAtendente.PROGRESSAO_NIVEL_5,
    },
]

LISTA_NIVEIS_ATIVOS = [c["nivel"] for c in CONFIG_NIVEIS]

# COMMAND ----------

df_progresso = df_pontos_pg.filter(F.col("nivel_atual").isin(LISTA_NIVEIS_ATIVOS))

if df_progresso.isEmpty():
    dbutils.notebook.exit(
        "Nenhum atendente atingiu os níveis configurados nesta rodada."
    )

df_completo = (
    df_progresso.alias("pontos")
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
        F.col("atendentes.atendente_id").alias("atendente_id"),
        "atendentes.username",
        "atendentes.vocativo",
        "atendentes.telefone",
        F.col("missoes.id").alias("missao_id"),
        F.col("missoes.nome").alias("missao_nome"),
        "pontos.nivel_atual",
        "pontos.pontos_totais",
        "missoes.moedas_desbloqueadas_nivel_1",
        "missoes.moedas_desbloqueadas_nivel_2",
        "missoes.moedas_desbloqueadas_nivel_3",
        "missoes.moedas_desbloqueadas_nivel_4",
        "missoes.moedas_desbloqueadas_nivel_5",
    )
)

df_calculado = df_completo.withColumn(
    "premio_total_missao", calcular_potencial_total_moedas_por_missao()
)

df_calculado = df_calculado.withColumn(
    "premio_conquistado_nivel", calcular_moedas_desbloqueadas_por_nivel_por_missao()
)

df_calculado.cache()
count_total = df_calculado.count()
print(f"Total de registros carregados em memória: {count_total}")

if count_total == 0:
    df_calculado.unpersist()
    dbutils.notebook.exit("Nenhum registro válido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Histórico de mensagem enviadas e definição de niveis

# COMMAND ----------

mensagens_enviadas = DeltaTable.forName(spark, TABELA_MENSAGENS_ENVIADAS_PATH).toDF()

mensagens_historico_real = mensagens_enviadas.filter(
    F.col("eh_somente_teste") == F.lit(False)
)
mensagens_historico_real.cache()

todos_erros = []

NIVEL_TO_LABEL_MAP = {
    "nivel-1": "Esquenta",
    "nivel-2": "Iniciante",
    "nivel-3": "Intermediário",
    "nivel-4": "Avançado",
    "nivel-5": "Lenda Viva",
}


def gerar_payload_dinamico(row, telefone_formatado: str, tipo_mensagem_atual) -> dict:
    label_nivel = NIVEL_TO_LABEL_MAP.get(row.nivel_atual, row.nivel_atual)
    premio_formatado = prepara_valor_premiacao(
        row.premio_conquistado_nivel, valor_potencial=None, converter_reais=True
    )
    return {
        "telefone_destinatario": telefone_formatado,
        "tipo_regua": "atendente",
        "tipo_mensagem": tipo_mensagem_atual.value,
        "nome_atendente": row.vocativo,
        "nome_missao": row.missao_nome,
        "valor_premio": premio_formatado,
        "nivel_conquistado": label_nivel,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Loop de Envio por Nível (começando pelo nível mais alto)

# COMMAND ----------

for nivel in reversed(CONFIG_NIVEIS):
    nivel_atual_loop = nivel["nivel"]
    tipo_mensagem_enum = nivel["tipo_mensagem"]

    if disponiveis_para_envio <= 0:
        print("Limite diário atingido!")
        break

    print(f"\nTratando mensagens {nivel_atual_loop} ({tipo_mensagem_enum.value})")

    df_nivel = df_calculado.filter(F.col("nivel_atual") == nivel_atual_loop)

    if df_nivel.isEmpty():
        continue

    window_spec = Window.partitionBy("atendente_id").orderBy(
        F.col("premio_total_missao").desc()
    )

    df_deduplicado = (
        df_nivel.withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    df_para_envio = filtrar_atendentes_sem_mensagem_hoje(
        df_deduplicado,
        mensagens_historico_real,
        tipo_mensagem=tipo_mensagem_enum.value,
        data_inicio_corte=rodada_atual.inicio,
    )

    df_para_envio = df_para_envio.dropDuplicates(["telefone"]).limit(
        disponiveis_para_envio
    )

    qtd_envio = df_para_envio.count()
    if qtd_envio == 0:
        print(f"Todos elegíveis do {nivel_atual_loop} já receberam mensagem hoje.")
        continue

    print(f"Enviando {qtd_envio} mensagens para {nivel_atual_loop}")

    rows_to_send_iterator = df_para_envio.toLocalIterator()

    def payload_generator(row, tel, tipo=tipo_mensagem_enum):
        return gerar_payload_dinamico(row, tel, tipo)

    mensagens_enviadas_descartadas, erros_nivel = enviar_mensagens_e_salvar(
        spark=spark,
        rows_to_send=rows_to_send_iterator,
        tipo_mensagem=tipo_mensagem_enum,
        payload_generator_func=payload_generator,
        tabela_mensagens_path=TABELA_MENSAGENS_ENVIADAS_PATH,
        tabela_erros_path=TABELA_ERROS_ENVIO_PATH,
        eh_somente_teste=SOMENTE_TESTES,
    )

    todos_erros.extend(erros_nivel)
    disponiveis_para_envio -= qtd_envio

df_calculado.unpersist()
mensagens_historico_real.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório de erros

# COMMAND ----------

print_relatorio_erros(todos_erros)
