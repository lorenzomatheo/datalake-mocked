"""
Funções comuns para notebooks de réguas de atendente.
Centraliza lógica reutilizada entre report_diario.py e virou_lenda_viva.py.
"""

import random
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Tuple

import pyspark.sql.functions as F
import pytz
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from maggulake.ativacao.whatsapp.enums import TipoMensagemReguaAtendente
from maggulake.ativacao.whatsapp.exceptions import NumeroTelefoneInvalidoError
from maggulake.ativacao.whatsapp.hyperflow.config import (
    CLIENT_ID_HYPERFLOW_PROD,
    MAX_MENSAGENS_24_HORAS,
)
from maggulake.ativacao.whatsapp.hyperflow.enviar_mensagem_via_hyperflow import (
    enviar_mensagem_whatsapp_via_hyperflow_regua,
)
from maggulake.ativacao.whatsapp.hyperflow.utils import padroniza_numero_whatsapp
from maggulake.ativacao.whatsapp.schemas.erros_envio import (
    ErroEnvioLog,
    erros_envio_schema,
)
from maggulake.ativacao.whatsapp.schemas.mensagens_enviadas import (
    MensagemEnviada,
    mensagens_enviadas_schema,
)
from maggulake.ativacao.whatsapp.utils import extrair_vocativo
from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.io.postgres import PostgresAdapter

# Constantes
CAMBIO_MAGGULETES_REAL = 100
MIN_DELAY_SECONDS = 0.1
MAX_DELAY_SECONDS = 2.0


@dataclass
class Rodada:
    id: str
    inicio: date
    termino: date


def setup_environment_and_widgets(dbutils, notebook_name: str):
    """
    Configura environment e widgets padrão para notebooks de régua de atendente.
    """
    env = DatabricksEnvironmentBuilder.build(notebook_name, dbutils)

    TABELA_MENSAGENS_ENVIADAS_PATH = (
        f"{env.settings.catalog}.raw.regua_atendente_mensagens_enviadas"
    )
    TABELA_ERROS_ENVIO_PATH = (
        f"{env.settings.catalog}.raw.regua_atendente_mensagens_enviadas_erros"
    )

    # Widgets criados depois do environment
    dbutils.widgets.dropdown("somente_testes", "True", ["False", "True"])
    dbutils.widgets.dropdown("debug", "True", ["False", "True"])

    SOMENTE_TESTES = dbutils.widgets.get("somente_testes") == "True"
    DEBUG = dbutils.widgets.get("debug") == "True"

    # PostgreSQL
    USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
    PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
    postgres = PostgresAdapter("prod", USER, PASSWORD, utilizar_read_replica=True)

    return (
        env,
        TABELA_MENSAGENS_ENVIADAS_PATH,
        TABELA_ERROS_ENVIO_PATH,
        SOMENTE_TESTES,
        DEBUG,
        postgres,
    )


def verificar_limite_mensagens_diarias(
    spark: SparkSession, tabela_mensagens_path: str, debug: bool = False
) -> int:
    """
    Verifica quantas mensagens foram enviadas nas últimas 24h e retorna quantas ainda podem ser enviadas.
    """
    mensagens_enviadas_dt = DeltaTable.forName(spark, tabela_mensagens_path)
    mensagens_enviadas_dt = mensagens_enviadas_dt.toDF()

    msgs_enviadas_nas_ultimas_24h = (
        mensagens_enviadas_dt.filter(
            F.col("data_hora_envio")
            >= (F.current_timestamp() - F.expr("INTERVAL 24 HOURS"))
        )
        .filter(F.col("eh_somente_teste") == F.lit(False))
        .count()
    )

    disponiveis_para_envio = MAX_MENSAGENS_24_HORAS - msgs_enviadas_nas_ultimas_24h

    if debug:
        print(
            f"Mensagens enviadas nas últimas 24 horas: {msgs_enviadas_nas_ultimas_24h}"
        )
        print(f"Limite máximo de mensagens em 24 horas: {MAX_MENSAGENS_24_HORAS}")
        print(f"Mensagens disponíveis para envio: {disponiveis_para_envio}")

    return disponiveis_para_envio


def calcular_rodadas(
    df_rodadas: DataFrame, incluir_anterior: bool = True
) -> Rodada | tuple[Rodada, Rodada]:
    # Rodada atual
    atuais = (
        df_rodadas.filter(
            (F.current_date() >= F.col("data_inicio"))
            & (F.current_date() <= F.col("data_termino"))
        )
        .select("id", "data_inicio", "data_termino")
        .limit(1)
        .collect()
    )
    if len(atuais) != 1:
        raise ValueError("Não foi possível determinar a rodada atual.")

    rodada_atual = Rodada(
        id=str(atuais[0]["id"]),
        inicio=atuais[0]["data_inicio"],
        termino=atuais[0]["data_termino"],
    )

    if not incluir_anterior:
        return rodada_atual

    # Rodada anterior
    anteriores = (
        df_rodadas.filter(F.col("data_termino") < F.current_date())
        .orderBy(F.col("data_termino").desc())
        .select("id", "data_inicio", "data_termino")
        .limit(1)
        .collect()
    )
    if not anteriores:
        raise ValueError("Não foi possível determinar a rodada anterior.")

    rodada_anterior = Rodada(
        id=str(anteriores[0]["id"]),
        inicio=anteriores[0]["data_inicio"],
        termino=anteriores[0]["data_termino"],
    )

    return rodada_atual, rodada_anterior


def ler_dados_basicos(
    spark: SparkSession, postgres: PostgresAdapter
) -> tuple[DataFrame, DataFrame, DataFrame]:
    # Ler rodadas
    df_rodadas_pg = postgres.read_table(spark, "gameficacao_rodada").select(
        "id", "data_inicio", "data_termino"
    )

    # Ler atendentes
    df_atendentes_pg = postgres.get_atendentes()
    df_atendentes_pg = df_atendentes_pg[df_atendentes_pg["nome"] != ""]
    df_atendentes_pg["vocativo"] = df_atendentes_pg["nome"].apply(extrair_vocativo)
    df_atendentes_pg = spark.createDataFrame(df_atendentes_pg)
    df_atendentes_pg = df_atendentes_pg.filter(
        (F.col("status_ativacao") == "cadastrado")
        & F.col("deve_receber_comunicacoes_no_whatsapp")
    )

    # Ler missões
    df_missoes_pg = postgres.read_table(spark, table="gameficacao_missao").select(
        "id",
        "nome",
        "nome_interno",
        "data_inicio",
        "data_termino",
        "removido_em",
        "moedas_desbloqueadas_nivel_1",
        "moedas_desbloqueadas_nivel_2",
        "moedas_desbloqueadas_nivel_3",
        "moedas_desbloqueadas_nivel_4",
        "moedas_desbloqueadas_nivel_5",
        "pontos_necessarios_nivel_1",
        "pontos_necessarios_nivel_2",
        "pontos_necessarios_nivel_3",
        "pontos_necessarios_nivel_4",
        "pontos_necessarios_nivel_5",
    )

    # Filtrar missões ativas hoje
    df_missoes_pg = df_missoes_pg.filter(
        (F.col("removido_em").isNull()) | (F.col("removido_em") > F.current_date())
    ).filter(
        (F.col("data_termino") >= F.current_date())
        & (F.col("data_inicio") <= F.current_date())
    )

    return df_rodadas_pg, df_atendentes_pg, df_missoes_pg


def ler_pontos_rodada_atual(
    spark: SparkSession, postgres: PostgresAdapter, rodada_atual_id: str
):
    """
    Lê pontos da rodada atual, pegando apenas o registro mais recente por atendente.
    """
    df_pontos_pg = postgres.read_table(spark, table="gameficacao_saldodepontos").select(
        "id",
        "atendente_id",
        "missao_id",
        "rodada_id",
        "pontos_totais",
        "nivel_atual",
        "atualizado_em",
    )

    # Precaução: filtrar atendente_id nulo
    df_pontos_pg = df_pontos_pg.filter(F.col("atendente_id").isNotNull())

    # Filtrar rodada atual
    df_pontos_pg = df_pontos_pg.filter(F.col("rodada_id") == rodada_atual_id)

    # Pegar apenas o registro mais recente por atendente
    window_spec = Window.partitionBy("atendente_id").orderBy(
        F.col("atualizado_em").desc()
    )
    df_pontos_pg = (
        df_pontos_pg.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    return df_pontos_pg


def prepara_valor_premiacao(
    qtd_moedas: int, valor_potencial: int | None = None, converter_reais: bool = True
) -> str | Tuple[str, str]:
    if converter_reais:
        premio = f"R${qtd_moedas / CAMBIO_MAGGULETES_REAL:.0f}"
        potencial = (
            f"R${valor_potencial / CAMBIO_MAGGULETES_REAL:.0f}"
            if valor_potencial
            else None
        )
    else:
        premio = f"{qtd_moedas} magguletes"
        potencial = f"{valor_potencial} magguletes" if valor_potencial else None

    if valor_potencial is not None:
        return premio, potencial
    return premio


def calcular_moedas_desbloqueadas_por_nivel(df_nivel_atual: DataFrame) -> DataFrame:
    """
    Calcula total de moedas desbloqueadas para cada atendente baseado no nível atual.
    """
    # NOTE: o F.lit(0) serve para evitar nulls na soma (null + int = null)
    df_moedas_desbloqueadas = (
        df_nivel_atual.withColumn(
            "moedas_desbloqueadas",
            F.when(
                F.col("nivel_atual") == "nivel-1",
                F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0)),
            )
            .when(
                F.col("nivel_atual") == "nivel-2",
                F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_2"), F.lit(0)),
            )
            .when(
                F.col("nivel_atual") == "nivel-3",
                F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_2"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_3"), F.lit(0)),
            )
            .when(
                F.col("nivel_atual") == "nivel-4",
                F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_2"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_3"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_4"), F.lit(0)),
            )
            .when(
                F.col("nivel_atual") == "nivel-5",
                F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_2"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_3"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_4"), F.lit(0))
                + F.coalesce(F.col("moedas_desbloqueadas_nivel_5"), F.lit(0)),
            )
            .otherwise(F.lit(0)),
        )
        .groupBy("atendente_id")
        .agg(F.sum("moedas_desbloqueadas").alias("total_moedas_desbloqueadas"))
    )

    return df_moedas_desbloqueadas


def calcular_potencial_total_moedas(df_nivel_atual: DataFrame) -> DataFrame:
    """
    Calcula potencial total de moedas para cada atendente (se chegasse ao nível 5 em tudo).
    """
    df_potencial_moedas = df_nivel_atual.groupBy("atendente_id").agg(
        F.sum(
            F.col("moedas_desbloqueadas_nivel_1")
            + F.col("moedas_desbloqueadas_nivel_2")
            + F.col("moedas_desbloqueadas_nivel_3")
            + F.col("moedas_desbloqueadas_nivel_4")
            + F.col("moedas_desbloqueadas_nivel_5")
        ).alias("potencial_total_moedas")
    )

    return df_potencial_moedas


def calcular_moedas_desbloqueadas_por_nivel_por_missao() -> F.Column:
    """
    Retorna a expressão PySpark para o prêmio acumulado de moedas, dependendo do nivel_atual.
    """
    n1 = F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0))
    n2 = F.coalesce(F.col("moedas_desbloqueadas_nivel_2"), F.lit(0))
    n3 = F.coalesce(F.col("moedas_desbloqueadas_nivel_3"), F.lit(0))
    n4 = F.coalesce(F.col("moedas_desbloqueadas_nivel_4"), F.lit(0))
    return (
        F.when(F.col("nivel_atual") == "nivel-1", n1)
        .when(F.col("nivel_atual") == "nivel-2", n1 + n2)
        .when(F.col("nivel_atual") == "nivel-3", n1 + n2 + n3)
        .when(F.col("nivel_atual") == "nivel-4", n1 + n2 + n3 + n4)
        .when(F.col("nivel_atual") == "nivel-5", F.col("premio_total_missao"))
        .otherwise(F.lit(0))
    )


def calcular_potencial_total_moedas_por_missao() -> F.Column:
    """
    Retorna a expressão para somar todas as moedas desbloqueadas na missão (Nível 1 ao 5).
    """
    return (
        F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0))
        + F.coalesce(F.col("moedas_desbloqueadas_nivel_2"), F.lit(0))
        + F.coalesce(F.col("moedas_desbloqueadas_nivel_3"), F.lit(0))
        + F.coalesce(F.col("moedas_desbloqueadas_nivel_4"), F.lit(0))
        + F.coalesce(F.col("moedas_desbloqueadas_nivel_5"), F.lit(0))
    )


def filtrar_atendentes_sem_mensagem_hoje(
    df_atendentes: DataFrame,
    mensagens_enviadas_dt: DataFrame,
    tipo_mensagem: str = None,
    data_inicio_corte: date = None,
) -> DataFrame:
    """
    Remove atendentes que já receberam mensagem.

    - Se data_inicio_corte for informada: Verifica se recebeu a mensagem A PARTIR dessa data (janela da rodada).
    - Se data_inicio_corte for None: Verifica apenas HOJE (comportamento padrão).
    """
    # Filtro base: Apenas mensagens reais
    filtro_mensagens = mensagens_enviadas_dt.filter(
        F.col("eh_somente_teste") == F.lit(False)
    )

    # Lógica de Janela de Tempo
    if data_inicio_corte:
        # Verifica mensagens enviadas DEPOIS do início da rodada
        filtro_mensagens = filtro_mensagens.filter(
            F.to_date(F.col("data_hora_envio")) >= F.lit(data_inicio_corte)
        )
    else:
        # Verifica apenas hoje (padrão antigo)
        filtro_mensagens = filtro_mensagens.filter(
            F.to_date(F.col("data_hora_envio")) == F.current_date()
        )

    # Se tipo específico foi solicitado, adiciona ao filtro
    if tipo_mensagem:
        filtro_mensagens = filtro_mensagens.filter(
            F.col("tipo_mensagem") == tipo_mensagem
        )

    # Pega lista de atendentes que já receberam mensagem
    atendentes_com_mensagem = filtro_mensagens.select("id_atendente").distinct()
    # Remove esses atendentes da lista principal
    df_atendentes_filtrado = df_atendentes.join(
        atendentes_com_mensagem,
        on=df_atendentes.atendente_id == atendentes_com_mensagem.id_atendente,
        how="left_anti",
    )
    return df_atendentes_filtrado


def enviar_mensagens_e_salvar(
    spark: SparkSession,
    rows_to_send: List,
    tipo_mensagem: TipoMensagemReguaAtendente,
    payload_generator_func,
    tabela_mensagens_path: str,
    tabela_erros_path: str,
    eh_somente_teste: bool = True,
) -> Tuple[List[MensagemEnviada], List[ErroEnvioLog]]:
    """
    Envia mensagens para lista de atendentes e salva histórico.
    """

    mensagens_enviadas_para_salvar = []
    erros_para_salvar = []

    try:
        for row in rows_to_send:
            try:
                telefone_formatado = padroniza_numero_whatsapp(row.telefone)
            except NumeroTelefoneInvalidoError:
                erros_para_salvar.append(
                    ErroEnvioLog(
                        id=str(uuid.uuid4()),
                        id_atendente=row.atendente_id,
                        username_atendente=row.username,
                        numero_destinatario=row.telefone,
                        data_hora_erro=datetime.now(pytz.timezone("America/Sao_Paulo")),
                        eh_somente_teste=eh_somente_teste,
                        tipo_erro="NumeroTelefoneInvalidoError",
                        subtipo_erro="NumeroTelefoneInvalidoError",
                    )
                )
                continue

            # Gera payload específico usando a função fornecida
            payload = payload_generator_func(row, telefone_formatado)

            # Envia mensagem (ou simula se for teste)
            response = None
            if not eh_somente_teste:
                response = enviar_mensagem_whatsapp_via_hyperflow_regua(
                    CLIENT_ID_HYPERFLOW_PROD, payload
                )
                print(
                    f"Mensagem enviada com sucesso para '{row.vocativo}' no telefone: {telefone_formatado}"
                )
            else:
                print(
                    f"\n[MODO TESTE ON] - O seguinte payload seria enviado: {payload}"
                )

            # Salva registro da mensagem
            mensagens_enviadas_para_salvar.append(
                MensagemEnviada(
                    id=str(uuid.uuid4()),
                    remetente="hyperflow-databricks",
                    mensagem=str(payload),
                    numero_destinatario=telefone_formatado,
                    id_atendente=row.atendente_id,
                    username_atendente=row.username,
                    data_hora_envio=datetime.now(pytz.timezone("America/Sao_Paulo")),
                    eh_somente_teste=eh_somente_teste,
                    tipo_mensagem=tipo_mensagem.value,
                    subtipo_mensagem=tipo_mensagem.value,
                    status_request=getattr(response, "status_code", None)
                    if response
                    else None,
                    response_request=getattr(response, "text", None)
                    if response
                    else None,
                )
            )

            # Delay para evitar bloqueio
            delay_seconds = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            time.sleep(delay_seconds)

    finally:
        # Sempre salva o histórico, mesmo em caso de erro
        print("O loop encerrou...")

        if len(mensagens_enviadas_para_salvar) > 0:
            print(
                f"Salvando {len(mensagens_enviadas_para_salvar)} mensagens enviadas..."
            )
            df_sent = spark.createDataFrame(
                mensagens_enviadas_para_salvar, schema=mensagens_enviadas_schema
            )
            df_sent.write.format("delta").mode("append").saveAsTable(
                tabela_mensagens_path
            )
            print("Mensagens salvas com sucesso.")

        if len(erros_para_salvar) > 0:
            print(f"Salvando {len(erros_para_salvar)} erros de envio...")
            df_erros = spark.createDataFrame(
                erros_para_salvar, schema=erros_envio_schema
            )
            df_erros.write.format("delta").mode("append").saveAsTable(tabela_erros_path)
            print("Erros salvos com sucesso.")

    return mensagens_enviadas_para_salvar, erros_para_salvar


def print_relatorio_erros(erros_para_salvar: List[ErroEnvioLog]) -> None:
    if len(erros_para_salvar) < 1:
        return

    print(f"Total de erros no envio: {len(erros_para_salvar)}")
    for erro in erros_para_salvar:
        print(
            f"Atendente ID: {erro.id_atendente:>6d}, "
            f"Telefone: {erro.numero_destinatario:>12s}, "
            f"Erro: {erro.tipo_erro}"
        )
