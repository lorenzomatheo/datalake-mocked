# Databricks notebook source
# MAGIC %md
# MAGIC # Eval Puro: Recomendacoes de Alternativos (LLM Judge)
# MAGIC
# MAGIC Usa um LLM (Gemini) para avaliar se cada recomendacao de alternativo (substituicao) esta correta.
# MAGIC
# MAGIC **Dois avaliadores separados por tipo de produto bipado:**
# MAGIC - **Medicamento**: avalia intercambiabilidade farmaceutica (com regras flexiveis para OTC/MIP)
# MAGIC - **Nao medicamento**: avalia equivalencia de uso (mesma necessidade do consumidor)
# MAGIC
# MAGIC **Metricas (por tipo + geral):**
# MAGIC - **Precision** = recomendacoes corretas / total de pares avaliados
# MAGIC - **Hit Rate** = produtos com pelo menos 1 recomendacao correta / total de produtos amostrados

# COMMAND ----------

# MAGIC %pip install agno google-genai langchain-google-vertexai vertexai pydantic langsmith
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiente

# COMMAND ----------

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

import pyspark.sql.functions as F
from agno.agent import Agent
from agno.models.google import Gemini
from google.api_core.exceptions import GoogleAPIError
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import Row

from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.environment.tables import BigqueryGold, Table
from maggulake.llm.agno_tracer import TracedAgent
from maggulake.llm.models import get_model_name
from maggulake.llm.vertex import setup_langsmith, setup_vertex_ai_credentials
from maggulake.prompts import (
    EVAL_ALTERNATIVOS_MEDICAMENTO_PROMPT,
    EVAL_ALTERNATIVOS_NAO_MEDICAMENTO_PROMPT,
    EVAL_ALTERNATIVOS_SYSTEM,
)
from maggulake.schemas import (
    schema_eval_puro_alternativos_recomendacoes_detalhes,
    schema_eval_puro_alternativos_recomendacoes_historico,
)
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "eval_puro_alternativos_recomendacoes",
    dbutils,
    widgets={"tamanho_amostra": "500"},
)
df_produtos = env.table(Table.produtos_refined)

spark = env.spark

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracao dos Agentes

# COMMAND ----------


@dataclass
class ResultadoPar:
    chave_par: str
    ref_ean: str
    rec_ean: str
    ref_nome: str
    rec_nome: str
    tipo_produto_buscado: str
    avaliacao: str
    motivo: str


@dataclass
class ConfiguracaoAvaliador:
    agente: Agent
    prompt_template: str
    formatar_ref: Callable[[Row], str]
    formatar_rec: Callable[[Row], str]
    label: str


@dataclass
class MetricasAvaliacao:
    avaliadas: int
    corretas: int
    precision: float
    produtos: int
    produtos_hit: int
    hit_rate: float


class ResultadoAvaliacao(str, Enum):
    CORRETA = "CORRETA"
    INCORRETA = "INCORRETA"


class AvaliacaoRecomendacao(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    avaliacao: ResultadoAvaliacao = Field(
        description="""Resultado da avaliacao de substituicao entre os dois produtos."""
    )
    motivo: str = Field(description="""Explicacao tecnica justificando a avaliacao.""")


# COMMAND ----------

project_id, location = setup_vertex_ai_credentials(spark)
# TODO: trocar para LARGE se necessitar de maior precisao na avaliacao
GEMINI_MODEL = get_model_name(provider="gemini_vertex", size="SMALL")


def criar_agente(description: str) -> TracedAgent:
    return TracedAgent(
        Agent(
            model=Gemini(
                id=GEMINI_MODEL,
                vertexai=True,
                project_id=project_id,
                location=location,
                include_thoughts=False,
                search=False,
            ),
            description=description,
            output_schema=AvaliacaoRecomendacao,
            markdown=False,
            reasoning=False,
        ),
    )


avaliador_medicamento = criar_agente(EVAL_ALTERNATIVOS_SYSTEM)
avaliador_nao_medicamento = criar_agente(EVAL_ALTERNATIVOS_SYSTEM)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dados

# COMMAND ----------

TAMANHO_AMOSTRA = int(dbutils.widgets.get("tamanho_amostra"))

# COMMAND ----------

# Info do produto de referencia (medicamentos e nao medicamentos)
# Sem filtro de principio_ativo para incluir nao medicamentos na amostra
df_ref = df_produtos.select(
    F.col("ean").alias("ref_ean"),
    F.col("nome").alias("ref_nome"),
    F.col("eh_medicamento").alias("ref_eh_medicamento"),
    F.col("eh_otc").alias("ref_eh_otc"),
    F.col("principio_ativo").alias("ref_principio_ativo"),
    F.col("dosagem").alias("ref_dosagem"),
    F.col("forma_farmaceutica").alias("ref_forma_farmaceutica"),
    F.col("via_administracao").alias("ref_via_administracao"),
    F.col("tipo_medicamento").alias("ref_tipo_medicamento"),
    F.col("categorias").alias("ref_categorias"),
)

# Info do produto recomendado
df_rec = df_produtos.select(
    F.col("ean").alias("rec_ean"),
    F.col("nome").alias("rec_nome"),
    F.col("eh_medicamento").alias("rec_eh_medicamento"),
    F.col("eh_otc").alias("rec_eh_otc"),
    F.col("principio_ativo").alias("rec_principio_ativo"),
    F.col("dosagem").alias("rec_dosagem"),
    F.col("forma_farmaceutica").alias("rec_forma_farmaceutica"),
    F.col("via_administracao").alias("rec_via_administracao"),
    F.col("tipo_medicamento").alias("rec_tipo_medicamento"),
    F.col("categorias").alias("rec_categorias"),
)

# COMMAND ----------

# Candidatos: bigquery.metricas_produto_maggu.recomendacoes (somente substituicao, dia anterior)
ontem = agora_em_sao_paulo().date() - timedelta(days=1)
recomendacoes = (
    env.table(BigqueryGold.view_recomendacoes)
    .filter(F.to_date("recommendation_time") == F.lit(ontem))
    .filter(F.col("rec_tipo_venda") == "substituicao")
    .select("ref_ean", "rec_ean")
    .distinct()
)

total_universo = recomendacoes.count()
print(f"Total de pares no universo: {total_universo}")

# Amostra aleatoria por produto (ref_ean distintos)
# Seed fixa para reprodutibilidade entre execucoes
produtos_amostrados = (
    recomendacoes.select("ref_ean")
    .distinct()
    .orderBy(F.rand(seed=42))
    .limit(TAMANHO_AMOSTRA)
)

amostra = (
    recomendacoes.join(produtos_amostrados, "ref_ean", "inner")
    .join(df_ref, "ref_ean", "inner")
    .join(df_rec, "rec_ean", "inner")
)

pares = amostra.collect()
total_produtos_amostrados = produtos_amostrados.count()

# Divide pares por tipo do produto bipado (ref) para usar o avaliador correto
# Produtos com eh_medicamento=None caem em nao_medicamento
pares_med = [p for p in pares if p["ref_eh_medicamento"]]
pares_nao_med = [p for p in pares if not p["ref_eh_medicamento"]]

print(f"Produtos amostrados: {total_produtos_amostrados}")
print(f"Pares a avaliar: {len(pares)}")
print(f"  - Medicamentos: {len(pares_med)} pares")
print(f"  - Nao medicamentos: {len(pares_nao_med)} pares")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Formatacao de info por tipo
# MAGIC
# MAGIC Cada tipo de produto envia informacoes diferentes pro LLM:
# MAGIC - **Medicamento**: principio ativo, dosagem, forma farmaceutica, via, tipo (OTC/Prescricao)
# MAGIC - **Nao medicamento**: nome e categorias

# COMMAND ----------


def formatar_ref_medicamento(row: Row) -> str:
    regime = "OTC/MIP" if row["ref_eh_otc"] else "Prescricao"
    linhas = [f"- Nome: {row['ref_nome']}", f"- Regime: {regime}"]

    if row["ref_principio_ativo"]:
        linhas.append(f"- Principio ativo: {row['ref_principio_ativo']}")

    if row["ref_dosagem"]:
        linhas.append(f"- Dosagem: {row['ref_dosagem']}")

    if row["ref_forma_farmaceutica"]:
        linhas.append(f"- Forma farmaceutica: {row['ref_forma_farmaceutica']}")

    if row["ref_via_administracao"]:
        linhas.append(f"- Via de administracao: {row['ref_via_administracao']}")

    if row["ref_tipo_medicamento"]:
        linhas.append(f"- Tipo: {row['ref_tipo_medicamento']}")

    return "\n".join(linhas)


def formatar_ref_nao_medicamento(row: Row) -> str:
    linhas = [f"- Nome: {row['ref_nome']}"]
    categorias = row["ref_categorias"]
    if categorias:
        linhas.append(f"- Categorias: {', '.join(categorias)}")
    return "\n".join(linhas)


def formatar_rec_medicamento(row: Row) -> str:
    regime = "OTC/MIP" if row["rec_eh_otc"] else "Prescricao"
    linhas = [f"- Nome: {row['rec_nome']}", f"- Regime: {regime}"]

    if row["rec_principio_ativo"]:
        linhas.append(f"- Principio ativo: {row['rec_principio_ativo']}")

    if row["rec_dosagem"]:
        linhas.append(f"- Dosagem: {row['rec_dosagem']}")

    if row["rec_forma_farmaceutica"]:
        linhas.append(f"- Forma farmaceutica: {row['rec_forma_farmaceutica']}")

    if row["rec_via_administracao"]:
        linhas.append(f"- Via de administracao: {row['rec_via_administracao']}")

    if row["rec_tipo_medicamento"]:
        linhas.append(f"- Tipo: {row['rec_tipo_medicamento']}")

    return "\n".join(linhas)


def formatar_rec_nao_medicamento(row: Row) -> str:
    linhas = [f"- Nome: {row['rec_nome']}"]
    categorias = row["rec_categorias"]
    if categorias:
        linhas.append(f"- Categorias: {', '.join(categorias)}")
    return "\n".join(linhas)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Avaliacao com LLM

# COMMAND ----------

config_medicamento = ConfiguracaoAvaliador(
    agente=avaliador_medicamento,
    prompt_template=EVAL_ALTERNATIVOS_MEDICAMENTO_PROMPT,
    formatar_ref=formatar_ref_medicamento,
    formatar_rec=formatar_rec_medicamento,
    label="medicamento",
)

config_nao_medicamento = ConfiguracaoAvaliador(
    agente=avaliador_nao_medicamento,
    prompt_template=EVAL_ALTERNATIVOS_NAO_MEDICAMENTO_PROMPT,
    formatar_ref=formatar_ref_nao_medicamento,
    formatar_rec=formatar_rec_nao_medicamento,
    label="nao_medicamento",
)

# COMMAND ----------


def avaliar_pares(
    pares_lista: list[Row],
    config: ConfiguracaoAvaliador,
) -> list[ResultadoPar]:
    """Avalia uma lista de pares com o avaliador e prompt especificos."""
    resultados_lista: list[ResultadoPar] = []

    for i, par in enumerate(pares_lista, 1):
        prompt = config.prompt_template.format(
            id=f"{par['ref_ean']}_{par['rec_ean']}",
            ref_info=config.formatar_ref(par),
            rec_info=config.formatar_rec(par),
        )

        try:
            resposta = config.agente.run(prompt)
            if hasattr(resposta, "content") and hasattr(resposta.content, "avaliacao"):
                avaliacao = resposta.content.avaliacao
                motivo = resposta.content.motivo
            else:
                avaliacao = "ERRO"
                motivo = f"Formato inesperado: {resposta}"
        except (GoogleAPIError, ValueError, AttributeError, KeyError) as e:
            print(
                f"  [{config.label}] Erro no par {i} (ref={par['ref_ean']}, rec={par['rec_ean']}): {e}"
            )
            avaliacao = "ERRO"
            motivo = str(e)
            time.sleep(0.05)

        resultados_lista.append(
            ResultadoPar(
                chave_par=f"{par['ref_ean']}_{par['rec_ean']}",
                ref_ean=par["ref_ean"],
                rec_ean=par["rec_ean"],
                ref_nome=par["ref_nome"],
                rec_nome=par["rec_nome"],
                tipo_produto_buscado=config.label,
                avaliacao=avaliacao,
                motivo=motivo,
            )
        )

        if i % 50 == 0:
            avaliadas = sum(1 for r in resultados_lista if r.avaliacao != "ERRO")
            corretas = sum(1 for r in resultados_lista if r.avaliacao == "CORRETA")
            pct = corretas / avaliadas * 100 if avaliadas > 0 else 0
            print(
                f"  [{config.label}] [{i}/{len(pares_lista)}] Precision parcial: {corretas}/{avaliadas} ({pct:.1f}%)"
            )

    return resultados_lista


# COMMAND ----------

# MAGIC %md
# MAGIC ### Avaliando medicamentos

# COMMAND ----------

print(f"Avaliando {len(pares_med)} pares de medicamentos...")
resultados_med = avaliar_pares(pares_med, config_medicamento)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Avaliando nao medicamentos

# COMMAND ----------

print(f"Avaliando {len(pares_nao_med)} pares de nao medicamentos...")
resultados_nao_med = avaliar_pares(pares_nao_med, config_nao_medicamento)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado

# COMMAND ----------

resultados = resultados_med + resultados_nao_med


def calcular_metricas(res: list[ResultadoPar], label: str) -> MetricasAvaliacao:
    avaliadas = [r for r in res if r.avaliacao != "ERRO"]
    corretas = [r for r in res if r.avaliacao == "CORRETA"]
    erros = [r for r in res if r.avaliacao == "ERRO"]
    precision = len(corretas) / len(avaliadas) * 100 if avaliadas else 0

    # Denominador do hit rate usa todos os produtos amostrados (incluindo erros)
    # para nao inflar a metrica quando ha falhas de chamada ao LLM
    produtos_amostrados = {r.ref_ean for r in res}
    produtos_hit = {r.ref_ean for r in corretas}
    hit_rate = (
        len(produtos_hit) / len(produtos_amostrados) * 100 if produtos_amostrados else 0
    )

    print(f"\n--- {label} ---")
    print(f"Avaliadas: {len(avaliadas)} pares ({len(erros)} erros descartados)")
    print(f"Corretas: {len(corretas)}")
    print(f"Precision: {precision:.2f}%")
    print(f"Hit rate: {len(produtos_hit)}/{len(produtos_amostrados)} ({hit_rate:.2f}%)")

    return MetricasAvaliacao(
        avaliadas=len(avaliadas),
        corretas=len(corretas),
        precision=precision,
        produtos=len(produtos_amostrados),
        produtos_hit=len(produtos_hit),
        hit_rate=hit_rate,
    )


print(f"Universo total: {total_universo} pares")
print(f"Produtos amostrados: {total_produtos_amostrados}")

metricas_med = calcular_metricas(resultados_med, "Medicamentos")
metricas_nao_med = calcular_metricas(resultados_nao_med, "Nao medicamentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detalhamento

# COMMAND ----------

df_resultados = spark.createDataFrame(resultados)
df_resultados.filter(F.col("avaliacao") != "ERRO").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historico
# MAGIC
# MAGIC Salva uma linha por tipo de produto (medicamento e nao_medicamento)
# MAGIC para acompanhar evolucao de cada avaliador separadamente.

# COMMAND ----------

env.create_table_if_not_exists(
    Table.eval_puro_alternativos_recomendacoes_historico,
    schema_eval_puro_alternativos_recomendacoes_historico,
)

# Uma linha por tipo de produto
historico_rows = spark.createDataFrame(
    [
        (
            agora_em_sao_paulo(),
            "medicamento",
            metricas_med.avaliadas,
            metricas_med.corretas,
            metricas_med.precision,
            metricas_med.produtos,
            metricas_med.produtos_hit,
            metricas_med.hit_rate,
        ),
        (
            agora_em_sao_paulo(),
            "nao_medicamento",
            metricas_nao_med.avaliadas,
            metricas_nao_med.corretas,
            metricas_nao_med.precision,
            metricas_nao_med.produtos,
            metricas_nao_med.produtos_hit,
            metricas_nao_med.hit_rate,
        ),
    ],
    schema=schema_eval_puro_alternativos_recomendacoes_historico,
)

historico_rows.write.mode("append").saveAsTable(
    Table.eval_puro_alternativos_recomendacoes_historico.value
)

print(
    f"Metricas salvas em {Table.eval_puro_alternativos_recomendacoes_historico.value} para {agora_em_sao_paulo().date()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detalhes por par (para revisao manual)

# COMMAND ----------

hoje = agora_em_sao_paulo()

env.create_table_if_not_exists(
    Table.eval_puro_alternativos_recomendacoes_detalhes,
    schema_eval_puro_alternativos_recomendacoes_detalhes,
)

detalhes_rows = spark.createDataFrame(
    [
        (
            hoje,
            r.chave_par,
            r.ref_ean,
            r.ref_nome,
            r.rec_ean,
            r.rec_nome,
            r.tipo_produto_buscado,
            r.avaliacao,
            r.motivo,
        )
        for r in resultados
    ],
    schema=schema_eval_puro_alternativos_recomendacoes_detalhes,
)

detalhes_rows.write.mode("append").option("mergeSchema", "true").saveAsTable(
    Table.eval_puro_alternativos_recomendacoes_detalhes.value
)

print(
    f"Detalhes salvos em {Table.eval_puro_alternativos_recomendacoes_detalhes.value} ({len(resultados)} pares)"
)
