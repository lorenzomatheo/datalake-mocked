# Databricks notebook source
# MAGIC %md
# MAGIC # Enriquece Marca e Fabricante
# MAGIC - Enriquecimento de produtos sem a informação de marca e fabricante
# MAGIC - No pipeline temos diferentes fontes da verdade para buscar essas infos e possivelmente preencher
# MAGIC   (Consulta Remedio, RD, etc)
# MAGIC - Enriquecemento acontece quando as infos não foram preenchidas por nenhuma das fontes
# MAGIC   mencionadas anteriormente
# MAGIC - Utiliza busca na web para preencher a informação

# COMMAND ----------

# MAGIC %pip install agno google-genai pydantic langchain_google_genai langchain_google_vertexai langsmith
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import asyncio
import logging
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Dict, List

import pyspark.sql.functions as F
import pytz
from agno.agent import Agent
from agno.models.google import Gemini
from agno.utils.log import configure_agno_logging
from delta.tables import DeltaTable
from google.api_core.exceptions import ClientError, GoogleAPICallError

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.llm.agno_tracer import TracedAgent
from maggulake.llm.models import get_model_name
from maggulake.llm.vertex import setup_langsmith, setup_vertex_ai_credentials
from maggulake.pipelines.schemas import MarcaFabricanteResponse
from maggulake.prompts import MARCA_FABRICANTE_PROMPT_INSTRUCTIONS
from maggulake.prompts.websearch_marca_fabricante import (
    MAX_FABRICANTE_LENGTH,
    MAX_MARCA_LENGTH,
)
from maggulake.schemas.websearch_marca_fabricante import (
    schema_websearch_marca_fabricante,
)
from maggulake.utils.iters import create_batches
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

# para não ficar aparecendo logs do tipo INFO
quiet = logging.getLogger("agno_quiet")
quiet.setLevel(logging.WARNING)
quiet.propagate = False

configure_agno_logging(
    custom_default_logger=quiet,
    custom_agent_logger=quiet,
    custom_team_logger=quiet,
    custom_workflow_logger=quiet,
)

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "enriquece_marca_fabricante",
    dbutils,
    widgets={
        "order_by_sales": ["true", "true", "false"],
        "termo_produto": "",
        "max_products": "10000",
        "async_batch_size": "100",
    },
)

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

DEBUG = dbutils.widgets.get("debug") == "true"
ORDER_BY_SALES = dbutils.widgets.get("order_by_sales") == "true"
TERMO_PRODUTO = dbutils.widgets.get("termo_produto")
MAX_REGISTROS_BUSCAR = int(dbutils.widgets.get("max_products"))
ASYNC_BATCH_SIZE = int(dbutils.widgets.get("async_batch_size"))
TIMEOUT_PROCESSAMENTO = 120
CUTOFF_DAYS = 180

# COMMAND ----------

# LLM
GEMINI_MODEL = get_model_name(provider="gemini_vertex", size="SMALL")
PROJECT_ID, LOCATION = setup_vertex_ai_credentials(spark)

# COMMAND ----------

spark = env.spark
produtos_enriquecidos_table = Table.produtos_refined.value
resultados_websearch_table = Table.enriquecimento_websearch.value

# COMMAND ----------


def seleciona_produtos_para_websearch(
    env, CUTOFF_DAYS: int, MAX_REGISTROS_BUSCAR: int
) -> List[Dict[str, Any]]:
    print(
        f"{agora_em_sao_paulo_str()} - Inicia seleção de produtos para enriquecimento"
    )

    cutoff_date = datetime.now() - timedelta(days=CUTOFF_DAYS)

    df_filter = (
        env.spark.read.table(produtos_enriquecidos_table)
        .withColumn("nome", F.coalesce(F.col("nome_comercial"), F.col("nome")))
        .withColumn("descricao", F.coalesce(F.col("descricao"), F.col("indicacao")))
        .filter(
            (F.col("nome").isNotNull() & (F.length(F.col("nome")) >= 10))
            & (F.col("descricao").isNotNull())
        )
        .filter((F.col("marca").isNull()) | (F.col("fabricante").isNull()))
    )

    df_websearch_historico = env.spark.read.table(resultados_websearch_table).select(
        "ean", "websearch_em"
    )

    df_com_historico = df_filter.join(df_websearch_historico, on=["ean"], how="left")

    condicao_tempo = F.col("websearch_em").isNull() | (
        F.col("websearch_em") <= F.lit(cutoff_date)
    )

    df_enriquecer = df_com_historico.filter(condicao_tempo).select(
        "ean", "nome", "descricao", "eh_medicamento", "marca", "fabricante"
    )

    total_encontrados = df_enriquecer.count()
    print(
        f"{agora_em_sao_paulo_str()} - Foram encontrados {total_encontrados} registros para enriquecer"
    )

    if TERMO_PRODUTO:
        print(
            f"{agora_em_sao_paulo_str()} - Filtrando produtos com o termo: {TERMO_PRODUTO}"
        )
        df_enriquecer = df_enriquecer.filter(
            F.col("nome").ilike(f"%{TERMO_PRODUTO}%")
            | F.col("marca").ilike(f"%{TERMO_PRODUTO}%")
            | F.col("fabricante").ilike(f"%{TERMO_PRODUTO}%")
        )

    if ORDER_BY_SALES:
        print(f"{agora_em_sao_paulo_str()} - Ordenando produtos por venda")
        produtos_mais_vendidos_ids_df = env.bigquery_adapter.get_produtos_mais_vendidos(
            dias=7, classificacao_abc="A", apenas_lojas_ativas=True
        )

        vendas_rank_df = produtos_mais_vendidos_ids_df.select("ean").withColumn(
            "sales_rank", F.monotonically_increasing_id()
        )

        df_enriquecer = (
            df_enriquecer.join(vendas_rank_df, on="ean", how="left")
            .withColumn("sales_rank", F.coalesce(F.col("sales_rank"), F.lit(999999)))
            .orderBy("sales_rank")
            .drop("sales_rank")
        )

    total_original = df_enriquecer.count()
    if total_original > MAX_REGISTROS_BUSCAR:
        msg = (
            f"{agora_em_sao_paulo_str()} - ⚠️  Limitando processamento devido a max_products: "
            f"{total_original} → {MAX_REGISTROS_BUSCAR} produtos"
        )
        print(msg)
    df_enriquecer = df_enriquecer.limit(MAX_REGISTROS_BUSCAR)

    produtos_para_buscar = [row.asDict() for row in df_enriquecer.collect()]

    print(
        f"{agora_em_sao_paulo_str()} - Total de produtos selecionados para enriquecimento: {len(produtos_para_buscar)}"
    )

    if DEBUG:
        print("Display de alguns produtos que serão buscados:")
        df_enriquecer.limit(100).display()

    return produtos_para_buscar


# COMMAND ----------

produtos_para_buscar = seleciona_produtos_para_websearch(
    env, CUTOFF_DAYS, MAX_REGISTROS_BUSCAR
)

# COMMAND ----------

if len(produtos_para_buscar) == 0:
    dbutils.notebook.exit("Não tem produtos para enriquecer usando o websearch")
else:
    print(
        f"Vao ser enriquecidos os campo marca e fabricante de {len(produtos_para_buscar)} produtos"
    )

# COMMAND ----------


def is_valid_value(value: str) -> bool:
    if not value:
        return False
    value_lower = str(value).lower()
    # NOTE: isso existe pois o LLM pode retornar a string "null" caso não saiba, mas não quero salvar isso nos dados
    return value_lower != "null" and "qual " not in value_lower


# COMMAND ----------


def create_agent():
    return TracedAgent(
        Agent(
            model=Gemini(
                id=GEMINI_MODEL,
                vertexai=True,
                project_id=PROJECT_ID,
                location=LOCATION,
                search=True,
                include_thoughts=False,
            ),
            output_schema=MarcaFabricanteResponse,
            use_json_mode=True,
            reasoning=False,
            expected_output="Um objeto JSON contendo os campos 'marca' e 'fabricante'.",
            description="Agente especializado em identificar marca e fabricante de produtos através de busca na web.",
            instructions=[MARCA_FABRICANTE_PROMPT_INSTRUCTIONS],
            markdown=False,
            retries=3,
            exponential_backoff=True,
            debug_mode=DEBUG,
        ),
    )


def process_single_product(row_dict: dict) -> dict:
    ean = row_dict['ean']
    nome = row_dict['nome']
    descricao = row_dict.get('descricao', '')

    agent = create_agent()

    query = f"Qual a marca e fabricante do produto {nome}? EAN: {ean}. Descrição: {descricao}."

    try:
        result = agent.run(query)

        marca = None
        fabricante = None

        if hasattr(result, 'content'):
            content = result.content
            if hasattr(content, 'marca'):
                marca = content.marca
                fabricante = content.fabricante
            elif isinstance(content, dict):
                marca = content.get('marca')
                fabricante = content.get('fabricante')

        marca_final = marca[:MAX_MARCA_LENGTH] if is_valid_value(marca) else None
        fabricante_final = (
            fabricante[:MAX_FABRICANTE_LENGTH] if is_valid_value(fabricante) else None
        )

        return {
            'ean': ean,
            'marca_websearch': marca_final,
            'fabricante_websearch': fabricante_final,
            'websearch_em': datetime.now(pytz.timezone("America/Sao_Paulo")),
        }

    except (ClientError, GoogleAPICallError) as e:
        print(
            f"  ❌ Google API Error processing {ean}: {type(e).__name__} - {str(e)[:100]}"
        )
    # NOTE: removendo broad exception catch para evitar mascarar erros, deixei comentado caso fique insustentavel
    # except Exception as e:
    #     print(
    #        f"  ❌ Unexpected error processing {ean}: {type(e).__name__} - {str(e)[:100]}"
    #    )

    return {
        'ean': ean,
        'marca_websearch': None,
        'fabricante_websearch': None,
        'websearch_em': datetime.now(pytz.timezone("America/Sao_Paulo")),
    }


async def process_single_product_async(
    row_dict: dict, timeout: float = TIMEOUT_PROCESSAMENTO
):
    p = partial(process_single_product, row_dict)
    return await asyncio.wait_for(asyncio.to_thread(p), timeout)


async def process_batch_async(
    batch_data: List[dict], timeout: float = TIMEOUT_PROCESSAMENTO
) -> List[dict]:
    return await asyncio.gather(
        *[process_single_product_async(row, timeout) for row in batch_data],
        return_exceptions=True,
    )


def _coalesce_field_expr(field: str) -> str:
    return (
        f"CASE WHEN target.{field} IS NULL THEN source.{field} ELSE target.{field} END"
    )


def salvar_batch_no_delta(df_batch):
    if df_batch.isEmpty():
        return

    df_to_save = df_batch.withColumn(
        'fabricante_websearch', F.initcap(F.col('fabricante_websearch'))
    ).withColumn('marca_websearch', F.initcap(F.col('marca_websearch')))

    delta_table = DeltaTable.forName(spark, resultados_websearch_table)

    delta_table.alias("target").merge(
        source=df_to_save.alias("source"),
        condition="target.ean = source.ean",
    ).whenMatchedUpdate(
        set={
            "marca_websearch": _coalesce_field_expr("marca_websearch"),
            "fabricante_websearch": _coalesce_field_expr("fabricante_websearch"),
            "websearch_em": "source.websearch_em",
        }
    ).whenNotMatchedInsertAll().execute()


async def processa_e_salva_batches(batches: List[List[dict]]) -> int:
    total_batches = len(batches)
    produtos_processados = 0

    for idx, batch in enumerate(batches, 1):
        print(f"{agora_em_sao_paulo_str()} - Processing batch {idx}/{total_batches}...")

        batch_results = await process_batch_async(batch, timeout=TIMEOUT_PROCESSAMENTO)

        valid_results = []
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                print(
                    f"\t⚠️ Exception for item {batch[i].get('ean')}: {str(result)[:100]}"
                )
            else:
                valid_results.append(result)

        if valid_results:
            df_batch = spark.createDataFrame(
                valid_results, schema_websearch_marca_fabricante
            )
            salvar_batch_no_delta(df_batch)
            produtos_processados += len(valid_results)

        print(
            f"\t✅ {agora_em_sao_paulo_str()} - Batch {idx}/{total_batches} done "
            f"({len(valid_results)} processed/saved | Total: {produtos_processados})"
        )

    return produtos_processados


# COMMAND ----------

print(
    f"{agora_em_sao_paulo_str()} - Starting async processing with fresh agents per request"
)

batches: List[List[dict]] = list(create_batches(produtos_para_buscar, ASYNC_BATCH_SIZE))
print(f"📦 Total de batches: {len(batches)}")
print(f"🔢 Tamanho de cada batch: {ASYNC_BATCH_SIZE}")

total_processados = await processa_e_salva_batches(batches)

print(
    f"\n{agora_em_sao_paulo_str()} - Enriquecimento completo! "
    f"Total de {total_processados} produtos processados."
)

# COMMAND ----------

if DEBUG:
    df_resultados_salvos = spark.read.table(resultados_websearch_table)
    display(df_resultados_salvos.orderBy(F.col("websearch_em").desc()).limit(20))
