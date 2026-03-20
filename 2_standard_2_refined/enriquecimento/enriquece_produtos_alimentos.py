# Databricks notebook source
# MAGIC %pip install -- langchain langchain-openai langchain-core langchain-community langchainhub tiktoken langchain-google-genai duckduckgo-search tavily-python agno pycountry google-genai langchain-google-vertexai langsmith

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial
# MAGIC
# MAGIC Este notebook enriquece produtos da categoria **Alimentos e Suplementos**.

# COMMAND ----------


import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.llm.embeddings import (
    EmbeddingEncodingModels,
    get_max_tokens_by_embedding_model,
)
from maggulake.llm.models import get_llm_config
from maggulake.llm.tagging_processor import TaggingProcessor
from maggulake.llm.vertex import (
    create_vertex_context_cache,
    setup_langsmith,
)
from maggulake.pipelines.filter_notify import (
    filtra_notifica_produtos_enriquecimento,
)
from maggulake.pipelines.helpers import (
    get_produtos_standard_com_eh_medicamento,
)
from maggulake.pipelines.schemas.enriquece_alimentos_suplementos import (
    EnriquecimentoAlimentosSuplementos,
    enriquece_alimentos_suplementos_schema,
    schema_enriquece_alimentos_suplementos,
)
from maggulake.pipelines.short_products.produtos_texto_curto import (
    aplica_nome_descricao_externos,
)
from maggulake.pipelines.utils import (
    format_column_value,
    save_enrichment_batch,
    validate_llm_results,
)
from maggulake.prompts import (
    PROMPT_ENRIQUECIMENTO_ALIMENTOS_SAUDE_SYSTEM,
    PROMPT_ENRIQUECIMENTO_ALIMENTOS_SAUDE_USER_TEMPLATE,
)
from maggulake.utils.iters import create_batches
from maggulake.utils.strings import truncate_text_to_tokens
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

SUPER_CATEGORIA = "Alimentos e Suplementos"
TABLE_OUTPUT = Table.extract_product_info_suplementos

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "gpt_enriquece_suplementos",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "llm_provider": ["gemini_vertex", "gemini_vertex", "openai"],
        "max_products": "30000",
        "threshold_execucao": "1.0",
    },
)

# COMMAND ----------

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

DEBUG = dbutils.widgets.get("debug") == "true"
LLM_PROVIDER = dbutils.widgets.get("llm_provider")
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
spark = env.spark
countAccumulator = spark.sparkContext.accumulator(0)

# COMMAND ----------

llm_config = get_llm_config(LLM_PROVIDER, spark, dbutils, size="SMALL")

CONTEXT_CACHE_NAME = None
if llm_config.provider == "gemini_vertex":
    CONTEXT_CACHE_NAME = create_vertex_context_cache(
        model=llm_config.model,
        system_instruction=PROMPT_ENRIQUECIMENTO_ALIMENTOS_SAUDE_SYSTEM,
        project_id=llm_config.project_id,
        location=llm_config.location,
        ttl=7200,
    )

EMBEDDING_ENCODING = EmbeddingEncodingModels.CL100K_BASE.value
MAX_TOKENS = get_max_tokens_by_embedding_model(EMBEDDING_ENCODING)
BATCH_SIZE: int = 100
MIN_TEXT_LEN: int = 10
TABELA_PRODUTOS_NOME_DESCRICAO_EXTERNOS = Table.produtos_nome_descricao_externos.value
THRESHOLD_EXECUCAO = float(dbutils.widgets.get("threshold_execucao"))
DAYS_BACK = 15

# COMMAND ----------

# Cria a tabela se não existir
env.create_table_if_not_exists(TABLE_OUTPUT, schema_enriquece_alimentos_suplementos)

# COMMAND ----------

produtos_enriquecidos = env.table(TABLE_OUTPUT)
produtos = get_produtos_standard_com_eh_medicamento(env, eh_medicamento=False)

# COMMAND ----------

# Categorias Cascata (pega apenas o enriquecimento mais recente por EAN)
categorias_agregadas = env.table(Table.categorias_cascata)

window_spec_gerado_em = Window.partitionBy("ean").orderBy(F.desc("gerado_em"))
categorias_cascata_recente = (
    categorias_agregadas.withColumn(
        "super_categoria_principal", F.col('categorias.super_categoria')
    )
    .filter(F.array_contains(F.col("super_categoria_principal"), SUPER_CATEGORIA))
    .withColumn("row_num", F.row_number().over(window_spec_gerado_em))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .select("ean", "super_categoria_principal")
)

produtos = produtos.join(categorias_cascata_recente, on=["ean"], how="inner")

# COMMAND ----------

produtos = produtos.dropDuplicates(["ean"])
produtos = produtos.dropna(subset=["ean"])

if spark.catalog.tableExists(TABELA_PRODUTOS_NOME_DESCRICAO_EXTERNOS):
    produtos_texto_externos = spark.read.table(TABELA_PRODUTOS_NOME_DESCRICAO_EXTERNOS)
    produtos = aplica_nome_descricao_externos(
        produtos, produtos_texto_externos, MIN_TEXT_LEN
    )

produtos = produtos.dropna(subset=["nome", "descricao"])  # input para enriquecimento
produtos = produtos.filter(
    (F.length(F.col("nome")) >= MIN_TEXT_LEN)
    & (F.length(F.col("descricao")) >= MIN_TEXT_LEN)
)

# COMMAND ----------

df_filtra_refazer = (
    produtos.alias("p")
    .join(produtos_enriquecidos.alias("pr"), on=["ean"], how="left_anti")
    .select(
        "ean", "nome", "marca", "fabricante", "categorias", "descricao", "gerado_em"
    )
)

# COMMAND ----------

total_original = df_filtra_refazer.count()
if MAX_PRODUCTS and total_original > MAX_PRODUCTS:
    print(f"⚠️  Limitando: {total_original} → {MAX_PRODUCTS}")
    df_filtra_refazer = df_filtra_refazer.limit(MAX_PRODUCTS)

produtos_pendentes = filtra_notifica_produtos_enriquecimento(
    original_df=produtos,
    df_to_enrich=df_filtra_refazer,
    threshold=THRESHOLD_EXECUCAO,
    days_back=DAYS_BACK,
    script_name="gpt_enriquece_suplementos",
)

if not len(produtos_pendentes) > 0:
    dbutils.notebook.exit("Sem produtos para atuar")

# COMMAND ----------

llm_caller = TaggingProcessor(
    provider=llm_config.provider,
    api_key=llm_config.api_key,
    project_id=llm_config.project_id,
    location=llm_config.location,
    model=llm_config.model,
    prompt_template=PROMPT_ENRIQUECIMENTO_ALIMENTOS_SAUDE_USER_TEMPLATE,
    output_schema=EnriquecimentoAlimentosSuplementos,
    temperature=0.1,
    cached_content=CONTEXT_CACHE_NAME,
    system_instruction=PROMPT_ENRIQUECIMENTO_ALIMENTOS_SAUDE_SYSTEM,
)

# COMMAND ----------


def formata_info_produto(produto: T.Row) -> str:
    return truncate_text_to_tokens(
        "\n\n".join(
            [
                f"EAN do produto: {format_column_value(produto.ean)}",
                f"Nome do produto: {format_column_value(produto.nome)}",
                f"Marca do produto: {format_column_value(produto.marca)}",
                f"Fabricante do produto: {format_column_value(produto.fabricante)}",
                f"Descrição do produto: {format_column_value(produto.descricao)}",
                f"Categorias do produto: {format_column_value(produto.categorias)}",
            ]
        ),
        MAX_TOKENS,
        EMBEDDING_ENCODING,
    )


# COMMAND ----------

if DEBUG:
    prompt_teste = formata_info_produto(produtos_pendentes[0])
    print(prompt_teste)
    print(llm_caller.executa_tagging_chain(prompt_teste))


# COMMAND ----------

SCHEMA_FIELDS = [field.name for field in enriquece_alimentos_suplementos_schema.fields]

# COMMAND ----------


async def extrair_info(lista_produtos: list[T.Row]) -> list[dict]:
    prompts = [formata_info_produto(p) for p in lista_produtos]
    if not prompts:
        return []
    resultados = await llm_caller.executa_tagging_chain_async_batch(prompts)
    return (
        [r if isinstance(r, dict) else {} for r in resultados]
        if isinstance(resultados, list)
        else []
    )


async def extrai_info_e_salva(lista_produtos: list[T.Row]) -> None:
    global countAccumulator
    for lote in create_batches(lista_produtos, BATCH_SIZE):
        resultados = await extrair_info(lote)

        valid_rows = validate_llm_results(resultados, SCHEMA_FIELDS)

        if not valid_rows:
            continue

        count = save_enrichment_batch(
            spark,
            valid_rows,
            TABLE_OUTPUT.value,
            enriquece_alimentos_suplementos_schema,
        )

        countAccumulator.add(count)

        print(
            f"{agora_em_sao_paulo_str()} - Pendentes: {len(lista_produtos) - countAccumulator.value}/{len(lista_produtos)}"
        )


# COMMAND ----------

await extrai_info_e_salva(produtos_pendentes)

# COMMAND ----------

if DEBUG:
    env.table(TABLE_OUTPUT).filter(
        F.col("ean").isin([p.ean for p in produtos_pendentes])
    ).limit(1000).display()
