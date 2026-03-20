# Databricks notebook source
# MAGIC %pip install -- langchain langchain-openai langchain-core langchain-community langchainhub tiktoken langchain-google-genai langchain-google-vertexai langsmith

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Power Phrase - Geração de Frases de Impacto para Produtos
# MAGIC
# MAGIC Este notebook utiliza modelos de linguagem (LLMs) para gerar power phrases
# MAGIC (frases de impacto) que destacam a proposta única de valor de cada produto.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from datetime import datetime

import pyspark.sql.functions as F
import pytz
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Row

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
from maggulake.pipelines.schemas import (
    EnriquecimentoPowerPhrase,
    ResultadoPowerPhrase,
    power_phrase_produtos_schema,
)
from maggulake.prompts import (
    POWER_PHRASE_PROMPT,
    POWER_PHRASE_SYSTEM_INSTRUCTION,
    POWER_PHRASE_USER_TEMPLATE,
)
from maggulake.utils.iters import create_batches
from maggulake.utils.strings import truncate_text_to_tokens
from maggulake.utils.time import agora_em_sao_paulo_str as agora

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets de Configuração

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inicialização do Ambiente

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "power_phrase_produtos",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "llm_provider": ["gemini_vertex", "openai", "gemini_vertex"],
        "max_products": "10000",
        "batch_size": "100",
        "timeout_batch": "600",
    },
)

# COMMAND ----------

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

spark = env.spark

# Configurações dos widgets
DEBUG = dbutils.widgets.get("debug") == "true"
LLM_PROVIDER = dbutils.widgets.get("llm_provider")
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
TIMEOUT = int(dbutils.widgets.get("timeout_batch"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuração do LLM

# COMMAND ----------

llm_config = get_llm_config(LLM_PROVIDER, spark, dbutils, size="SMALL")

CONTEXT_CACHE_NAME = None
if llm_config.provider == "gemini_vertex":
    CONTEXT_CACHE_NAME = create_vertex_context_cache(
        model=llm_config.model,
        system_instruction=POWER_PHRASE_SYSTEM_INSTRUCTION,
        project_id=llm_config.project_id,
        location=llm_config.location,
        ttl=7200,
    )

# COMMAND ----------

EMBEDDING_ENCODING = EmbeddingEncodingModels.CL100K_BASE.value
MAX_TOKENS = get_max_tokens_by_embedding_model(EMBEDDING_ENCODING)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parâmetros de Notificação

# COMMAND ----------

# Notificações via discord
THRESHOLD_EXECUCAO = 0.3
DAYS_BACK = 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de Dados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura de produtos refinados

# COMMAND ----------

produtos = env.table(Table.produtos_refined).dropDuplicates(["ean"]).cache()

# Filtra produtos com informação mínima útil
produtos = produtos.filter(
    F.col("nome").isNotNull() & F.col("informacoes_para_embeddings").isNotNull()
)

print(f"📊 Total de produtos disponíveis: {produtos.count()}")
produtos.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura de produtos já enriquecidos

# COMMAND ----------

produtos_enriquecidos = env.table(Table.power_phrase_produtos).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos Dados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação do Processador LLM

# COMMAND ----------

_prompt_template = (
    POWER_PHRASE_USER_TEMPLATE if CONTEXT_CACHE_NAME else POWER_PHRASE_PROMPT
)
_system_instruction = POWER_PHRASE_SYSTEM_INSTRUCTION if CONTEXT_CACHE_NAME else None

power_phrase_processor = TaggingProcessor(
    provider=llm_config.provider,
    api_key=llm_config.api_key,
    model=llm_config.model,
    prompt_template=_prompt_template,
    output_schema=EnriquecimentoPowerPhrase,
    project_id=llm_config.project_id,
    location=llm_config.location,
    cached_content=CONTEXT_CACHE_NAME,
    system_instruction=_system_instruction,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Função de formatação do prompt

# COMMAND ----------


def prepara_dados_produto_para_prompt(produto: Row) -> str:
    nome = produto.nome or ""
    descricao = produto.descricao or ""
    info_embeddings = produto.informacoes_para_embeddings or ""
    eh_med = produto.eh_medicamento

    campos = [
        f"Nome: {nome}",
    ]

    # 10 é um bom número para considerar se o conteúdo é relevante o suficiente

    if len(descricao.strip()) > 10:
        campos.append(f"Descrição: {descricao}")

    if len(info_embeddings.strip()) > 10:
        campos.append(f"Informações adicionais: {info_embeddings}")

    campos.append(f"É medicamento: {eh_med}")

    texto_completo = "\n\n".join(campos)
    return truncate_text_to_tokens(texto_completo, MAX_TOKENS, EMBEDDING_ENCODING)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtragem de produtos pendentes

# COMMAND ----------


def filtra_a_fazer(produtos: DataFrame, produtos_enriquecidos: DataFrame) -> DataFrame:
    return (
        produtos.alias("p")
        .join(
            produtos_enriquecidos.alias("pe"),
            on=["ean"],
            how="left_anti",
        )
        .select("p.*")
    )


# COMMAND ----------

df_produtos_pendentes = filtra_a_fazer(produtos, produtos_enriquecidos)
print(f"📋 Produtos pendentes de enriquecimento: {df_produtos_pendentes.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notificação e limitação de produtos

# COMMAND ----------

produtos_pendentes: list[Row] = filtra_notifica_produtos_enriquecimento(
    original_df=produtos,
    df_to_enrich=df_produtos_pendentes,
    threshold=THRESHOLD_EXECUCAO,
    days_back=DAYS_BACK,
    script_name="gera_power_phrase_produtos",
)

print(f"📦 Produtos após filtro de notificação: {len(produtos_pendentes)}")

# COMMAND ----------

# Limita o número máximo de produtos a processar
total_original = len(produtos_pendentes)
if total_original > MAX_PRODUCTS:
    produtos_pendentes = produtos_pendentes[:MAX_PRODUCTS]
    print(
        f"⚠️  Limitando processamento a {MAX_PRODUCTS} produtos (total disponível: {total_original})"
    )

print(f"✅ Total de produtos a processar: {len(produtos_pendentes)}")

# COMMAND ----------

if not len(produtos_pendentes) > 0:
    dbutils.notebook.exit("✋ Sem produtos para atuar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação de Batches

# COMMAND ----------

batches: list[list[Row]] = list(create_batches(produtos_pendentes, BATCH_SIZE))

print(f"📊 Total de produtos a processar: {len(produtos_pendentes)}")
print(f"📦 Total de batches: {len(batches)}")
print(f"🔢 Tamanho de cada batch: {BATCH_SIZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste com 1 Produto

# COMMAND ----------

if DEBUG and len(batches) > 0:
    produto_teste = batches[0][0]
    prompt_teste = prepara_dados_produto_para_prompt(produto_teste)
    print("🧪 Testando com produto:")
    print(prompt_teste)
    print("\n" + "=" * 80 + "\n")
    resultado_teste = power_phrase_processor.executa_tagging_chain(prompt_teste)
    print("📝 Resultado:")
    print(resultado_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento e Salvamento

# COMMAND ----------


async def processa_e_salva_batches(
    batches: list[list[Row]], processor: TaggingProcessor, spark, table_name: str
) -> None:
    total_batches = len(batches)
    produtos_processados = 0

    for idx, batch in enumerate(batches, 1):
        print(f"{agora()} - 🔄 Processando batch {idx}/{total_batches}...")

        # Prepara os prompts para o batch
        prompts: list[str] = [prepara_dados_produto_para_prompt(item) for item in batch]

        # Processa o batch de forma assíncrona
        resultados = await processor.executa_tagging_chain_async_batch(
            prompts, timeout=TIMEOUT
        )

        # Valida e converte resultados
        respostas: list[ResultadoPowerPhrase] = []
        for item, resultado in zip(batch, resultados):
            if not isinstance(resultado, dict):
                print(
                    f"\t⚠️  Produto {item.ean} não retornou resposta no formato esperado"
                )
                continue

            respostas.append(
                ResultadoPowerPhrase(
                    ean=item.ean,
                    power_phrase=resultado.get("power_phrase"),
                    atualizado_em=datetime.now(pytz.timezone("America/Sao_Paulo")),
                )
            )

        if not respostas:
            print(f"\t⚠️  Batch {idx} não retornou respostas válidas")
            continue

        # Converte para DataFrame e salva
        respostas_dict = [r.to_dict() for r in respostas]
        respostas_df = spark.createDataFrame(
            respostas_dict, power_phrase_produtos_schema
        )

        # Garante que não tenta salvar EANs duplicados
        respostas_df = respostas_df.dropDuplicates(["ean"])

        # Merge no Delta Lake
        delta_table = DeltaTable.forName(spark, table_name)
        target_columns = {f.name for f in delta_table.toDF().schema.fields}
        source_columns = set(respostas_df.columns)
        common_columns = (target_columns & source_columns) - {"ean"}
        respostas_df = respostas_df.select(
            "ean", *[c for c in respostas_df.columns if c in common_columns]
        )
        merge_columns = {col: f"source.{col}" for col in common_columns}
        delta_table.alias("target").merge(
            source=respostas_df.alias("source"),
            condition="target.ean = source.ean",
        ).whenMatchedUpdate(set=merge_columns).whenNotMatchedInsert(
            values={col: f"source.{col}" for col in respostas_df.columns}
        ).execute()

        produtos_processados += len(respostas)
        print(
            f"\t✅ {agora()} - Batch {idx}/{total_batches} salvo com sucesso "
            f"({len(respostas)} produtos | Total processado: {produtos_processados})"
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do Enriquecimento

# COMMAND ----------

await processa_e_salva_batches(
    batches,
    power_phrase_processor,
    spark,
    Table.power_phrase_produtos.value,
)
