# Databricks notebook source
# MAGIC %md
# MAGIC # Resume Indicação dos produtos

# COMMAND ----------

# MAGIC %pip install -- langchain langchain-openai langchain-core langchain-community langchainhub tiktoken langchain-google-genai google-genai langchain-google-vertexai langsmith

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from datetime import datetime
from typing import List

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
from maggulake.pipelines.schemas.resume_indicacao_produtos import (
    ResultadoResumeIndicacao,
    ResumeIndicacaoProdutos,
    resume_indicacao_produtos_schema,
)
from maggulake.prompts import (
    RESUME_INDICACAO_PROMPT,
    RESUME_INDICACAO_SYSTEM_INSTRUCTION,
    RESUME_INDICACAO_USER_TEMPLATE,
)
from maggulake.utils.iters import create_batches
from maggulake.utils.strings import truncate_text_to_tokens
from maggulake.utils.time import agora_em_sao_paulo_str as agora

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variaveis de ambiente

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "gpt_resume_indicacao_produtos",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "llm_provider": ["gemini_vertex", "openai"],
        "batch_size": "100",
        "timeout_batch": "600",
        "max_products": "20000",
        "debug": ["false", "true"],
    },
)

# COMMAND ----------

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

LLM_PROVIDER = dbutils.widgets.get("llm_provider")

# COMMAND ----------

# Configurações
DEBUG = dbutils.widgets.get("debug") == "true"
spark = env.spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuração do LLM

# COMMAND ----------

llm_config = get_llm_config(LLM_PROVIDER, spark, dbutils, size="SMALL")

CONTEXT_CACHE_NAME = None
if llm_config.provider == "gemini_vertex":
    CONTEXT_CACHE_NAME = create_vertex_context_cache(
        model=llm_config.model,
        system_instruction=RESUME_INDICACAO_SYSTEM_INSTRUCTION,
        project_id=llm_config.project_id,
        location=llm_config.location,
        ttl=7200,
    )


EMBEDDING_ENCODING = EmbeddingEncodingModels.CL100K_BASE.value
MAX_TOKENS = get_max_tokens_by_embedding_model(EMBEDDING_ENCODING)

# COMMAND ----------

# Notificacoes via discord
THRESHOLD_EXECUCAO = 0.5
DAYS_BACK = 15

# Parametros de execução
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
TIMEOUT = int(dbutils.widgets.get("timeout_batch"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura produtos standard

# COMMAND ----------

produtos = env.table(Table.produtos_standard).dropDuplicates(["ean"]).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura produtos já enriquecidos

# COMMAND ----------

produtos_enriquecidos = env.table(Table.gpt_descricoes_curtas_produtos).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtra produtos a serem enriquecidos

# COMMAND ----------


def filtra_a_fazer(
    produtos: DataFrame, produtos_enriquecidos: DataFrame | None
) -> DataFrame:
    # Quem já foi enriquecido não será novamente enriquecido
    # TODO: Definir um tempo de validade desse enriquecimento, para enriquecer novamente quem ficou muito velho
    produtos_pendentes = (
        produtos.alias("p")
        .join(produtos_enriquecidos.alias("pr"), on=["ean"], how="left_anti")
        .select("p.*")
    )

    # Remove produtos sem informação mínima útil para gerar descrição curta
    # Precisa ter pelo menos nome + (descrição OU indicação com mais de 10 caracteres)
    produtos_pendentes = produtos_pendentes.filter(
        (F.col("nome").isNotNull())
        & (
            (F.length(F.coalesce(F.col("descricao"), F.lit(""))) > 10)
            | (F.length(F.coalesce(F.col("indicacao"), F.lit(""))) > 10)
        )
    )

    return produtos_pendentes


# COMMAND ----------

produtos_pendentes: DataFrame = filtra_a_fazer(produtos, produtos_enriquecidos)

# COMMAND ----------

produtos_pendentes_qtd = produtos_pendentes.count()

if not produtos_pendentes_qtd > 0:
    dbutils.notebook.exit("Sem produtos para atuar")

print(f"Produtos pendentes de enriquecimento: {produtos_pendentes_qtd}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aviso no discord se a quantidade de produtos crescer muito

# COMMAND ----------

produtos_pendentes: list[Row] = filtra_notifica_produtos_enriquecimento(
    spark,
    original_df=produtos,
    df_to_enrich=produtos_pendentes,
    threshold=THRESHOLD_EXECUCAO,
    days_back=DAYS_BACK,
    script_name="gpt_resume_indicacao_produtos",
)


print(
    f"Produtos após filtro de notificação: {len(produtos_pendentes)} produtos pendentes"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepara para o enriquecimento

# COMMAND ----------

_prompt_template = (
    RESUME_INDICACAO_USER_TEMPLATE if CONTEXT_CACHE_NAME else RESUME_INDICACAO_PROMPT
)

resume_indicacao_processor = TaggingProcessor(
    provider=llm_config.provider,
    api_key=llm_config.api_key,
    model=llm_config.model,
    prompt_template=_prompt_template,
    output_schema=ResumeIndicacaoProdutos,
    project_id=llm_config.project_id,
    location=llm_config.location,
    cached_content=CONTEXT_CACHE_NAME,
    system_instruction=(
        RESUME_INDICACAO_SYSTEM_INSTRUCTION if CONTEXT_CACHE_NAME else None
    ),
)

# COMMAND ----------


def prepara_dados_produto_para_o_prompt(p: Row) -> str:
    campos = [f"EAN: {p.ean}", f"Nome do produto: {p.nome or ''}"]

    optional_fields = [
        ("principio_ativo", "Princípio Ativo"),
        ("dosagem", "Dosagem"),
        ("marca", "Marca"),
        ("fabricante", "Fabricante"),
        ("tarja", "Tarja"),
    ]

    for attr, label in optional_fields:
        if val := getattr(p, attr, None):
            campos.append(f"{label}: {val}")

    for attr, label in [("descricao", "Descrição"), ("indicacao", "Indicação")]:
        val = getattr(p, attr, "") or ""
        if len(val.strip()) > 10:
            campos.append(f"{label}: {val}")

    eh_med = "Sim" if getattr(p, "eh_medicamento", False) else "Não"
    campos.append(f"É medicamento: {eh_med}")

    texto_completo = "\n\n".join(campos)
    return truncate_text_to_tokens(texto_completo, MAX_TOKENS, EMBEDDING_ENCODING)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Limita quantidade de produtos a serem refinados

# COMMAND ----------

# Limita o número máximo de produtos a processar
if len(produtos_pendentes) > MAX_PRODUCTS:
    print(
        f"⚠️  Limitando processamento a {MAX_PRODUCTS} produtos (total disponível: {len(produtos_pendentes)})"
    )
    produtos_pendentes = produtos_pendentes[:MAX_PRODUCTS]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria batches para iniciar o processamento

# COMMAND ----------

batches: list[list[Row]] = list(create_batches(produtos_pendentes, BATCH_SIZE))

print(f"📊 Total de produtos a processar: {len(produtos_pendentes)}")
print(f"📦 Total de batches: {len(batches)}")
print(f"🔢 Tamanho de cada batch: {BATCH_SIZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testa para 1 produto

# COMMAND ----------

resume_indicacao_processor.executa_tagging_chain(batches[0][0])

# COMMAND ----------

await resume_indicacao_processor.executa_tagging_chain_async(
    batches[0][0], timeout=TIMEOUT
)


# COMMAND ----------

await resume_indicacao_processor.executa_tagging_chain_async_batch(
    batches[0], timeout=TIMEOUT
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Executa e salva o enriquecimento

# COMMAND ----------


async def processa_e_salva_batches(
    batches: List[List[Row]], processor: TaggingProcessor, spark, table_name: str
):
    total_batches = len(batches)

    for idx, batch in enumerate(batches, 1):
        print(f"{agora()} - 🔄 Processando batch {idx}/{total_batches}...")

        prompts: list[str] = [
            prepara_dados_produto_para_o_prompt(item) for item in batch
        ]

        resultados = await processor.executa_tagging_chain_async_batch(
            prompts, timeout=TIMEOUT
        )

        # Extrai as respostas válidas usando dataclass
        respostas: list[ResultadoResumeIndicacao] = []
        for item, r in zip(batch, resultados):
            if not isinstance(r, dict):
                print(
                    f"\t⚠️ Um produto do batch {idx} não retornou respostas no formato esperado: {r}"
                )
                continue

            respostas.append(
                ResultadoResumeIndicacao(
                    ean=item.ean,
                    descricao_curta=r.get("descricao_curta"),
                    atualizado_em=datetime.now(pytz.timezone("America/Sao_Paulo")),
                )
            )

        if not respostas:
            print(f"\t⚠️ Batch {idx} não retornou respostas válidas")
            continue

        respostas_dict = [r.to_dict() for r in respostas]
        respostas_df = spark.createDataFrame(
            respostas_dict, resume_indicacao_produtos_schema
        )

        # Just in case... Garante que não tenta salvar EANs duplicados na tabela
        respostas_df = respostas_df.dropDuplicates(["ean"])

        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("target").merge(
            source=respostas_df.alias("source"),
            condition="target.ean = source.ean",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print(
            f"\t✅ {agora()} - Batch {idx}/{total_batches} salvo com sucesso ({len(respostas)} produtos)"
        )


# COMMAND ----------

await processa_e_salva_batches(
    batches,
    resume_indicacao_processor,
    spark,
    Table.gpt_descricoes_curtas_produtos.value,
)
