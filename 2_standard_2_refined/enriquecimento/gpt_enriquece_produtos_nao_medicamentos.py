# Databricks notebook source
# MAGIC %pip install -- langchain langchain-openai langchain-core langchain-community langchainhub tiktoken langchain-google-genai duckduckgo-search tavily-python agno pycountry google-genai langchain-google-vertexai langsmith

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

from datetime import datetime
from typing import Union

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql import types as T

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.llm.embeddings import (
    EmbeddingEncodingModels,
    get_max_tokens_by_embedding_model,
)
from maggulake.llm.models import get_llm_config
from maggulake.llm.tagging_processor import TaggingProcessor
from maggulake.llm.vertex import setup_langsmith
from maggulake.pipelines.filter_notify import (
    filtra_notifica_produtos_enriquecimento,
)
from maggulake.pipelines.schemas.enriquece_produtos_nao_medicamentos import (
    EnriquecimentoProdutosNaoMedicamentos,
    colunas_obrigatorios_enriquecimento_nao_medicamentos,
    enriquece_nao_medicamentos_schema,
)
from maggulake.pipelines.short_products.produtos_texto_curto import (
    aplica_nome_descricao_externos,
)
from maggulake.prompts import MAGGU_SYSTEM_PROMPT
from maggulake.utils.iters import create_batches
from maggulake.utils.strings import truncate_text_to_tokens
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "gpt_enriquece_produtos_nao_medicamentos",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "llm_provider": ["gemini_vertex", "gemini_vertex", "openai"],
        "max_products": "60000",
    },
)

# COMMAND ----------

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

# Configurações
DEBUG = dbutils.widgets.get("debug") == "true"
LLM_PROVIDER = dbutils.widgets.get("llm_provider")
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
spark = env.spark

# accumulator initialization
countAccumulator = spark.sparkContext.accumulator(0)

# COMMAND ----------

# LLM
llm_config = get_llm_config(LLM_PROVIDER, spark, dbutils, size="SMALL")

EMBEDDING_ENCODING = EmbeddingEncodingModels.CL100K_BASE.value
MAX_TOKENS = get_max_tokens_by_embedding_model(EMBEDDING_ENCODING)

# Parametros de controle
BATCH_SIZE: int = 100
MIN_TEXT_LEN: int = 10

TABELA_PRODUTOS_NOME_DESCRICAO_EXTERNOS = Table.produtos_nome_descricao_externos.value


# COMMAND ----------

# Notificacoes via discord
THRESHOLD_EXECUCAO = 0.3  # Nunca vai rodar mais que 30% da base
DAYS_BACK = 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de tabelas

# COMMAND ----------

# Esses sao os produtos já enriquecidos
produtos_enriquecidos = env.table(Table.extract_product_info_nao_medicamentos).cache()

# COMMAND ----------

if DEBUG:
    # So pra visualizar quantos EANs a gente ja tem na tabela
    lista_eans_enriquecidos = [
        row.ean for row in produtos_enriquecidos.select("ean").distinct().collect()
    ]
    print(f"{len(lista_eans_enriquecidos)} EANs ja enriquecidos anteriormente.")
    produtos_enriquecidos.limit(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos produtos ainda nao refinados

# COMMAND ----------

produtos = env.table(Table.produtos_standard).cache()

# COMMAND ----------

# Lê a tabela consolidada de eh_medicamento
eh_medicamento_completo = env.table(Table.coluna_eh_medicamento_completo).select(
    "ean", F.col("eh_medicamento").alias("eh_medicamento_consolidado")
)

# Faz join com produtos para obter a classificação consolidada
produtos = produtos.join(eh_medicamento_completo, on=["ean"], how="left")

if DEBUG:
    print("📊 Produtos com classificação consolidada:")
    print(f"   - Total: {produtos.count()}")
    print(
        f"   - Com eh_medicamento_consolidado: {produtos.filter(F.col('eh_medicamento_consolidado').isNotNull()).count()}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtros

# COMMAND ----------

# DBTITLE 1,Untitled
# Esse notebook so faz sentido para nao medicamentos.
produtos = produtos.filter(
    F.coalesce(F.col("eh_medicamento_consolidado"), F.col("eh_medicamento"))
    == F.lit(False)
)

# EAN e necessario para tentar enriquecimento externo.
produtos = produtos.dropna(subset=["ean"])

# Complemento gerado pelo notebook complementa_nome_descricao_produtos.
if spark.catalog.tableExists(TABELA_PRODUTOS_NOME_DESCRICAO_EXTERNOS):
    produtos_texto_externos = spark.read.table(TABELA_PRODUTOS_NOME_DESCRICAO_EXTERNOS)
    produtos = aplica_nome_descricao_externos(
        produtos,
        produtos_texto_externos,
        MIN_TEXT_LEN,
    )

# Campos muito importantes nao podem ser nulos, senao a LLM alucina.
produtos = produtos.dropna(subset=["nome", "descricao"])

# Aplica o corte minimo depois da tentativa de completar com fontes externas.
produtos = produtos.filter(
    (F.length(F.col("nome")) >= MIN_TEXT_LEN)
    & (F.length(F.col("descricao")) >= MIN_TEXT_LEN)
)

# COMMAND ----------

if DEBUG:
    print("Quantidade de produtos na camada de entrada: ", produtos.count())
    produtos.limit(100).display()

# TODO: notei que alguns produtos estao marcados como "nao medicamento" porem possuem principio ativo.
# Isso ta certo? (creio que nao) A gente quer isso na nossa base mesmo?
# Exemplo:
# 7897930777163 - Kit Sabonete em Barra Cetaphil Limpeza Suave com 3 unidades
# Principio ativo: ESTEARICO ACIDO + GLICEROL + OLEO MINERAL + SODIO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seleciona os produtos que vamos (re)enriquecer

# COMMAND ----------

# Produtos que ja foram enriquecidos porem estao incompletos serão refeitos
produtos_enriquecidos = produtos_enriquecidos.dropna(
    subset=colunas_obrigatorios_enriquecimento_nao_medicamentos
)


# COMMAND ----------

# Filtro dos produtos que ainda nao foram enriquecidos
df_filtra_refazer = (
    produtos.alias("p")
    .join(produtos_enriquecidos.alias("pr"), on=["ean"], how="left_anti")
    .select(  # Seleciona colunas usadas pela LLM
        "ean",
        "nome",
        "marca",
        "fabricante",
        "categorias",
        "descricao",
        "gerado_em",
    )
)

# COMMAND ----------

if DEBUG:
    print(
        "Quantidade de produtos que estao na camada de entrada e precisam ser enriquecidos: ",
        df_filtra_refazer.count(),
    )
    df_filtra_refazer.limit(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Controla a quantidade de produtos

# COMMAND ----------

total_original = df_filtra_refazer.count()
if MAX_PRODUCTS and total_original > MAX_PRODUCTS:
    print(
        f"⚠️  Limitando processamento devido a max_products: {total_original} → {MAX_PRODUCTS} produtos"
    )
    df_filtra_refazer = df_filtra_refazer.limit(MAX_PRODUCTS)

# COMMAND ----------

produtos_pendentes = filtra_notifica_produtos_enriquecimento(
    original_df=produtos,
    df_to_enrich=df_filtra_refazer,
    threshold=THRESHOLD_EXECUCAO,
    days_back=DAYS_BACK,
    script_name="gpt_enriquece_produtos_nao_medicamentos",
)

# COMMAND ----------

if not len(produtos_pendentes) > 0:
    dbutils.notebook.exit("Sem produtos para atuar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando fluxo de enriquecimento

# COMMAND ----------

llm_caller = TaggingProcessor(
    provider=llm_config.provider,
    model=llm_config.model,
    api_key=llm_config.api_key,
    project_id=llm_config.project_id,
    location=llm_config.location,
    prompt_template=MAGGU_SYSTEM_PROMPT,
    output_schema=EnriquecimentoProdutosNaoMedicamentos,
    temperature=0.1,
)

# COMMAND ----------


def formata_valor_coluna(value: Union[None, str, list]) -> str:
    # Recebe o valor de uma coluna e formata para string, lidando com os diferentes tipos de dados

    if value is None:
        return "N/A"

    if isinstance(value, list):
        if len(value) == 0:
            return "N/A"
        return ", ".join(str(v) for v in value)

    return value


# COMMAND ----------


def formata_info_produto(produto: T.Row) -> str:
    return truncate_text_to_tokens(
        "\n\n".join(
            [
                f"EAN do produto: {formata_valor_coluna(produto.ean)}",
                f"Nome do produto: {formata_valor_coluna(produto.nome)}",
                f"Marca do produto: {formata_valor_coluna(produto.marca)}",
                f"Fabricante do produto: {formata_valor_coluna(produto.fabricante)}",
                f"Descrição do produto: {formata_valor_coluna(produto.descricao)}",
                f"Categorias do produto: {formata_valor_coluna(produto.categorias)}",
            ]
        ),
        MAX_TOKENS,
        EMBEDDING_ENCODING,
    )


# COMMAND ----------

prompt_teste = formata_info_produto(produtos.head())
print(llm_caller.executa_tagging_chain(prompt_teste))


# COMMAND ----------


async def extrai_info_e_salva(lista_produtos: list[T.Row]) -> None:
    global countAccumulator

    for lote in create_batches(lista_produtos, BATCH_SIZE):
        resultados = await extrair_info(lote)
        salva_info_produto(resultados)

        print(
            agora_em_sao_paulo_str()
            + " - Quantidade de produtos pendentes: "
            + f"{len(lista_produtos) - countAccumulator.value} de {len(lista_produtos)}"
        )


# COMMAND ----------


async def extrair_info(lista_produtos: list[T.Row]) -> list[dict]:
    prompts = [formata_info_produto(produto) for produto in lista_produtos]

    if not prompts:
        return []

    resultados_modelo = await llm_caller.executa_tagging_chain_async_batch(prompts)

    if not isinstance(resultados_modelo, list):
        return []

    return [
        resultado if isinstance(resultado, dict) else {}
        for resultado in resultados_modelo
    ]


# COMMAND ----------


SCHEMA_FIELDS = [field.name for field in enriquece_nao_medicamentos_schema.fields]

# COMMAND ----------


def validate_results(results: list[dict]) -> list[dict]:
    validated: list[Row] = []

    for res in results:
        if not isinstance(res, dict):
            continue

        complete_res = {field: res.get(field, None) for field in SCHEMA_FIELDS}

        # Evita um monte de "null" como string
        for key, _ in complete_res.items():
            if isinstance(complete_res[key], str) and complete_res[key] == "null":
                complete_res[key] = None

        complete_res["atualizado_em"] = datetime.now()  # Update timestamp

        validated.append(Row(**complete_res))

    return validated


# COMMAND ----------


def salva_info_produto(info: list[dict]) -> None:
    global countAccumulator

    validated_rows = validate_results(info)

    if len(validated_rows) == 0:
        return  # early exit

    # Convert validated rows to a DataFrame
    df_results = spark.createDataFrame(
        validated_rows, enriquece_nao_medicamentos_schema
    )

    # NOTE: existe um custo por ler e reler essa tabela cada vez que passa no loop.
    # No futuro devemos investigar formas mais eficientes de fazer isso.
    # Penso que idealmente a gente deveria gerar todos os enriquecimentos e manter como variavel no python
    # De tempos em tempos salvamos no disco para aliviar a memoria. E colocamos um `finally`
    # para garantir que salvamos o que foi processado ate o momento.
    delta_table = DeltaTable.forName(
        spark, Table.extract_product_info_nao_medicamentos.value
    )

    # Perform the merge (upsert)
    delta_table.alias("target").merge(
        source=df_results.alias("source"),
        condition="target.ean = source.ean",
    ).whenMatchedUpdate(
        set={col: f"source.{col}" for col in SCHEMA_FIELDS if col not in ('ean')}
    ).whenNotMatchedInsert(
        values={col: f"source.{col}" for col in SCHEMA_FIELDS}
    ).execute()

    # Update the accumulator
    countAccumulator.add(len(validated_rows))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Executando enriquecimento e salvando produtos na tabela delta

# COMMAND ----------

# executando o enriquecimento
await extrai_info_e_salva(produtos_pendentes)

# Ajustar o tamanho do batch pra nao ficar muito grande

# COMMAND ----------

# Conferir produtos enriquecidos

if DEBUG:
    produtos_recem_enriquecidos = (
        env.table(Table.extract_product_info_nao_medicamentos)
        .filter(F.col("ean").isin([p.ean for p in produtos_pendentes]))
        .cache()
    )

    produtos_recem_enriquecidos.limit(1000).display()
