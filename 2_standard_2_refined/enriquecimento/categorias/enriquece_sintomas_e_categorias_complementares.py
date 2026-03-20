# Databricks notebook source
# MAGIC %pip install -- langchain langchain-core langchain-community langchainhub tiktoken html2text langchain-google-vertexai duckduckgo-search tavily-python agno pycountry google-genai langsmith
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ### Task Inputs

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import types as T

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.llm.models import get_model_name
from maggulake.llm.tagging_processor import TaggingProcessor
from maggulake.llm.vertex import (
    create_vertex_context_cache,
    setup_langsmith,
    setup_vertex_ai_credentials,
)
from maggulake.pipelines.filter_notify import (
    filtra_notifica_produtos_enriquecimento,
)
from maggulake.pipelines.schemas.enriquece_sintomas_categorias_complementares import (
    CategoriasPorSintoma,
    SintomasProduto,
    categorias_sintomas_pyspark_schema,
    schema_categorias_sintomas,
    schema_indicacoes_categorias,
    schema_sintomas,
    sintomas_pyspark_schema,
)
from maggulake.prompts import (
    PROMPT_CATEGORIAS_SYSTEM_INSTRUCTION,
    PROMPT_CATEGORIAS_USER_TEMPLATE,
    PROMPT_SINTOMAS_SYSTEM_INSTRUCTION,
    PROMPT_SINTOMAS_USER_TEMPLATE,
)
from maggulake.utils.iters import batched
from maggulake.utils.time import agora_em_sao_paulo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "enriquece_sintomas_e_categorias_complementares",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "max_products": "5000",
        "ignorar_filtros_de_execucao": "false",
    },
)
setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

# Cria as tabelas Delta se não existirem
env.create_table_if_not_exists(Table.sintomas, schema_sintomas)
env.create_table_if_not_exists(Table.categorias_sintomas, schema_categorias_sintomas)
env.create_table_if_not_exists(
    Table.indicacoes_categorias, schema_indicacoes_categorias
)

# COMMAND ----------

DEBUG = dbutils.widgets.get("debug") == "true"
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
IGNORAR_FILTROS = dbutils.widgets.get("ignorar_filtros_de_execucao") == "true"
spark = env.spark
countAccumulator = spark.sparkContext.accumulator(0)
BATCH_SIZE = 500

# Parametros para notificacoes do discord
THRESHOLD_EXECUCAO = 0.2
DAYS_BACK = 30

# COMMAND ----------

PROJECT_ID, LOCATION = setup_vertex_ai_credentials(spark)
LLM_MODEL = get_model_name(provider="gemini_vertex", size="SMALL")

print(
    f"🤖 Gemini Vertex AI configurado — Modelo: {LLM_MODEL}, Project: {PROJECT_ID}, Location: {LOCATION}"
)

# COMMAND ----------

CACHE_SINTOMAS = create_vertex_context_cache(
    model=LLM_MODEL,
    system_instruction=PROMPT_SINTOMAS_SYSTEM_INSTRUCTION,
    project_id=PROJECT_ID,
    location=LOCATION,
    ttl=14400,
)

CACHE_CATEGORIAS = create_vertex_context_cache(
    model=LLM_MODEL,
    system_instruction=PROMPT_CATEGORIAS_SYSTEM_INSTRUCTION,
    project_id=PROJECT_ID,
    location=LOCATION,
    ttl=14400,
)

llm_caller_sintomas = TaggingProcessor(
    provider="gemini_vertex",
    project_id=PROJECT_ID,
    location=LOCATION,
    model=LLM_MODEL,
    prompt_template=PROMPT_SINTOMAS_USER_TEMPLATE,
    output_schema=SintomasProduto,
    temperature=0.1,
    cached_content=CACHE_SINTOMAS,
    system_instruction=PROMPT_SINTOMAS_SYSTEM_INSTRUCTION,
)

llm_caller_categorias = TaggingProcessor(
    provider="gemini_vertex",
    project_id=PROJECT_ID,
    location=LOCATION,
    model=LLM_MODEL,
    prompt_template=PROMPT_CATEGORIAS_USER_TEMPLATE,
    output_schema=CategoriasPorSintoma,
    temperature=0.1,
    cached_content=CACHE_CATEGORIAS,
    system_instruction=PROMPT_CATEGORIAS_SYSTEM_INSTRUCTION,
)

# COMMAND ----------

# MAGIC %md ## Leitura e Filtragem de Produtos

# COMMAND ----------

sintomas_raw = env.table(Table.sintomas).cache()
categorias_raw = env.table(Table.categorias_sintomas).cache()
produtos = (
    env.table(Table.produtos_standard)
    .cache()
    .dropna(subset=["nome"])
    .filter(F.col("eh_medicamento") == True)
    .filter(~(F.col("descricao").isNull() | (F.col("descricao") == "")))
    .cache()
)

print(f"Total de produtos medicamentos com descrição: {produtos.count()}")

# COMMAND ----------

# MAGIC %md ## Fase 1 — Sintomas por Produto

# COMMAND ----------


def formata_info_produto_sintomas(produto) -> str:
    descricao = (produto.descricao or "").replace("\n", " ")[:1000]
    return f"EAN: {produto.ean}\nNOME: {produto.nome}\nDESCRIÇÃO: {descricao}"


def filtra_a_fazer_produtos(produtos):
    """Anti-join com sintomas_gpt para processar apenas produtos ainda não enriquecidos."""
    sintomas_processados = env.table(Table.sintomas).select("ean")
    return (
        produtos.alias("p")
        .join(sintomas_processados.alias("s"), on="ean", how="left_anti")
        .select("p.*")
    )


def salva_sintomas_batch(resultados: list[dict]) -> int:
    """Salva resultados do LLM na tabela Delta sintomas_gpt via merge."""
    rows_validas = [
        T.Row(
            ean=r["ean"],
            resposta=r.get("resposta"),
            atualizado_em=agora_em_sao_paulo(),
        )
        for r in resultados
        if isinstance(r, dict) and r.get("ean")
    ]

    if not rows_validas:
        return 0

    df_results = spark.createDataFrame(rows_validas, sintomas_pyspark_schema)
    df_results = df_results.dropDuplicates(["ean"])

    delta_table = DeltaTable.forName(spark, Table.sintomas.value)
    delta_table.alias("target").merge(
        source=df_results.alias("source"),
        condition="target.ean = source.ean",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    return len(rows_validas)


# COMMAND ----------

df_ps = filtra_a_fazer_produtos(produtos)

total_original = df_ps.count()
if MAX_PRODUCTS and total_original > MAX_PRODUCTS:
    print(f"⚠️  Limitando: {total_original} → {MAX_PRODUCTS}")
    df_ps = df_ps.limit(MAX_PRODUCTS)

produtos_pendentes = filtra_notifica_produtos_enriquecimento(
    original_df=produtos,
    df_to_enrich=df_ps,
    ignorar_filtros_de_execucao=IGNORAR_FILTROS,
    threshold=THRESHOLD_EXECUCAO,
    days_back=DAYS_BACK,
    script_name="enriquece_sintomas_e_categorias_complementares",
)


print(f"Total de produtos a processar (sintomas): {len(produtos_pendentes)}")

# COMMAND ----------

if not len(produtos_pendentes) > 0:
    dbutils.notebook.exit("Sem produtos para atuar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste Chamada Sintoma

# COMMAND ----------

if DEBUG:
    produto_teste = produtos.head()
    prompt_sintomas_teste = (
        f"EAN: {produto_teste.ean}\n"
        f"NOME: {produto_teste.nome}\n"
        f"DESCRIÇÃO: {(produto_teste.descricao or '')[:1000]}"
    )
    print("📝 Prompt sintomas:")
    print(prompt_sintomas_teste)
    resultado_sintomas = llm_caller_sintomas.executa_tagging_chain(
        prompt_sintomas_teste
    )
    print("✅ Resultado sintomas:", resultado_sintomas)

# COMMAND ----------


async def extrai_e_salva_sintomas(lista_produtos: list) -> None:
    global countAccumulator
    total = len(lista_produtos)
    for lote in batched(lista_produtos, BATCH_SIZE):
        prompts = [formata_info_produto_sintomas(p) for p in lote]
        resultados = await llm_caller_sintomas.executa_tagging_chain_async_batch(
            prompts
        )

        count = salva_sintomas_batch([r for r in resultados if isinstance(r, dict)])
        countAccumulator.add(count)
        faltam = total - countAccumulator.value
        print(
            f"[Sintomas] Processados: {countAccumulator.value}/{total} — Faltam: {faltam}"
        )


await extrai_e_salva_sintomas(produtos_pendentes)

# COMMAND ----------

# MAGIC %md ## Fase 2 — Categorias por Sintoma

# COMMAND ----------


def parse_sintomas(resposta_raw: str, max_sintomas: int = 7) -> list[str]:
    """Converte string pipe-separated em lista de sintomas."""
    if not resposta_raw or "n/a" in resposta_raw.lower():
        return []
    return [
        s for sr in resposta_raw.split("|") if (s := sr.strip(' ."')) and len(s) < 30
    ][:max_sintomas]


def filtra_a_fazer_sintomas(sintomas_df):
    """Anti-join com categorias_gpt para processar apenas sintomas ainda não enriquecidos."""
    categorias_processadas = env.table(Table.categorias_sintomas).select("sintoma")
    return (
        sintomas_df.alias("s")
        .join(categorias_processadas.alias("c"), on="sintoma", how="left_anti")
        .select("s.*")
    )


def salva_categorias_batch(sintomas: list[str], resultados: list[dict]) -> int:
    """Salva resultados do LLM na tabela Delta categorias_gpt via merge (key: sintoma)."""
    rows_validas = [
        T.Row(
            sintoma=sintoma,
            resposta=r.get("resposta") if isinstance(r, dict) else None,
            atualizado_em=agora_em_sao_paulo(),
        )
        for sintoma, r in zip(sintomas, resultados)
        if isinstance(r, dict)
    ]

    if not rows_validas:
        return 0

    df_results = spark.createDataFrame(rows_validas, categorias_sintomas_pyspark_schema)
    df_results = df_results.dropDuplicates(["sintoma"])

    delta_table = DeltaTable.forName(spark, Table.categorias_sintomas.value)
    delta_table.alias("target").merge(
        source=df_results.alias("source"),
        condition="target.sintoma = source.sintoma",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    return len(rows_validas)


# COMMAND ----------

# Explode sintomas brutos em lista única de sintomas distintos
sintomas_explodidos = (
    sintomas_raw.rdd.flatMap(lambda row: parse_sintomas(row.resposta))
    .distinct()
    .map(lambda s: {"sintoma": s})
    .toDF()
    .cache()
)

print(f"Total de sintomas únicos: {sintomas_explodidos.count()}")

# COMMAND ----------

sintomas_a_fazer_df = filtra_a_fazer_sintomas(sintomas_explodidos)
total_sintomas = sintomas_a_fazer_df.count()
print(f"Sintomas ainda a processar: {total_sintomas}")

if not total_sintomas > 0:
    dbutils.notebook.exit("Sem sintomas novos para atuar")

sintomas_a_fazer = sintomas_a_fazer_df.rdd.map(lambda s: s.sintoma).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste chamada categoria_sintoma

# COMMAND ----------

if DEBUG:
    categoria_sintoma_teste = sintomas_a_fazer_df.head()["sintoma"]
    print(f"\n📝 Prompt categorias para sintoma: '{categoria_sintoma_teste}'")
    resultado_categorias = llm_caller_categorias.executa_tagging_chain(
        categoria_sintoma_teste
    )
    print("✅ Resultado categorias:", resultado_categorias)

# COMMAND ----------

BATCH_SIZE_CATEGORIAS = 10
countAccumulator = spark.sparkContext.accumulator(0)


async def extrai_e_salva_categorias(lista_sintomas: list[str]) -> None:
    global countAccumulator
    total = len(lista_sintomas)
    for lote in batched(lista_sintomas, BATCH_SIZE_CATEGORIAS):
        resultados = await llm_caller_categorias.executa_tagging_chain_async_batch(lote)

        count = salva_categorias_batch(lote, resultados)
        countAccumulator.add(count)
        faltam = total - countAccumulator.value
        print(
            f"[Categorias] Processados: {countAccumulator.value}/{total} — Faltam: {faltam}"
        )


await extrai_e_salva_categorias(sintomas_a_fazer)

# COMMAND ----------

# MAGIC %md ## Fase 3 — Constrói e Salva indicacoes_categorias

# COMMAND ----------


def parse_categorias(resposta_raw: str, max_categorias: int = 5) -> list[str]:
    """Converte string pipe-separated em lista de categorias."""
    if not resposta_raw or "n/a" in resposta_raw.lower():
        return []
    return [
        c
        for cr in resposta_raw.split("|")
        if (c := cr.strip(' ."').lower()) and len(c) < 30
    ][:max_categorias]


# COMMAND ----------

# Expande sintomas por produto (ean → [(ean, sintoma)])
sintomas_por_produto = (
    sintomas_raw.rdd.flatMap(
        lambda row: [
            {"ean": row.ean, "sintoma": s} for s in parse_sintomas(row.resposta)
        ]
    )
    .toDF()
    .cache()
)

# Parse categorias por sintoma (sintoma → [categoria1, categoria2, ...])
categorias_por_sintoma = (
    categorias_raw.rdd.map(
        lambda row: {
            "sintoma": row.sintoma,
            "categorias": parse_categorias(row.resposta),
        }
    )
    .toDF()
    .cache()
)

# COMMAND ----------

# Join e agrupamento por EAN
sc = (
    sintomas_por_produto.join(categorias_por_sintoma, on="sintoma", how="left")
    .select(
        "ean",
        F.struct(
            F.col("sintoma").alias("indicacao"),
            F.col("categorias"),
        ).alias("categorias_de_complementares_por_indicacao"),
    )
    .cache()
)

categorias_de_complementares_por_indicacao = (
    sc.groupBy("ean")
    .agg(
        F.collect_list("categorias_de_complementares_por_indicacao").alias(
            "categorias_de_complementares_por_indicacao"
        )
    )
    .withColumn("atualizado_em", F.lit(agora_em_sao_paulo()))
    .cache()
)

print(
    f"Total de produtos com categorias complementares: {categorias_de_complementares_por_indicacao.count()}"
)

# COMMAND ----------

categorias_de_complementares_por_indicacao.write.format("delta").mode(
    "overwrite"
).saveAsTable(Table.indicacoes_categorias.value)

print(f"✅ Tabela {Table.indicacoes_categorias.value} atualizada com sucesso.")

# COMMAND ----------

if DEBUG:
    print(f"🔍 Amostra da tabela de saída ({Table.indicacoes_categorias.value}):")
    env.table(Table.indicacoes_categorias).limit(5).display()
