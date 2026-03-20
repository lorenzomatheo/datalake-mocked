# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Judge - Avaliação de Qualidade de Enriquecimento de Produtos
# MAGIC
# MAGIC Este notebook utiliza um LLM como juiz para avaliar a qualidade do enriquecimento de dados dos produtos
# MAGIC
# MAGIC O processo:
# MAGIC 1. Coleta produtos em campanha e concorrentes, bem como produtos mais vendidos
# MAGIC 2. Avalia a qualidade do enriquecimento usando Gemini
# MAGIC 3. Salva sugestões de correções na tabela de avaliação

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install langchain_google_genai langchain_core langchain_google_vertexai openai pydantic html2text agno google-genai vertexai langsmith -U
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import asyncio
import logging
from pprint import pprint

import pyspark.sql.functions as F
from agno.agent import Agent
from agno.models.google import Gemini
from agno.utils.log import configure_agno_logging
from delta.tables import DeltaTable
from langchain_core.prompts.prompt import PromptTemplate
from pyspark.sql import Row

from maggulake.enums import (
    FaixaEtaria,
    FormaFarmaceutica,
    SexoRecomendado,
    TiposReceita,
    ViaAdministracao,
)
from maggulake.environment import CopilotTable, DatabricksEnvironmentBuilder, Table
from maggulake.llm.agno_tracer import TracedAgent
from maggulake.llm.models import get_model_name
from maggulake.llm.vertex import (
    create_vertex_context_cache,
    setup_langsmith,
    setup_vertex_ai_credentials,
)
from maggulake.pipelines.schemas import (
    QualidadeEnriquecimento,
    TriagemRapida,
    qualidade_enriquecimento_schema,
)
from maggulake.prompts import (
    LLM_JUDGE_SYSTEM_INSTRUCTION_TEMPLATE,
    LLM_JUDGE_USER_TEMPLATE,
)
from maggulake.utils.iters import create_batches
from maggulake.utils.time import agora_em_sao_paulo, agora_em_sao_paulo_str

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "gpt_llm_judge",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "refazer_eans_qualidade_ruim": ["false", "true", "false"],
        "max_products": "500",
        "quality_threshold": "0.4",
    },
)

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

# para não ficar aparecendo logs do tipo INFO
logger = logging.getLogger("agno_quiet")
logger.setLevel(logging.WARNING)
logger.propagate = False

configure_agno_logging(
    custom_default_logger=logger,
    custom_agent_logger=logger,
    custom_team_logger=logger,
    custom_workflow_logger=logger,
)

spark = env.spark
DEBUG = dbutils.widgets.get("debug") == "true"
refazer_eans_qualidade_ruim = (
    dbutils.widgets.get("refazer_eans_qualidade_ruim") == "true"
)
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
QUALITY_THRESHOLD = float(dbutils.widgets.get("quality_threshold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configurações do LLM e Parâmetros

# COMMAND ----------

# Accumulator para contagem
countAccumulator = spark.sparkContext.accumulator(0)

# LLM setup
GEMINI_MODEL = get_model_name(provider="gemini", size="SMALL")
MAX_TOKENS = 12000

# Batch processing
BATCH_SIZE = 20  # Otimizado para rate limits

# Vertex AI credentials
PROJECT_ID, LOCATION = setup_vertex_ai_credentials(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definição dos Enums a serem seguidos

# COMMAND ----------

# Enums para validação
ENUM_CONSTRAINTS = {
    "forma_farmaceutica": FormaFarmaceutica.list(),
    "idade_recomendada": FaixaEtaria.list(),
    "sexo_recomendado": SexoRecomendado.list(),
    "tipo_receita": TiposReceita.list(),
    "via_administracao": ViaAdministracao.list(),
}

SYSTEM_INSTRUCTION = LLM_JUDGE_SYSTEM_INSTRUCTION_TEMPLATE.format(
    forma_farmaceutica_values=ENUM_CONSTRAINTS["forma_farmaceutica"],
    idade_recomendada_values=ENUM_CONSTRAINTS["idade_recomendada"],
    sexo_recomendado_values=ENUM_CONSTRAINTS["sexo_recomendado"],
    tipo_receita_values=ENUM_CONSTRAINTS["tipo_receita"],
    via_administracao_values=ENUM_CONSTRAINTS["via_administracao"],
)

CONTEXT_CACHE_NAME = create_vertex_context_cache(
    model=GEMINI_MODEL,
    system_instruction=SYSTEM_INSTRUCTION,
    project_id=PROJECT_ID,
    location=LOCATION,
    ttl=7200,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Dados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query para produtos em campanha

# COMMAND ----------

postgres = env.postgres_replica_adapter

# COMMAND ----------

df_eans_campanha = postgres.get_produtos_em_campanha(spark).select("ean").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query para produtos concorrentes

# COMMAND ----------

# TODO: deveríamos executar essas queries sobre o BigQuery, mais rápido.
# TODO: a gente ainda atualiza essa view `production.standard.produtos_campanha_substitutos`?

# Query para obter produtos concorrentes (retorna DataFrame, não lista)
df_concorrentes = spark.sql(
    """
    WITH competitor_eans AS (
        SELECT
            p.ean_em_campanha,
            t.ean_data.ean as ean_concorrente,
            t.ean_data.score as ean_concorrente_score,
            t.ean_data.frase as ean_concorrente_frase
        FROM production.standard.produtos_campanha_substitutos p
        LATERAL VIEW EXPLODE(p.eans_recomendar) t AS ean_data
        WHERE t.ean_data.score > 0.9  -- Mínimo pra ser considerado relevante nesse notebook
    )
    SELECT DISTINCT
        ean_concorrente as ean
    FROM competitor_eans
    INNER JOIN production.refined.produtos_refined pr ON pr.ean = ean_concorrente
    INNER JOIN production.refined.produtos_refined pr2 ON pr2.ean = ean_em_campanha
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query para Top 1000 produtos mais vendidos

# COMMAND ----------

# Query para obter os top 1000 produtos mais vendidos usando BigQuery (retorna DataFrame, não lista)
df_vendas_item = env.table(CopilotTable.vendas_item)
df_vendas_venda = env.table(CopilotTable.vendas)

df_top_vendidos = (
    df_vendas_item.alias('vi')
    .join(
        df_vendas_venda.alias('v'),
        on=F.col('vi.venda_id') == F.col('v.id'),
        how='inner',
    )
    .filter(F.col('vi.ean').isNotNull())
    .groupBy('vi.ean')
    .agg(F.sum('vi.quantidade').alias('total_vendido'))
    .orderBy(F.desc('total_vendido'))
    .limit(1000)
    .select(F.col('ean'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### União de todos os EANs para avaliar

# COMMAND ----------

df_eans_total = (
    df_eans_campanha.union(df_concorrentes).union(df_top_vendidos).distinct().cache()
)


# COMMAND ----------

count_campanha = df_eans_campanha.count()
count_concorrente = df_concorrentes.count()
count_top_vendidos = df_top_vendidos.count()
count_total = df_eans_total.count()

# NOTE: é muito importante que esse total seja mantido fora de um debug,
# pois apesar de lenta essa célula nos traz informação importante.
print(f"Total de EANs em campanha: {count_campanha}")
print(f"Total de EANs concorrentes: {count_concorrente}")
print(f"Total de EANs top vendidos: {count_top_vendidos}")
print(f"Total de EANs para avaliar (união): {count_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos Dados para Avaliação

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura produtos_refined

# COMMAND ----------

produtos_refined = env.table(Table.produtos_refined).cache()


# COMMAND ----------

# Seleciona apenas os campos necessários
df_info_referencia = produtos_refined.select(
    'ean',
    'bula',
    'descricao',
    'nome',
    'categorias_de_complementares_por_indicacao',
    'indicacao',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtro produtos relevantes

# COMMAND ----------


produtos_refined_filtrado = produtos_refined.join(df_eans_total, on="ean", how="inner")
df_info_referencia_filtrado = df_info_referencia.join(
    df_eans_total, on="ean", how="inner"
)

# COMMAND ----------

df_products = (
    produtos_refined_filtrado.join(df_info_referencia_filtrado, how='inner', on=['ean'])
).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carrega tabela de avaliações já realizadas

# COMMAND ----------

tabela_llm_as_judge = env.table(Table.gpt_judge_evaluator_product)

# Remover os registros inválidos, assim eles são refeitos
tabela_llm_as_judge = tabela_llm_as_judge.filter(F.col("is_valid") == True)

# COMMAND ----------

# DataFrames com EANs já salvos
df_eans_salvos = tabela_llm_as_judge.select('ean').distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define EANs para refazer baseado na qualidade

# COMMAND ----------


if refazer_eans_qualidade_ruim:
    df_eans_qualidade_ruim = (
        tabela_llm_as_judge.filter(F.col('overall_score') <= QUALITY_THRESHOLD)
        .select('ean')
        .distinct()
    )

    # Conta para logging
    count_qualidade_ruim = df_eans_qualidade_ruim.count()
    print(
        f"EANs com qualidade ruim (score <= {QUALITY_THRESHOLD}): {count_qualidade_ruim}"
    )
else:
    df_eans_qualidade_ruim = spark.createDataFrame([], "ean string").select('ean')
    count_qualidade_ruim = 0

# COMMAND ----------

# Define escopo final usando left_anti join
# EANs novos = EANs totais que NÃO estão salvos
df_eans_novos = df_eans_total.join(df_eans_salvos, on='ean', how='left_anti')

# União de EANs novos + EANs para refazer
df_escopo_refazer = df_eans_novos.union(df_eans_qualidade_ruim).distinct().cache()

# Contagens para logging
count_eans_novos = df_eans_novos.count()
count_escopo_total = df_escopo_refazer.count()

print(f"EANs novos (não avaliados): {count_eans_novos}")
print(f"Total de EANs no escopo de execução: {count_escopo_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação: encerra se não há produtos para processar

# COMMAND ----------


if count_escopo_total == 0:
    dbutils.notebook.exit("Sem produtos para atualizar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup do Modelo de Avaliação

# COMMAND ----------

# MAGIC %md
# MAGIC ### Template do Prompt de Avaliação

# COMMAND ----------

TRIAGE_PROMPT_TEMPLATE = PromptTemplate(
    input_variables=["product_info"],
    template="INFORMAÇÕES DO PRODUTO PARA TRIAGEM:\n{product_info}",
)

EVALUATION_PROMPT_TEMPLATE = PromptTemplate(
    input_variables=["reference_info", "product_info"],
    template=LLM_JUDGE_USER_TEMPLATE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funções Auxiliares de Formatação

# COMMAND ----------


def is_valid(value):
    """Verifica se um valor é válido (não None, não vazio, não lista/array vazio)."""
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    if isinstance(value, (list, tuple)) and len(value) == 0:
        return False
    return True


def formata_info_produto(produto):
    """Formata informações do produto para o prompt de avaliação."""

    fields = [
        ("ean", produto.ean),
        ("nome", produto.nome),
        ("marca", produto.marca),
        ("fabricante", produto.fabricante),
        ("principio_ativo", produto.principio_ativo),
        ("descricao", produto.descricao),
        ("eh_medicamento", produto.eh_medicamento),
        ("tipo_medicamento", produto.tipo_medicamento),
        ("dosagem", produto.dosagem),
        ("variantes", produto.variantes),
        ("eh_tarjado", produto.eh_tarjado),
        ("categorias", produto.categorias),
        ("tipo_de_receita_completo", produto.tipo_de_receita_completo),
        ("classes_terapeuticas", produto.classes_terapeuticas),
        ("especialidades", produto.especialidades),
        ("nome_comercial", produto.nome_comercial),
        ("tipo_receita", produto.tipo_receita),
        ("indicacao", produto.indicacao),
        ("eh_otc", produto.eh_otc),
        ("contraindicacoes", produto.contraindicacoes),
        ("efeitos_colaterais", produto.efeitos_colaterais),
        ("instrucoes_de_uso", produto.instrucoes_de_uso),
        ("advertencias_e_precaucoes", produto.advertencias_e_precaucoes),
        ("interacoes_medicamentosas", produto.interacoes_medicamentosas),
        ("condicoes_de_armazenamento", produto.condicoes_de_armazenamento),
        ("validade_apos_abertura", produto.validade_apos_abertura),
        ("eh_controlado", produto.eh_controlado),
        ("forma_farmaceutica", produto.forma_farmaceutica),
        ("via_administracao", produto.via_administracao),
        ("volume_quantidade", produto.volume_quantidade),
        ("idade_recomendada", produto.idade_recomendada),
        ("sexo_recomendado", produto.sexo_recomendado),
        ("power_phrase", produto.power_phrase),
        ("tags_complementares", produto.tags_complementares),
        ("tags_potencializam_uso", produto.tags_potencializam_uso),
        ("tags_atenuam_efeitos", produto.tags_atenuam_efeitos),
        ("tags_substitutos", produto.tags_substitutos),
        ("tags_agregadas", produto.tags_agregadas),
    ]

    # Filtra apenas campos válidos
    valid_fields = [f"'{field}': {value}" for field, value in fields if is_valid(value)]

    return "\n\n".join(valid_fields)


def formata_info_ref(produto):
    """Formata informações de referência do produto."""
    fields = [
        ("ean", produto.ean),
        ("bula", produto.bula),
        ("indicacao", produto.indicacao),
        ("descricao", produto.descricao),
        ("nome", produto.nome),
        (
            "categorias_de_complementares_por_indicacao",
            produto.categorias_de_complementares_por_indicacao,
        ),
    ]

    # Filtra apenas campos válidos
    valid_fields = [f"'{field}': {value}" for field, value in fields if is_valid(value)]

    return "\n\n".join(valid_fields)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Definição do agente

# COMMAND ----------

llm_model = Gemini(
    id=GEMINI_MODEL,
    vertexai=True,
    project_id=PROJECT_ID,
    location=LOCATION,
    temperature=0.1,
    cached_content=CONTEXT_CACHE_NAME,
)
pprint(llm_model)


# COMMAND ----------


triage_evaluator = TracedAgent(
    Agent(
        model=llm_model,
        output_schema=TriagemRapida,
        markdown=True,
        reasoning=False,
        debug_mode=False,
        telemetry=False,
        debug_level=1,
    ),
)

evaluator = TracedAgent(
    Agent(
        model=llm_model,
        output_schema=QualidadeEnriquecimento,
        instructions=None if CONTEXT_CACHE_NAME else SYSTEM_INSTRUCTION,
        markdown=True,
        reasoning=False,
        debug_mode=False,
        telemetry=False,
        debug_level=1,
    ),
)

pprint(evaluator)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Teste do Prompt

# COMMAND ----------

# Seleciona um produto para teste
produto_teste = df_products.head()

if produto_teste is not None:
    prompt = formata_info_produto(produto_teste)
    ref_prompt = formata_info_ref(produto_teste)

    final_prompt = EVALUATION_PROMPT_TEMPLATE.format(
        reference_info=ref_prompt,
        product_info=prompt,
    )

    evaluator.print_response(
        final_prompt,
        show_message=False,
        show_reasoning=False,
    )
else:
    print("Nenhum produto encontrado para teste")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta de Produtos para Processamento

# COMMAND ----------

df_produtos_processar = df_products.join(df_escopo_refazer, on='ean', how='inner')

# COMMAND ----------

if MAX_PRODUCTS > 0:
    df_produtos_processar = df_produtos_processar.limit(MAX_PRODUCTS)
    print(f"Aplicando limite de {MAX_PRODUCTS} produtos")

# COMMAND ----------

produtos = df_produtos_processar.collect()

print(f"Quantidade de produtos a serem avaliados: {len(produtos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema de Resultados

# COMMAND ----------

SCHEMA_FIELDS = [field.name for field in qualidade_enriquecimento_schema.fields]
SCHEMA_FIELDS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define funções de processamento

# COMMAND ----------


def salva_info_produto(info):
    """Salva os resultados da avaliação na tabela Delta."""
    global countAccumulator

    validated_rows = []

    for res in info:
        complete_res = {field: res.get(field, None) for field in SCHEMA_FIELDS}
        complete_res["data_execucao"] = agora_em_sao_paulo()
        validated_rows.append(Row(**complete_res))

    df_results = spark.createDataFrame(validated_rows, qualidade_enriquecimento_schema)

    delta_table = DeltaTable.forName(spark, Table.gpt_judge_evaluator_product.value)
    delta_table.alias("target").merge(
        source=df_results.alias("source"),
        condition="target.ean = source.ean",
    ).whenMatchedUpdate(
        set={col: f"source.{col}" for col in SCHEMA_FIELDS}
    ).whenNotMatchedInsert(
        values={col: f"source.{col}" for col in SCHEMA_FIELDS}
    ).execute()

    countAccumulator.add(len(validated_rows))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Passada 1: Triagem rápida (sem bula, schema leve)

# COMMAND ----------


async def extrair_triagem(produtos: list[Row]) -> tuple[list[dict], list[str]]:
    """Triagem rápida: avalia produtos sem bula, retornando score e campos com problema.

    Returns:
        Tupla (resultados_parseados, eans_com_falha).
        EANs com falha devem ser encaminhados para avaliação profunda.
    """

    tasks = []
    for produto in produtos:
        prompt = formata_info_produto(produto)

        final_prompt = TRIAGE_PROMPT_TEMPLATE.format(
            product_info=prompt,
        )

        tasks.append(triage_evaluator.arun(final_prompt))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_results = []
    eans_com_falha = []
    total = len(results)
    for i, result in enumerate(results):
        ts = agora_em_sao_paulo_str()
        idx_str = f"{i + 1}/{total}"
        if isinstance(result, Exception):
            logger.warning(
                "%s - [TRIAGEM ERROR] Ignorando Task %s: %s", ts, idx_str, result
            )
            eans_com_falha.append(produtos[i].ean)
            continue

        if not hasattr(result, "content"):
            logger.warning(
                "%s - [TRIAGEM CONTENT WARNING] Unexpected em %s: %s",
                ts,
                idx_str,
                result,
            )
            eans_com_falha.append(produtos[i].ean)
            continue

        content = result.content

        if not isinstance(content, TriagemRapida):
            logger.warning(
                "%s - [TRIAGEM TYPE WARNING] Unexpected type em %s: %s",
                ts,
                idx_str,
                type(content),
            )
            eans_com_falha.append(produtos[i].ean)
            continue

        successful_results.append(content.model_dump())

    return successful_results, eans_com_falha


async def executa_triagem(produtos: list[Row]) -> tuple[list[str], list[dict]]:
    """Executa triagem em batches. Retorna (eans_reprovados, resultados_aprovados)."""

    eans_reprovados: set[str] = set()
    resultados_aprovados: list[dict] = []
    total = len(produtos)

    for idx, batch in enumerate(create_batches(produtos, BATCH_SIZE), 1):
        print(f"[TRIAGEM] Batch {idx}/{(total // BATCH_SIZE) + 1}")
        triagens, eans_com_falha = await extrair_triagem(batch)

        # Falhas na triagem vão direto para avaliação profunda
        eans_reprovados.update(eans_com_falha)

        for t in triagens:
            if t["overall_score"] <= QUALITY_THRESHOLD or not t["is_valid"]:
                eans_reprovados.add(t["ean"])
            else:
                resultados_aprovados.append(t)

    print(
        f"[TRIAGEM] Concluída: {len(resultados_aprovados)} aprovados, "
        f"{len(eans_reprovados)} reprovados (vão para avaliação profunda)"
    )
    return list(eans_reprovados), resultados_aprovados


def salva_triagem_aprovados(resultados_aprovados: list[dict]) -> None:
    """Salva produtos aprovados na triagem com registro simplificado."""
    if not resultados_aprovados:
        return

    rows = []
    for t in resultados_aprovados:
        row = {field: None for field in SCHEMA_FIELDS}
        row["ean"] = t["ean"]
        row["overall_score"] = t["overall_score"]
        row["is_valid"] = t["is_valid"]
        row["field_evaluations"] = []
        row["data_execucao"] = agora_em_sao_paulo()
        rows.append(Row(**row))

    df_results = spark.createDataFrame(rows, qualidade_enriquecimento_schema)

    delta_table = DeltaTable.forName(spark, Table.gpt_judge_evaluator_product.value)
    delta_table.alias("target").merge(
        source=df_results.alias("source"),
        condition="target.ean = source.ean",
    ).whenMatchedUpdate(
        set={col: f"source.{col}" for col in SCHEMA_FIELDS}
    ).whenNotMatchedInsert(
        values={col: f"source.{col}" for col in SCHEMA_FIELDS}
    ).execute()

    print(f"[TRIAGEM] {len(rows)} produtos aprovados salvos na tabela")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Passada 2: Avaliação profunda (com bula, schema completo)

# COMMAND ----------


async def extrair_info(produtos: list[Row]) -> list[dict]:
    """Extrai informações de qualidade usando o LLM de forma assíncrona."""

    tasks = []
    for produto in produtos:
        prompt = formata_info_produto(produto)
        ref_prompt = formata_info_ref(produto)

        final_prompt = EVALUATION_PROMPT_TEMPLATE.format(
            reference_info=ref_prompt,
            product_info=prompt,
        )

        tasks.append(evaluator.arun(final_prompt))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_results = []
    total = len(results)
    for i, result in enumerate(results):
        ts = agora_em_sao_paulo_str()
        idx_str = f"{i + 1}/{total}"
        if isinstance(result, Exception):
            logger.warning(
                "%s - [DEEP ERROR] Ignorando a Task %s: %s", ts, idx_str, result
            )
            continue

        if not hasattr(result, "content"):
            logger.warning(
                "%s - [DEEP CONTENT WARNING] Unexpected content em %s: %s",
                ts,
                idx_str,
                result,
            )
            continue

        content = result.content

        if not isinstance(content, QualidadeEnriquecimento):
            logger.warning(
                "%s - [DEEP TYPE WARNING] Unexpected type em %s: %s. Content: %s",
                ts,
                idx_str,
                type(content),
                content,
            )
            continue

        print(f"{ts} - [DEEP] Produto {idx_str} processado com sucesso.")
        successful_results.append(content.model_dump())

    return successful_results


# COMMAND ----------


async def extrai_info_e_salva(ps: list[Row]) -> None:
    """Processa produtos reprovados em batches e salva os resultados."""
    global countAccumulator

    total = len(ps)
    for idx, b in enumerate(create_batches(ps, BATCH_SIZE), 1):
        print(f"[DEEP] Batch {idx}/{(total // BATCH_SIZE) + 1}")
        rs = await extrair_info(b)
        salva_info_produto(rs)

        feitos = countAccumulator.value
        pendentes = total - feitos
        pct = (feitos / total) * 100 if total > 0 else 0
        print(
            f"{agora_em_sao_paulo_str()} - [DEEP] Batch {idx}: {feitos}/{total} "
            f"produtos processados ({pct:.1f}%). Pendentes: {pendentes}"
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do Processamento — Passada 1: Triagem

# COMMAND ----------

eans_reprovados, resultados_aprovados = await executa_triagem(produtos)

# COMMAND ----------

# Salva produtos aprovados na triagem (sem necessidade de avaliação profunda)
salva_triagem_aprovados(resultados_aprovados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do Processamento — Passada 2: Avaliação Profunda

# COMMAND ----------

# Filtra apenas os produtos reprovados na triagem para avaliação profunda
eans_reprovados_set = set(eans_reprovados)
produtos_para_deep = [p for p in produtos if p.ean in eans_reprovados_set]

print(
    f"Passada 2: {len(produtos_para_deep)} produtos para avaliação profunda "
    f"(de {len(produtos)} total)"
)

# COMMAND ----------

if produtos_para_deep:
    await extrai_info_e_salva(produtos_para_deep)
else:
    print(
        "Todos os produtos passaram na triagem. Nenhuma avaliação profunda necessária."
    )

# COMMAND ----------

if DEBUG:
    env.table(Table.gpt_judge_evaluator_product).sort(
        F.col("data_execucao").desc()
    ).limit(100).display()
