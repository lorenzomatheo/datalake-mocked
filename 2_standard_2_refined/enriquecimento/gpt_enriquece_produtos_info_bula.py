# Databricks notebook source
# MAGIC %pip install -- langchain langchain-openai langchain-core langchain-community langchainhub tiktoken html2text langchain-google-genai duckduckgo-search tavily-python agno pycountry google-genai langchain-google-vertexai google-auth requests langsmith
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

from datetime import datetime
from typing import Any

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
from maggulake.llm.vertex import create_vertex_context_cache, setup_langsmith
from maggulake.pipelines.filter_notify import (
    filtra_notifica_produtos_enriquecimento,
)
from maggulake.pipelines.schemas.enriquece_produtos_info_bula import (
    EnriquecimentoInfoBula,
    campos_obrigatorios_enriquece_bula,
    enriquece_info_bula_schema,
)
from maggulake.prompts import (
    MAGGU_SYSTEM_INSTRUCTION,
    MAGGU_SYSTEM_PROMPT,
    MAGGU_USER_TEMPLATE,
)
from maggulake.utils.iters import create_batches
from maggulake.utils.strings import truncate_text_to_tokens
from maggulake.utils.time import agora_em_sao_paulo_str as agora

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "gpt_enriquece_produtos_info_bula",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "llm_provider": ["medgemma", "gemini_vertex", "openai"],
        "max_products": "5000",
        "ignorar_filtros_de_execucao": "false",
    },
)

# COMMAND ----------

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

# Configurações
DEBUG = dbutils.widgets.get("debug") == "true"
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
IGNORAR_FILTROS = dbutils.widgets.get("ignorar_filtros_de_execucao") == "true"
spark = env.spark

# accumulator initialization
countAccumulator = spark.sparkContext.accumulator(0)

# COMMAND ----------

# LLM Configuration
LLM_PROVIDER = dbutils.widgets.get("llm_provider")

llm_config = get_llm_config(
    provider=LLM_PROVIDER,
    spark=spark,
    dbutils=dbutils,
    size="SMALL",
)

EMBEDDING_ENCODING = EmbeddingEncodingModels.CL100K_BASE.value
MAX_TOKENS = get_max_tokens_by_embedding_model(EMBEDDING_ENCODING)
BATCH_SIZE = 200

# COMMAND ----------

# Notificacoes via discord
THRESHOLD_EXECUCAO = 0.3
DAYS_BACK = 15


# COMMAND ----------

# Leitura dos produtos já enriquecidos
# TODO: rodar migração para remover a coluna "categorias" dessa tabela.
# Não fiz agora para não apagar o que já foi gerado lá no passado.
produtos_enriquecidos = env.table(Table.extract_product_info).cache()
produtos_enriquecidos_completo = produtos_enriquecidos


# COMMAND ----------

if DEBUG:
    # So pra visualizar quantos EANs a gente ja tem na tabela
    lista_eans_enriquecidos = [
        row.ean for row in produtos_enriquecidos.select("ean").distinct().collect()
    ]
    print(f"{len(lista_eans_enriquecidos)} registros ja enriquecidos anteriormente.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos produtos ainda nao refinados

# COMMAND ----------

# Lê produtos da camada standard
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

    medicamentos = produtos.filter(
        F.coalesce(F.col("eh_medicamento_consolidado"), F.col("eh_medicamento"))
    )
    total_medicamentos = medicamentos.count()
    medicamentos_sem_bula = medicamentos.filter(F.col("bula").isNull()).count()

    print("\n📋 Análise de Bulas:")
    print(f"   - Total de medicamentos: {total_medicamentos}")
    print(f"   - Medicamentos SEM bula: {medicamentos_sem_bula}")
    print(f"   - Medicamentos COM bula: {total_medicamentos - medicamentos_sem_bula}")
    if total_medicamentos > 0:
        porcentagem_sem_bula = (medicamentos_sem_bula / total_medicamentos) * 100
        print(f"   - % sem bula: {porcentagem_sem_bula:.2f}%")

# COMMAND ----------

# Filtros:

# remove quando "nome" e "bula" sao nulo, pois sao as colunas mais importantes
produtos = produtos.dropna(subset=["nome"])
produtos = produtos.dropna(subset=["bula"])

# O minimo de caracteres para considerar um campo como preenchido.
# Esse valor eh arbitrario, pode ser baixo, mas nao deve ser zero.
minimo_caracteres = 10

# remove quando ambos descricao e indicacao são nulos
produtos = produtos.filter(
    ~(
        (F.length(F.col("descricao")) < minimo_caracteres)
        & (F.length(F.col("indicacao")) < minimo_caracteres)
    )
)

# Esse notebook vale só pra medicamentos
# Prioriza eh_medicamento_consolidado, se não existir usa eh_medicamento original
produtos = produtos.filter(
    F.coalesce(F.col("eh_medicamento_consolidado"), F.col("eh_medicamento")) == True
)

# COMMAND ----------

if DEBUG:
    print("Quantidade de produtos na camada de entrada: ", produtos.count())
    produtos.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seleciona os produtos que vamos (re)enriquecer

# COMMAND ----------

# Produtos que não possuem colunas cruciais preenchidas serão marcados para enriquecimento
produtos_enriquecidos = produtos_enriquecidos.dropna(
    subset=campos_obrigatorios_enriquece_bula
)

# COMMAND ----------

# NOTE: por conta do campo `categorias_de_complementares_por_indicacao`, esse notebook
# precisa vir depois do `enriquece_sintomas_e_categorias_complementares`
df_filtra_refazer = (
    produtos.alias("p")
    .join(produtos_enriquecidos.alias("pr"), on=["ean"], how="left_anti")
    .select(
        "ean",
        "nome",
        "tipo_de_receita_completo",
        "categorias_de_complementares_por_indicacao",
        "bula",
        "indicacao",
        "descricao",
        "principio_ativo",
        "dosagem",
        "tipo_medicamento",
        "categorias",
        "classes_terapeuticas",
        "especialidades",
        "informacoes_para_embeddings",
        "gerado_em",
    )
    .dropDuplicates(["ean"])
)

# COMMAND ----------

if DEBUG:
    print(
        "Quantidade de produtos que estao na camada de entrada e precisam ser enriquecidos: ",
        df_filtra_refazer.count(),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando chain de enriquecimento

# COMMAND ----------

if llm_config.provider == "gemini_vertex":
    _cache_name = create_vertex_context_cache(
        model=llm_config.model,
        system_instruction=MAGGU_SYSTEM_INSTRUCTION,
        project_id=llm_config.project_id,
        location=llm_config.location,
        ttl=14400,
    )
    _prompt_template = MAGGU_USER_TEMPLATE
    _system_instruction = MAGGU_SYSTEM_INSTRUCTION
else:
    _cache_name = None
    _prompt_template = MAGGU_SYSTEM_PROMPT
    _system_instruction = None

llm_processor = TaggingProcessor(
    provider=llm_config.provider,
    model=llm_config.model,
    prompt_template=_prompt_template,
    output_schema=EnriquecimentoInfoBula,
    api_key=llm_config.api_key,
    project_id=llm_config.project_id,
    location=llm_config.location,
    cached_content=_cache_name,
    system_instruction=_system_instruction,
)

# COMMAND ----------


def formata_valor_coluna(value: Any) -> str:
    # Recebe o valor de uma coluna e formata para string, lidando com os diferentes tipos de dados

    if value is None:
        return "N/A"

    if isinstance(value, list):
        new_value = [value for value in value if value is not None]

        if not new_value:
            return "N/A"

        # NOTE: coloquei esse limite para evitar textos muito longos
        return ", ".join(new_value[:5])

    return value


# COMMAND ----------


def formata_info_produto(produto: Row) -> str:
    bula = produto.bula or ""

    return truncate_text_to_tokens(
        "\n\n".join(
            [
                f"EAN: {formata_valor_coluna(produto.ean)}",
                f"Nome: {formata_valor_coluna(produto.nome)}",
                f"Princípio ativo: {formata_valor_coluna(produto.principio_ativo)}",
                f"Dosagem: {formata_valor_coluna(produto.dosagem)}",
                f"Tipo de receita: {formata_valor_coluna(produto.tipo_de_receita_completo)}",
                f"Indicação: {formata_valor_coluna(produto.indicacao)}",
                f"Descrição: {formata_valor_coluna(produto.descricao)}",
                f"Tipo de medicamento: {formata_valor_coluna(produto.tipo_medicamento)}",
                f"Bula:\n{bula}",
            ]
        ),
        MAX_TOKENS,
        EMBEDDING_ENCODING,
    )


# COMMAND ----------

# TODO: criar outro notebook que vai rodar o `buscar_idade_sexo_web`

# COMMAND ----------

if DEBUG:
    prompt_teste = formata_info_produto(
        produtos.filter(F.col("ean") == '7896014667000').head()
    )
    print(llm_processor.executa_tagging_chain(prompt_teste))

# COMMAND ----------

produtos_pendentes = filtra_notifica_produtos_enriquecimento(
    original_df=produtos,
    df_to_enrich=df_filtra_refazer,
    threshold=THRESHOLD_EXECUCAO,
    days_back=DAYS_BACK,
    script_name="gpt_enriquece_produtos_info_bula",
    ignorar_filtros_de_execucao=IGNORAR_FILTROS,
    enriched_df=produtos_enriquecidos_completo,
    required_fields=campos_obrigatorios_enriquece_bula,
)

# COMMAND ----------

total_original = len(produtos_pendentes)
if len(produtos_pendentes) > MAX_PRODUCTS:
    produtos_pendentes = produtos_pendentes[:MAX_PRODUCTS]
    print(
        f"⚠️  Limitando processamento devido a max_products: {total_original} → {MAX_PRODUCTS} produtos"
    )

print(f"Total de produtos a processar: {len(produtos_pendentes)}")

# COMMAND ----------

if not len(produtos_pendentes) > 0:
    dbutils.notebook.exit("Sem produtos para atuar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executando enriquecimento e salvando produtos na tabela delta

# COMMAND ----------


async def extrai_info_e_salva(lista_produtos: list[T.Row]) -> None:
    global countAccumulator

    for lote in create_batches(lista_produtos, BATCH_SIZE):
        resultados = await extrair_info(lote)
        salva_info_produto(resultados)

        print(
            agora()
            + " - Quantidade de produtos pendentes: "
            + f"{len(lista_produtos) - countAccumulator.value} de {len(lista_produtos)}"
        )


# COMMAND ----------


async def extrair_info(lista_produtos: list[T.Row]) -> list[dict]:
    prompts = [formata_info_produto(produto) for produto in lista_produtos]

    if not prompts:
        return []

    resultados_modelo = await llm_processor.executa_tagging_chain_async_batch(prompts)

    if not isinstance(resultados_modelo, list):
        return []

    return [
        resultado if isinstance(resultado, dict) else {}
        for resultado in resultados_modelo
    ]


# COMMAND ----------

SCHEMA_FIELDS = [field.name for field in enriquece_info_bula_schema.fields]

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
    # NOTE: Removi o try/except. Se der erro, deixa o notebook quebrar e a pessoa vir resolver logo.
    # Nao faz sentido continuar o enriquecimento se ele nao consegue ser salvo na tabela...

    global countAccumulator

    validated_rows = validate_results(info)

    if len(validated_rows) == 0:
        print("Nenhum resultado validado para salvar.")
        return  # early exit

    # Convert validated rows to a DataFrame
    df_results = spark.createDataFrame(validated_rows, enriquece_info_bula_schema)

    df_results = df_results.dropDuplicates(["ean"])

    # NOTE: existe um custo por ler e reler essa tabela cada vez que passa no loop.
    # No futuro devemos investigar formas mais eficientes de fazer isso.
    # Penso que idealmente a gente deveria gerar todos os enriquecimentos e manter como variavel no python
    # De tempos em tempos salvamos no disco para aliviar a memoria. E colocamos um `finally`
    # para garantir que salvamos o que foi processado ate o momento.
    delta_table = DeltaTable.forName(spark, Table.extract_product_info.value)

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

# executando o enriquecimento
await extrai_info_e_salva(produtos_pendentes)


# COMMAND ----------

# Conferir produtos enriquecidos

if DEBUG:
    produtos_recem_enriquecidos = spark.read.table(
        Table.extract_product_info.value
    ).filter(F.trim(F.col("ean")).isin([p.ean.strip() for p in produtos_pendentes]))

    produtos_recem_enriquecidos.orderBy(F.col("atualizado_em").desc()).limit(
        1000
    ).display()
