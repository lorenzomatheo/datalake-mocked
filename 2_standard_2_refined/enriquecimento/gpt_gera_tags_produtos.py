# Databricks notebook source
# MAGIC %md
# MAGIC # Gera Tags pros produtos

# COMMAND ----------

# MAGIC %pip install langchain langchain-openai langchain-core langchain-community langchainhub tiktoken html2text langchain-google-genai agno pycountry google-genai langchain-google-vertexai langsmith
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

import json
from datetime import datetime, timedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable
from pyspark.sql import Row

from maggulake.enums import TermosControlados
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.llm.models import get_llm_config
from maggulake.llm.tagging_processor import TaggingProcessor
from maggulake.llm.vertex import (
    create_vertex_context_cache,
    setup_langsmith,
)
from maggulake.pipelines.filter_notify import (
    filtra_notifica_produtos_enriquecimento,
)
from maggulake.pipelines.schemas import EnriquecimentoTags
from maggulake.prompts import TAGS_SYSTEM_INSTRUCTION_TEMPLATE, TAGS_USER_TEMPLATE
from maggulake.schemas import schema_refined_gpt_gera_tags
from maggulake.utils.iters import create_batches
from maggulake.utils.parse_schema import parse_schema
from maggulake.utils.strings import truncate_text_to_tokens
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

# spark
env = DatabricksEnvironmentBuilder.build(
    "enriquecimento_tags",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "llm_provider": ["gemini_vertex", "gemini_vertex", "openai"],
        "refazer_tags": ["false", "false", "true"],
        "max_products": "10000",
        "timeout_batches": "1800",
        "batch_size": "500",
    },
)

# COMMAND ----------

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

# NOTE: Isso não é equivalente a um "full_refresh"!!!
REFAZER_TAGS_RUINS: bool = dbutils.widgets.get("refazer_tags") == "true"
TIMEOUT_BATCHES = int(dbutils.widgets.get("timeout_batches"))


# accumulator initialization
countAccumulator = env.spark.sparkContext.accumulator(0)

# COMMAND ----------

# Configurações
spark = env.spark
DEBUG = dbutils.widgets.get("debug") == "true"

# LLM
LLM_PROVIDER = dbutils.widgets.get("llm_provider")
llm_config = get_llm_config(LLM_PROVIDER, spark, dbutils, size="SMALL")

# notifica_produtos_enriquecimento
THRESHOLD_EXECUCAO = 0.8
DAYS_BACK = 15

# Configurações de execução
CUTOFF_DAYS = 365
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura produtos refined

# COMMAND ----------

produtos_refined_df = env.table(Table.produtos_refined).cache()

# NOTE: mantido fora do DEBUG para persistir o cache()
print(f"Total de produtos disponíveis na camada refined: {produtos_refined_df.count()}")

# COMMAND ----------

# Não quero refazer o enriquecimento para diferentes EANs...
produtos_refined_df = produtos_refined_df.dropDuplicates(["ean"])

print(
    f"Total de produtos distintos por EAN na camada refined: {produtos_refined_df.count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de tags já geradas

# COMMAND ----------


print("Lendo produtos com tags já geradas...")
produtos_tags_table = env.table(Table.enriquecimento_tags_produtos)

print(f"Total de produtos com tags já geradas: {produtos_tags_table.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando chain de enriquecimento

# COMMAND ----------

EMBEDDING_ENCODING = "cl100k_base"
MAX_TOKENS = 13000

termos_controlados = TermosControlados.list()
termos_controlados_str = ", ".join(termos_controlados)

SYSTEM_INSTRUCTION = TAGS_SYSTEM_INSTRUCTION_TEMPLATE.format(
    termos_controlados=termos_controlados_str
)

CONTEXT_CACHE_NAME = None
if llm_config.provider == "gemini_vertex":
    CONTEXT_CACHE_NAME = create_vertex_context_cache(
        model=llm_config.model,
        system_instruction=SYSTEM_INSTRUCTION,
        project_id=llm_config.project_id,
        location=llm_config.location,
        ttl=14400,
    )

gemini_caller = TaggingProcessor(
    provider=llm_config.provider,
    model=llm_config.model,
    api_key=llm_config.api_key,
    project_id=llm_config.project_id,
    location=llm_config.location,
    prompt_template=TAGS_USER_TEMPLATE,
    output_schema=EnriquecimentoTags,
    cached_content=CONTEXT_CACHE_NAME,
    system_instruction=SYSTEM_INSTRUCTION,
)

# COMMAND ----------


def formata_info_produto(produto):
    campos = [
        ###---infos para medicamentos---###
        ("Ean do produto", produto.ean),
        ("Principio ativo do produto", produto.principio_ativo),
        ("Forma farmaceutica", produto.forma_farmaceutica),
        ("Via de administração", produto.via_administracao),
        ("Idade recomendada", produto.idade_recomendada),
        ("Sexo recomendado", produto.sexo_recomendado),
        ("Categorias do produto", produto.categorias),
        ("Marca do produto", produto.marca),
        ("Fabricante do produto", produto.fabricante),
        ##---infos para não medicamentos---###
        ("Descrição", produto.descricao),
        ("Indicação", produto.indicacao),
        (
            "Categorias de produtos complementares para cada indicacao do produto",
            produto.categorias_de_complementares_por_indicacao,
        ),
        ("Contraindicações", produto.contraindicacoes),
        ("Efeitos colaterais", produto.efeitos_colaterais),
        ("Classes terapeuticas", produto.classes_terapeuticas),
    ]

    # Passa pro prompt somente caso o valor seja significativo, removendo None, "" e outros falsy cases
    info_list = [f"{label}: {valor}" for label, valor in campos if valor]

    return truncate_text_to_tokens(
        "\n\n".join(info_list),
        MAX_TOKENS,
        EMBEDDING_ENCODING,
    )


# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testando enriquecimento para um produto

# COMMAND ----------

produto_teste = produtos_refined_df.head()

if produto_teste is None:
    dbutils.notebook.exit("Sem produtos disponíveis para teste")

prompt = formata_info_produto(produto_teste)

print(
    "Prompt que será passado para o Chain de Enriquecimento:\n----------------------\n"
)
print(prompt)


# COMMAND ----------

resultado_teste = gemini_caller.executa_tagging_chain(prompt)

print("Resultado do enriquecimento de teste:")
print(json.dumps(resultado_teste, indent=4, ensure_ascii=False))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Garante que algumas colunas seguem regras de negócio específicas

# COMMAND ----------


def processa_eans_refazer_fora_compliance(
    refazer_tags: bool,
    termos_controlados: list[str],
) -> list[str]:
    """
    Identifica produtos que precisam ter suas tags refeitas por:
    1. Conterem nomes de medicamentos controlados nas tags
    2. Conterem termos problemáticos (ex: probiótico)
    """

    if not refazer_tags:
        print("Refazer tags desabilitado, pulando etapa de verificação.")
        return []  # early exit

    print("Iniciando análise de tags fora de compliance...")

    # Lê a tabela de tags já geradas
    tags_df = produtos_tags_table.select(
        "ean",
        "tags_complementares",
        "tags_substitutos",
        "tags_potencializam_uso",
        "tags_atenuam_efeitos",
        "tags_agregadas",
    )

    # ===== 1. CONCATENA TODAS AS TAGS EM LOWERCASE =====
    # Importante: Os termos controlados podem estar presentes nas tags substitutos,
    # só não nas tags que são usadas para buscar complementares.
    tags_com_todas_concatenadas = tags_df.withColumn(
        "todas_tags_lower",
        F.lower(
            F.concat_ws(
                " ",
                F.coalesce(F.col("tags_complementares"), F.lit("")),
                F.coalesce(F.col("tags_potencializam_uso"), F.lit("")),
                F.coalesce(F.col("tags_atenuam_efeitos"), F.lit("")),
                F.coalesce(F.col("tags_agregadas"), F.lit("")),
            )
        ),
    ).cache()  # Cache pois vamos usar múltiplas vezes

    # ===== 2. IDENTIFICA TAGS COM TERMOS CONTROLADOS =====
    termos_problematicos = ["probiotic", "probiótico"]
    lista_termos_verificar = termos_controlados + termos_problematicos

    print(f"   - Total de termos a verificar: {len(lista_termos_verificar)}")

    if not lista_termos_verificar:
        print("   ⚠️  Nenhum termo para verificar. Pulando análise.")
        tags_com_todas_concatenadas.unpersist()
        return []

    # Cria condição OR para verificar se QUALQUER termo aparece nas tags
    condicao_termos = None
    for termo in lista_termos_verificar:
        condicao_atual = F.col("todas_tags_lower").contains(termo.lower())
        condicao_termos = (
            condicao_atual
            if condicao_termos is None
            else (condicao_termos | condicao_atual)
        )

    # Aplica o filtro
    tags_fora_compliance = tags_com_todas_concatenadas.filter(condicao_termos)
    eans_fora_compliance = tags_fora_compliance.select("ean").distinct().cache()
    count_total = eans_fora_compliance.count()

    print(f"   - Tags fora de compliance encontradas: {count_total}")

    if DEBUG and count_total > 0:
        tags_fora_compliance.select(
            "ean",
            "tags_complementares",
            "tags_substitutos",
            "tags_potencializam_uso",
            "tags_atenuam_efeitos",
            "tags_agregadas",
            "todas_tags_lower",
        ).withColumn(
            "mensagem_log", F.lit("Tag contém termo controlado ou problemático")
        ).limit(200).display()

    # ===== 3. RESULTADO FINAL =====
    lista_eans_refazer_tags = [row.ean for row in eans_fora_compliance.collect()]

    # Cleanup
    tags_com_todas_concatenadas.unpersist()
    eans_fora_compliance.unpersist()

    print(f"\n✅ Total de produtos com tags a refazer: {len(lista_eans_refazer_tags)}")

    return lista_eans_refazer_tags


# COMMAND ----------

lista_eans_refazer_tags = processa_eans_refazer_fora_compliance(
    refazer_tags=REFAZER_TAGS_RUINS,
    termos_controlados=termos_controlados,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verifica produtos ainda nao foram enriquecidos completamente

# COMMAND ----------


def seleciona_produtos_para_enriquecer() -> list[str]:
    """
    Seleciona produtos que vão passar pelo processo de enriquecimento de tags:
    - Não possuem nenhuma informação de tags preenchidas
    - Nunca passaram pelo enriquecimento ou o enriquecimento é antigo (> CUTOFF_DAYS)
    """

    print(
        f"{agora_em_sao_paulo_str()} - [INÍCIO] Seleção de produtos para enriquecimento (Cutoff: {CUTOFF_DAYS} dias)"
    )

    # --- 1. Carrega todos os produtos ---
    total_base = produtos_refined_df.count()
    print(
        f"{agora_em_sao_paulo_str()} - [1/4] Total de produtos na tabela de produtos refined: {total_base}"
    )

    # --- 2. Carrega tags já geradas ---
    df_enriquecimento_tags_historico = produtos_tags_table.withColumnRenamed(
        'atualizado_em', 'tags_atualizado_em'
    ).select(
        "ean",
        "tags_atualizado_em",
    )

    # --- 3. Join dos produtos com tags ---
    df_com_historico = produtos_refined_df.join(
        df_enriquecimento_tags_historico, on=["ean"], how="left"
    )

    # Armazena o join em cache, usado para fazer varios counts
    df_com_historico.persist()

    # --- 4. Aplica condições de seleção ---

    # Condição 1: Nunca foi processado OU processamento é antigo
    cutoff_date = datetime.now() - timedelta(days=CUTOFF_DAYS)
    condicao_tempo = F.col("tags_atualizado_em").isNull() | (
        F.col("tags_atualizado_em") <= F.lit(cutoff_date)
    )

    # Condição 2: Pelo menos alguma das tags vazias
    condicao_tags_vazias = (
        F.col("tags_complementares").isNull()
        | F.col("tags_substitutos").isNull()
        | F.col("tags_potencializam_uso").isNull()
        | F.col("tags_atenuam_efeitos").isNull()
        | F.col("tags_agregadas").isNull()
    )

    # Contagem de motivos (executa contagens no DF em cache)
    total_motivo_tempo = df_com_historico.filter(condicao_tempo).count()
    total_motivo_vazio = df_com_historico.filter(condicao_tags_vazias).count()

    print(
        f"{agora_em_sao_paulo_str()} - > Motivo 'Tempo' (Nunca processado ou antigo): {total_motivo_tempo}"
    )
    print(f"{agora_em_sao_paulo_str()} - > Motivo 'Tags Vazias': {total_motivo_vazio}")

    # Combina as duas condições
    condicao_para_enriquecer = condicao_tags_vazias & condicao_tempo

    df_enriquecer = df_com_historico.filter(condicao_para_enriquecer)

    total_encontrados = df_enriquecer.count()
    msg = (
        f"{agora_em_sao_paulo_str()} - [RESULTADO] Total FINAL para enriquecer "
        f"(Tags Vazias E (Nunca proc. OU Antigo)): {total_encontrados}"
    )
    print(msg)

    # --- 5. Resultado Final ---
    eans_para_enriquecer = [
        row.ean for row in df_enriquecer.select("ean").distinct().collect()
    ]

    # Libera o cache do join
    df_com_historico.unpersist()

    print(
        f"{agora_em_sao_paulo_str()} - Análise Finalizada. {len(eans_para_enriquecer)} eans precisam de tags."
    )

    return eans_para_enriquecer


# COMMAND ----------

lista_produtos_novos = seleciona_produtos_para_enriquecer()


# COMMAND ----------

# Essa é uma lista de eans (str) a serem enriquecidos, por isso tem que tirar duplicatas
lista_produtos_pendentes = list(set(lista_eans_refazer_tags + lista_produtos_novos))


# enriquece com produtos_refazer_tags somente se refazer_tags = True
ean_list = lista_produtos_novos if not REFAZER_TAGS_RUINS else lista_produtos_pendentes


# COMMAND ----------

df_eans = spark.createDataFrame([(ean,) for ean in ean_list], ["ean"])

df_produtos_pendentes = produtos_refined_df.join(
    df_eans, on="ean", how="inner"
).dropDuplicates(["ean"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtra pra não enriquecer a base toda de uma vez

# COMMAND ----------

produtos_pendentes = filtra_notifica_produtos_enriquecimento(
    produtos_refined_df,
    df_produtos_pendentes,
    THRESHOLD_EXECUCAO,
    DAYS_BACK,
    "gpt_gera_tags_produtos",
)

print("\nQuantidade de produtos que serão enriquecidos: ", len(produtos_pendentes))


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
# MAGIC ## Funções de execução async em batch

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

    resultados_modelo = await gemini_caller.executa_tagging_chain_async_batch(
        prompts,
        timeout=TIMEOUT_BATCHES,
    )

    if not isinstance(resultados_modelo, list):
        return []

    return [
        resultado if isinstance(resultado, dict) else {}
        for resultado in resultados_modelo
    ]


# COMMAND ----------

schema = parse_schema(schema_refined_gpt_gera_tags)

# COMMAND ----------


def validate_results(results: list[dict]) -> list[T.Row]:
    validated: list[Row] = []

    for res in results:
        if not isinstance(res, dict):
            continue

        complete_res = {field.name: res.get(field.name, None) for field in schema}

        # Evita um monte de "null" como string
        for key, _ in complete_res.items():
            if isinstance(complete_res[key], str) and complete_res[key] == "null":
                complete_res[key] = None

        # Garante que registros sem ean NÃO sejam salvos
        if not complete_res.get('ean'):
            print(f"\tAVISO: Pulando registro por 'ean' ausentes: {res}")
            continue

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
    df_results = spark.createDataFrame(validated_rows, schema)

    delta_table = DeltaTable.forName(spark, Table.enriquecimento_tags_produtos.value)

    print(f"\tSalvando resultados para {len(validated_rows)}...")

    # Perform the merge (upsert)
    delta_table.alias("target").merge(
        source=df_results.alias("source"),
        condition="target.ean = source.ean",
    ).whenMatchedUpdate(
        set={
            field.name: f"source.{field.name}"
            for field in schema.fields
            if field.name not in ('ean', 'match_tags_em')
        }
    ).whenNotMatchedInsert(
        values={field.name: f"source.{field.name}" for field in schema.fields}
    ).execute()

    # Update the accumulator
    countAccumulator.add(len(info))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Realiza enriquecimento

# COMMAND ----------

print(f"Enriquecendo {len(produtos_pendentes)} produtos com prompt padrão")

await extrai_info_e_salva(produtos_pendentes)
