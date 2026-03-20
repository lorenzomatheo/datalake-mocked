# Databricks notebook source
# MAGIC %pip install langchain langchain-core langchain-community langchain_milvus langchain_openai pymilvus openai html2text rapidfuzz langchain_google_genai langchain-google-vertexai
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Match Tags
# MAGIC
# MAGIC Este notebook enriquece produtos sem tags através de busca vetorial:
# MAGIC - Identifica produtos que não possuem tags preenchidas
# MAGIC - Consulta no Milvus o produto mais semelhante que já possui tags
# MAGIC - Herda as tags do produto similar
# MAGIC - Usa estratégia de cutoff para não reprocessar produtos recentemente atualizados
# MAGIC
# MAGIC ## Funcionamento
# MAGIC 1. Seleciona produtos com tags vazias que não foram processados recentemente
# MAGIC 2. Busca produtos similares no Milvus usando o nome como query
# MAGIC 3. Copia as tags do produto mais similar (score > 0.6)
# MAGIC 4. Registra timestamp do match para controle de reprocessamento

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Configuração Inicial

# COMMAND ----------

from datetime import datetime, timedelta

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_refined_gpt_gera_tags
from maggulake.utils.time import agora_em_sao_paulo_str as agora
from maggulake.vector_search_retriever import get_milvus_retriever

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment e Configurações

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "match_tags",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "max_products": "1000",
        "batch_size": "100",
    },
)

spark = env.spark

# COMMAND ----------

# Configurações
DEBUG = dbutils.widgets.get("debug") == "true"

# Acesso aos recursos do Environment
OPENAI_ORGANIZATION = env.settings.openai_organization
OPENAI_API_KEY = env.settings.openai_api_key
MILVUS_URI = env.settings.milvus_uri
MILVUS_TOKEN = env.settings.milvus_token

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros do Match

# COMMAND ----------

# Configuracoes do match tags:
CUTOFF_DAYS: int = 30  # Dias para considerar o match como antigo e executar novamente
SCORE_THRESHOLD: float = 0.6  # Score mínimo para considerar um match válido
MAX_PRODUCTS: int = int(dbutils.widgets.get("max_products"))
BATCH_SIZE: int = int(dbutils.widgets.get("batch_size"))

if DEBUG:
    print(f"{agora()} - Configurações:")
    print(f"   - CUTOFF_DAYS: {CUTOFF_DAYS}")
    print(f"   - SCORE_THRESHOLD: {SCORE_THRESHOLD}")
    print(f"   - MAX_PRODUCTS: {MAX_PRODUCTS}")
    print(f"   - BATCH_SIZE: {BATCH_SIZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de Dados

# COMMAND ----------

# Le produtos enriquecidos
produtos_enriquecidos = env.table(Table.produtos_refined).cache()

if DEBUG:
    print(
        f"{agora()} - Total de produtos enriquecidos carregados: {produtos_enriquecidos.count()}"
    )

# COMMAND ----------

# Le tabela de tags geradas anteriormente

gpt_extract_tags_df = env.table(Table.enriquecimento_tags_produtos)
gpt_extract_tags_table = DeltaTable.forName(
    spark, Table.enriquecimento_tags_produtos.value
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seleciona produtos para match

# COMMAND ----------


def seleciona_produtos_para_fazer_o_match(
    df_produtos_enriquecidos: DataFrame, df_tags_historico: DataFrame
) -> list[tuple[str, str, str]]:
    print(f"{agora()} - Inicia seleção de produtos")

    cutoff_date = datetime.now() - timedelta(days=CUTOFF_DAYS)

    # Junta produtos enriquecidos com a tabela de tags
    df_com_tags = df_produtos_enriquecidos.select('ean', 'nome').join(
        df_tags_historico, how='left', on=['ean']
    )

    # Filtra produtos onde AO MENOS UMA das tags está vazia ou nula
    # (produto tem pelo menos uma tag não preenchida)
    condicao_alguma_tag_vazia = (
        (F.col('tags_complementares').isNull() | (F.col('tags_complementares') == ''))
        | (F.col('tags_substitutos').isNull() | (F.col('tags_substitutos') == ''))
        | (
            F.col('tags_potencializam_uso').isNull()
            | (F.col('tags_potencializam_uso') == '')
        )
        | (
            F.col('tags_atenuam_efeitos').isNull()
            | (F.col('tags_atenuam_efeitos') == '')
        )
        | (F.col('tags_agregadas').isNull() | (F.col('tags_agregadas') == ''))
    )

    df_filtrado = df_com_tags.filter(
        condicao_alguma_tag_vazia & F.col('nome').isNotNull()
    )

    df_filtrado = df_com_tags.filter(
        condicao_alguma_tag_vazia & F.col('nome').isNotNull()
    )

    # Condição de tempo: nunca passou pelo match ou o match é antigo
    condicao_tempo = F.col("match_tags_em").isNull() | (
        F.col("match_tags_em") <= F.lit(cutoff_date)
    )

    produtos_to_update: list[tuple[str, str, str]] = (
        df_filtrado.filter(condicao_tempo)
        .select("ean", "nome")
        .distinct()
        .rdd.map(tuple)
        .collect()
    )

    total_produtos_sem_tags = len(produtos_to_update)
    print(f"{agora()} - ⚠️ Total de produtos sem tags: {total_produtos_sem_tags}")

    if len(produtos_to_update) > MAX_PRODUCTS:
        produtos_to_update = produtos_to_update[:MAX_PRODUCTS]
        print(f"{agora()} - Limitando para MAX_PRODUCTS ({MAX_PRODUCTS}) produtos")

    print(
        f"{agora()} - 🔄 Total de produtos a serem enriquecidos: {len(produtos_to_update)}"
    )

    return produtos_to_update


# COMMAND ----------

produtos_tags_null_para_ret = seleciona_produtos_para_fazer_o_match(
    produtos_enriquecidos, gpt_extract_tags_df
)

if len(produtos_tags_null_para_ret) < 1:
    dbutils.notebook.exit("Sem produtos para completar tags.")

print(f"Produtos para completar tags: {len(produtos_tags_null_para_ret)}")


# COMMAND ----------

# Cria retriever do Milvus

retriever_tags = get_milvus_retriever(
    OPENAI_ORGANIZATION,
    OPENAI_API_KEY,
    MILVUS_URI,
    MILVUS_TOKEN,
    filtro="tags_complementares IS NOT NULL AND tags_substitutos IS NOT NULL AND tags_potencializam_uso IS NOT NULL AND tags_atenuam_efeitos IS NOT NULL AND tags_agregadas IS NOT NULL AND tags_complementares != '' AND tags_substitutos != '' AND tags_potencializam_uso != '' AND tags_atenuam_efeitos != '' AND tags_agregadas != ''",
    max_results=1,  # Somente o mais similar
    score_threshold=SCORE_THRESHOLD,
    campos_output=[
        "ean",
        "nome",
        "tags_complementares",
        "tags_substitutos",
        "tags_potencializam_uso",
        "tags_atenuam_efeitos",
        "tags_agregadas",
    ],
    format_return=False,
)

# COMMAND ----------

if DEBUG:
    # Testa o retriever com um produto de exemplo
    test_result = retriever_tags.invoke('MOUSSE HEDERA SPA OLIVA E DAMA')
    print(f"{agora()} - Teste do retriever:")
    print(test_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processa produtos sem tags

# COMMAND ----------


def salva_batch_no_delta(batch_results: list[dict]) -> None:
    if not batch_results:
        return

    df_batch = spark.createDataFrame(batch_results, schema=schema_refined_gpt_gera_tags)
    # Isso é somente "just in case", mas não deveria haver duplicatas no batch
    df_batch = df_batch.dropDuplicates(["ean"])

    colunas_tags = [a for a in gpt_extract_tags_df.columns if 'tags' in a]
    colunas_para_atualizar = colunas_tags + ['match_tags_em']
    dynamic_tags_set = {
        f"target.{col}": f"source.{col}" for col in colunas_para_atualizar
    }

    gpt_extract_tags_table.alias("target").merge(
        source=df_batch.alias("source"),
        condition="target.ean = source.ean",
    ).whenMatchedUpdate(set=dynamic_tags_set).whenNotMatchedInsertAll().execute()

    print(f"✅ {agora()} - Batch de {len(batch_results)} produtos salvo com sucesso")


# COMMAND ----------


def process_produtos_sem_tags(produtos_sem_tags: list[tuple[str, str, str]]) -> int:
    batch_results: list[dict] = []
    total = len(produtos_sem_tags)
    produtos_enriquecidos = 0

    print(f"{agora()} - Iniciando processamento de {total} produtos")
    print(f"{agora()} - Salvando a cada {BATCH_SIZE} produtos processados")

    for idx, (ean, nome_produto) in enumerate(produtos_sem_tags, start=1):
        print(
            f"{agora()} - Processando produto {idx}/{total} ({idx / total * 100:.1f}%) - '{nome_produto}'"
        )

        data = retriever_tags.invoke(nome_produto)

        if not data or len(data) < 1:
            print(f"\t{agora()} - Nenhum match encontrado")
            continue

        print(f"\t{agora()} - Deu match com -> '{data[0]['entity']['nome']}'")

        for doc in data:
            entity = doc["entity"]

            # Extract only the tag-related keys
            tag_keys = [key for key in entity.keys() if key.startswith("tags_")]

            # Check if all tag values are not None
            if not all(entity[key] is not None for key in tag_keys):
                continue

            # TODO: Hoje a gente ainda usa a estrutura de "tag|tag" para as tags,
            # No futuro seria excelente se a gente conseguisse migrar para array[string].
            # Fizemos assim no passado pois o retriever do databricks nao suportava arrays.
            # Infelizmente ficou como legado...
            batch_results.append(
                {
                    "ean": ean,
                    **{key: "|".join(entity[key]) for key in tag_keys},
                    "match_tags_em": datetime.now(),
                }
            )
            produtos_enriquecidos += 1

        # Salva o batch incrementalmente para não perder dados em caso de interrupção
        if len(batch_results) >= BATCH_SIZE:
            salva_batch_no_delta(batch_results)
            batch_results = []

    # Salva o último batch se houver produtos restantes
    if batch_results:
        salva_batch_no_delta(batch_results)

    print(
        f"{agora()} - ✅ Total de produtos que tiveram tags preenchidas: {produtos_enriquecidos}"
    )

    return produtos_enriquecidos


# COMMAND ----------

# MAGIC %md
# MAGIC ## Processa e salva resultados

# COMMAND ----------

total_enriquecidos = process_produtos_sem_tags(produtos_tags_null_para_ret)

# COMMAND ----------

print(
    f"{agora()} - ✅ Processo concluído! Total de produtos enriquecidos: {total_enriquecidos}"
)
