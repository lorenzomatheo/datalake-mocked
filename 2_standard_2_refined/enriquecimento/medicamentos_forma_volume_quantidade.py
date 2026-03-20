# Databricks notebook source
# MAGIC %md
# MAGIC # Enriquece a tabela `medicamentos_forma_volume_quantidade`
# MAGIC
# MAGIC A partir da coluna `volume_quantidade` da tabela de produtos, vamos abrir essa informacao em mais colunas: `quantidade` e `unidade_medida`.
# MAGIC
# MAGIC Essa informacao pode ser importante para:
# MAGIC
# MAGIC 1. Categorizar produtos entre pequeno, medio ou grande, com base no volume do SKU (Isso eh o que estou tentando fazer agora).
# MAGIC 2. Calcular o volume de cada SKU, permitindo o calculo de duracao
# MAGIC
# MAGIC
# MAGIC Essa informacao faz sentido somente para medicamentos (`eh_medicamento == true`).
# MAGIC

# COMMAND ----------

get_ipython().system(
    'pip install boto3 s3fs langchain_openai langchain_google_genai "pydantic>=2.0" langchain-google-vertexai langsmith'
)

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## imports

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

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
from maggulake.pipelines.schemas import EnriquecimentoFormaVolumeQuantidade
from maggulake.prompts import MAGGU_SYSTEM_INSTRUCTION, MAGGU_USER_TEMPLATE
from maggulake.utils.iters import create_batches
from maggulake.utils.strings import truncate_text_to_tokens

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "medicamentos_forma_volume_quantidade",
    dbutils,
    widgets={
        "llm_provider": ["gemini_vertex", "gemini_vertex", "openai"],
        "max_products": "100000",
    },
)

# COMMAND ----------

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

# Parametros para notificacoes do discord
THRESHOLD_EXECUCAO = 0.3
DAYS_BACK = 15

# Env Variables
DEBUG = dbutils.widgets.get("debug") == "true"
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))

# LLM Parameters
EMBEDDING_ENCODING = EmbeddingEncodingModels.CL100K_BASE.value
MAX_TOKENS = get_max_tokens_by_embedding_model(EMBEDDING_ENCODING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler tabela de produtos refined

# COMMAND ----------

df_produtos = env.table(Table.produtos_refined)

# COMMAND ----------

df_produtos = (
    df_produtos.select("forma_farmaceutica", "volume_quantidade")
    .dropna()
    .dropDuplicates()
)

# COMMAND ----------

if DEBUG:
    df_produtos.limit(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler tabela ja calculada previamente

# COMMAND ----------

df_ja_processados = env.table(Table.medicamentos_forma_volume_quantidade)

# COMMAND ----------

if DEBUG:
    df_ja_processados.limit(1000).display()


# COMMAND ----------

df_joined = df_produtos.join(
    df_ja_processados, ["forma_farmaceutica", "volume_quantidade"], "left"
)


# todas as linhas em que falta ao menos 1 coluna a ser preenchida
df_falta = df_joined.filter(
    F.col("quantidade").isNull() | F.col("unidade_medida").isNull()
)

# todas as linhas que nao tem "quantidade"
df_falta_quantidade = df_joined.filter(
    F.col("quantidade").isNull() & (F.col("unidade_medida").isNotNull())
)

# todas as linhas que nao tem "unidade_medida"
df_falta_unidade_medida = df_joined.filter(
    (F.col("quantidade").isNotNull()) & (F.col("unidade_medida").isNull())
)

# todas as linhas
df_falta_os_dois = df_joined.filter(
    (F.col("quantidade").isNull()) & (F.col("unidade_medida").isNull())
)

# Printa estatisticas
n_qtde = df_falta_quantidade.count()
n_um = df_falta_unidade_medida.count()
n_ambos = df_falta_os_dois.count()

print(f"Linhas em que falta preencher somente 'quantidade': {n_qtde}")
print(f"Linhas em que falta preencher somente 'unidade_medida': {n_um}")
print(f"Linhas em que falta preencher ambos: {n_ambos}")

n_total = df_joined.count()
print(f"Total de linhas da tabela: {n_total}")
print(f"Total de linhas para refinarmos: {n_qtde + n_um + n_ambos}")
print(f"% a ser preenchida: {((n_qtde + n_um + n_ambos) / n_total) * 100:.2f}%")

if n_qtde + n_um + n_ambos == 0:
    dbutils.notebook.exit("✅ SUCESSO: Todos os dados já estão enriquecidos!")
else:
    print("Iniciando enriquecimento via LLM...")

# COMMAND ----------

if DEBUG:
    df_falta_quantidade.limit(100).display()
    df_falta_unidade_medida.limit(100).display()
    df_falta_os_dois.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriquecimento via LLM
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cria o tagging chain
# MAGIC

# COMMAND ----------


def formata_linha_pro_llm(row):
    return truncate_text_to_tokens(
        "\n\n".join(
            [
                f"'forma_farmaceutica': {row.forma_farmaceutica}",
                f"'volume_quantidade': {row.volume_quantidade}",
            ]
        ),
        MAX_TOKENS,
        EMBEDDING_ENCODING,
    )


# COMMAND ----------

LLM_PROVIDER = dbutils.widgets.get("llm_provider")
llm_config = get_llm_config(LLM_PROVIDER, spark, dbutils, size="SMALL")

CONTEXT_CACHE_NAME = None
if llm_config.provider == "gemini_vertex":
    CONTEXT_CACHE_NAME = create_vertex_context_cache(
        model=llm_config.model,
        system_instruction=MAGGU_SYSTEM_INSTRUCTION,
        project_id=llm_config.project_id,
        location=llm_config.location,
        ttl=7200,
    )

gemini_caller = TaggingProcessor(
    provider=llm_config.provider,
    model=llm_config.model,
    api_key=llm_config.api_key,
    project_id=llm_config.project_id,
    location=llm_config.location,
    prompt_template=MAGGU_USER_TEMPLATE,
    output_schema=EnriquecimentoFormaVolumeQuantidade,
    cached_content=CONTEXT_CACHE_NAME,
    system_instruction=MAGGU_SYSTEM_INSTRUCTION,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### testa para 1 caso:

# COMMAND ----------


prompt = formata_linha_pro_llm(df_falta.head())

gemini_caller.executa_tagging_chain(prompt)


# COMMAND ----------


DATAFRAME_SCHEMA = T.StructType(
    [
        T.StructField("forma_farmaceutica", T.StringType(), False),
        T.StructField("volume_quantidade", T.StringType(), False),
        T.StructField("quantidade", T.DoubleType(), False),
        T.StructField("unidade_medida", T.StringType(), False),
    ]
)

SCHEMA_FIELDS = [field.name for field in DATAFRAME_SCHEMA.fields]

print(SCHEMA_FIELDS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Executa loop async

# COMMAND ----------

spark = env.spark
sc = spark.sparkContext

countAccumulator = sc.accumulator(0)


async def extrai_info_e_salva(collected_list: list[Row]):
    global countAccumulator

    rows = df_falta.count()
    # batch_size = 200
    for b in create_batches(collected_list, 200):
        rs = await extrair_info(b)
        if rs:
            salva_info_produto(rs)
        else:
            print("Erro ao extrair informações.")
            continue

        countAccumulator.add(len(b))
        print(
            f"Quantidade de linhas pendentes: {rows - countAccumulator.value} de {rows}"
        )


async def extrair_info(rows: list[Row]):
    return await gemini_caller.executa_tagging_chain_async_batch(
        [formata_linha_pro_llm(row) for row in rows]
    )


def salva_info_produto(info):
    global countAccumulator

    validated_rows = []
    for res in info:
        if isinstance(res, dict):
            complete_res = {field: res.get(field, None) for field in SCHEMA_FIELDS}

            try:
                complete_res["quantidade"] = float(complete_res["quantidade"])
            except TypeError:
                # Por algum motivo retornou None, então não converte para float
                complete_res["quantidade"] = None

            validated_rows.append(Row(**complete_res))
        elif isinstance(res, Exception):
            print(f"Erro ao processar: {res}")

    if validated_rows:
        df_results = spark.createDataFrame(validated_rows, DATAFRAME_SCHEMA)
        df_results.write.mode("append").saveAsTable(
            Table.medicamentos_forma_volume_quantidade.value
        )


# COMMAND ----------

produtos_to_process = df_falta.collect()
total_original = len(produtos_to_process)

if total_original > MAX_PRODUCTS:
    produtos_to_process = produtos_to_process[:MAX_PRODUCTS]
    print(
        f"⚠️  Limitando processamento devido a max_products: {total_original} → {MAX_PRODUCTS} produtos"
    )

print(f"Total de produtos a processar: {len(produtos_to_process)}")

await extrai_info_e_salva(produtos_to_process)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva no delta

# COMMAND ----------


new_df = spark.read.table(Table.medicamentos_forma_volume_quantidade.value)

# summarize by "forma_farmaceutica" and "volume_quantidade", Coalesce the other columns (quantidade and unidade_medida)
new_df = new_df.groupBy("forma_farmaceutica", "volume_quantidade").agg(
    F.first("quantidade", ignorenulls=True).alias("quantidade"),
    F.first("unidade_medida", ignorenulls=True).alias("unidade_medida"),
)
new_df.display()

# save again, overwrite
new_df.write.mode("overwrite").saveAsTable(
    Table.medicamentos_forma_volume_quantidade.value
)

# COMMAND ----------

# TODO: no futuro precisa pensar numa forma inteligente de apagar algumas linhas caso necessário.
