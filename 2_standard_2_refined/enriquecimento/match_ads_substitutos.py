# Databricks notebook source
# MAGIC %md
# MAGIC # Match Ads Substitutos

# COMMAND ----------

# MAGIC %pip install langchain_google_genai langchain-google-vertexai openai pymilvus
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiente

# COMMAND ----------

from time import sleep

import pyspark.sql.functions as F
import pyspark.sql.types as T
from requests.exceptions import ConnectionError, JSONDecodeError, Timeout

from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.utils.iters import batched
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "match_ads_agregados",
    dbutils,
    widgets={"ean_to_debug": ""},
)
STAGE = env.settings.name_short
DEBUG = dbutils.widgets.get("debug") == "true"
EAN_TO_DEBUG = str(dbutils.widgets.get("ean_to_debug"))
CATALOG = env.settings.catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

# spark
spark = env.spark
bq = env.bigquery_adapter

# delta tables
PRODUTOS_REFINED_TABLE = f"{CATALOG}.refined.produtos_refined"
PRODUTOS_SUBSTITUTOS = f"{CATALOG}.standard.produtos_campanha_substitutos"

SCORE_THRESHOLD = 0.7
MAX_RESULTS = 400
TEXT_COLUMNS = [
    "ean",
    "eans_alternativos",
    "informacoes_para_embeddings",
    "power_phrase",
    "marca",
]

# COMMAND ----------

concorrentes_df = bq.get_concorrentes().toPandas()

if "status" in concorrentes_df.columns:
    print(concorrentes_df)
    raise ValueError("Erro ao executar a query de concorrentes, verifique!")


if DEBUG:
    print(
        "Concorrentes carregados com sucesso!!"
        f" Um total de {len(concorrentes_df)} produtos concorrentes."
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Identifica produtos em campanha ativas atualmente

# COMMAND ----------

produtos_em_campanha_ativa_industria = (
    bq.get_produtos_em_campanha(tipo="INDUSTRIA").select("ean").collect()
)
produtos_em_campanha_ativa_varejo = (
    bq.get_produtos_em_campanha(tipo="VAREJO").select("ean").collect()
)
produtos_em_campanha_pausada = (
    bq.get_produtos_em_campanha(status="PAUSADA").select("ean").collect()
)  # para gerar concorrentes análise indústria
produtos_foco = list(
    set(
        [row["ean"] for row in produtos_em_campanha_ativa_industria]
        + [row["ean"] for row in produtos_em_campanha_ativa_varejo]
        + [row["ean"] for row in produtos_em_campanha_pausada]
    )
)

if DEBUG:
    print(f"Quantidade de produtos foco: {len(produtos_foco)}")

# COMMAND ----------

produtos_foco_df = spark.createDataFrame(
    [(ean,) for ean in produtos_foco], "ean string"
)
produtos_refined_em_campanha = spark.read.table(PRODUTOS_REFINED_TABLE).join(
    F.broadcast(produtos_foco_df), "ean"
)

produtos_medicamentos = produtos_refined_em_campanha.filter(
    F.col("eh_medicamento") == True
)

produtos_nao_medicamentos = produtos_refined_em_campanha.filter(
    (F.col("eh_medicamento") == False) | F.col("eh_medicamento").isNull()
)

# COMMAND ----------

if DEBUG:
    produtos_nao_medicamentos.filter(F.col('ean').isin(EAN_TO_DEBUG)).display()

# COMMAND ----------

if DEBUG:
    produtos_medicamentos.filter(F.col('ean').isin(EAN_TO_DEBUG)).display()

# COMMAND ----------

info_medicamentos = (
    produtos_medicamentos.withColumn(
        "medicamentos_referencia",
        F.from_json("medicamentos_referencia", "array<array<string>>"),
    )
    .withColumn(
        "medicamentos_similares",
        F.from_json("medicamentos_similares", "array<array<string>>"),
    )
    .selectExpr(
        "ean",
        "informacoes_para_embeddings",
        "transform(medicamentos_referencia, x -> x[0]) AS nomes_referencias",
        "transform(medicamentos_similares, x -> x[0]) AS nomes_similares",
        "eh_otc",
        "marca",
    )
)

# COMMAND ----------

if DEBUG:
    info_medicamentos.filter(F.col('ean').isin(EAN_TO_DEBUG)).display()

# COMMAND ----------

medicamentos_referencia = info_medicamentos.selectExpr(
    "ean AS ean_em_campanha",
    "explode(nomes_referencias) AS prompt",
    "0.9 AS score_pre_definido",
    "eh_otc",
    "marca",
)

medicamentos_similares = info_medicamentos.selectExpr(
    "ean AS ean_em_campanha",
    "explode(nomes_similares) AS prompt",
    "0.8 AS score_pre_definido",
    "eh_otc",
    "marca",
)

# Quando tem poucos nomes de medicamentos referencia ou similares,
# a busca semantica com todos os filtros acaba cortando os resultados
# o valor 5 é arbitrário, mas é um valor que parece funcionar bem
medicamentos_sem_alternativos = info_medicamentos.filter(
    "not size(nomes_referencias) > 5 or not size(nomes_similares) > 5"
).selectExpr(
    "ean AS ean_em_campanha",
    "informacoes_para_embeddings AS prompt",
    "null::decimal AS score_pre_definido",
    "eh_otc",
    "marca",
)

medicamentos = (
    medicamentos_referencia.unionByName(medicamentos_similares).unionByName(
        medicamentos_sem_alternativos
    )
).cache()

medicamentos_otc = medicamentos.filter("eh_otc = true")
medicamentos_nao_otc = medicamentos.filter("not (eh_otc <=> true)")

prompts_medicamentos_otc = medicamentos_otc.select("prompt").distinct()
prompts_medicamentos_nao_otc = medicamentos_nao_otc.select("prompt").distinct()

# COMMAND ----------

if DEBUG:
    medicamentos_otc.filter(F.col('ean_em_campanha').isin(EAN_TO_DEBUG)).display()

# COMMAND ----------

if DEBUG:
    print(f"Quantidade de medicamentos OTC: {medicamentos_otc.count()}")
    print(f"Quantidade de medicamentos não OTC: {medicamentos_nao_otc.count()}")

# COMMAND ----------

nao_medicamentos = produtos_nao_medicamentos.selectExpr(
    "ean AS ean_em_campanha",
    "informacoes_para_embeddings AS prompt",
)

prompts_nao_medicamentos = nao_medicamentos.select("prompt").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria retriever

# COMMAND ----------


schema_recomendacoes = T.StructType(
    [
        T.StructField("prompt", T.StringType(), False),
        T.StructField(
            "recomendacoes",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("ean", T.StringType(), True),
                        T.StructField("score", T.DoubleType(), True),
                        T.StructField("frase", T.StringType(), True),
                        T.StructField("marca", T.StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

# COMMAND ----------


def parse_response(response):
    return [
        {
            "ean": ean,
            "score": r["distance"],
            "frase": r["entity"]["power_phrase"],
            "marca": r["entity"]["marca"],
        }
        for r in response
        for ean in [r["entity"]["ean"]] + (r["entity"]["eans_alternativos"] or [])
    ]


batch_size = 50 if CATALOG == "production" else 10

# COMMAND ----------


def processar_batch_com_retry(retriever, batch_list, batch_index, max_retries=3):
    retry_count = 0

    while retry_count < max_retries:
        try:
            recomendacoes_batch = retriever.batch(batch_list)
            return [
                {"prompt": prompt, "recomendacoes": parse_response(recomendacao)}
                for prompt, recomendacao in zip(batch_list, recomendacoes_batch)
            ]
        except (ConnectionError, Timeout, JSONDecodeError) as e:
            retry_count += 1
            print(
                f"{agora_em_sao_paulo_str()} Erro no batch {batch_index} "
                f"(tentativa {retry_count}/{max_retries}): {e}"
            )

            if retry_count < max_retries:
                sleep_time = 2**retry_count
                print(f"Aguardando {sleep_time}s antes de tentar novamente...")
                sleep(sleep_time)

    print(f"Falha após {max_retries} tentativas. Batch {batch_index} será pulado.")
    return [{"prompt": prompt, "recomendacoes": []} for prompt in batch_list]


# COMMAND ----------


def get_recomendacoes(prompts_df, filtro: str, tipo_produto: str):
    prompts_distintos = [
        row["prompt"] for row in prompts_df.select("prompt").distinct().collect()
    ]

    total_prompts = len(prompts_distintos)

    if total_prompts == 0:
        return spark.createDataFrame([], schema=schema_recomendacoes)

    print(
        f"{agora_em_sao_paulo_str()} Processando {total_prompts} prompts distintos de {tipo_produto}"
    )

    retriever = env.vector_search.get_data_api_retriever(
        filtro=filtro,
        max_results=MAX_RESULTS,
        score_threshold=SCORE_THRESHOLD,
        campos_output=TEXT_COLUMNS,
        format_return=False,
    )

    resultados = []
    for i, batch in enumerate(batched(prompts_distintos, batch_size)):
        batch_list = list(batch)
        resultados.extend(processar_batch_com_retry(retriever, batch_list, i + 1))

        if (i + 1) % 10 == 0:
            print(
                f"{agora_em_sao_paulo_str()} Processados {(i + 1) * batch_size} prompts "
                f"de {tipo_produto} ({((i + 1) * batch_size / total_prompts * 100):.1f}%)"
            )

    print(
        f"{agora_em_sao_paulo_str()} Finalizado processamento de {total_prompts} prompts "
        f"de {tipo_produto}"
    )

    return spark.createDataFrame(resultados, schema=schema_recomendacoes)


# COMMAND ----------

FILTRO_MEDICAMENTO_NAO_OTC = f"ean not in {produtos_foco} and not array_contains_any(eans_alternativos, {produtos_foco}) and eh_medicamento == true and principio_ativo !='' "

FILTRO_MEDICAMENTO_OTC = f"ean not in {produtos_foco} and not array_contains_any(eans_alternativos, {produtos_foco}) and eh_medicamento==true and categorias is not null"

FILTRO_NAO_MEDICAMENTO = f"ean not in {produtos_foco} and not array_contains_any(eans_alternativos, {produtos_foco}) and eh_medicamento==false and categorias is not null"


# COMMAND ----------

tamanho = prompts_medicamentos_otc.count()
print(f"{agora_em_sao_paulo_str()} Iniciando o match para {tamanho} medicamentos OTC")

prompts_medicamentos_otc_com_recomendacoes = prompts_medicamentos_otc.join(
    get_recomendacoes(
        prompts_medicamentos_otc,
        filtro=FILTRO_MEDICAMENTO_OTC,
        tipo_produto="medicamentos OTC",
    ),
    on="prompt",
    how="left",
).cache()

print(
    f"{agora_em_sao_paulo_str()} Total de prompts de medicamentos OTC com recomendações: {prompts_medicamentos_otc_com_recomendacoes.count()}"
)

# COMMAND ----------

tamanho_nao_otc = prompts_medicamentos_nao_otc.count()
print(
    f"{agora_em_sao_paulo_str()} Iniciando o match para {tamanho_nao_otc} medicamentos não OTC"
)

prompts_medicamentos_nao_otc_com_recomendacoes = prompts_medicamentos_nao_otc.join(
    get_recomendacoes(
        prompts_medicamentos_nao_otc,
        filtro=FILTRO_MEDICAMENTO_NAO_OTC,
        tipo_produto="medicamentos não OTC",
    ),
    on="prompt",
    how="left",
).cache()

print(
    f"{agora_em_sao_paulo_str()} Total de prompts de medicamentos não OTC com recomendações: {prompts_medicamentos_nao_otc_com_recomendacoes.count()}"
)

# COMMAND ----------

tamanho_nao_medicamentos = prompts_nao_medicamentos.count()
print(
    f"{agora_em_sao_paulo_str()} Iniciando o match para {tamanho_nao_medicamentos} produtos não medicamentos"
)

prompts_nao_medicamentos_com_recomendacoes = prompts_nao_medicamentos.join(
    get_recomendacoes(
        prompts_nao_medicamentos,
        filtro=FILTRO_NAO_MEDICAMENTO,
        tipo_produto="não medicamentos",
    ),
    on="prompt",
    how="left",
).cache()

print(
    f"{agora_em_sao_paulo_str()} Total de prompts de produtos não medicamentos com recomendações: {prompts_nao_medicamentos_com_recomendacoes.count()}"
)

# COMMAND ----------

if DEBUG:
    prompts_medicamentos_otc_com_recomendacoes.limit(100).display()

# COMMAND ----------

FILTRO_MARCA = (
    "e.marca is null or recomendacao.marca is null or e.marca != recomendacao.marca"
)

recomendacoes_med_otc_exploded = (
    medicamentos_otc.alias("e")
    .join(prompts_medicamentos_otc_com_recomendacoes.alias("r"), "prompt")
    .selectExpr(
        "e.ean_em_campanha",
        "e.score_pre_definido",
        "explode(r.recomendacoes) AS recomendacao",
    )
    .filter(FILTRO_MARCA)
    .select(
        "ean_em_campanha",
        "recomendacao.ean",
        F.coalesce("score_pre_definido", "recomendacao.score").alias("score"),
        "recomendacao.frase",
    )
    .sort("ean_em_campanha", F.desc("score"))
    .dropDuplicates(["ean_em_campanha", "ean"])
    .cache()
)

# COMMAND ----------

if DEBUG:
    recomendacoes_med_otc_exploded.filter(
        F.col('ean_em_campanha').isin(EAN_TO_DEBUG)
    ).display()

# COMMAND ----------

recomendacoes_med_nao_otc_exploded = (
    medicamentos_nao_otc.alias("e")
    .join(prompts_medicamentos_nao_otc_com_recomendacoes.alias("r"), "prompt")
    .selectExpr(
        "e.ean_em_campanha",
        "e.score_pre_definido",
        "explode(r.recomendacoes) AS recomendacao",
    )
    .filter(FILTRO_MARCA)
    .select(
        "ean_em_campanha",
        "recomendacao.ean",
        F.coalesce("score_pre_definido", "recomendacao.score").alias("score"),
        "recomendacao.frase",
    )
    .sort("ean_em_campanha", F.desc("score"))
    .dropDuplicates(["ean_em_campanha", "ean"])
    .cache()
)


# COMMAND ----------

if DEBUG:
    recomendacoes_med_nao_otc_exploded.filter(
        F.col('ean_em_campanha').isin(EAN_TO_DEBUG)
    ).display()

# COMMAND ----------

recomendacoes_nao_med_exploded = (
    nao_medicamentos.alias("e")
    .join(prompts_nao_medicamentos_com_recomendacoes.alias("r"), "prompt")
    .selectExpr(
        "e.ean_em_campanha",
        "explode(r.recomendacoes) AS recomendacao",
    )
    .select(
        "ean_em_campanha",
        "recomendacao.ean",
        "recomendacao.score",
        "recomendacao.frase",
    )
    .sort("ean_em_campanha", F.desc("score"))
    .dropDuplicates(["ean_em_campanha", "ean"])
    .cache()
)

# COMMAND ----------

if DEBUG:
    recomendacoes_nao_med_exploded.filter(
        F.col('ean_em_campanha').isin(EAN_TO_DEBUG)
    ).display()

# COMMAND ----------

recomendacoes_med_otc = (
    recomendacoes_med_otc_exploded.groupBy("ean_em_campanha")
    .agg(
        F.collect_list(F.struct("ean", "score", "frase")).alias("eans_recomendar"),
    )
    .cache()
)

recomendacoes_med_nao_otc = (
    recomendacoes_med_nao_otc_exploded.groupBy("ean_em_campanha")
    .agg(
        F.collect_list(F.struct("ean", "score", "frase")).alias("eans_recomendar"),
    )
    .cache()
)

recomendacoes_nao_med = (
    recomendacoes_nao_med_exploded.groupBy("ean_em_campanha")
    .agg(
        F.collect_list(F.struct("ean", "score", "frase")).alias("eans_recomendar"),
    )
    .cache()
)

recomendacoes = (
    recomendacoes_med_otc.unionByName(recomendacoes_med_nao_otc)
    .unionByName(recomendacoes_nao_med)
    .cache()
)

# COMMAND ----------

if DEBUG:
    recomendacoes.filter(F.col('ean_em_campanha').isin(EAN_TO_DEBUG)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona eans concorrentes com o score 1.0

# COMMAND ----------

competitor_matches = []
for i, row in concorrentes_df.iterrows():
    ean = row["produto_ean"]
    linhas_com_esse_ean = concorrentes_df[concorrentes_df["produto_ean"] == ean]

    competitor_match = {
        "ean_em_campanha": ean,
        "eans_recomendar": [
            {
                "ean": row["ean_concorrente"],
                "score": 1.0,  # prioritário na escolha da recomendação
                "frase": row["power_phrase"],
            }
        ],
    }
    competitor_matches.append(competitor_match)
    print(f"Adicionando 1 concorrente para o ean {ean}")

print(f"Encerrado com {len(competitor_matches)} match-concorrentes")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Converte para dataframe

# COMMAND ----------

schema = T.StructType(
    [
        T.StructField("ean_em_campanha", T.StringType(), True),
        T.StructField(
            "eans_recomendar",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("ean", T.StringType(), True),
                        T.StructField("score", T.DoubleType(), True),
                        T.StructField("frase", T.StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

df_matches = recomendacoes
df_competitor = spark.createDataFrame(competitor_matches, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtros para produtos descobertos pela AI com score diferente de 1.0
# MAGIC
# MAGIC - Se o produto buscado não for medicamento, o recomendado também não deve ser.
# MAGIC - Se o produto buscado não for medicamento, o produto recomendado deve ser da mesma categoria.
# MAGIC - Se o produto buscado for medicamento, o recomendado deve ter o mesmo princípio ativo.
# MAGIC - Se o produto buscado for medicamento, o recomendado deve ter a mesma via de administração.

# COMMAND ----------

eans_em_campanha = df_matches.select("ean_em_campanha").distinct()

# COMMAND ----------

# pega as informacoes necessarias para os filtros
df_infos_adicionais = spark.read.table('production.refined.produtos_refined').select(
    'ean', 'eh_medicamento', 'categorias', 'principio_ativo', 'via_administracao'
)

# COMMAND ----------

df = (
    df_matches.withColumn("recommendation", F.explode_outer("eans_recomendar"))
    .withColumn("recom_ean", F.col("recommendation.ean"))
    .withColumn("recom_score", F.col("recommendation.score"))
    .withColumn("recom_frase", F.col("recommendation.frase"))
)

# COMMAND ----------

# join com as infos dos eans_em_campanha
df_info_campanha = (
    df.alias('emc')
    .join(
        df_infos_adicionais.withColumnRenamed('ean', 'ean_em_campanha').alias(
            'info_campanha'
        ),
        F.col("emc.ean_em_campanha") == F.col("info_campanha.ean_em_campanha"),
        how='left',
    )
    .select(
        'emc.ean_em_campanha',
        'recom_ean',
        'recom_score',
        'recom_frase',
        F.col('eh_medicamento').alias('eh_medicamento_em_campanha'),
        F.col('categorias').alias('categorias_em_campanha'),
        F.col('principio_ativo').alias('principio_ativo_em_campanha'),
        F.col('via_administracao').alias('via_administracao_em_campanha'),
    )
)

# COMMAND ----------

# join das infos dos eans_recomendados
df_info_recom = (
    df_info_campanha.alias('rec')
    .join(
        df_infos_adicionais.withColumnRenamed('ean', 'recom_ean').alias('info_recom'),
        F.col("rec.recom_ean") == F.col("info_recom.recom_ean"),
        how='left',
    )
    .select(
        'ean_em_campanha',
        F.col('eh_medicamento_em_campanha'),
        F.col('categorias_em_campanha'),
        F.col('principio_ativo_em_campanha'),
        F.col('via_administracao_em_campanha'),
        'rec.recom_ean',
        'recom_score',
        'recom_frase',
        F.col('eh_medicamento').alias('eh_medicamento_recom'),
        F.col('categorias').alias('categorias_recom'),
        F.col('principio_ativo').alias('principio_ativo_recom'),
        F.col('via_administracao').alias('via_administracao_recom'),
    )
)

# COMMAND ----------

df_with_filter = (
    df_info_recom.withColumn(
        # Regras para nao medicamentos
        "non_med_rule",
        (~F.col("eh_medicamento_em_campanha"))
        & (~F.col("eh_medicamento_recom"))
        & (
            F.size(
                F.array_intersect(
                    F.coalesce(F.col("categorias_em_campanha"), F.array()),
                    F.coalesce(F.col("categorias_recom"), F.array()),
                )
            )
            > 0
        ),
    )
    .withColumn(
        # Regra para medicamentos
        "med_rule",
        (F.col("eh_medicamento_em_campanha"))
        & (F.col("eh_medicamento_recom"))
        & (
            F.coalesce(F.col("principio_ativo_em_campanha"), F.lit(""))
            == F.coalesce(F.col("principio_ativo_recom"), F.lit(""))
        )
        & (
            F.coalesce(F.col("via_administracao_em_campanha"), F.lit(""))
            == F.coalesce(F.col("via_administracao_recom"), F.lit(""))
        ),
    )
    .withColumn("passes_rules", F.col("non_med_rule") | F.col("med_rule"))
    .filter(F.col("passes_rules"))
    .drop("non_med_rule", "med_rule")
    .groupBy("ean_em_campanha")
    .agg(
        F.collect_list(
            F.struct(
                F.col('recom_ean').alias('ean'),
                F.col('recom_score').alias('score'),
                F.col('recom_frase').alias('frase'),
            )
        ).alias('eans_recomendar')
    )
)

# COMMAND ----------

# garante que os eans buscados não vão ser filtrados, apenas as recomendações
df = eans_em_campanha.join(df_with_filter, how='left', on='ean_em_campanha')

# COMMAND ----------

# junta com os concorrentes cadastrados
df = df.unionByName(df_competitor)

# COMMAND ----------

if DEBUG:
    df.filter(F.col('ean_em_campanha').isin(EAN_TO_DEBUG)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trata Duplicatas
# MAGIC
# MAGIC Abaixo precisamos remover as duplicatas de ean_em_campanha que surgiram após adicionar os concorrentes.

# COMMAND ----------

# Agrupar valores iguais da coluna "ean_em_campanha"
df = df.groupBy("ean_em_campanha").agg(
    F.flatten(F.collect_list("eans_recomendar")).alias("eans_recomendar")
)

# Ordenar pelo score novamente e remover duplicatas de cada item da coluna "eans_recomendar"
new_records = []
for row in df.collect():
    ean_em_campanha = row["ean_em_campanha"]
    eans_recomendar = row["eans_recomendar"]

    # Ordenar pelo score (maior para menor)
    eans_recomendar.sort(key=lambda x: x["score"], reverse=True)

    # Remover duplicatas mantendo apenas o primeiro (maior score) para cada EAN
    seen_eans = set()
    unique_eans_recomendar = []

    for item in eans_recomendar:
        ean = item["ean"]
        # Verificar se já vimos este EAN antes
        if ean not in seen_eans:
            seen_eans.add(ean)
            unique_eans_recomendar.append(item)

    new_records.append(
        {
            "ean_em_campanha": ean_em_campanha,
            "eans_recomendar": unique_eans_recomendar,
        }
    )
df = spark.createDataFrame(new_records, schema=schema)

# COMMAND ----------

if DEBUG:
    # Deveria ter somente um único linha para esse ean campanha
    df.filter(F.col("ean_em_campanha") == EAN_TO_DEBUG).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando os dados

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(PRODUTOS_SUBSTITUTOS)
