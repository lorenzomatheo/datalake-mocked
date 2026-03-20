# Databricks notebook source
# MAGIC %md
# MAGIC # Propaga Tags por Similaridade de Produtos
# MAGIC
# MAGIC Este notebook propaga tags entre produtos com mesma composição farmacêutica:
# MAGIC - Agrupa produtos por (`principio_ativo`, `dosagem`, `forma_farmaceutica`)
# MAGIC - Seleciona o produto com maior `quality_score` (>= 40) como referência
# MAGIC - Propaga as tags do produto referência para produtos sem tags do mesmo grupo
# MAGIC
# MAGIC ## Regras de Negócio
# MAGIC - Apenas produtos com os 3 campos preenchidos (NOT NULL e não vazios)
# MAGIC - Normalização: lowercase + trim para comparação
# MAGIC - Threshold: quality_score >= 40 para ser doador
# MAGIC - Desempate: atualizado_em mais recente
# MAGIC - Grupos unários (1 produto) são ignorados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Configuração Inicial

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_mescla_tags_similaridade
from maggulake.utils.strings import normalize_text_alphanumeric
from maggulake.utils.time import agora_em_sao_paulo_str as agora

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment e Configurações

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "propaga_tags_por_similaridade",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark

# COMMAND ----------

# Configurações
DEBUG = dbutils.widgets.get("debug") == "true"
QUALITY_SCORE_THRESHOLD = 40.0
# Cria tabela se não existir
env.create_table_if_not_exists(
    Table.mescla_tags_similaridade,
    schema_mescla_tags_similaridade,
)

# COMMAND ----------

# Lê tabela de tags
gpt_tags_df = env.table(Table.enriquecimento_tags_produtos)
info_bula_df = env.table(Table.extract_product_info)
produtos_refined_df = env.table(Table.produtos_refined).filter(F.col("eh_medicamento"))

# COMMAND ----------
# retonar registros mais recentes
colunas_dados = [c for c in info_bula_df.columns if c not in ["ean", "atualizado_em"]]

df_info_bula_recente = (
    info_bula_df.groupBy("ean")
    .agg(F.max(F.struct("atualizado_em", *colunas_dados)).alias("mais_recente"))
    .select("ean", "mais_recente.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de Dados

# COMMAND ----------

# Lê produtos refined
produtos_refined = produtos_refined_df.select(
    "ean",
    F.col("principio_ativo").alias("principio_ativo_refined"),
    F.col("dosagem").alias("dosagem_refined"),
    F.col("forma_farmaceutica").alias("forma_farmaceutica_refined"),
    "quality_score",
    "atualizado_em",
)

# Lê tabela de enriquecimento info_bula
produtos_info_bula = df_info_bula_recente.select(
    "ean",
    F.col("principio_ativo").alias("principio_ativo_llm"),
    F.col("dosagem").alias("dosagem_llm"),
    F.col("forma_farmaceutica").alias("forma_farmaceutica_llm"),
)

# Join com priorização: LLM primeiro, fallback para refined
produtos_com_campos_priorizados = produtos_refined.join(
    produtos_info_bula, on="ean", how="left"
).select(
    "ean",
    F.coalesce(
        F.when(F.col("principio_ativo_llm") != "", F.col("principio_ativo_llm")),
        F.when(
            F.col("principio_ativo_refined") != "", F.col("principio_ativo_refined")
        ),
    ).alias("principio_ativo"),
    F.coalesce(
        F.when(F.col("dosagem_refined") != "", F.col("dosagem_refined")),
        F.when(F.col("dosagem_llm") != "", F.col("dosagem_llm")),
    ).alias("dosagem"),
    F.coalesce(
        F.when(F.col("forma_farmaceutica_llm") != "", F.col("forma_farmaceutica_llm")),
        F.when(
            F.col("forma_farmaceutica_refined") != "",
            F.col("forma_farmaceutica_refined"),
        ),
    ).alias("forma_farmaceutica"),
    "quality_score",
    "atualizado_em",
)

# COMMAND ----------

if DEBUG:
    print(
        f"{agora()} - Total de produtos para análise: {produtos_com_campos_priorizados.count()}"
    )
    print(f"{agora()} - Total de produtos com tags: {gpt_tags_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtragem e Normalização

# COMMAND ----------

# Filtra produtos com os 3 campos preenchidos
produtos_elegiveis = produtos_com_campos_priorizados.filter(
    (F.col("principio_ativo").isNotNull())
    & (F.col("principio_ativo") != "")
    & (F.col("dosagem").isNotNull())
    & (F.col("dosagem") != "")
    & (F.col("forma_farmaceutica").isNotNull())
    & (F.col("forma_farmaceutica") != "")
)

# COMMAND ----------

# Normaliza campos para comparação

normalize_udf = F.udf(normalize_text_alphanumeric, "string")

produtos_normalizados = (
    produtos_elegiveis.withColumn(
        "principio_ativo_norm", normalize_udf(F.col("principio_ativo"))
    )
    .withColumn("dosagem_norm", normalize_udf(F.col("dosagem")))
    .withColumn("forma_farmaceutica_norm", normalize_udf(F.col("forma_farmaceutica")))
)

# COMMAND ----------

# Join com tags para saber quais produtos já têm tags completas
produtos_com_info_tags = produtos_normalizados.join(
    gpt_tags_df.select(
        "ean",
        "tags_complementares",
        "tags_substitutos",
        "tags_potencializam_uso",
        "tags_atenuam_efeitos",
        "tags_agregadas",
        "match_tags_em",
    ),
    on="ean",
    how="left",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificação de Grupos e Produto Referência

# COMMAND ----------

# Define condição para tags completas (todas preenchidas)
condicao_tags_completas = (
    F.col("tags_complementares").isNotNull()
    & (F.col("tags_complementares") != "")
    & F.col("tags_substitutos").isNotNull()
    & (F.col("tags_substitutos") != "")
    & F.col("tags_potencializam_uso").isNotNull()
    & (F.col("tags_potencializam_uso") != "")
    & F.col("tags_atenuam_efeitos").isNotNull()
    & (F.col("tags_atenuam_efeitos") != "")
    & F.col("tags_agregadas").isNotNull()
    & (F.col("tags_agregadas") != "")
)

# COMMAND ----------

# Cria window para ranquear produtos dentro de cada grupo
window_spec = Window.partitionBy(
    "principio_ativo_norm", "dosagem_norm", "forma_farmaceutica_norm"
).orderBy(
    F.col("quality_score").desc_nulls_last(),
    F.col("atualizado_em").desc_nulls_last(),
)

# COMMAND ----------

# Adiciona ranking e conta produtos por grupo
produtos_ranqueados = produtos_com_info_tags.withColumn(
    "rank_no_grupo", F.row_number().over(window_spec)
).withColumn(
    "total_produtos_grupo",
    F.count("*").over(
        Window.partitionBy(
            "principio_ativo_norm", "dosagem_norm", "forma_farmaceutica_norm"
        )
    ),
)

# COMMAND ----------

# Filtra apenas grupos com 2+ produtos
produtos_em_grupos = produtos_ranqueados.filter(F.col("total_produtos_grupo") >= 2)

total_grupos = (
    produtos_em_grupos.select(
        "principio_ativo_norm", "dosagem_norm", "forma_farmaceutica_norm"
    )
    .distinct()
    .count()
)

# COMMAND ----------

# Seleciona produtos referência (rank 1 com quality_score >= 40 e tags completas)
produtos_referencia = produtos_em_grupos.filter(
    (F.col("rank_no_grupo") == 1)
    & (F.col("quality_score") >= QUALITY_SCORE_THRESHOLD)
    & condicao_tags_completas
).select(
    F.col("principio_ativo_norm").alias("ref_principio_ativo_norm"),
    F.col("dosagem_norm").alias("ref_dosagem_norm"),
    F.col("forma_farmaceutica_norm").alias("ref_forma_farmaceutica_norm"),
    F.col("ean").alias("ref_ean"),
    F.col("principio_ativo").alias("ref_principio_ativo"),
    F.col("dosagem").alias("ref_dosagem"),
    F.col("forma_farmaceutica").alias("ref_forma_farmaceutica"),
    F.col("quality_score").alias("ref_quality_score"),
    F.col("tags_complementares").alias("ref_tags_complementares"),
    F.col("tags_substitutos").alias("ref_tags_substitutos"),
    F.col("tags_potencializam_uso").alias("ref_tags_potencializam_uso"),
    F.col("tags_atenuam_efeitos").alias("ref_tags_atenuam_efeitos"),
    F.col("tags_agregadas").alias("ref_tags_agregadas"),
    F.col("total_produtos_grupo"),
)

if DEBUG:
    total_grupos_com_referencia = produtos_referencia.count()
    print(
        f"{agora()} - Grupos com produto referência válido (score >= {QUALITY_SCORE_THRESHOLD} e tags completas): {total_grupos_com_referencia}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logs e Métricas - Top Grupos

# COMMAND ----------

# Exibe top 10 grupos por quantidade de produtos
if DEBUG:
    print(f"\n{agora()} - Top 10 grupos com mais produtos:\n")
    print("=" * 120)

    top_grupos = (
        produtos_referencia.orderBy(F.col("total_produtos_grupo").desc())
        .limit(10)
        .select(
            "ref_principio_ativo",
            "ref_forma_farmaceutica",
            "ref_dosagem",
            "total_produtos_grupo",
            "ref_quality_score",
        )
    )
    for row in top_grupos.collect():
        print(
            f"  • {row.ref_principio_ativo} | {row.ref_forma_farmaceutica} | {row.ref_dosagem}"
        )
        print(
            f"    Produtos no grupo: {row.total_produtos_grupo} | Quality Score Ref: {row.ref_quality_score:.2f}"
        )
        print("-" * 120)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seleção de Produtos para Atualização

# COMMAND ----------

# Define condição para produtos com tag vazia ou com conteúdo inválido
condicao_alguma_tag_vazia = (
    F.col("tags_complementares").isNull()
    | (F.col("tags_complementares") == "")
    | (F.col("tags_complementares") == "Não se aplica")
    | (F.col("tags_complementares") == "Não aplicável")
    | F.col("tags_substitutos").isNull()
    | (F.col("tags_substitutos") == "")
    | (F.col("tags_substitutos") == "Não se aplica")
    | (F.col("tags_substitutos") == "Não aplicável")
    | F.col("tags_potencializam_uso").isNull()
    | (F.col("tags_potencializam_uso") == "")
    | (F.col("tags_potencializam_uso") == "Não se aplica")
    | (F.col("tags_potencializam_uso") == "Não aplicável")
    | F.col("tags_atenuam_efeitos").isNull()
    | (F.col("tags_atenuam_efeitos") == "")
    | (F.col("tags_atenuam_efeitos") == "Não se aplica")
    | (F.col("tags_atenuam_efeitos") == "Não aplicável")
    | F.col("tags_agregadas").isNull()
    | (F.col("tags_agregadas") == "")
    | (F.col("tags_agregadas") == "Não se aplica")
    | (F.col("tags_agregadas") == "Não aplicável")
)

# COMMAND ----------

# Produtos que precisam receber tags (não são referência e têm tags vazias ou com conteudo inválido)
produtos_para_atualizar = produtos_em_grupos.filter(
    (F.col("rank_no_grupo") > 1) & condicao_alguma_tag_vazia
).select(
    "ean",
    "principio_ativo_norm",
    "dosagem_norm",
    "forma_farmaceutica_norm",
    "principio_ativo",
    "dosagem",
    "forma_farmaceutica",
)

if DEBUG:
    total_produtos_para_atualizar = produtos_para_atualizar.count()
    print(
        f"\n{agora()} - Total de produtos que serão atualizados: {total_produtos_para_atualizar}"
    )

# COMMAND ----------

# Early exit se não há produtos para atualizar
if produtos_para_atualizar.count() < 1:
    dbutils.notebook.exit("Sem produtos para atualizar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Propagação das Tags

# COMMAND ----------

# Join produtos para atualizar com produtos referência
produtos_com_tags_propagadas = produtos_para_atualizar.join(
    produtos_referencia,
    (
        produtos_para_atualizar.principio_ativo_norm
        == produtos_referencia.ref_principio_ativo_norm
    )
    & (produtos_para_atualizar.dosagem_norm == produtos_referencia.ref_dosagem_norm)
    & (
        produtos_para_atualizar.forma_farmaceutica_norm
        == produtos_referencia.ref_forma_farmaceutica_norm
    ),
    how="inner",
).select(
    produtos_para_atualizar.ean,
    F.col("ref_tags_complementares").alias("tags_complementares"),
    F.col("ref_tags_substitutos").alias("tags_substitutos"),
    F.col("ref_tags_potencializam_uso").alias("tags_potencializam_uso"),
    F.col("ref_tags_atenuam_efeitos").alias("tags_atenuam_efeitos"),
    F.col("ref_tags_agregadas").alias("tags_agregadas"),
    F.current_timestamp().alias("atualizado_em"),
)

# COMMAND ----------

total_a_propagar = produtos_com_tags_propagadas.count()
print(f"{agora()} - Total de produtos com tags para mesclar: {total_a_propagar}")

if DEBUG and total_a_propagar > 0:
    print(f"\n{agora()} - Amostra de produtos que receberão tags:")
    produtos_com_tags_propagadas.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando Resultados

# COMMAND ----------

# Colunas de tags para atualizar
colunas_tags = [
    "tags_complementares",
    "tags_substitutos",
    "tags_potencializam_uso",
    "tags_atenuam_efeitos",
    "tags_agregadas",
    "atualizado_em",
]

# Prepara DataFrame final
df_para_salvar = produtos_com_tags_propagadas.select("ean", *colunas_tags)

df_para_salvar.write.format("delta").mode("overwrite").saveAsTable(
    Table.mescla_tags_similaridade.value
)

print(f"{agora()} - ✅ Merge concluído com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Final

# COMMAND ----------

print("\n" + "=" * 120)
print(f"{agora()} - RESUMO DA EXECUÇÃO")
print("=" * 120)
print(f"Total de grupos identificados: {total_grupos}")
print(
    f"Grupos com produto referência válido: {produtos_referencia.count()} (score >= {QUALITY_SCORE_THRESHOLD})"
)
print(f"Produtos atualizados: {total_a_propagar}")
print("=" * 120)
