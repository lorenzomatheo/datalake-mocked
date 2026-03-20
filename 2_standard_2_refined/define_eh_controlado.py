# Databricks notebook source
# MAGIC %md
# MAGIC # Define eh_controlado e eh_otc
# MAGIC
# MAGIC Este notebook define as colunas `eh_otc` e `eh_controlado` para os produtos.
# MAGIC
# MAGIC **Prioridade para `eh_otc`:**
# MAGIC 1. LMIP (Lista de Medicamentos Isentos de Prescrição) - fonte oficial da ANVISA
# MAGIC 2. IQVIA (já processado anteriormente no pipeline)
# MAGIC 3. Inferência por tarja (Sem Tarja = OTC)
# MAGIC
# MAGIC **Regras de negócio:**
# MAGIC - `eh_otc = True` → produto está na LMIP (match por principio_ativo + forma_farmaceutica)
# MAGIC - `eh_controlado = True` → tarja preta OU tipos de receita especiais OU na lista de controlados
# MAGIC - `eh_otc = True` implica `eh_controlado = False`
# MAGIC - `eh_controlado = True` implica `recomendar = False`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T

from maggulake.enums.medicamentos import TiposTarja
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.mappings.anvisa_lmip import normaliza_principio_ativo
from maggulake.utils.df_utils import remove_accents_lower
from maggulake.utils.strings import normalize_text_alphanumeric

# COMMAND ----------

normaliza_principio_ativo_udf = F.udf(normaliza_principio_ativo, T.StringType())
normalize_text_alphanumeric_udf = F.udf(normalize_text_alphanumeric, T.StringType())
remove_accents_lower_udf = F.udf(remove_accents_lower, T.StringType())

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "define_eh_controlado",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark
DEBUG = dbutils.widgets.get("debug") == "true"

# link sheets Controlados_PRT nº 344
sheets_controlados = dbutils.secrets.get(
    scope="databricks", key="sheets_termos_controlados"
)

# COMMAND ----------

produtos = spark.read.table(Table.produtos_em_processamento.value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carrega a LMIP e define eh_otc baseado na lista oficial da ANVISA
# MAGIC
# MAGIC A LMIP (Lista de Medicamentos Isentos de Prescrição) é a fonte oficial da ANVISA
# MAGIC para identificar medicamentos OTC/MIP.
# MAGIC
# MAGIC Match é feito por:
# MAGIC - `principio_ativo` normalizado (lowercase, sem acentos)
# MAGIC - `forma_farmaceutica` (array contains, também normalizado)
# MAGIC
# MAGIC TODO: Fase 2 - Adicionar validação de dosagem via LLM para casos onde a
# MAGIC concentração do produto é maior que a `concentracao_maxima` da LMIP.

# COMMAND ----------

# Carrega a LMIP
lmip_df = (
    spark.read.table(Table.lmip_anvisa.value)
    .select(
        "principio_ativo",
        "formas_farmaceuticas",
        "concentracao_maxima",
    )
    .cache()
)

print(f"Total de registros na LMIP: {lmip_df.count()}")

# COMMAND ----------

# Normaliza o princípio ativo dos produtos para fazer match com a LMIP
produtos_com_pa_normalizado = produtos.withColumn(
    "principio_ativo_normalizado",
    normaliza_principio_ativo_udf(F.col("principio_ativo")),
).withColumn(
    "forma_farmaceutica_normalizada",
    remove_accents_lower_udf(F.col("forma_farmaceutica")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Match com a LMIP
# MAGIC
# MAGIC O match é feito em duas etapas:
# MAGIC 1. Match exato por princípio ativo normalizado
# MAGIC 2. Match por forma farmacêutica (verifica se a forma do produto está no array de formas da LMIP)
# MAGIC
# MAGIC **Importante:** Se o produto tem forma farmacêutica nula, consideramos match apenas por princípio ativo.

# COMMAND ----------

# Join com a LMIP para identificar produtos OTC
produtos_com_lmip = (
    produtos_com_pa_normalizado.alias("p")
    .join(
        F.broadcast(lmip_df.alias("l")),
        F.col("p.principio_ativo_normalizado") == F.col("l.principio_ativo"),
        "left",
    )
    .withColumn(
        "match_forma_farmaceutica",
        F.when(
            F.col("p.forma_farmaceutica_normalizada").isNull(),
            F.lit(True),  # Se não tem forma farmacêutica, considera match só por PA
        ).otherwise(
            # Verifica se alguma forma farmacêutica da LMIP está contida na forma do produto
            # Usa LOWER() em ambos os lados para garantir case-insensitivity
            F.expr("""
                EXISTS(
                    l.formas_farmaceuticas,
                    forma -> LOWER(p.forma_farmaceutica_normalizada) LIKE CONCAT('%', LOWER(forma), '%')
                        OR LOWER(forma) LIKE CONCAT('%', LOWER(p.forma_farmaceutica_normalizada), '%')
                )
            """)
        ),
    )
    .withColumn(
        "eh_otc_lmip",
        F.when(
            F.col("l.principio_ativo").isNotNull() & F.col("match_forma_farmaceutica"),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
)

# Preserva o valor original de eh_otc antes de sobrescrever
produtos_com_lmip = produtos_com_lmip.withColumn("eh_otc_antes", F.col("eh_otc"))

# Conta matches
print(
    f"Produtos com match na LMIP (eh_otc_lmip=True): {produtos_com_lmip.filter('eh_otc_lmip').count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atualiza eh_otc com prioridade: LMIP > valor existente (IQVIA) > inferência por tarja

# COMMAND ----------

# Atualiza eh_otc com prioridade: LMIP > IQVIA (valor existente) > tarja
produtos_com_otc = produtos_com_lmip.withColumn(
    "fonte_eh_otc",
    F.when(F.col("eh_otc_lmip") == True, F.lit("LMIP"))
    .when(F.col("eh_otc").isNotNull(), F.lit("IQVIA"))
    .when(F.col("tarja") == "Sem Tarja", F.lit("TARJA"))
    .otherwise(F.lit(None)),
).withColumn(
    "eh_otc",
    F.when(F.col("eh_otc_lmip") == True, F.lit(True))  # LMIP tem prioridade máxima
    .when(F.col("eh_otc").isNotNull(), F.col("eh_otc"))  # Mantém IQVIA se já definido
    .when((F.col("tarja") == "Sem Tarja"), F.lit(True))  # Inferência por tarja
    .otherwise(F.col("eh_otc")),
)


# COMMAND ----------

# Mostra a distribuição de eh_otc por fonte
if DEBUG:
    print("=" * 60)
    print("DISTRIBUIÇÃO DE eh_otc POR FONTE")
    print("=" * 60)

    fonte_counts = (
        produtos_com_otc.groupBy("fonte_eh_otc").count().orderBy("fonte_eh_otc")
    )
    fonte_counts.limit(1000).display()

    total_otc = produtos_com_otc.filter("eh_otc = true").count()
    total_lmip = produtos_com_otc.filter("fonte_eh_otc = 'LMIP'").count()
    total_iqvia = produtos_com_otc.filter("fonte_eh_otc = 'IQVIA'").count()
    total_tarja = produtos_com_otc.filter("fonte_eh_otc = 'TARJA'").count()
    total_sem_fonte = produtos_com_otc.filter(
        "eh_otc = true AND fonte_eh_otc IS NULL"
    ).count()

    print(f"Total de produtos OTC (eh_otc=True): {total_otc}")
    print(f"  - Via LMIP (ANVISA): {total_lmip}")
    print(f"  - Via IQVIA (já existente): {total_iqvia}")
    print(f"  - Via inferência por tarja: {total_tarja}")
    if total_sem_fonte > 0:
        print(f"  - Sem fonte identificada: {total_sem_fonte}")
    print("=" * 60)

# COMMAND ----------

# Análise de mudanças: produtos que eram NOT OTC e agora são OTC
if DEBUG:
    print("=" * 60)
    print("ANÁLISE DE MUDANÇAS - PRODUTOS QUE VIRARAM OTC")
    print("=" * 60)

    # Antes: eh_otc original (do IQVIA ou NULL)
    # Depois: eh_otc após aplicar LMIP e tarja

    # Produtos que não eram OTC (NULL ou False) e agora são OTC (True)
    novos_otc = produtos_com_otc.filter(
        "(eh_otc_antes IS NULL OR eh_otc_antes = false) AND eh_otc = true"
    ).count()

    # Produtos que já eram OTC e continuam sendo
    mantidos_otc = produtos_com_otc.filter(
        "eh_otc_antes = true AND eh_otc = true"
    ).count()

    # Produtos que eram OTC mas não deveriam ser (não mudamos para false, só para true)
    # Isso não acontece no fluxo atual, mas vamos verificar
    eram_otc_agora_nao = produtos_com_otc.filter(
        "eh_otc_antes = true AND (eh_otc IS NULL OR eh_otc = false)"
    ).count()

    novos_otc_via_lmip = produtos_com_otc.filter(
        "(eh_otc_antes IS NULL OR eh_otc_antes = false) AND fonte_eh_otc = 'LMIP'"
    ).count()
    novos_otc_via_tarja = produtos_com_otc.filter(
        "(eh_otc_antes IS NULL OR eh_otc_antes = false) AND fonte_eh_otc = 'TARJA'"
    ).count()

    print(f"Produtos que viraram OTC (não eram antes): {novos_otc}")
    print(f"  - Via LMIP: {novos_otc_via_lmip}")
    print(f"  - Via Tarja: {novos_otc_via_tarja}")
    print(f"Produtos que já eram OTC (mantidos): {mantidos_otc}")
    if eram_otc_agora_nao > 0:
        print(f"⚠️ Produtos que eram OTC e deixaram de ser: {eram_otc_agora_nao}")
    print("=" * 60)

# COMMAND ----------

# ⚠️ ATENÇÃO: Produtos que eram explicitamente eh_otc=False e foram trocados para True
# Esses são os casos mais críticos - eram marcados como NÃO OTC e agora são OTC

if DEBUG:
    produtos_false_para_true = produtos_com_otc.filter(
        "eh_otc_antes = false AND eh_otc = true"
    )

    qtd_false_para_true = produtos_false_para_true.count()
    print("=" * 60)
    print("⚠️ CASOS CRÍTICOS: eh_otc=False → eh_otc=True")
    print("=" * 60)
    print(
        f"Total de produtos que eram eh_otc=False e viraram True: {qtd_false_para_true}"
    )

    if qtd_false_para_true > 0:
        print("\nExemplos desses produtos:")
        produtos_false_para_true.limit(1000).display()
    else:
        print("✅ Nenhum produto teve eh_otc alterado de False para True")
    print("=" * 60)


# COMMAND ----------

# Remove colunas temporárias
produtos_com_otc = produtos_com_otc.drop(
    "principio_ativo_normalizado",
    "forma_farmaceutica_normalizada",
    "match_forma_farmaceutica",
    "eh_otc_lmip",
    "eh_otc_antes",
    "formas_farmaceuticas",
    "concentracao_maxima",
    "fonte_eh_otc",
)

# Atualiza para usar apenas colunas do produto original
produtos_com_otc = produtos_com_otc.select(
    [F.col(f"p.{c}") if c not in ["eh_otc"] else F.col(c) for c in produtos.columns]
)

print(
    f"Produtos com eh_otc=True após LMIP: {produtos_com_otc.filter('eh_otc = true').count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define medicamentos controlados pela planilha ANVISA (sheets)

# COMMAND ----------

df_termos_controlados = spark.createDataFrame(pd.read_csv(sheets_controlados))

df_termos_controlados = (
    df_termos_controlados.select(
        F.col("molecula").cast(T.StringType()).alias("termos_controlados"),
        F.col("eh_controlado").cast(T.BooleanType()).alias("eh_controlado"),
    )
    .filter(F.col("eh_controlado") == True)
    .select(remove_accents_lower_udf(F.col("termos_controlados")).alias("nome"))
    .dropDuplicates()
)

# COMMAND ----------

# MAGIC %md ### Acha os medicamentos controlados buscando se o principio_ativo do  medicamento casa com a molécula que aparece na planilha `Controlados_PRT nº 344, de 12/05/1998`

# COMMAND ----------

produtos_para_match = (
    produtos_com_otc.drop('eh_controlado')
    # Principio_ativo normalizado
    .withColumn(
        "principio_ativo_limpo",
        normaliza_principio_ativo_udf(F.col("principio_ativo")),
    )
    # Extração principio ativo do nome
    .withColumn(
        "p_regex_normalizado",
        normalize_text_alphanumeric_udf(
            F.regexp_extract(F.col("nome"), r"(?i)^([\p{L}\-]+(?:\s+[\p{L}\-]+)?)", 1)
        ),
    )
    # Define o campo de busca: prioriza principio ativo limpo, se nulo e regex for >= 5, usa regex
    .withColumn(
        "principio_para_match",
        F.coalesce(
            F.col("principio_ativo_limpo"),
            F.when(
                F.length(F.col("p_regex_normalizado")) >= 5,
                F.col("p_regex_normalizado"),
            ),
        ),
    )
)

produtos_controlados = (
    produtos_para_match.alias("p")
    .join(
        F.broadcast(df_termos_controlados.alias("c_principio")),
        F.expr(
            "p.eh_medicamento = true"
            " AND p.principio_para_match LIKE concat('%', c_principio.nome, '%')"
        ),
        "left",
    )
    .selectExpr('p.*', 'c_principio.nome is not null as eh_controlado')
    .dropDuplicates(["ean"])
    .drop("principio_ativo_limpo", "p_regex_normalizado", "principio_para_match")
)

produtos_controlados.filter("eh_controlado").count()

# COMMAND ----------

# MAGIC %md ### Inclui tarja preta e alguns tipos de receita como controlados

# COMMAND ----------

tipos_de_receita_controlados = [
    "a1 amarela",
    "a2 amarela",
    "a3 amarela",
    "b1 azul",
    "b2 azul",
    "c1 branca 2 vias",
    "c2 branca",
    "c5 branca 2 vias",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Regras de consistência entre eh_otc e eh_controlado
# MAGIC
# MAGIC - Se `eh_otc = True`, então `eh_controlado = False` (OTC não pode ser controlado)
# MAGIC
# MAGIC - Se `eh_medicamento = False`, então `eh_otc = False` E `eh_controlado = False`
# MAGIC
# MAGIC - Se `tarja = 'preta'` ou `tarja = 'vermelha sob restricao'`, então `eh_controlado = True` E `eh_otc = False`

# COMMAND ----------

produtos_controlados = produtos_controlados.withColumn(
    "eh_otc",
    F.when(F.col('eh_medicamento') == False, F.lit(False))  # Não medicamento não é OTC
    .when(
        F.col("tarja").ilike(f"%{TiposTarja.PRETA.value.lower()}%"), F.lit(False)
    )  # Tarja Preta não é OTC
    .when(
        F.col('tarja').ilike(f"%{TiposTarja.VERMELHA_SOB_RESTRICAO.value.lower()}%"),
        F.lit(False),
    )  # Tarja Vermelha Sob Restrição não é OTC
    .when(
        (F.col('eh_medicamento')) & (F.col('eh_controlado') == False), F.lit(True)
    )  # Medicamento não controlados é otc
    .when(
        (F.col('eh_medicamento')) & (F.col('eh_controlado')), F.lit(False)
    )  # Medicamento controlado não é otc
    .otherwise(F.col("eh_otc")),
).withColumn(
    "eh_controlado",
    # Se não é medicamento, não é controlado
    F.when(F.col('eh_medicamento') == False, F.lit(False))
    # OTC não é controlado
    .when(F.col("eh_otc") == True, F.lit(False))
    # Regras de negócio para medicamentos controlados
    .when(F.col("tarja").ilike(f"%{TiposTarja.PRETA.value.lower()}%"), F.lit(True))
    .when(
        F.col("tarja").ilike(f"%{TiposTarja.VERMELHA_SOB_RESTRICAO.value.lower()}%"),
        F.lit(True),
    )
    .when(F.col("tipo_receita").isin(tipos_de_receita_controlados), F.lit(True))
    .otherwise(F.col("eh_controlado")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salva os resultados

# COMMAND ----------

produtos_controlados.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(
    Table.produtos_em_processamento.value
)
