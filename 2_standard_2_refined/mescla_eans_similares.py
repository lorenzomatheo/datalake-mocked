# Databricks notebook source
# MAGIC %md
# MAGIC # Mesclar produtos de `eans` mas que na verdade sao iguais
# MAGIC
# MAGIC Nesse notebook vamos manipular as colunas `eans_alternativos`, `status`.

# COMMAND ----------

from collections import namedtuple

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.mappings.minas_mais import MinasMaisParser
from maggulake.produtos.valida_ean import valida_ean
from maggulake.schemas import (
    refined_eans_alternativos,
    schema_ean_alternativo_incompativel,
)
from maggulake.tables import Raw
from maggulake.utils.strings import normalize_text_alphanumeric

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções Auxiliares

# COMMAND ----------

# TODO: Da pra dar uma boa limpada nesse notebook movendo essas funcoes para utils e tbm dividindo o notebook em partes menores. Isso facilita a manutencao.


def criar_correcoes_hardcoded(spark):
    """Cria DataFrame com correções manuais de EANs equivalentes.

    TODO: Mover para planilha de correções manuais ou arquivo externo.
    """
    correcoes_named_tuple = namedtuple(
        "correcao", ["ean", "ean_principal", "eans_alternativos"]
    )

    # fmt: off
    correcoes = [
        # balinha valda 50g
        correcoes_named_tuple(ean="7891137003698", ean_principal="7891137003698", eans_alternativos=["7891137003711", "78900776"]),
        correcoes_named_tuple(ean="7891137003711", ean_principal="7891137003698", eans_alternativos=["7891137003698", "78900776"]),
        correcoes_named_tuple(ean="78900776", ean_principal="7891137003698", eans_alternativos=["7891137003698", "7891137003711"]),
        # Paracetamol 750MG/CPR 10CPR CARTELA - PRATI
        correcoes_named_tuple(ean="7899547543902", ean_principal="7899547536904", eans_alternativos=["7899547536904", "7899547543773", "7898148295081", "7895144423333"]),
        correcoes_named_tuple(ean="7899547543773", ean_principal="7899547536904", eans_alternativos=["7899547536904", "7899547543902", "7898148295081", "7895144423333"]),
        correcoes_named_tuple(ean="7899547536904", ean_principal="7899547536904", eans_alternativos=["7899547543902", "7899547543773", "7898148295081", "7895144423333"]),
        correcoes_named_tuple(ean="7898148295081", ean_principal="7899547536904", eans_alternativos=["7899547536904", "7899547543902", "7899547543773", "7895144423333"]),
        correcoes_named_tuple(ean="7895144423333", ean_principal="7899547536904", eans_alternativos=["7899547536904", "7899547543902", "7899547543773", "7898148295081"]),
        # Produtos sem alternativos (resetados)
        correcoes_named_tuple(ean="7896714255903", ean_principal="7896714255903", eans_alternativos=[]),
        correcoes_named_tuple(ean="7896004749730", ean_principal="7896004749730", eans_alternativos=[]),
        correcoes_named_tuple(ean="7896004749648", ean_principal="7896004749648", eans_alternativos=[]),
        correcoes_named_tuple(ean="7894916149808", ean_principal="7894916149808", eans_alternativos=[]),
        correcoes_named_tuple(ean="7899547513219", ean_principal="7899547513219", eans_alternativos=[]),
        correcoes_named_tuple(ean="2000000144221", ean_principal="2000000144221", eans_alternativos=[]),
        correcoes_named_tuple(ean="7891317127800", ean_principal="7891317127800", eans_alternativos=[]),
        correcoes_named_tuple(ean="686235", ean_principal="686235", eans_alternativos=[]),
        correcoes_named_tuple(ean="7896181926238", ean_principal="7896181926238", eans_alternativos=[]),
        correcoes_named_tuple(ean="7896422501682", ean_principal="7896422501682", eans_alternativos=[]),
        correcoes_named_tuple(ean="7899095257832", ean_principal="7899095257832", eans_alternativos=[]),
        correcoes_named_tuple(ean="7896523227443", ean_principal="7896523227443", eans_alternativos=[]),
        correcoes_named_tuple(ean="7898216365555", ean_principal="7898216365555", eans_alternativos=[]),
        correcoes_named_tuple(ean="9000000012349", ean_principal="9000000012349", eans_alternativos=[]),
        correcoes_named_tuple(ean="7895296240109", ean_principal="7895296240109", eans_alternativos=[]),
        correcoes_named_tuple(ean="7896658025471", ean_principal="7896658025471", eans_alternativos=[]),
    ]
    # fmt: on

    df = spark.createDataFrame(
        [(c.ean, c.ean_principal, c.eans_alternativos) for c in correcoes],
        ["ean_correcao", "ean_principal_correcao", "eans_alternativos_correcao"],
    )
    eans_lista = [c.ean for c in correcoes]
    return df, eans_lista


def aplicar_correcoes_ean_principal(df, correcoes_df):
    """Aplica correções de ean_principal ao DataFrame."""
    return (
        df.join(
            correcoes_df.select("ean_correcao", "ean_principal_correcao"),
            df["ean"] == correcoes_df["ean_correcao"],
            "left",
        )
        .withColumn(
            "ean_principal",
            F.when(
                F.col("ean_correcao").isNotNull(), F.col("ean_principal_correcao")
            ).otherwise(F.col("ean_principal")),
        )
        .drop("ean_correcao", "ean_principal_correcao")
    )


def aplicar_correcoes_eans_alternativos(df, correcoes_df):
    """Aplica correções de eans_alternativos ao DataFrame."""
    return (
        df.join(
            correcoes_df.select("ean_correcao", "eans_alternativos_correcao"),
            df["ean"] == correcoes_df["ean_correcao"],
            "left",
        )
        .withColumn(
            "eans_alternativos",
            F.when(
                F.col("ean_correcao").isNotNull(), F.col("eans_alternativos_correcao")
            ).otherwise(F.col("eans_alternativos")),
        )
        .drop("ean_correcao", "eans_alternativos_correcao")
    )


def preencher_campos_produto_principal(df, columns_to_exclude):
    """Preenche campos nulos usando valores do produto principal escolhido.

    Usa struct para single-pass window function (melhor performance).
    """
    window_spec = Window.partitionBy("ean_principal").orderBy(F.desc("ean_escolhido"))

    columns_to_fill = [col for col in df.columns if col not in columns_to_exclude]
    struct_cols = [F.col(c) for c in columns_to_fill]

    df_with_struct = df.withColumn(
        "_fill_struct",
        F.first(F.struct(*struct_cols), ignorenulls=True).over(window_spec),
    )

    for col_name in columns_to_fill:
        df_with_struct = df_with_struct.withColumn(
            col_name,
            F.when(
                (F.col("ean_escolhido") == False) & F.col(col_name).isNull(),
                F.col(f"_fill_struct.{col_name}"),
            ).otherwise(F.col(col_name)),
        )

    return df_with_struct.drop("_fill_struct")


def executar_fechamento_transitivo(df_para_grafo, max_iterations=5):
    """Executa algoritmo de fechamento transitivo para juntar EANs alternativos.

    O amigo do meu amigo é meu amigo.

    Returns:
        DataFrame com eans_alternativos expandidos transitivamente
        bool indicando se convergiu
    """
    current_eans = df_para_grafo.select("ean", "eans_alternativos")
    num_produtos_grafo = current_eans.count()

    if num_produtos_grafo == 0:
        print("⚠️ Nenhum produto para processar no grafo transitivo")
        return current_eans, True

    print(f"Processando {num_produtos_grafo} produtos no fechamento transitivo")
    convergiu = False

    for iteration in range(max_iterations):
        print(f"Iteração {iteration + 1} do fechamento transitivo...")

        # Explode para encontrar conexões
        expanded = current_eans.select(
            F.col("ean").alias("ean_origem"),
            F.explode("eans_alternativos").alias("ean_conexao"),
        )

        # Join para pegar alternativos dos alternativos
        expanded_with_friends = expanded.alias("e").join(
            current_eans.alias("c"), F.col("e.ean_conexao") == F.col("c.ean"), "left"
        )

        # Agrega novos alternativos
        new_alternativos = expanded_with_friends.groupBy("ean_origem").agg(
            F.array_distinct(
                F.flatten(F.collect_set(F.coalesce("c.eans_alternativos", F.array())))
            ).alias("novos_eans")
        )

        # Merge com alternativos existentes e remove auto-referências
        updated = (
            current_eans.alias("curr")
            .join(
                new_alternativos.alias("new"),
                F.col("curr.ean") == F.col("new.ean_origem"),
                "left",
            )
            .withColumn(
                "eans_alternativos",
                F.array_distinct(
                    F.concat(
                        F.coalesce("curr.eans_alternativos", F.array()),
                        F.coalesce("new.novos_eans", F.array()),
                    )
                ),
            )
            .withColumn(
                "eans_alternativos", F.expr("filter(eans_alternativos, x -> x != ean)")
            )
            .select("curr.ean", "eans_alternativos")
        )

        # Checkpoint para quebrar linhagem
        if iteration % 2 == 1:
            updated = updated.checkpoint()

        # Verifica convergência (single-pass aggregation com null-safety)
        tamanhos = (
            current_eans.select(
                F.coalesce(
                    F.sum(F.size(F.coalesce("eans_alternativos", F.array()))), F.lit(0)
                ).alias("anterior")
            )
            .crossJoin(
                updated.select(
                    F.coalesce(
                        F.sum(F.size(F.coalesce("eans_alternativos", F.array()))),
                        F.lit(0),
                    ).alias("novo")
                )
            )
            .collect()[0]
        )

        tamanho_anterior = tamanhos["anterior"]
        tamanho_novo = tamanhos["novo"]
        current_eans = updated

        if tamanho_anterior == tamanho_novo:
            print(f"✅ Convergiu na iteração {iteration + 1}")
            convergiu = True
            break
        if iteration == max_iterations - 1:
            print(
                f"⚠️ Fechamento transitivo não convergiu após {iteration + 1} iterações"
            )
            print(f"   Total de EANs alternativos: {tamanho_novo}")

    return current_eans.repartition(400, "ean"), convergiu


def filtrar_eans_por_marca_fabricante(df, normalize_udf):
    """Filtra eans_alternativos removendo EANs com marca/fabricante incompatíveis.

    Returns:
        tuple: (df_filtrado, df_incompativeis)
    """
    # Normaliza marca/fabricante
    df = df.withColumn("marca_norm", normalize_udf(F.col("marca"))).withColumn(
        "fabricante_norm", normalize_udf(F.col("fabricante"))
    )

    # Cria ref_marca para broadcast
    ref_marca = df.select(
        F.col("ean").alias("alt"),
        F.col("marca_norm").alias("marca_alt_norm"),
        F.col("fabricante_norm").alias("fabricante_alt_norm"),
    ).filter(
        F.col("marca_alt_norm").isNotNull() & F.col("fabricante_alt_norm").isNotNull()
    )

    # Join para validar compatibilidade
    joined = df.select(
        "ean",
        "marca_norm",
        "fabricante_norm",
        F.explode_outer("eans_alternativos").alias("alt"),
    ).join(F.broadcast(ref_marca), on="alt", how="left")

    # EANs incompatíveis (marca/fabricante diferente)
    incompativeis = (
        joined.filter(
            (F.col("alt").isNotNull())
            & (
                F.col("marca_norm").isNull()
                | F.col("marca_alt_norm").isNull()
                | (F.col("marca_alt_norm") != F.col("marca_norm"))
                | F.col("fabricante_norm").isNull()
                | F.col("fabricante_alt_norm").isNull()
                | (F.col("fabricante_alt_norm") != F.col("fabricante_norm"))
            )
        )
        .select(
            F.col("ean").alias("ean_principal"), F.col("alt").alias("ean_alternativo")
        )
        .distinct()
    )

    # EANs compatíveis
    filtrados = (
        joined.filter(
            F.col("marca_norm").isNotNull()
            & (F.col("marca_alt_norm") == F.col("marca_norm"))
            & F.col("fabricante_norm").isNotNull()
            & (F.col("fabricante_alt_norm") == F.col("fabricante_norm"))
        )
        .groupBy("ean")
        .agg(F.array_distinct(F.collect_list("alt")).alias("eans_alternativos"))
    )

    # Atualiza DataFrame original
    df_final = (
        df.drop("eans_alternativos")
        .alias("p")
        .join(filtrados.alias("f"), on="ean", how="left")
        .select(
            "p.*",
            F.coalesce(
                F.col("f.eans_alternativos"), F.lit([]).cast("array<string>")
            ).alias("eans_alternativos"),
        )
        .drop("marca_norm", "fabricante_norm")
    )

    return df_final, incompativeis


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Ambiente
# MAGIC

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "mescla_eans_similares",
    dbutils,
    widgets={"clear_cache": ["false", "true"]},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição do ambiente

# COMMAND ----------

# Configurações

spark = env.spark
postgres = env.postgres_replica_adapter

CLEAR_CACHE = dbutils.widgets.get("clear_cache") == "true"
DEBUG = dbutils.widgets.get("debug") == "true"

# Configurações Spark para otimização de memória
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria tabela refined_eans_alternativos e ean_alternativo_incompativel se não existir

# COMMAND ----------

env.create_table_if_not_exists(
    Table.ean_alternativo_incompativel, schema_ean_alternativo_incompativel
)

# COMMAND ----------

env.create_table_if_not_exists(
    Table.refined_eans_alternativos, schema=refined_eans_alternativos.schema
)

# COMMAND ----------

if CLEAR_CACHE:
    spark.catalog.clearCache()
    print("Cache limpo")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura da tabela de `produtos_standard`

# COMMAND ----------

produtos_standard = env.table(Table.produtos_standard).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriquece marca/fabricante com dados do websearch
# MAGIC
# MAGIC Integra marca e fabricante enriquecidos via LLM (websearch) quando disponíveis.
# MAGIC Prioriza dados de produtos_standard sobre websearch.

# COMMAND ----------

enriquecimento_websearch = (
    env.table(Table.enriquecimento_websearch)
    .select("ean", "marca_websearch", "fabricante_websearch")
    .cache()
)

produtos_standard = (
    produtos_standard.alias("p")
    .join(enriquecimento_websearch.alias("w"), on="ean", how="left")
    .withColumn("marca", F.coalesce(F.col("p.marca"), F.col("w.marca_websearch")))
    .withColumn(
        "fabricante", F.coalesce(F.col("p.fabricante"), F.col("w.fabricante_websearch"))
    )
    .select("p.*", "marca", "fabricante")
)

# Libera memória do cache temporário
enriquecimento_websearch.unpersist()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos dados do Minas Mais

# COMMAND ----------

minas_mais_table = env.table(Raw.minas_mais)
minas_mais_parser = MinasMaisParser(spark)
minas_mais_df = minas_mais_parser.to_produtos_raw(minas_mais_table.spark_df)
minas_mais_eans = (
    minas_mais_df.select("ean", "eans_alternativos")
    .withColumn(
        "eans_alternativos", F.array_remove(F.col("eans_alternativos"), F.col("ean"))
    )  # Remove EAN principal
    .cache()
)


# COMMAND ----------

alternativos_demais = minas_mais_eans.filter(F.size("eans_alternativos") > 50).count()

if alternativos_demais > 0:
    print(f"Removendo {alternativos_demais} registros com mais de 50 eans alternativos")

    minas_mais_eans = minas_mais_eans.filter(F.size("eans_alternativos") <= 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Métrica de Qualidade de Cadastro

# COMMAND ----------


# Calcula qualidade de cadastro baseado em campos nulos ou vazios
def _is_null_or_empty_score(col_name, data_type):
    """Retorna 1 se coluna é nula/vazia, 0 caso contrário. Usado para cálculo de qualidade."""
    if isinstance(data_type, T.ArrayType):
        return F.when(F.size(F.col(col_name)) == 0, 1).otherwise(0)
    else:
        return F.when(F.col(col_name).isNull() | (F.col(col_name) == ""), 1).otherwise(
            0
        )


# COMMAND ----------

qualidade_cadastro_expr = sum(
    _is_null_or_empty_score(col.name, col.dataType) for col in produtos_standard.schema
)
produtos_standard = produtos_standard.withColumn(
    "qualidade_cadastro", 1 - qualidade_cadastro_expr / len(produtos_standard.columns)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agrupamento de produtos de EANs equivalentes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Colunas auxiliares: eans_alternativos, ean_sem_zeros_esquerda, ean_principal

# COMMAND ----------

produtos_standard = (
    produtos_standard.withColumn(
        "eans_alternativos",
        F.when(F.col("eans_alternativos").isNull(), F.array()).otherwise(
            F.col("eans_alternativos")
        ),
    )
    .withColumn("ean_sem_zeros_esquerda", F.regexp_replace(F.col("ean"), "^[0]+", ""))
    .withColumn("ean_principal", F.col("ean_sem_zeros_esquerda"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correções Manuais de EANs Equivalentes
# MAGIC
# MAGIC Aplica correções hardcoded para produtos de EANs diferentes mas equivalentes.

# COMMAND ----------

correcoes_df, eans_correcoes = criar_correcoes_hardcoded(spark)
produtos_standard = aplicar_correcoes_ean_principal(produtos_standard, correcoes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encontra eans em campanha para priorizar

# COMMAND ----------

eans_em_campanha = set(postgres.get_eans_em_campanha(spark, tipo=None))

# COMMAND ----------

produtos_standard = produtos_standard.withColumn(
    "esta_em_campanha", F.col("ean").isin(eans_em_campanha)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coluna `ean_escolhido` para preenchimento do produto principal

# COMMAND ----------

window_spec = Window.partitionBy("ean_principal").orderBy(
    F.desc("esta_em_campanha"), F.desc("qualidade_cadastro")
)
produtos_standard = produtos_standard.withColumn(
    "ean_escolhido",
    F.when(F.row_number().over(window_spec) == 1, True).otherwise(False),
)

# COMMAND ----------

# Preenche campos nulos do produto principal escolhido
columns_to_exclude = [
    "id",
    "ean",
    "ean_sem_zeros_esquerda",
    "qualidade_cadastro",
    "ean_escolhido",
    "ean_principal",
    "fonte",
]
produtos_standard = preencher_campos_produto_principal(
    produtos_standard, columns_to_exclude
).cache()
if DEBUG:
    produtos_standard.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atualização da coluna "eans_alternativos"

# COMMAND ----------

# Otimizado: groupBy ao invés de window function para evitar partições grandes
all_eans_grouped = produtos_standard.groupBy("ean_sem_zeros_esquerda").agg(
    F.collect_list("ean").alias("all_eans")
)

produtos_standard = produtos_standard.join(
    all_eans_grouped, "ean_sem_zeros_esquerda", "left"
)

# eans_alternativos vai ser o primeiro nao vazio entre eans_alternativos e all_eans
produtos_standard = produtos_standard.withColumn(
    "eans_alternativos",
    F.when(
        F.size(F.col("eans_alternativos")) == 0,
        F.expr("filter(all_eans, x -> x != ean)"),
    ).otherwise(F.col("eans_alternativos")),
).drop("all_eans")

# COMMAND ----------

# MAGIC %md
# MAGIC Busca eans com nome presentes no nomes_alternativos de produtos que tem o campo preenchido

# COMMAND ----------

products_exploded = (
    produtos_standard.filter(
        F.col("nomes_alternativos").isNotNull()
        & (F.size(F.col("nomes_alternativos")) > 0)
    )
    .select(
        F.col("ean").alias("campaign_ean"),
        F.explode(F.col("nomes_alternativos")).alias("alt_name_raw"),
    )
    .select(F.col("campaign_ean"), F.trim(F.col("alt_name_raw")).alias("alt_name"))
    .cache()
)

# Match ean 'nome' com algum nome alternativo e agrupa
result = (
    produtos_standard.join(
        products_exploded, F.trim(F.col("nome")) == F.col("alt_name"), "inner"
    )
    .filter(F.col("campaign_ean") != F.col("ean"))
    .groupBy(F.col("campaign_ean").alias('ean'))
    .agg(F.collect_list(F.col("ean")).alias("matched_eans"))
)


# COMMAND ----------


@F.pandas_udf("array<string>")
def filtra_eans_validos_udf(eans_coluna):
    def filtra_uma_linha(eans):
        if eans is None:
            return []

        return [ean for ean in eans if valida_ean(ean)]

    return eans_coluna.apply(filtra_uma_linha)


# COMMAND ----------

produtos_standard = (
    produtos_standard.join(result, how="left", on=['ean'])
    .withColumn(
        'eans_alternativos',
        F.when(
            F.col('matched_eans').isNotNull(),
            F.array_distinct(
                F.concat(
                    F.coalesce(F.col('eans_alternativos'), F.array()),
                    F.col('matched_eans'),
                )
            ),
        ).otherwise(F.coalesce(F.col('eans_alternativos'), F.array())),
    )
    .drop('matched_eans')
    .withColumn("eans_alternativos", filtra_eans_validos_udf("eans_alternativos"))
)

# Libera memória de DataFrames temporários
products_exploded.unpersist()

if DEBUG:
    produtos_standard.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Junta com eans_alternativos Minas Mais

# COMMAND ----------

# Apenas produtos que existem em produtos_standard

produtos_standard = (
    produtos_standard.join(
        minas_mais_eans.select(
            "ean", F.col("eans_alternativos").alias("eans_alternativos_mm")
        ),
        on="ean",
        how="left",
    )
    .withColumn(
        'eans_alternativos',
        F.when(
            F.col('eans_alternativos_mm').isNotNull(),
            F.array_distinct(
                F.concat(F.col('eans_alternativos'), F.col('eans_alternativos_mm'))
            ),
        ).otherwise(F.array_distinct(F.col('eans_alternativos'))),
    )
    .drop('eans_alternativos_mm')
)

# Libera memória
minas_mais_eans.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação da coluna `status` ("ativo" ou "inativo")

# COMMAND ----------

# Status: ativo se ean == ean_principal. Se inativo sem alternativos, torna ativo
condicao_sem_alt = (F.col("ean_principal") != F.col("ean")) & (
    F.size(F.col("eans_alternativos")) == 0
)

produtos_standard = produtos_standard.withColumn(
    "status",
    F.when(F.col("ean_principal") == F.col("ean"), "ativo")
    .when(condicao_sem_alt, "ativo")
    .otherwise("inativo"),
).withColumn(
    "eans_alternativos",
    F.when(condicao_sem_alt, F.array(F.col("ean_sem_zeros_esquerda"))).otherwise(
        F.col("eans_alternativos")
    ),
)

# COMMAND ----------

produtos_standard = produtos_standard.drop(
    "ean_principal",
    "all_eans",
    "qualidade_cadastro",
    "ean_escolhido",
    "ean_sem_zeros_esquerda",
    "esta_em_campanha",
).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Junta eans_alternativos (Otimizado com algoritmo iterativo)
# MAGIC
# MAGIC O amigo do meu amigo e meu amigo
# MAGIC
# MAGIC Algoritmo de fechamento transitivo iterativo para evitar explosão de memória.
# MAGIC Filtra produtos com mais de 50 EANs alternativos antes de processar.

# COMMAND ----------

spark.sparkContext.setCheckpointDir("/tmp/maggu-checkpoints")

# Filtro de segurança: remove produtos com arrays gigantescos
MAX_EANS_SAFE = 50
produtos_para_grafo = produtos_standard.filter(
    (F.size("eans_alternativos") > 0) & (F.size("eans_alternativos") <= MAX_EANS_SAFE)
)
produtos_sem_grafo = produtos_standard.filter(
    (F.size("eans_alternativos") == 0) | (F.size("eans_alternativos") > MAX_EANS_SAFE)
)

print(f"Produtos para grafo: {produtos_para_grafo.count()}")
print(f"Produtos sem grafo: {produtos_sem_grafo.count()}")

# COMMAND ----------

# TODO: Adicionar validação de propriedades de fechamento transitivo (simetria: se A→B então B→A)
# para garantir consistência do grafo. Não crítico para o caso de uso atual.

# Executa fechamento transitivo (O amigo do meu amigo é meu amigo)
eans_com_alternativos, convergiu = executar_fechamento_transitivo(produtos_para_grafo)
num_produtos_grafo = eans_com_alternativos.count()

# COMMAND ----------

if num_produtos_grafo > 0:
    # Junta resultado do grafo com produtos originais
    produtos_com_grafo = (
        produtos_para_grafo.drop("eans_alternativos")
        .alias("p")
        .join(eans_com_alternativos.alias("e"), on="ean", how="left")
        .select("p.*", "e.eans_alternativos")
    )

    # Union com produtos que não entraram no grafo
    produtos_standard = produtos_com_grafo.unionByName(produtos_sem_grafo).checkpoint()

    total_eans_alternativos = produtos_standard.agg(
        F.coalesce(F.sum(F.size(F.coalesce("eans_alternativos", F.array()))), F.lit(0))
    ).collect()[0][0]

    print(f"✅ Grafo de EANs processado com {produtos_standard.count()} produtos")
    print(f"   Total de EANs alternativos: {total_eans_alternativos}")
else:
    # Sem produtos para processar no grafo, usa produtos originais
    produtos_standard = produtos_sem_grafo.checkpoint()
    print(
        f"✅ Processamento concluído com {produtos_standard.count()} produtos (sem grafo transitivo)"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retirando ean_alternativo de marca e fabricante diferente do ean_principal

# COMMAND ----------

normalize_udf = F.udf(normalize_text_alphanumeric, "string")
produtos_standard, df_ean_alternativos_incompativel = filtrar_eans_por_marca_fabricante(
    produtos_standard, normalize_udf
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplica correções de eans_alternativos (FINAL)

# COMMAND ----------

produtos_standard = aplicar_correcoes_eans_alternativos(produtos_standard, correcoes_df)

if DEBUG:
    display(
        produtos_standard.filter(F.col("ean").isin(eans_correcoes))
        .select(["id", "ean", "eans_alternativos", "nome", "fonte", "status"])
        .limit(50)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove duplicados priorizando produtos ativos
# MAGIC
# MAGIC Validações foram movidas para notebook dedicado: 3_analytics/qualidade/valida_produtos_eans_alternativos.py

# COMMAND ----------

# Remove duplicados priorizando:
# 1. Produtos ativos (status == "ativo")
# 2. Produtos em campanha
# 3. Produtos com fonte prioritária (campanhas > guia > outros)
# 4. Primeira ocorrência (para ter resultado determinístico)
window_dedup = Window.partitionBy("ean").orderBy(
    F.when(F.col("status") == "ativo", 0).otherwise(1),
    F.when(F.col("esta_em_campanha"), 0).otherwise(1),
    F.when(F.col("fonte").contains("campanha"), 0)
    .when(F.col("fonte").contains("guia"), 1)
    .otherwise(2),
    F.col("id"),  # Desempate determinístico
)

produtos_standard = (
    produtos_standard.withColumn(
        "esta_em_campanha", F.col("ean").isin(eans_em_campanha)
    )
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "esta_em_campanha")
    .checkpoint()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva no Delta

# COMMAND ----------

# Salva tabela de incompatibilidades (marca/fabricante diferentes)
df_ean_alternativos_incompativel.write.format("delta").mode("overwrite").saveAsTable(
    Table.ean_alternativo_incompativel.value
)

# COMMAND ----------

eans_alternativos_final = produtos_standard.select(
    "ean", "eans_alternativos", "status"
).dropDuplicates()

eans_alternativos_final.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(Table.refined_eans_alternativos.value)
