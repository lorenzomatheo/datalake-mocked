from dataclasses import dataclass
from typing import Literal

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from maggulake.environment import Environment, Table
from maggulake.mappings.buscador_bigdatacorp import BuscadorBigDataCorp

FonteExterna = Literal["BigDataCorp"]


@dataclass(frozen=True)
class ProdutosTextoCurtoConfig:
    env: Environment
    tabela_produtos_texto_curto: Table = Table.produtos_nome_descricao_curta
    tabela_produtos_texto_externos: Table = Table.produtos_nome_descricao_externos
    tabela_produtos_raw: Table = Table.produtos_raw
    fonte_bigboost: FonteExterna = "BigDataCorp"


PRODUTOS_TEXTO_CURTO_SCHEMA = """
    id STRING,
    ean STRING,
    nome STRING,
    descricao STRING,
    nome_curto BOOLEAN,
    descricao_curta BOOLEAN,
    nome_length INT,
    descricao_length INT,
    gerado_em TIMESTAMP,
    atualizado_em TIMESTAMP
"""

PRODUTOS_TEXTO_EXTERNOS_SCHEMA = """
    ean STRING,
    nome STRING,
    descricao STRING,
    fonte_nome STRING,
    fonte_descricao STRING,
    atualizado_em TIMESTAMP
"""


def _cria_tabela_produtos_texto_curto(config: ProdutosTextoCurtoConfig) -> None:
    config.env.create_table_if_not_exists(
        config.tabela_produtos_texto_curto,
        PRODUTOS_TEXTO_CURTO_SCHEMA,
    )


def _cria_tabela_produtos_texto_externos(config: ProdutosTextoCurtoConfig) -> None:
    config.env.create_table_if_not_exists(
        config.tabela_produtos_texto_externos,
        PRODUTOS_TEXTO_EXTERNOS_SCHEMA,
    )


def _upsert_delta_table(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
    keys: list[str],
) -> None:
    if df.isEmpty():
        return

    condition = " AND ".join([f"target.{key} = source.{key}" for key in keys])
    delta_table = DeltaTable.forName(spark, table_name)

    (
        delta_table.alias("target")
        .merge(df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def prepara_produtos_texto_curto(produtos_df: DataFrame, min_len: int) -> DataFrame:
    nome_length = F.length(F.coalesce(F.col("nome"), F.lit("")))
    descricao_length = F.length(F.coalesce(F.col("descricao"), F.lit("")))

    return (
        produtos_df.withColumn("nome_length", nome_length)
        .withColumn("descricao_length", descricao_length)
        .withColumn("nome_curto", F.col("nome_length") < F.lit(min_len))
        .withColumn("descricao_curta", F.col("descricao_length") < F.lit(min_len))
        .filter(F.col("nome_curto") | F.col("descricao_curta"))
        .select(
            "id",
            "ean",
            "nome",
            "descricao",
            "nome_curto",
            "descricao_curta",
            "nome_length",
            "descricao_length",
            "gerado_em",
        )
        .withColumn("atualizado_em", F.current_timestamp())
    )


def enriquece_nome_descricao_externos(
    eans_df: DataFrame,
    config: ProdutosTextoCurtoConfig,
) -> DataFrame:
    eans = eans_df.select("ean").distinct()

    buscador_bigdatacorp = BuscadorBigDataCorp(config.env)
    bigdatacorp_df = buscador_bigdatacorp.pesquisa_eans(eans).select(
        F.col("ean"),
        F.col("nome").alias("nome_bigboost"),
        F.col("descricao").alias("descricao_bigboost"),
    )

    return (
        eans.alias("e")
        .join(bigdatacorp_df.alias("bb"), "ean", "left")
        .select(
            "ean",
            F.col("bb.nome_bigboost").alias("nome"),
            F.col("bb.descricao_bigboost").alias("descricao"),
            F.when(
                F.col("bb.nome_bigboost").isNotNull(), F.lit(config.fonte_bigboost)
            ).alias("fonte_nome"),
            F.when(
                F.col("bb.descricao_bigboost").isNotNull(),
                F.lit(config.fonte_bigboost),
            ).alias("fonte_descricao"),
            F.current_timestamp().alias("atualizado_em"),
        )
    )


def salva_produtos_texto_curto(
    produtos_df: DataFrame,
    config: ProdutosTextoCurtoConfig,
) -> None:
    _cria_tabela_produtos_texto_curto(config)
    # TODO: Me parece uma tabela que vai ser basicamente só append. Se for verdade daria pra deixar mais simples.
    _upsert_delta_table(
        config.env.spark,
        config.tabela_produtos_texto_curto.value,
        produtos_df,
        ["id", "ean"],
    )


def salva_produtos_texto_externos(
    produtos_df: DataFrame,
    config: ProdutosTextoCurtoConfig,
) -> None:
    _cria_tabela_produtos_texto_externos(config)
    _upsert_delta_table(
        config.env.spark,
        config.tabela_produtos_texto_externos.value,
        produtos_df,
        ["ean"],
    )


def aplica_nome_descricao_externos(
    produtos_df: DataFrame,
    externos_df: DataFrame,
    min_len: int,
) -> DataFrame:
    externos = externos_df.select(
        F.col("ean"),
        F.col("nome").alias("nome_externo"),
        F.col("descricao").alias("descricao_externa"),
    )
    joined = produtos_df.join(externos, "ean", "left")
    nome_length = F.length(F.coalesce(F.col("nome"), F.lit("")))
    descricao_length = F.length(F.coalesce(F.col("descricao"), F.lit("")))

    return (
        joined.withColumn(
            "nome",
            F.when(
                nome_length < F.lit(min_len),
                F.coalesce(F.col("nome_externo"), F.col("nome")),
            ).otherwise(F.col("nome")),
        )
        .withColumn(
            "descricao",
            F.when(
                descricao_length < F.lit(min_len),
                F.coalesce(F.col("descricao_externa"), F.col("descricao")),
            ).otherwise(F.col("descricao")),
        )
        .drop("nome_externo", "descricao_externa")
    )
