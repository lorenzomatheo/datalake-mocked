from datetime import timedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from maggulake.integrations.discord import ID_CANAL_DISCORD, enviar_mensagem_discord
from maggulake.utils.time import agora_em_sao_paulo

RECENT_PRODUCTS_PERCENTAGE_THRESHOLD = 0.9


def _diagnostico_nunca_enriquecidos(df: DataFrame) -> str:
    n = df.count()
    if not n:
        return ""
    lines = [f"Produtos nunca enriquecidos - {n} serão processados normalmente:"]
    if "gerado_em" in df.columns:
        stats = df.select(
            F.min("gerado_em").alias("min"), F.max("gerado_em").alias("max")
        ).collect()[0]
        if stats["min"]:
            lines.append(f"  - gerado_em mais antigo: {stats['min']}")
        if stats["max"]:
            lines.append(f"  - gerado_em mais recente: {stats['max']}")
    return "\n".join(lines)


def _diagnostico_a_refazer(
    df: DataFrame, enriched_df: DataFrame, required_fields: list[str]
) -> str:
    n = df.count()
    if not n:
        return ""
    cols = [f for f in required_fields if f in enriched_df.columns]
    null_counts = (
        enriched_df.join(df.select("ean"), on="ean", how="inner")
        .select([F.sum(F.col(f).isNull().cast("int")).alias(f) for f in cols])
        .collect()[0]
        .asDict()
    )
    fields = sorted(
        [(f, c) for f, c in null_counts.items() if c], key=lambda x: x[1], reverse=True
    )
    lines = [
        f"Produtos a refazer - {n} já enriquecidos, re-selecionados por campos obrigatórios nulos:"
    ]
    lines += [f"  - {f}: {c} nulos" for f, c in fields] or [
        "  (nenhum campo nulo identificado)"
    ]
    return "\n".join(lines)


def filtra_notifica_produtos_enriquecimento(
    original_df: DataFrame,
    df_to_enrich: DataFrame,
    threshold: float,
    days_back: int,
    script_name: str,
    id_canal_databricks: int | str = ID_CANAL_DISCORD,
    ignorar_filtros_de_execucao: bool = False,
    enriched_df: DataFrame | None = None,
    required_fields: list[str] | None = None,
) -> list[T.Row]:
    agora = agora_em_sao_paulo()
    header = f"Aviso de execução do script '{script_name}' no dia {agora.date()} :\n\n"

    if df_to_enrich.isEmpty():
        print(header + "Nenhum produto encontrado para atualizar.")
        return []

    date_days_back = agora - timedelta(days=days_back)
    quantidade_total = original_df.count()

    if enriched_df is not None:
        df_nunca_enriquecidos = df_to_enrich.join(
            enriched_df.select("ean"), on="ean", how="left_anti"
        )
        df_a_refazer = df_to_enrich.join(
            enriched_df.select("ean"), on="ean", how="inner"
        )
        resultado_nunca_enriquecidos = df_nunca_enriquecidos.collect()
    else:
        df_a_refazer = df_to_enrich
        resultado_nunca_enriquecidos = []

    quantidade_a_refazer = df_a_refazer.count()

    if (
        quantidade_a_refazer == 0
        or quantidade_a_refazer / quantidade_total <= threshold
    ):
        print(
            header
            + f"Produtos a refazer: {quantidade_a_refazer} dentro do limite de {threshold * 100}% da base."
        )
        return resultado_nunca_enriquecidos + df_a_refazer.collect()

    if "gerado_em" not in df_a_refazer.columns:
        raise ValueError(
            "A coluna 'gerado_em' não existe no DataFrame de produtos a refazer."
        )

    df_a_refazer = df_a_refazer.withColumn(
        "is_recent", F.col("gerado_em") >= F.lit(date_days_back)
    )
    # NOTE: null em 'gerado_em' não é tratado como recente intencionalmente
    count_dict = {
        row["is_recent"]: row["count"]
        for row in df_a_refazer.groupBy("is_recent").count().collect()
    }
    quantidade_recentes = count_dict.get(True, 0)
    quantidade_antigos = count_dict.get(False, 0)

    if (
        not ignorar_filtros_de_execucao
        and quantidade_recentes / quantidade_total
        > RECENT_PRODUCTS_PERCENTAGE_THRESHOLD
    ):
        print(
            header
            + f"Produtos a refazer: {quantidade_recentes} recentes serão atualizados, "
            f"{quantidade_antigos} antigos ignorados nesta execução."
        )
        return (
            resultado_nunca_enriquecidos
            + df_a_refazer.filter(F.col("is_recent")).drop("is_recent").collect()
        )

    diagnosticos = "\n\n".join(
        filter(
            None,
            [
                _diagnostico_a_refazer(df_a_refazer, enriched_df, required_fields or [])
                if enriched_df is not None
                else "",
                _diagnostico_nunca_enriquecidos(df_nunca_enriquecidos)
                if enriched_df is not None
                else "",
            ],
        )
    )
    aviso = (
        header
        + f"Produtos a refazer: {quantidade_a_refazer} re-selecionados com datas anteriores a "
        f"{date_days_back.date()}, excedendo o threshold de {threshold * 100}%.\n\n"
        + diagnosticos
    )

    enviar_mensagem_discord(id_canal_databricks, aviso)

    if ignorar_filtros_de_execucao:
        print(
            f"AVISO: ignorar_filtros_de_execucao=True. Processando mesmo assim.\n{aviso}"
        )
        return resultado_nunca_enriquecidos + df_a_refazer.drop("is_recent").collect()

    raise RuntimeError(aviso)
