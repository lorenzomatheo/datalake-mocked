import uuid
from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def to_schema(df, schema):
    tmp_path = f'/tmp/maggu/to_schema/{uuid.uuid4().hex}'
    df.write.json(tmp_path)
    return df.sparkSession.read.json(tmp_path, schema=schema, mode='FAILFAST')


def verifica_coluna_completude(
    df: DataFrame,
    partition_by_cols: List[str],
    tie_break_cols: List[Column],
) -> DataFrame:
    """
    Remove registros duplicados com base em colunas de partição, mantendo o
    registro com o maior número de colunas preenchidas (não nulas).

    Seleciona o registro mais completo e, caso este não tenha 'descricao',
    preenche com a melhor descrição disponível entre os registros duplicados.

    Ordem de seleção:
    1. O registro com maior `completeness_score` (mais colunas preenchidas) é escolhido.
    2. Se a `descricao` do registro escolhido for vazia, ela é preenchida com a
       `descricao` mais longa encontrada em qualquer um dos registros duplicados.
    3. O critério de desempate (tie_break_cols) garante consistência.

    Args:
        df (DataFrame): O DataFrame de entrada.
        partition_by_cols (List[str]): Lista de nomes de colunas para agrupar
                                       os registros (ex: ["ean"]).
        tie_break_cols (List[Column]): Lista de colunas para usar como critério
                                     de desempate se a pontuação de
                                     preenchimento for a mesma.

    Returns:
        DataFrame: Um novo DataFrame sem as duplicatas.
    """
    # Passo 1: Calcular a pontuação de preenchimento
    all_cols = df.columns
    completeness_checks = []
    for col_name in all_cols:
        col_type = df.schema[col_name].dataType
        if str(col_type).startswith("ArrayType"):
            check = F.when(
                F.col(col_name).isNotNull() & (F.size(F.col(col_name)) > 0), 1
            ).otherwise(0)
        elif str(col_type) == "StringType":
            check = F.when(
                F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != ""), 1
            ).otherwise(0)
        else:
            check = F.when(F.col(col_name).isNotNull(), 1).otherwise(0)
        completeness_checks.append(check)

    df_processed = df.withColumn("_completeness_score", sum(completeness_checks))

    # Passo 2: Identificar e transplantar a melhor descrição disponível
    if "descricao" in all_cols:
        desc_window = Window.partitionBy(*partition_by_cols).orderBy(
            F.length(F.col("descricao")).desc()
        )
        df_processed = df_processed.withColumn(
            "_best_available_descricao",
            F.first(F.when(F.col("descricao").isNotNull(), F.col("descricao"))).over(
                desc_window
            ),
        )
        df_processed = df_processed.withColumn(
            "descricao",
            F.when(
                (F.col("descricao").isNull()) | (F.trim(F.col("descricao")) == ""),
                F.col("_best_available_descricao"),
            ).otherwise(F.col("descricao")),
        )

    # Passo 3: Rankear os registros para encontrar o vencedor
    window_spec = Window.partitionBy(*partition_by_cols).orderBy(
        F.col("_completeness_score").desc(), *tie_break_cols
    )
    ranked_df = df_processed.withColumn("_rank", F.row_number().over(window_spec))

    # Passo 4: Filtrar e Limpar
    winner_df = ranked_df.filter(F.col("_rank") == 1)

    temp_cols_to_drop = [col for col in winner_df.columns if col.startswith("_")]

    return winner_df.drop(*temp_cols_to_drop)
