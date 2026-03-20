from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def calcula_coluna_maior_valor(df: DataFrame, colunas: List[str]) -> DataFrame:
    """
    Adiciona a coluna 'coluna_mais_frequente' em um DataFrame, identificando a coluna com o maior valor.
    Retorna None caso todas as colunas sejam nulas ou iguais a 0.

    Args:
        df (DataFrame): O DataFrame original.
        colunas (List[str]): Lista com os nomes das colunas a serem analisadas.

    Returns:
        DataFrame: O DataFrame atualizado com todas as colunas originais e a coluna 'coluna_mais_frequente'.
    """
    # Encontrar o maior valor entre as colunas
    df = df.withColumn("max_value", F.greatest(*colunas))

    # Construir a lógica condicional explicitamente com F.when
    cond = None
    for c in colunas:
        if cond is None:
            cond = F.when(F.col(c) == F.col("max_value"), F.lit(c))
        else:
            cond = cond.when(F.col(c) == F.col("max_value"), F.lit(c))

    # Adicionar as condições para lidar com valores nulos e iguais a 0
    df = df.withColumn(
        "coluna_mais_frequente",
        F.when(
            F.expr(
                " AND ".join(f"{c} IS NULL" for c in colunas)
            ),  # Todas as colunas são nulas
            F.lit(None),
        )
        .when(
            sum(F.col(c) for c in colunas) == 0,  # Todas as colunas são iguais a 0
            F.lit(None),
        )
        .otherwise(cond),  # Adicionar o condicional das colunas com maior valor
    ).drop("max_value")

    return df
