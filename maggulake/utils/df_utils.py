import unicodedata

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from maggulake.utils.strings import remove_accents


# TODO: adicionar type hints
def escape_string(s):
    if pd.isna(s):
        return None
    return str(s).replace("'", "''")


def normaliza_coluna(name):  # TODO: type hints
    name = name.lower()
    name = ''.join(
        c for c in unicodedata.normalize('NFKD', name) if not unicodedata.combining(c)
    )
    name = name.replace(' ', '_').replace('-', '_').replace('ç', 'c')
    return name


def normaliza_nomes_colunas(df: DataFrame) -> DataFrame:
    current_column_names = df.columns
    new_column_names = [normaliza_coluna(col) for col in current_column_names]
    for old_name, new_name in zip(current_column_names, new_column_names):
        df = df.withColumnRenamed(old_name, new_name)
    return df


def normalize_column_accent(
    df: DataFrame, column_name: str, target_column_name: str | None = None
) -> DataFrame:
    """
    Normaliza os caracteres acentuados da coluna especificada para seus equivalentes sem acento.
    Pode-se informar `target_column_name` para gravar o valor normalizado em uma nova coluna.
    """
    normalized_column = target_column_name or column_name
    return df.withColumn(
        normalized_column,
        F.translate(
            F.col(column_name),
            "áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ",
            "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC",
        ),
    )


def remove_accents_lower(text: str) -> str:
    if text is None:
        return None
    return remove_accents(text).lower()
