from typing import Optional, Tuple

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame


def is_field_filled(df: DataFrame, column: str) -> Column:
    """
    Retorna expressão (Column) que avalia para 1 se o campo está preenchido, 0 caso contrário.
    Trata strings vazias e arrays vazios.
    """
    col_type = df.schema[column].dataType
    is_array = isinstance(col_type, T.ArrayType)

    if is_array:
        return F.when(
            (F.col(column).isNotNull()) & (F.size(F.col(column)) > 0), F.lit(1)
        ).otherwise(F.lit(0))
    else:
        return F.when(
            (F.col(column).isNotNull())
            & (F.col(column) != "")
            & (F.lower(F.col(column)) != "null")
            & (F.trim(F.col(column)) != "[]"),
            F.lit(1),
        ).otherwise(F.lit(0))


def is_field_compliant(
    df: DataFrame,
    column: str,
    enum_values: Optional[Tuple] = None,
    rule_expr: Optional[str] = None,
) -> Column:
    """
    Retorna expressão (Column) que avalia para 1 se o campo está em compliance, 0 caso contrário.
    Se o campo for nulo/vazio, retorna 0.

    Args:
        enum_values: Tupla de valores válidos.
        rule_expr: Expressão SQL booleana customizada (ex: "length(col) > 5").
    """
    # Verifica preenchimento primeiro
    filled = is_field_filled(df, column)

    compliance_check = F.lit(1)

    if enum_values:
        # Verifica se valor está no enum
        compliance_check = F.col(column).isin(list(enum_values))
    elif rule_expr:
        # Verifica regra customizada
        compliance_check = F.expr(rule_expr)

    return F.when((filled == 1) & compliance_check, F.lit(1)).otherwise(F.lit(0))
