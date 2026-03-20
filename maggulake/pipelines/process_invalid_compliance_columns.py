from typing import Dict

from maggulake.utils.fuzzy_matching import fuzzy_matching


def process_invalid_columns(
    row: list, column_to_enum: Dict[str, list[str]]
) -> Dict[str, str | None]:
    """
    Process invalid columns in a row using fuzzy matching and the corresponding enum lists.

    :param row: A PySpark Row.
    :param column_to_enum: A dictionary mapping lowercase column names to precomputed enum lists.
    :return: A dictionary with column names and their corrected values.
    """
    corrected_values = {}

    # Itera sobre todas as colunas do mapeamento
    for column, enum_list in column_to_enum.items():
        original_value = getattr(row, column, None)

        if original_value is None or original_value not in enum_list:
            corrected_value = (
                fuzzy_matching(original_value, enum_list) if original_value else None
            )

            if corrected_value:
                corrected_values[column] = corrected_value

    return corrected_values
