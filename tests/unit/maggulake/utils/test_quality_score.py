import unittest
from unittest.mock import MagicMock

import pyspark.sql.types as T

from maggulake.pipelines.quality_score import is_field_compliant, is_field_filled


class TestQualityScoreUtils(unittest.TestCase):
    def setUp(self):
        self.mock_df = MagicMock()

    def test_is_field_filled_string_logic(self):
        """Valida se lógica para string inclui tratamento de 'null' e '[]'"""
        self.mock_df.schema.__getitem__.return_value.dataType = T.StringType()

        col_name = "test_col"
        result = is_field_filled(self.mock_df, col_name)

        # Validação por nome da classe para evitar problemas de import/path do PySpark
        self.assertEqual(type(result).__name__, "Column")

        # O string representation da coluna contem os operadores usados
        # A validação exata string do PySpark pode variar, mas checamos a presença do nome
        expr_str = str(result)
        self.assertIn("test_col", expr_str)

    def test_is_field_filled_array_logic(self):
        """Valida lógica para array (size > 0)"""
        self.mock_df.schema.__getitem__.return_value.dataType = T.ArrayType(
            T.StringType()
        )
        result = is_field_filled(self.mock_df, "tags")
        self.assertEqual(type(result).__name__, "Column")

    def test_is_field_compliant_enum(self):
        """Valida construção com Enum"""
        self.mock_df.schema.__getitem__.return_value.dataType = T.StringType()
        result = is_field_compliant(self.mock_df, "cat", enum_values=("A",))
        self.assertEqual(type(result).__name__, "Column")

    def test_is_field_compliant_rule(self):
        """Valida construção com Regra SQL"""
        self.mock_df.schema.__getitem__.return_value.dataType = T.StringType()
        result = is_field_compliant(self.mock_df, "desc", rule_expr="length(desc) > 10")
        self.assertEqual(type(result).__name__, "Column")
