from unittest.mock import MagicMock, PropertyMock

import pyspark.sql.types as T
import pytest

from maggulake.utils.valores_invalidos import (
    COLUNAS_EXCLUIR,
    COLUNAS_PERMITIR_NUMERICO,
    LITERAIS_INVALIDOS,
    nullifica_valores_invalidos,
    valor_eh_invalido,
)


class TestValorEhInvalido:
    def test_none_is_invalid(self):
        assert valor_eh_invalido(None) is True

    def test_empty_string_is_invalid(self):
        assert valor_eh_invalido("") is True

    def test_whitespace_only_is_invalid(self):
        assert valor_eh_invalido("   ") is True
        assert valor_eh_invalido("\t") is True
        assert valor_eh_invalido("\n") is True
        assert valor_eh_invalido("  \t\n  ") is True

    @pytest.mark.parametrize("literal", LITERAIS_INVALIDOS)
    def test_invalid_literals(self, literal):
        assert valor_eh_invalido(literal) is True

    @pytest.mark.parametrize(
        "value",
        ["N/A", "N/a", "n/A", "NONE", "None", "nOnE", "NULL", "Null", "NAN", "Nan"],
    )
    def test_invalid_literals_case_insensitive(self, value):
        assert valor_eh_invalido(value) is True

    @pytest.mark.parametrize(
        "value",
        [
            "  N/A  ",
            " none ",
            "\tnull\t",
            "  ---  ",
            " n/i ",
        ],
    )
    def test_invalid_literals_with_surrounding_whitespace(self, value):
        assert valor_eh_invalido(value) is True

    @pytest.mark.parametrize(
        "value",
        ["---", "...", "___", "-", ".", "--", "***", "!!", "??", "- -", "!@#$", "()"],
    )
    def test_punctuation_only_is_invalid(self, value):
        assert valor_eh_invalido(value) is True

    @pytest.mark.parametrize(
        "value", ["aaaa", "bbbb", "xxx", "zz", "AAAA", "XXXX", "ee"]
    )
    def test_repeated_same_letter_is_invalid(self, value):
        assert valor_eh_invalido(value) is True

    @pytest.mark.parametrize("value", ["a", "X", "1", "-", ".", "?"])
    def test_single_character_is_invalid(self, value):
        assert valor_eh_invalido(value) is True

    @pytest.mark.parametrize("value", ["12345", "0", "999", "00000"])
    def test_numbers_only_is_invalid_by_default(self, value):
        assert valor_eh_invalido(value) is True

    @pytest.mark.parametrize("value", ["12345", "7891234567890", "0000", "123"])
    def test_numbers_allowed_when_flag_set(self, value):
        assert valor_eh_invalido(value, permitir_numerico=True) is False

    @pytest.mark.parametrize(
        "value",
        [
            "Dipirona 500mg",
            "EMS S/A",
            "Acima de 12 anos",
            "Analgésico",
            "Vitamina C",
            "ab",
            "ok",
            "Sim",
            "Oral",
            "500mg",
            "1L",
        ],
    )
    def test_valid_values_are_preserved(self, value):
        assert valor_eh_invalido(value) is False

    def test_two_different_letters_is_valid(self):
        assert valor_eh_invalido("ab") is False
        assert valor_eh_invalido("AB") is False
        assert valor_eh_invalido("aB") is False

    def test_mixed_content_with_numbers_is_valid(self):
        assert valor_eh_invalido("Dipirona 500mg") is False
        assert valor_eh_invalido("100ml Solução") is False

    def test_url_like_string_is_valid(self):
        assert valor_eh_invalido("https://example.com/image.jpg") is False


class TestNullificaValoresInvalidos:
    def _make_mock_df(self, fields: list[tuple[str, T.DataType]]):
        mock_df = MagicMock()

        schema_fields = [T.StructField(name, dtype, True) for name, dtype in fields]
        schema = T.StructType(schema_fields)
        type(mock_df).schema = PropertyMock(return_value=schema)

        mock_df.withColumn.return_value = mock_df

        return mock_df

    def test_auto_detects_string_columns(self):
        df = self._make_mock_df(
            [
                ("id", T.StringType()),
                ("fabricante", T.StringType()),
                ("marca", T.StringType()),
                ("eh_medicamento", T.BooleanType()),
                ("quality_score", T.DoubleType()),
            ]
        )

        nullifica_valores_invalidos(df)

        called_columns = [call.args[0] for call in df.withColumn.call_args_list]
        assert "fabricante" in called_columns
        assert "marca" in called_columns
        assert "eh_medicamento" not in called_columns
        assert "quality_score" not in called_columns

    def test_excludes_default_columns(self):
        fields = [(col, T.StringType()) for col in COLUNAS_EXCLUIR]
        fields.append(("fabricante", T.StringType()))
        df = self._make_mock_df(fields)

        nullifica_valores_invalidos(df)

        called_columns = [call.args[0] for call in df.withColumn.call_args_list]
        for excluded in COLUNAS_EXCLUIR:
            assert excluded not in called_columns
        assert "fabricante" in called_columns

    def test_excludes_custom_columns(self):
        df = self._make_mock_df(
            [
                ("nome", T.StringType()),
                ("marca", T.StringType()),
            ]
        )

        nullifica_valores_invalidos(df, colunas_excluir=["nome"])

        called_columns = [call.args[0] for call in df.withColumn.call_args_list]
        assert "nome" not in called_columns
        assert "marca" in called_columns

    def test_uses_specific_columns_when_provided(self):
        df = self._make_mock_df(
            [
                ("fabricante", T.StringType()),
                ("marca", T.StringType()),
                ("descricao", T.StringType()),
            ]
        )

        nullifica_valores_invalidos(df, colunas=["fabricante"])

        called_columns = [call.args[0] for call in df.withColumn.call_args_list]
        assert "fabricante" in called_columns
        assert "marca" not in called_columns
        assert "descricao" not in called_columns

    def test_uses_explicit_schema_when_provided(self):
        df = self._make_mock_df(
            [
                ("descricao", T.StringType()),
                ("marca", T.StringType()),
            ]
        )

        schema_explicito = T.StructType(
            [
                T.StructField("descricao", T.StringType(), True),
                T.StructField("marca", T.StringType(), True),
                T.StructField("fabricante", T.StringType(), True),
            ]
        )

        nullifica_valores_invalidos(df, schema=schema_explicito)

        called_columns = [call.args[0] for call in df.withColumn.call_args_list]
        assert "descricao" in called_columns
        assert "marca" in called_columns
        assert "fabricante" in called_columns

    def test_does_not_touch_non_string_types(self):
        df = self._make_mock_df(
            [
                ("validade_receita_dias", T.LongType()),
                ("quality_score", T.DoubleType()),
                ("eh_medicamento", T.BooleanType()),
                ("gerado_em", T.TimestampType()),
            ]
        )

        nullifica_valores_invalidos(df)

        df.withColumn.assert_not_called()

    def test_ignores_array_of_non_string(self):
        df = self._make_mock_df(
            [
                ("scores", T.ArrayType(T.DoubleType())),
            ]
        )

        nullifica_valores_invalidos(df)

        df.withColumn.assert_not_called()


class TestConstants:
    def test_invalid_literals_are_lowercase(self):
        for literal in LITERAIS_INVALIDOS:
            assert literal == literal.lower(), f"Literal not lowercase: {literal}"

    def test_columns_allow_numeric_contains_ean(self):
        assert "ean" in COLUNAS_PERMITIR_NUMERICO
        assert "numero_registro" in COLUNAS_PERMITIR_NUMERICO

    def test_columns_exclude_contains_id(self):
        assert "id" in COLUNAS_EXCLUIR

    def test_idade_recomendada_not_in_allow_numeric(self):
        assert "idade_recomendada" not in COLUNAS_PERMITIR_NUMERICO

    def test_power_phrase_not_in_exclude(self):
        assert "power_phrase" not in COLUNAS_EXCLUIR
