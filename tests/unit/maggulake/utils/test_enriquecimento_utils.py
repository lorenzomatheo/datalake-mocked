from unittest.mock import MagicMock, patch

from pyspark.sql import Row
from pyspark.sql import types as T

from maggulake.pipelines.utils import (
    format_column_value,
    save_enrichment_batch,
    validate_llm_results,
)


class TestFormatColumnValue:
    def test_return_na_when_value_is_none(self):
        assert format_column_value(None) == "N/A"

    def test_return_na_when_list_is_empty(self):
        assert format_column_value([]) == "N/A"

    def test_join_list_items(self):
        assert format_column_value(["a", "b", 1]) == "a, b, 1"

    def test_convert_other_types_to_string(self):
        assert format_column_value(123) == "123"
        assert format_column_value("text") == "text"


class TestValidateLlmResults:
    def test_convert_valid_dict_to_row(self):
        results = [{"col1": "val1", "col2": 2}]
        schema_fields = ["col1", "col2"]

        validated = validate_llm_results(results, schema_fields)

        assert len(validated) == 1
        assert validated[0].col1 == "val1"
        assert validated[0].col2 == 2
        assert hasattr(validated[0], "atualizado_em")

    def test_handle_null_string(self):
        results = [{"col1": "null", "col2": "NULL"}]
        schema_fields = ["col1", "col2"]

        validated = validate_llm_results(results, schema_fields)

        assert validated[0].col1 is None
        assert validated[0].col2 is None

    def test_handle_missing_fields_as_none(self):
        results = [{"col1": "val1"}]
        schema_fields = ["col1", "col2"]

        validated = validate_llm_results(results, schema_fields)

        assert validated[0].col1 == "val1"
        assert validated[0].col2 is None

    def test_ignore_invalid_inputs(self):
        results = ["not a dict", None]
        schema_fields = ["col1"]

        validated = validate_llm_results(results, schema_fields)

        assert len(validated) == 0

    def test_filter_out_rows_with_null_ean(self):
        results = [
            {"ean": "123456", "nome_comercial": "Produto A"},
            {"ean": None, "nome_comercial": "Produto B"},
            {"ean": "789012", "nome_comercial": "Produto C"},
            {"nome_comercial": "Produto D"},
        ]
        schema_fields = ["ean", "nome_comercial"]

        validated = validate_llm_results(results, schema_fields)

        assert len(validated) == 2
        assert validated[0].ean == "123456"
        assert validated[0].nome_comercial == "Produto A"
        assert validated[1].ean == "789012"
        assert validated[1].nome_comercial == "Produto C"


class TestSaveEnrichmentBatch:
    @patch("maggulake.pipelines.utils.DeltaTable")
    def test_save_batch_correctly(self, mock_delta_table):
        # Setup mocks
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_table = MagicMock()
        mock_merge = MagicMock()
        mock_matched = MagicMock()
        mock_not_matched = MagicMock()

        mock_spark.createDataFrame.return_value = mock_df
        mock_df.dropDuplicates.return_value = mock_df
        mock_df.alias.return_value = mock_df
        mock_delta_table.forName.return_value = mock_table
        mock_table.alias.return_value = mock_table
        mock_table.merge.return_value = mock_merge
        mock_merge.whenMatchedUpdate.return_value = mock_matched
        mock_matched.whenNotMatchedInsert.return_value = mock_not_matched

        # Inputs
        rows = [Row(ean="123", col1="val")]
        schema = T.StructType(
            [
                T.StructField("ean", T.StringType()),
                T.StructField("col1", T.StringType()),
            ]
        )

        # Execute
        count = save_enrichment_batch(mock_spark, rows, "tabela", schema)

        # Verify
        assert count == 1
        mock_spark.createDataFrame.assert_called_with(rows, schema)
        mock_table.merge.assert_called()
        mock_not_matched.execute.assert_called()

    def test_return_zero_if_rows_empty(self):
        assert save_enrichment_batch(None, [], "tabela", None) == 0
