import pytest

from maggulake.llm.embeddings import (
    EmbeddingEncodingModels,
    get_max_tokens_by_embedding_model,
)


class TestEmbeddingEncodingModels:
    def test_enum_cl100k_base_value(self):
        assert EmbeddingEncodingModels.CL100K_BASE.value == "cl100k_base"


class TestGetMaxTokensByEmbeddingModel:
    def test_with_enum_member(self):
        result = get_max_tokens_by_embedding_model(EmbeddingEncodingModels.CL100K_BASE)
        assert result == 13000
        assert isinstance(result, int)

    def test_with_string_value(self):
        result = get_max_tokens_by_embedding_model("cl100k_base")
        assert result == 13000
        assert isinstance(result, int)

    def test_enum_and_string_return_same_value(self):
        result_enum = get_max_tokens_by_embedding_model(
            EmbeddingEncodingModels.CL100K_BASE
        )
        result_string = get_max_tokens_by_embedding_model("cl100k_base")
        assert result_enum == result_string

    def test_unsupported_model_raises_error(self):
        with pytest.raises(ValueError) as exc_info:
            get_max_tokens_by_embedding_model("invalid_model")

        assert "Unsupported embedding model: invalid_model" in str(exc_info.value)
        assert "cl100k_base" in str(exc_info.value)

    def test_empty_string_raises_error(self):
        with pytest.raises(ValueError) as exc_info:
            get_max_tokens_by_embedding_model("")

        assert "Unsupported embedding model" in str(exc_info.value)

    def test_none_raises_attribute_error(self):
        with pytest.raises(ValueError):
            get_max_tokens_by_embedding_model(None)

    def test_case_sensitivity(self):
        with pytest.raises(ValueError):
            get_max_tokens_by_embedding_model("CL100K_BASE")

        with pytest.raises(ValueError):
            get_max_tokens_by_embedding_model("CL100k_base")
