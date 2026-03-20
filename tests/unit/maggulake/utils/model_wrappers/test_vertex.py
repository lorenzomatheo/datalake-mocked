import warnings
from unittest.mock import MagicMock, patch

import pytest
from google.genai.errors import APIError

from maggulake.llm.vertex import (
    create_vertex_context_cache,
    get_location_by_model,
)


class TestCreateVertexContextCache:
    def test_returns_cache_name(self):
        mock_cache = MagicMock()
        mock_cache.name = "cachedContents/abc123"
        mock_client = MagicMock()
        mock_client.caches.create.return_value = mock_cache

        with patch(
            "maggulake.llm.vertex.genai.Client",
            return_value=mock_client,
        ):
            result = create_vertex_context_cache(
                model="gemini-2.5-flash",
                system_instruction="You are a helpful assistant.",
                project_id="my-project",
            )

        assert result == "abc123"

    def test_calls_client_with_correct_params(self):
        mock_cache = MagicMock()
        mock_cache.name = "cachedContents/xyz"
        mock_client = MagicMock()
        mock_client.caches.create.return_value = mock_cache

        with patch(
            "maggulake.llm.vertex.genai.Client",
            return_value=mock_client,
        ) as mock_client_cls:
            create_vertex_context_cache(
                model="gemini-2.5-pro",
                system_instruction="Instructions here.",
                project_id="proj-id",
                location="us-east1",
                ttl=7200,
            )

        mock_client_cls.assert_called_once_with(
            vertexai=True, project="proj-id", location="us-east1"
        )
        mock_client.caches.create.assert_called_once()
        call_kwargs = mock_client.caches.create.call_args
        assert call_kwargs.kwargs["model"] == "gemini-2.5-pro"

    def test_returns_none_when_tokens_below_minimum(self):
        mock_client = MagicMock()
        mock_client.caches.create.side_effect = APIError(
            400,
            {
                "message": "The cached content is of 277 tokens. "
                "The minimum token count to start caching is 1024.",
                "status": "INVALID_ARGUMENT",
            },
        )

        with patch(
            "maggulake.llm.vertex.genai.Client",
            return_value=mock_client,
        ):
            result = create_vertex_context_cache(
                model="gemini-2.5-flash",
                system_instruction="Short prompt.",
                project_id="proj",
            )

        assert result is None

    def test_reraises_non_token_errors(self):
        mock_client = MagicMock()
        mock_client.caches.create.side_effect = APIError(
            503,
            {"message": "Service Unavailable", "status": "UNAVAILABLE"},
        )

        with patch(
            "maggulake.llm.vertex.genai.Client",
            return_value=mock_client,
        ):
            with pytest.raises(APIError):
                create_vertex_context_cache(
                    model="gemini-2.5-flash",
                    system_instruction="Some prompt.",
                    project_id="proj",
                )

    def test_default_location_and_ttl(self):
        mock_cache = MagicMock()
        mock_cache.name = "cachedContents/default"
        mock_client = MagicMock()
        mock_client.caches.create.return_value = mock_cache

        with patch(
            "maggulake.llm.vertex.genai.Client",
            return_value=mock_client,
        ) as mock_client_cls:
            create_vertex_context_cache(
                model="gemini-2.5-flash",
                system_instruction="Instructions.",
                project_id="proj",
            )

        mock_client_cls.assert_called_once_with(
            vertexai=True, project="proj", location="us-central1"
        )


class TestGetLocationByModel:
    """Testes para a função get_location_by_model."""

    # Testes para modelos Gemini
    def test_gemini_flash_returns_us_central1(self):
        result = get_location_by_model("gemini-2.0-flash")
        assert result == "us-central1"

    def test_gemini_pro_returns_us_central1(self):
        result = get_location_by_model("gemini-2.5-pro")
        assert result == "us-central1"

    def test_gemini_flash_reasoning_returns_us_central1(self):
        result = get_location_by_model("gemini-2.5-flash")
        assert result == "us-central1"

    def test_gemini_prefix_returns_us_central1(self):
        result = get_location_by_model("gemini-test-model")
        assert result == "us-central1"

    # Testes para modelos desconhecidos
    def test_unknown_model_returns_us_central1_with_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = get_location_by_model("gpt-4o")

            assert result == "us-central1"
            assert len(w) == 1
            assert issubclass(w[-1].category, UserWarning)
            assert "Modelo desconhecido 'gpt-4o'" in str(w[-1].message)
            assert "us-central1" in str(w[-1].message)

    def test_empty_string_returns_us_central1_with_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = get_location_by_model("")

            assert result == "us-central1"
            assert len(w) == 1
            assert "Modelo desconhecido ''" in str(w[-1].message)

    def test_random_model_returns_us_central1_with_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = get_location_by_model("llama-3-70b")

            assert result == "us-central1"
            assert len(w) == 1

    # Testes edge cases
    def test_model_with_only_gemini_prefix(self):
        result = get_location_by_model("gemini")
        assert result == "us-central1"

    def test_model_with_gemini_in_middle(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = get_location_by_model("google-gemini-model")
            # Não começa com 'gemini', então retorna default
            assert result == "us-central1"
            assert len(w) == 1
