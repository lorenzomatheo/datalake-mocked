from unittest.mock import Mock, patch

import pytest
from openai import OpenAI

from maggulake.environment.vector_search import VectorSearch


class TestVectorSearch:
    @pytest.fixture
    def mock_openai(self):
        """Fixture que cria um mock do cliente OpenAI."""
        openai_mock = Mock(spec=OpenAI)
        openai_mock.organization = "test_org"
        openai_mock.api_key = "test_key"
        return openai_mock

    @pytest.fixture
    def vector_search(self, mock_settings, mock_openai):
        """Fixture que cria uma instância do VectorSearch com mocks."""
        return VectorSearch(settings=mock_settings, openai=mock_openai)

    def test_init(self, mock_settings, mock_openai):
        vector_search = VectorSearch(settings=mock_settings, openai=mock_openai)

        assert vector_search.settings == mock_settings
        assert vector_search.openai == mock_openai

    @patch('maggulake.environment.vector_search.MilvusClient')
    def test_milvus_property(self, mock_milvus_client, vector_search):
        mock_client_instance = Mock()
        mock_milvus_client.return_value = mock_client_instance

        # Primeira chamada
        result1 = vector_search.milvus

        # Segunda chamada (deve usar cache)
        result2 = vector_search.milvus

        # Verifica se o MilvusClient foi criado com os parâmetros corretos
        mock_milvus_client.assert_called_once_with(
            uri="http://localhost:19530", token="test_token"
        )

        # Verifica se retorna a mesma instância (cached_property)
        assert result1 == result2 == mock_client_instance

    @patch('maggulake.environment.vector_search.Milvus')
    def test_milvus_adapter_property(self, mock_milvus, vector_search):
        mock_adapter_instance = Mock()
        mock_milvus.return_value = mock_adapter_instance

        # Primeira chamada
        result1 = vector_search.milvus_adapter

        # Segunda chamada (deve usar cache)
        result2 = vector_search.milvus_adapter

        # Verifica se o Milvus foi criado com os parâmetros corretos
        mock_milvus.assert_called_once_with(
            uri="http://localhost:19530", token="test_token"
        )

        # Verifica se retorna a mesma instância (cached_property)
        assert result1 == result2 == mock_adapter_instance

    @patch('maggulake.environment.vector_search.get_milvus_retriever')
    def test_get_milvus_retriever_default_params(
        self, mock_get_milvus_retriever, vector_search
    ):
        mock_retriever = Mock()
        mock_get_milvus_retriever.return_value = mock_retriever

        result = vector_search.get_milvus_retriever()

        mock_get_milvus_retriever.assert_called_once_with(
            openai_organization="test_org",
            openai_api_key="test_key",
            milvus_uri="http://localhost:19530",
            milvus_token="test_token",
            filtro=None,
            max_results=5,  # MILVUS_DEFAULT_MAX_RESULTS
            score_threshold=0.5,  # MILVUS_DEFAULT_SCORE_THRESHOLD
            campos_output=None,
            format_return=True,
        )

        assert result == mock_retriever

    @patch('maggulake.environment.vector_search.get_milvus_retriever')
    def test_get_milvus_retriever_custom_params(
        self, mock_get_milvus_retriever, vector_search
    ):
        mock_retriever = Mock()
        mock_get_milvus_retriever.return_value = mock_retriever

        result = vector_search.get_milvus_retriever(
            filtro="category == 'test'",
            campos_output=["id", "name"],
            max_results=20,
            score_threshold=0.8,
            format_return=False,
        )

        mock_get_milvus_retriever.assert_called_once_with(
            openai_organization="test_org",
            openai_api_key="test_key",
            milvus_uri="http://localhost:19530",
            milvus_token="test_token",
            filtro="category == 'test'",
            max_results=20,
            score_threshold=0.8,
            campos_output=["id", "name"],
            format_return=False,
        )

        assert result == mock_retriever

    @patch('maggulake.environment.vector_search.get_data_api_retriever')
    def test_get_data_api_retriever_default_params(
        self, mock_get_data_api_retriever, vector_search
    ):
        mock_retriever = Mock()
        mock_get_data_api_retriever.return_value = mock_retriever

        result = vector_search.get_data_api_retriever()

        mock_get_data_api_retriever.assert_called_once_with(
            data_api_url="http://localhost:8000",
            data_api_key="test_api_key",
            filtro=None,
            max_results=5,  # DATA_API_DEFAULT_MAX_RESULTS
            score_threshold=0.5,  # DATA_API_DEFAULT_SCORE_THRESHOLD
            campos_output=None,
            format_return=False,
            rerank=False,
        )

        assert result == mock_retriever

    @patch('maggulake.environment.vector_search.get_data_api_retriever')
    def test_get_data_api_retriever_custom_params(
        self, mock_get_data_api_retriever, vector_search
    ):
        mock_retriever = Mock()
        mock_get_data_api_retriever.return_value = mock_retriever

        result = vector_search.get_data_api_retriever(
            filtro="status == 'active'",
            max_results=100,
            score_threshold=0.5,
            campos_output=["id", "description"],
            format_return=True,
            rerank=True,
        )

        mock_get_data_api_retriever.assert_called_once_with(
            data_api_url="http://localhost:8000",
            data_api_key="test_api_key",
            filtro="status == 'active'",
            max_results=100,
            score_threshold=0.5,
            campos_output=["id", "description"],
            format_return=True,
            rerank=True,
        )

        assert result == mock_retriever
