import pytest

from maggulake.llm.models import (
    LlmModels,
    get_llm_config,
    get_model_name,
)


class TestLlmModels:
    """Testes para o enum LlmModels."""

    def test_enum_values_exist(self):
        """Verifica se os modelos esperados existem no enum."""
        assert LlmModels.GEMINI_FLASH.value == "gemini-2.5-flash"
        assert LlmModels.GEMINI_PRO.value == "gemini-2.5-pro"
        assert LlmModels.GPT_4O.value == "gpt-4o"
        assert LlmModels.GPT_4O_MINI.value == "gpt-4o-mini"
        assert LlmModels.GPT_5_1.value == "gpt-5.1"
        assert LlmModels.GPT_5_MINI.value == "gpt-5-mini"
        assert LlmModels.GEMINI_FLASH_LITE.value == "gemini-2.5-flash-lite"


class TestGetModelName:
    """Testes para a função get_model_name."""

    # Testes para provider Gemini (reasoning é ignorado, controlado via thinking_budget)
    def test_gemini_small(self):
        """Gemini small deve retornar GEMINI_FLASH."""
        result = get_model_name("gemini", size="SMALL")
        assert result == LlmModels.GEMINI_FLASH.value

    def test_gemini_large(self):
        """Gemini large deve retornar GEMINI_PRO."""
        result = get_model_name("gemini", size="LARGE")
        assert result == LlmModels.GEMINI_PRO.value

    def test_gemini_reasoning_ignored(self):
        """Gemini ignora parâmetro reasoning (controlado via thinking_budget)."""
        result_no_reasoning = get_model_name("gemini", size="LARGE", reasoning=False)
        result_reasoning = get_model_name("gemini", size="LARGE", reasoning=True)
        assert result_no_reasoning == result_reasoning == LlmModels.GEMINI_PRO.value

    # Testes para provider Gemini Vertex
    def test_gemini_vertex_small(self):
        """Gemini Vertex small deve retornar GEMINI_FLASH."""
        result = get_model_name("gemini_vertex", size="SMALL")
        assert result == LlmModels.GEMINI_FLASH.value

    def test_gemini_vertex_large(self):
        """Gemini Vertex large deve retornar GEMINI_PRO."""
        result = get_model_name("gemini_vertex", size="LARGE")
        assert result == LlmModels.GEMINI_PRO.value

    # Testes para provider OpenAI
    def test_openai_small_reasoning(self):
        """OpenAI small com reasoning deve retornar GPT_5_MINI."""
        result = get_model_name("openai", size="SMALL", reasoning=True)
        assert result == LlmModels.GPT_5_MINI.value

    def test_openai_large_reasoning(self):
        """OpenAI large com reasoning deve retornar GPT_5_1."""
        result = get_model_name("openai", size="LARGE", reasoning=True)
        assert result == LlmModels.GPT_5_1.value

    def test_openai_small_no_reasoning(self):
        """OpenAI small sem reasoning deve retornar GPT_4O_MINI."""
        result = get_model_name("openai", size="SMALL", reasoning=False)
        assert result == LlmModels.GPT_4O_MINI.value

    def test_openai_large_no_reasoning(self):
        """OpenAI large sem reasoning deve retornar GPT_4O."""
        result = get_model_name("openai", size="LARGE", reasoning=False)
        assert result == LlmModels.GPT_4O.value

    # Testes para casos de erro
    def test_unsupported_provider_raises_error(self):
        """Provider não suportado deve levantar NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc_info:
            get_model_name("invalid_provider", size="SMALL", reasoning=True)

        assert "Unsupported provider" in str(exc_info.value)
        assert "openai" in str(exc_info.value)
        assert "gemini" in str(exc_info.value)
        assert "gemini_vertex" in str(exc_info.value)

    # Testes para defaults
    def test_default_size_is_small(self):
        """Sem especificar size, deve usar SMALL como default."""
        result = get_model_name("openai", reasoning=False)
        assert result == LlmModels.GPT_4O_MINI.value

    def test_default_reasoning_is_false(self):
        """Sem especificar reasoning, deve usar False como default."""
        result = get_model_name("openai", size="SMALL")
        assert result == LlmModels.GPT_4O_MINI.value

    # Testes case-insensitive
    def test_provider_case_insensitive(self):
        """Provider deve ser case-insensitive."""
        result_lower = get_model_name("openai", size="SMALL", reasoning=False)
        result_upper = get_model_name("OPENAI", size="SMALL", reasoning=False)
        result_mixed = get_model_name("OpenAI", size="SMALL", reasoning=False)

        assert result_lower == result_upper == result_mixed
        assert result_lower == LlmModels.GPT_4O_MINI.value


class TestGetLLMConfig:
    """Testes para a função get_llm_config."""

    @pytest.fixture
    def mock_spark(self, mocker):
        return mocker.Mock()

    @pytest.fixture
    def mock_dbutils(self, mocker):
        dbutils = mocker.Mock()
        dbutils.secrets.get.return_value = "fake-api-key"
        return dbutils

    @pytest.fixture
    def mock_setup_vertex(self, mocker):
        return mocker.patch(
            "maggulake.llm.models.setup_vertex_ai_credentials",
            return_value=("fake-project", "fake-location"),
        )

    def test_medgemma_config(self, mock_spark, mock_dbutils, mock_setup_vertex):
        """Testa configuração para medgemma."""
        config = get_llm_config("medgemma", mock_spark, mock_dbutils)

        mock_setup_vertex.assert_called_once_with(mock_spark)
        assert config.provider == "medgemma"
        assert config.model is None
        assert config.api_key is None
        assert config.project_id is None
        assert config.location is None

    def test_gemini_vertex_config(self, mock_spark, mock_dbutils, mock_setup_vertex):
        """Testa configuração para gemini_vertex."""
        config = get_llm_config("gemini_vertex", mock_spark, mock_dbutils)

        mock_setup_vertex.assert_called_once_with(mock_spark)
        assert config.provider == "gemini_vertex"
        assert config.model == "gemini-2.5-flash"  # Default reasoning=False, size=SMALL
        assert config.api_key is None
        assert config.project_id == "fake-project"
        assert config.location == "fake-location"

    def test_openai_config(self, mock_spark, mock_dbutils, mock_setup_vertex):
        """Testa configuração para openai."""
        config = get_llm_config("openai", mock_spark, mock_dbutils)

        mock_setup_vertex.assert_not_called()
        mock_dbutils.secrets.get.assert_called_with(scope="llm", key="OPENAI_API_KEY")
        assert config.provider == "openai"
        assert config.model == "gpt-4o-mini"  # Default reasoning=False, size=SMALL
        assert config.api_key == "fake-api-key"
        assert config.project_id is None
        assert config.location is None

    def test_invalid_provider(self, mock_spark, mock_dbutils):
        """Testa provider inválido."""
        with pytest.raises(ValueError, match="Unsupported provider"):
            get_llm_config("invalid", mock_spark, mock_dbutils)


class TestDefaultLlmModelEnvVar:
    """Testes para override via DEFAULT_LLM_MODEL."""

    def test_env_var_overrides_model_selection(self, monkeypatch):
        """DEFAULT_LLM_MODEL deve sobrescrever a seleção de modelo."""
        monkeypatch.setenv("DEFAULT_LLM_MODEL", "custom-model-v1")

        result = get_model_name("openai", size="SMALL", reasoning=False)
        assert result == "custom-model-v1"

    def test_env_var_overrides_all_providers(self, monkeypatch):
        """DEFAULT_LLM_MODEL deve funcionar para qualquer provider."""
        monkeypatch.setenv("DEFAULT_LLM_MODEL", "override-model")

        assert get_model_name("openai") == "override-model"
        assert get_model_name("gemini") == "override-model"
        assert get_model_name("gemini_vertex") == "override-model"

    def test_without_env_var_uses_default(self, monkeypatch):
        """Sem DEFAULT_LLM_MODEL, usa lógica padrão."""
        monkeypatch.delenv("DEFAULT_LLM_MODEL", raising=False)

        result = get_model_name("openai", size="SMALL", reasoning=False)
        assert result == LlmModels.GPT_4O_MINI.value
