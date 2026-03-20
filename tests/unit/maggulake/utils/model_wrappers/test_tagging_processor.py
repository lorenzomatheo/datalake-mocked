from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from maggulake.llm.models import LlmModels
from maggulake.llm.tagging_processor import TaggingProcessor


class SampleSchema(BaseModel):
    """Schema de exemplo para testes."""

    name: str
    age: int


class TestTaggingProcessorInitialization:
    """Testes para inicialização do TaggingProcessor."""

    def test_init_openai_provider_success(self):
        """Inicialização com provider OpenAI deve criar litellm_model correto."""
        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O_MINI,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
            organization="test-org",
        )

        assert processor.provider == "openai"
        assert processor.api_key == "test-key"
        assert processor.model == LlmModels.GPT_4O_MINI
        assert processor.temperature == 0.1
        assert processor.max_tokens == 8192
        assert processor.litellm_model == "gpt-4o-mini"

    def test_init_openai_without_api_key_raises_error(self):
        """Inicialização OpenAI sem API key deve levantar ValueError."""
        with pytest.raises(ValueError, match="api_key"):
            TaggingProcessor(
                provider="openai",
                model=LlmModels.GPT_4O_MINI,
                prompt_template="Test: {input}",
                output_schema=SampleSchema,
            )

    def test_init_gemini_provider_success(self):
        """Inicialização com provider Gemini deve criar litellm_model correto."""
        processor = TaggingProcessor(
            provider="gemini",
            model=LlmModels.GEMINI_FLASH,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-gemini-key",
            reasoning=True,
            thinking_budget=1000,
        )

        assert processor.provider == "gemini"
        assert processor.thinking_budget == 1000
        assert processor.litellm_model == "gemini/gemini-2.5-flash"

    def test_init_gemini_without_api_key_raises_error(self):
        """Inicialização Gemini sem API key deve levantar ValueError."""
        with pytest.raises(ValueError, match="api_key"):
            TaggingProcessor(
                provider="gemini",
                model=LlmModels.GEMINI_FLASH,
                prompt_template="Test: {input}",
                output_schema=SampleSchema,
            )

    def test_init_gemini_vertex_provider_success(self):
        """Inicialização com Gemini Vertex deve criar litellm_model correto."""
        processor = TaggingProcessor(
            provider="gemini_vertex",
            model=LlmModels.GEMINI_PRO,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            project_id="test-project",
            location="us-central1",
            reasoning=True,
            thinking_budget=2000,
        )

        assert processor.provider == "gemini_vertex"
        assert processor.thinking_budget == 2000
        assert processor.litellm_model == "vertex_ai/gemini-2.5-pro"

    def test_init_gemini_vertex_without_project_id_raises_error(self):
        """Gemini Vertex sem project_id deve levantar ValueError."""
        with pytest.raises(ValueError, match="project_id"):
            TaggingProcessor(
                provider="gemini_vertex",
                model=LlmModels.GEMINI_PRO,
                prompt_template="Test: {input}",
                output_schema=SampleSchema,
                location="us-central1",
            )

    def test_init_medgemma_no_validation(self):
        """MedGemma não requer validação de campos."""
        processor = TaggingProcessor(
            provider="medgemma",
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
        )
        assert processor.provider == "medgemma"
        assert not hasattr(processor, "litellm_model")

    def test_custom_temperature(self):
        """Deve aceitar temperatura customizada."""
        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
            temperature=0.7,
        )
        assert processor.temperature == 0.7

    def test_custom_max_tokens(self):
        """Deve aceitar max_tokens customizado."""
        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
            max_tokens=4096,
        )
        assert processor.max_tokens == 4096


class TestTaggingProcessorReasoning:
    """Testes para o parâmetro reasoning do TaggingProcessor."""

    def test_gemini_reasoning_false_sets_thinking_budget_zero(self):
        """Gemini com reasoning=False deve auto-setar thinking_budget=0."""
        processor = TaggingProcessor(
            provider="gemini",
            model=LlmModels.GEMINI_FLASH,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
            reasoning=False,
        )
        assert processor.thinking_budget == 0
        assert processor.reasoning is False

    def test_gemini_reasoning_true_keeps_thinking_budget_none(self):
        """Gemini com reasoning=True deve manter thinking_budget=None."""
        processor = TaggingProcessor(
            provider="gemini",
            model=LlmModels.GEMINI_FLASH,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
            reasoning=True,
        )
        assert processor.thinking_budget is None
        assert processor.reasoning is True

    def test_gemini_vertex_reasoning_false_sets_thinking_budget_zero(self):
        """Gemini Vertex com reasoning=False deve auto-setar thinking_budget=0."""
        processor = TaggingProcessor(
            provider="gemini_vertex",
            model=LlmModels.GEMINI_FLASH,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            project_id="test-project",
            location="us-central1",
            reasoning=False,
        )
        assert processor.thinking_budget == 0

    def test_openai_reasoning_does_not_affect_thinking_budget(self):
        """OpenAI não deve ser afetado pelo parâmetro reasoning."""
        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O_MINI,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
            reasoning=False,
        )
        assert processor.thinking_budget is None


class TestFormatMessages:
    """Testes para o método _format_messages."""

    def test_string_message_replaces_input(self):
        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Analyze: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
        )
        messages = processor._format_messages("hello world")
        assert len(messages) == 1
        assert messages[0] == {
            "role": "user",
            "content": "Analyze: hello world",
        }

    def test_dict_message_replaces_variables(self):
        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Name: {name}, Age: {age}",
            output_schema=SampleSchema,
            api_key="test-key",
        )
        messages = processor._format_messages({"name": "Alice", "age": 30})
        assert messages[0]["content"] == "Name: Alice, Age: 30"

    def test_system_instruction_added_without_cache(self):
        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
            system_instruction="You are helpful.",
        )
        messages = processor._format_messages("hello")
        assert len(messages) == 2
        assert messages[0] == {
            "role": "system",
            "content": "You are helpful.",
        }
        assert messages[1]["role"] == "user"

    def test_system_instruction_skipped_with_cache(self):
        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
            cached_content="cachedContents/abc123",
            system_instruction="You are helpful.",
        )
        messages = processor._format_messages("hello")
        assert len(messages) == 1
        assert messages[0]["role"] == "user"


class TestTaggingProcessorMethods:
    """Testes para métodos do TaggingProcessor."""

    @patch("maggulake.llm.tagging_processor.parse_structured_response")
    @patch("maggulake.llm.tagging_processor.completion")
    def test_executa_tagging_chain_calls_completion(self, mock_completion, mock_parse):
        """Verifica que executa_tagging_chain chama completion corretamente."""
        mock_response = MagicMock()
        mock_completion.return_value = mock_response
        mock_parse.return_value = {"name": "Test", "age": 25}

        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
        )

        result = processor.executa_tagging_chain("test message")

        assert result == {"name": "Test", "age": 25}
        mock_completion.assert_called_once()
        call_kwargs = mock_completion.call_args[1]
        assert call_kwargs["model"] == "gpt-4o"
        assert call_kwargs["api_key"] == "test-key"
        assert call_kwargs["response_format"] is SampleSchema
        mock_parse.assert_called_once_with(mock_response, SampleSchema)

    @patch("maggulake.llm.tagging_processor.completion")
    def test_executa_tagging_chain_vertex_params(self, mock_completion):
        """Verifica que Vertex AI passa project e location corretos."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.parsed = SampleSchema(name="X", age=1)
        mock_completion.return_value = mock_response

        processor = TaggingProcessor(
            provider="gemini_vertex",
            model=LlmModels.GEMINI_FLASH,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            project_id="my-project",
            location="us-central1",
        )

        processor.executa_tagging_chain("test")

        call_kwargs = mock_completion.call_args[1]
        assert call_kwargs["model"] == "vertex_ai/gemini-2.5-flash"
        assert call_kwargs["vertex_project"] == "my-project"
        assert call_kwargs["vertex_location"] == "us-central1"
        assert call_kwargs["thinking"] == {"type": "disabled"}

    @pytest.mark.asyncio
    @patch("maggulake.llm.tagging_processor.completion")
    async def test_executa_tagging_chain_async_structure(self, mock_completion):
        """Verifica estrutura do método async."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.parsed = None
        mock_response.choices[0].message.content = '{"name": "Async", "age": 30}'
        mock_completion.return_value = mock_response

        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
        )

        result = await processor.executa_tagging_chain_async("async test", timeout=10)
        assert result == {"name": "Async", "age": 30}

    @pytest.mark.asyncio
    @patch("maggulake.llm.tagging_processor.completion")
    async def test_executa_tagging_chain_async_batch_structure(self, mock_completion):
        """Verifica estrutura do método batch async."""

        def make_response(name):
            resp = MagicMock()
            resp.choices = [MagicMock()]
            resp.choices[0].message.parsed = None
            resp.choices[0].message.content = f'{{"name": "{name}", "age": 20}}'
            return resp

        mock_completion.side_effect = [
            make_response("msg1"),
            make_response("msg2"),
            make_response("msg3"),
        ]

        processor = TaggingProcessor(
            provider="openai",
            model=LlmModels.GPT_4O,
            prompt_template="Test: {input}",
            output_schema=SampleSchema,
            api_key="test-key",
        )

        messages = ["msg1", "msg2", "msg3"]
        results = await processor.executa_tagging_chain_async_batch(
            messages, timeout=10
        )

        assert len(results) == 3
        assert results[0] == {"name": "msg1", "age": 20}
        assert results[1] == {"name": "msg2", "age": 20}
        assert results[2] == {"name": "msg3", "age": 20}
