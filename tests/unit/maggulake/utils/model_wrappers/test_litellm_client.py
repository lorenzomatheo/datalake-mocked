from unittest.mock import MagicMock, patch

from pydantic import BaseModel

from maggulake.llm.litellm_client import (
    _empty_response,
    _extract_json_candidates,
    _strip_markdown,
    build_thinking_param,
    completion,
    parse_structured_response,
    resolve_litellm_model,
)


class SampleSchema(BaseModel):
    name: str
    age: int


class TestResolveLitellmModel:
    def test_openai_no_prefix(self):
        assert resolve_litellm_model("openai", "gpt-4o") == "gpt-4o"

    def test_gemini_prefix(self):
        result = resolve_litellm_model("gemini", "gemini-2.5-flash")
        assert result == "gemini/gemini-2.5-flash"

    def test_gemini_vertex_prefix(self):
        result = resolve_litellm_model("gemini_vertex", "gemini-2.5-flash")
        assert result == "vertex_ai/gemini-2.5-flash"

    def test_unknown_provider_no_prefix(self):
        assert resolve_litellm_model("unknown", "model") == "model"


class TestBuildThinkingParam:
    def test_none_returns_none(self):
        assert build_thinking_param(None) is None

    def test_zero_disables(self):
        assert build_thinking_param(0) == {"type": "disabled"}

    def test_positive_enables(self):
        result = build_thinking_param(1024)
        assert result == {"type": "enabled", "budget_tokens": 1024}


class TestCompletion:
    @patch("maggulake.llm.litellm_client.litellm.completion")
    def test_basic_call(self, mock_litellm):
        mock_response = MagicMock()
        mock_litellm.return_value = mock_response

        result = completion(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Hello"}],
        )

        assert result == mock_response
        mock_litellm.assert_called_once()
        call_kwargs = mock_litellm.call_args[1]
        assert call_kwargs["model"] == "gpt-4o"
        assert call_kwargs["temperature"] == 0.1
        assert call_kwargs["max_tokens"] == 8192

    @patch("maggulake.llm.litellm_client.litellm.completion")
    def test_optional_params_omitted_when_none(self, mock_litellm):
        mock_litellm.return_value = MagicMock()

        completion(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Hi"}],
        )

        call_kwargs = mock_litellm.call_args[1]
        assert "api_key" not in call_kwargs
        assert "vertex_project" not in call_kwargs
        assert "cached_content" not in call_kwargs
        assert "fallbacks" not in call_kwargs

    @patch("maggulake.llm.litellm_client.litellm.completion")
    def test_optional_params_included_when_set(self, mock_litellm):
        mock_litellm.return_value = MagicMock()

        completion(
            model="vertex_ai/gemini-2.5-flash",
            messages=[{"role": "user", "content": "Hi"}],
            api_key="key-123",
            vertex_project="proj",
            vertex_location="us-central1",
            thinking={"type": "disabled"},
            cached_content="cache-abc",
            fallbacks=["gpt-4o"],
        )

        call_kwargs = mock_litellm.call_args[1]
        assert call_kwargs["api_key"] == "key-123"
        assert call_kwargs["vertex_project"] == "proj"
        assert call_kwargs["vertex_location"] == "us-central1"
        assert call_kwargs["thinking"] == {"type": "disabled"}
        assert call_kwargs["cached_content"] == "cache-abc"
        assert call_kwargs["fallbacks"] == ["gpt-4o"]

    @patch("maggulake.llm.litellm_client.litellm.completion")
    def test_response_format_pydantic(self, mock_litellm):
        mock_litellm.return_value = MagicMock()

        completion(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Hi"}],
            response_format=SampleSchema,
        )

        call_kwargs = mock_litellm.call_args[1]
        assert call_kwargs["response_format"] is SampleSchema


class TestParseStructuredResponse:
    def test_parsed_attribute_pydantic(self):
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message.parsed = SampleSchema(name="Alice", age=30)
        response.choices[0].message.content = None

        result = parse_structured_response(response, SampleSchema)
        assert result == {"name": "Alice", "age": 30}

    def test_parsed_attribute_dict(self):
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message.parsed = {"name": "Bob", "age": 25}
        response.choices[0].message.content = None

        result = parse_structured_response(response, SampleSchema)
        assert result == {"name": "Bob", "age": 25}

    def test_json_content(self):
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message.parsed = None
        response.choices[0].message.content = '{"name": "Charlie", "age": 35}'

        result = parse_structured_response(response, SampleSchema)
        assert result == {"name": "Charlie", "age": 35}

    def test_json_in_markdown_block(self):
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message.parsed = None
        response.choices[
            0
        ].message.content = '```json\n{"name": "Dana", "age": 28}\n```'

        result = parse_structured_response(response, SampleSchema)
        assert result == {"name": "Dana", "age": 28}

    def test_json_with_surrounding_text(self):
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message.parsed = None
        response.choices[
            0
        ].message.content = 'Here is the result: {"name": "Eve", "age": 22} done.'

        result = parse_structured_response(response, SampleSchema)
        assert result == {"name": "Eve", "age": 22}

    def test_empty_content_returns_empty_response(self):
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message.parsed = None
        response.choices[0].message.content = ""

        result = parse_structured_response(response, SampleSchema)
        assert result == {"name": None, "age": None}

    def test_invalid_json_returns_empty_response(self):
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message.parsed = None
        response.choices[0].message.content = "not json at all"

        result = parse_structured_response(response, SampleSchema)
        assert result == {"name": None, "age": None}


class TestStripMarkdown:
    def test_json_code_block(self):
        assert _strip_markdown('```json\n{"a": 1}\n```') == '{"a": 1}'

    def test_generic_code_block(self):
        assert _strip_markdown('```\n{"a": 1}\n```') == '{"a": 1}'

    def test_no_code_block(self):
        assert _strip_markdown('{"a": 1}') == '{"a": 1}'

    def test_whitespace_trimmed(self):
        assert _strip_markdown('  {"a": 1}  ') == '{"a": 1}'


class TestExtractJsonCandidates:
    def test_clean_json(self):
        candidates = _extract_json_candidates('{"a": 1}')
        assert candidates == ['{"a": 1}', '{"a": 1}']

    def test_json_with_text(self):
        candidates = _extract_json_candidates('text {"a": 1} more')
        assert len(candidates) == 2
        assert candidates[1] == '{"a": 1}'


class TestEmptyResponse:
    def test_returns_none_fields(self):
        result = _empty_response(SampleSchema)
        assert result == {"name": None, "age": None}
