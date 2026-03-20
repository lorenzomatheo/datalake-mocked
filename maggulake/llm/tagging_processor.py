import asyncio
import json
from functools import partial
from typing import Any, Literal

from langsmith import traceable
from pydantic import BaseModel

from maggulake.llm.litellm_client import (
    build_thinking_param,
    completion,
    parse_structured_response,
    resolve_litellm_model,
)
from maggulake.llm.medgemma import (
    build_medgemma_prompt,
    call_medgemma,
    parse_medgemma_response,
)
from maggulake.llm.models import LlmModels

_PROVIDER_REQUIRED_FIELDS: dict[str, list[str]] = {
    "openai": ["api_key"],
    "gemini": ["api_key"],
    "gemini_vertex": ["project_id", "location"],
}


class TaggingProcessor:
    """
    Processa chamadas a LLMs com output estruturado via schema Pydantic.

    Abstrai providers (OpenAI, Gemini, MedGemma) para extrair dados estruturados
    de texto usando prompts. Usa LiteLLM como camada de transporte unificada.
    """

    def __init__(
        self,
        provider: Literal["openai", "gemini", "gemini_vertex", "medgemma"],
        prompt_template: str,
        output_schema: dict | type[BaseModel],
        model: LlmModels | None = None,
        api_key: str | None = None,
        project_id: str | None = None,
        location: str | None = None,
        organization: str | None = None,
        temperature: float = 0.1,
        reasoning: bool = False,
        thinking_budget: int | None = None,
        max_tokens: int = 8192,
        max_retries: int = 5,
        wait_exponential_kwargs: dict[str, int] | None = None,
        cached_content: str | None = None,
        system_instruction: str | None = None,
    ):
        self.provider = provider
        self.api_key = api_key
        self.project_id = project_id
        self.location = location
        self.organization = organization
        self.model = model
        self.prompt_template = prompt_template
        self.output_schema = output_schema
        self.temperature = temperature
        self.reasoning = reasoning
        self.thinking_budget = self._resolve_thinking_budget(
            provider, model, reasoning, thinking_budget
        )
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.wait_exponential_kwargs = wait_exponential_kwargs or {
            "min": 5,
            "max": 60,
            "multiplier": 2,
        }
        self.cached_content = cached_content
        self.system_instruction = system_instruction

        if self.provider == "medgemma":
            return

        self._validate_provider_fields()

        model_str = (
            self.model.value if isinstance(self.model, LlmModels) else str(self.model)
        )
        self.litellm_model = resolve_litellm_model(self.provider, model_str)

    def _validate_provider_fields(self) -> None:
        """Valida campos obrigatórios por provider."""
        required = _PROVIDER_REQUIRED_FIELDS.get(self.provider, [])
        missing = [f for f in required if not getattr(self, f)]
        if missing:
            fields = ", ".join(missing)
            raise ValueError(f"{fields} required for {self.provider} provider.")

    @staticmethod
    def _resolve_thinking_budget(
        provider: str,
        model: LlmModels | None,
        reasoning: bool,
        thinking_budget: int | None,
    ) -> int | None:
        model_value = model.value if isinstance(model, LlmModels) else model
        is_gemini = provider in ("gemini", "gemini_vertex")
        is_large = model_value == LlmModels.GEMINI_PRO.value
        can_disable = is_gemini and not reasoning and not is_large
        return 0 if can_disable else thinking_budget

    def _format_messages(self, message: str | dict[str, Any]) -> list[dict[str, str]]:
        """Formata o prompt template e retorna lista de messages OpenAI."""
        if isinstance(message, str):
            content = self.prompt_template.replace("{input}", message)
        else:
            content = self.prompt_template
            for key, value in message.items():
                content = content.replace("{" + key + "}", str(value))

        messages: list[dict[str, str]] = []
        if self.cached_content is None and self.system_instruction:
            messages.append({"role": "system", "content": self.system_instruction})
        messages.append({"role": "user", "content": content})
        return messages

    @traceable(run_type="chain")
    def executa_tagging_chain(self, message: str | dict[str, Any]) -> dict[str, Any]:
        if self.provider == "medgemma":
            prompt = build_medgemma_prompt(
                self.prompt_template,
                self.output_schema,
                message,
            )
            response = call_medgemma(
                [{"role": "user", "content": prompt}],
                self.max_tokens,
            )
            return parse_medgemma_response(response, self.output_schema)

        messages = self._format_messages(message)

        is_pydantic = isinstance(self.output_schema, type) and issubclass(
            self.output_schema, BaseModel
        )

        response = completion(
            model=self.litellm_model,
            messages=messages,
            response_format=(
                self.output_schema if is_pydantic else {"type": "json_object"}
            ),
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            api_key=self.api_key,
            organization=self.organization,
            vertex_project=self.project_id,
            vertex_location=self.location,
            thinking=build_thinking_param(self.thinking_budget),
            cached_content=self.cached_content,
            num_retries=self.max_retries,
        )

        if is_pydantic:
            return parse_structured_response(response, self.output_schema)

        content = response.choices[0].message.content
        try:
            return json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return {"content": content}

    @traceable(run_type="chain")
    async def executa_tagging_chain_async(
        self,
        message: str | dict[str, Any],
        timeout: float = 300,
    ) -> dict[str, Any]:
        p = partial(self.executa_tagging_chain, message)
        return await asyncio.wait_for(asyncio.to_thread(p), timeout)

    async def executa_tagging_chain_async_batch(
        self,
        mensagens: list[str | dict[str, Any]],
        timeout: float = 300,
    ) -> list[dict[str, Any] | BaseException]:
        return await asyncio.gather(
            *[self.executa_tagging_chain_async(m, timeout) for m in mensagens],
            return_exceptions=True,
        )
