"""Camada de abstração centralizada para chamadas LLM via LiteLLM.

Todas as chamadas a modelos de linguagem (exceto MedGemma, que usa endpoint
customizado, e Agno agents) devem passar por este módulo. Isso permite trocar
providers e modelos sem alterar código de negócio.
"""

import json
from typing import Any

import litellm
from langsmith import traceable
from pydantic import BaseModel

litellm.drop_params = True
litellm.set_verbose = False

PROVIDER_PREFIX: dict[str, str] = {
    "openai": "",
    "gemini": "gemini/",
    "gemini_vertex": "vertex_ai/",
}


def resolve_litellm_model(provider: str, model: str) -> str:
    """Converte provider + model name para o formato esperado pelo LiteLLM.

    Exemplos:
        ("openai", "gpt-4o") -> "gpt-4o"
        ("gemini", "gemini-2.5-flash") -> "gemini/gemini-2.5-flash"
        ("gemini_vertex", "gemini-2.5-flash") -> "vertex_ai/gemini-2.5-flash"
    """
    prefix = PROVIDER_PREFIX.get(provider, "")
    return f"{prefix}{model}"


def build_thinking_param(
    thinking_budget: int | None,
) -> dict[str, Any] | None:
    """Constrói o parâmetro ``thinking`` para modelos Gemini.

    Returns:
        None se thinking_budget for None (usar default do modelo).
        {"type": "disabled"} se thinking_budget == 0.
        {"type": "enabled", "budget_tokens": N} caso contrário.
    """
    if thinking_budget is None:
        return None
    if thinking_budget == 0:
        return {"type": "disabled"}
    return {"type": "enabled", "budget_tokens": thinking_budget}


@traceable(run_type="llm")
def completion(
    model: str,
    messages: list[dict[str, Any]],
    response_format: type[BaseModel] | dict | None = None,
    temperature: float = 0.1,
    max_tokens: int = 8192,
    api_key: str | None = None,
    organization: str | None = None,
    vertex_project: str | None = None,
    vertex_location: str | None = None,
    thinking: dict[str, Any] | None = None,
    cached_content: str | None = None,
    num_retries: int = 3,
    timeout: float = 300,
    fallbacks: list[str] | None = None,
    **kwargs: Any,
) -> litellm.ModelResponse:
    """Chamada centralizada a LLM via LiteLLM com tracing LangSmith.

    Args:
        model: Nome do modelo no formato LiteLLM
            (ex: ``"vertex_ai/gemini-2.5-flash"``, ``"gpt-4o"``).
        messages: Lista de mensagens no formato OpenAI.
        response_format: Schema Pydantic ou dict para structured output.
        temperature: Temperatura de geração.
        max_tokens: Máximo de tokens na resposta.
        api_key: API key do provider (OpenAI, Gemini API).
        organization: Organização OpenAI.
        vertex_project: Project ID do Google Cloud (Vertex AI).
        vertex_location: Região do Vertex AI.
        thinking: Config de thinking/reasoning para modelos Gemini.
        cached_content: ID de cached content do Vertex AI.
        num_retries: Número de retentativas em caso de erro.
        timeout: Timeout em segundos.
        fallbacks: Lista de modelos fallback no formato LiteLLM.
        **kwargs: Parâmetros adicionais passados ao ``litellm.completion()``.

    Returns:
        ``litellm.ModelResponse`` com a resposta do modelo.
    """
    params: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "num_retries": num_retries,
        "timeout": timeout,
        **kwargs,
    }

    optional = {
        "response_format": response_format,
        "api_key": api_key,
        "organization": organization,
        "vertex_project": vertex_project,
        "vertex_location": vertex_location,
        "thinking": thinking,
        "cached_content": cached_content,
        "fallbacks": fallbacks,
    }
    params.update({k: v for k, v in optional.items() if v is not None})

    return litellm.completion(**params)


def parse_structured_response(
    response: litellm.ModelResponse,
    output_schema: type[BaseModel],
) -> dict[str, Any]:
    """Extrai e valida resposta estruturada de um ``ModelResponse``.

    Tenta, em ordem:
    1. Atributo ``.parsed`` (OpenAI structured outputs).
    2. Parse do conteúdo como JSON + validação Pydantic.
    3. Extração de JSON de dentro de markdown code blocks.
    4. Fallback: dict com campos ``None``.
    """
    choice = response.choices[0]

    parsed = getattr(choice.message, "parsed", None)
    if parsed is not None:
        if isinstance(parsed, BaseModel):
            return parsed.model_dump()
        if isinstance(parsed, dict):
            return parsed

    content = choice.message.content
    if not content:
        return _empty_response(output_schema)

    for text in _extract_json_candidates(content):
        try:
            data = json.loads(text)
            return output_schema.model_validate(data).model_dump()
        except (json.JSONDecodeError, ValueError):
            continue

    return _empty_response(output_schema)


def _extract_json_candidates(content: str) -> list[str]:
    """Retorna candidatos a JSON da resposta, em ordem de prioridade."""
    stripped = _strip_markdown(content)
    candidates = [stripped]
    start = stripped.find("{")
    end = stripped.rfind("}") + 1
    if start != -1 and end > start:
        candidates.append(stripped[start:end])
    return candidates


def _strip_markdown(content: str) -> str:
    """Remove code blocks de markdown da resposta."""
    content = content.strip()
    if content.startswith("```json"):
        content = content[7:]
    elif content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]
    return content.strip()


def _empty_response(
    output_schema: type[BaseModel],
) -> dict[str, Any]:
    """Retorna dict com todos os campos do schema setados como None."""
    schema = output_schema.model_json_schema()
    return {f: None for f in schema.get("properties", {}).keys()}
