# TODO: Como vai ser utilizado em diversos lugares, valeria deixar exposto no Environment.

from maggulake.llm.agno_tracer import TracedAgent
from maggulake.llm.embeddings import (
    EmbeddingEncodingModels,
    get_max_tokens_by_embedding_model,
)
from maggulake.llm.litellm_client import (
    build_thinking_param,
    completion,
    parse_structured_response,
    resolve_litellm_model,
)
from maggulake.llm.models import (
    LLMConfig,
    LlmModels,
    get_llm_config,
    get_model_name,
)
from maggulake.llm.tagging_processor import TaggingProcessor
from maggulake.llm.vertex import (
    create_vertex_context_cache,
    get_location_by_model,
    setup_langsmith,
    setup_vertex_ai_credentials,
)

__all__ = [
    "EmbeddingEncodingModels",
    "get_max_tokens_by_embedding_model",
    "LlmModels",
    "get_model_name",
    "TaggingProcessor",
    "TracedAgent",
    "setup_vertex_ai_credentials",
    "setup_langsmith",
    "get_location_by_model",
    "LLMConfig",
    "get_llm_config",
    "create_vertex_context_cache",
    "completion",
    "parse_structured_response",
    "resolve_litellm_model",
    "build_thinking_param",
]
