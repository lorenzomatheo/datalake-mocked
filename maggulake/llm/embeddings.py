from enum import Enum


class EmbeddingEncodingModels(Enum):
    CL100K_BASE = "cl100k_base"


def get_max_tokens_by_embedding_model(model: str | EmbeddingEncodingModels) -> int:
    if isinstance(model, EmbeddingEncodingModels):
        model = model.value

    max_tokens_mapping = {
        EmbeddingEncodingModels.CL100K_BASE.value: 13000,
    }
    if model not in max_tokens_mapping:
        raise ValueError(
            f"Unsupported embedding model: {model}. "
            f"Options are: {list(max_tokens_mapping.keys())}"
        )
    return max_tokens_mapping[model]
