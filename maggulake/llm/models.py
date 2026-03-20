import os
from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional

from pyspark.sql import SparkSession

from maggulake.llm.vertex import setup_vertex_ai_credentials


class LlmModels(Enum):
    GEMINI_FLASH = "gemini-2.5-flash"
    GEMINI_PRO = "gemini-2.5-pro"
    GPT_4O = "gpt-4o"
    GPT_4O_MINI = "gpt-4o-mini"
    GPT_5_1 = "gpt-5.1"
    GPT_5_MINI = "gpt-5-mini"
    GEMINI_FLASH_LITE = "gemini-2.5-flash-lite"


@dataclass
class LLMConfig:
    provider: str
    model: Optional[str]
    api_key: Optional[str]
    project_id: Optional[str]
    location: Optional[str]


def get_model_name(
    provider: Literal["openai", "gemini", "gemini_vertex"],
    size: Literal["SMALL", "LARGE"] = "SMALL",
    reasoning: bool = False,
) -> str:
    default_model = os.environ.get("DEFAULT_LLM_MODEL")
    if default_model:
        return default_model

    match provider.lower():
        case "gemini" | "gemini_vertex":
            if size == "LARGE":
                return LlmModels.GEMINI_PRO.value
            return LlmModels.GEMINI_FLASH.value
        case "openai":
            if reasoning:
                if size == "LARGE":
                    return LlmModels.GPT_5_1.value
                return LlmModels.GPT_5_MINI.value
            if size == "LARGE":
                return LlmModels.GPT_4O.value
            return LlmModels.GPT_4O_MINI.value
        case _:
            raise NotImplementedError(
                "Unsupported provider. Use 'openai', 'gemini', or 'gemini_vertex'."
            )


def get_llm_config(
    provider: Literal["openai", "gemini_vertex", "medgemma"],
    spark: SparkSession,
    dbutils,
    size: Literal["SMALL", "LARGE"] = "SMALL",
    reasoning: bool = False,
) -> LLMConfig:
    match provider:
        case "medgemma":
            setup_vertex_ai_credentials(spark)
            return LLMConfig(
                provider=provider,
                model=None,
                api_key=None,
                project_id=None,
                location=None,
            )
        case "gemini_vertex":
            project_id, location = setup_vertex_ai_credentials(spark)
            return LLMConfig(
                provider=provider,
                model=get_model_name(
                    provider="gemini_vertex", size=size, reasoning=reasoning
                ),
                api_key=None,
                project_id=project_id,
                location=location,
            )
        case "openai":
            return LLMConfig(
                provider=provider,
                model=get_model_name(provider="openai", size=size, reasoning=reasoning),
                api_key=dbutils.secrets.get(scope="llm", key="OPENAI_API_KEY"),
                project_id=None,
                location=None,
            )
        case _:
            raise ValueError(f"Unsupported provider: {provider}")
