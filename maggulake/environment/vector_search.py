from dataclasses import dataclass
from functools import cached_property

from openai import OpenAI
from pymilvus import MilvusClient

from maggulake.environment.settings import Settings
from maggulake.io.milvus import Milvus
from maggulake.vector_search_retriever.data_api import (
    DEFAULT_MAX_RESULTS as DATA_API_DEFAULT_MAX_RESULTS,
)
from maggulake.vector_search_retriever.data_api import (
    DEFAULT_SCORE_THRESHOLD as DATA_API_DEFAULT_SCORE_THRESHOLD,
)
from maggulake.vector_search_retriever.data_api import get_data_api_retriever
from maggulake.vector_search_retriever.milvus import (
    DEFAULT_MAX_RESULTS as MILVUS_DEFAULT_MAX_RESULTS,
)
from maggulake.vector_search_retriever.milvus import (
    DEFAULT_SCORE_THRESHOLD as MILVUS_DEFAULT_SCORE_THRESHOLD,
)
from maggulake.vector_search_retriever.milvus import (
    get_milvus_retriever,
)


@dataclass
class VectorSearch:
    settings: Settings
    openai: OpenAI

    @cached_property
    def milvus(self):
        return MilvusClient(
            uri=self.settings.milvus_uri, token=self.settings.milvus_token
        )

    @cached_property
    def milvus_adapter(self):
        return Milvus(
            uri=self.settings.milvus_uri,
            token=self.settings.milvus_token,
        )

    def get_milvus_retriever(
        self,
        filtro: str | None = None,
        campos_output: list[str] | None = None,
        max_results: int = MILVUS_DEFAULT_MAX_RESULTS,
        score_threshold: float = MILVUS_DEFAULT_SCORE_THRESHOLD,
        format_return: bool = True,
    ):
        return get_milvus_retriever(
            openai_organization=self.openai.organization,
            openai_api_key=self.openai.api_key,
            milvus_uri=self.settings.milvus_uri,
            milvus_token=self.settings.milvus_token,
            filtro=filtro,
            max_results=max_results,
            score_threshold=score_threshold,
            campos_output=campos_output,
            format_return=format_return,
        )

    def get_data_api_retriever(
        self,
        filtro: str | None = None,
        max_results: int = DATA_API_DEFAULT_MAX_RESULTS,
        score_threshold: float = DATA_API_DEFAULT_SCORE_THRESHOLD,
        campos_output: list[str] | None = None,
        format_return: bool = False,
        rerank: bool = False,
    ):
        return get_data_api_retriever(
            data_api_url=self.settings.data_api_url,
            data_api_key=self.settings.data_api_key,
            filtro=filtro,
            max_results=max_results,
            score_threshold=score_threshold,
            campos_output=campos_output,
            format_return=format_return,
            rerank=rerank,
        )
