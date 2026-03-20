from typing import List, Optional
from urllib.parse import urljoin

import requests
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from requests.exceptions import JSONDecodeError

DEFAULT_MAX_RESULTS = 5
DEFAULT_SCORE_THRESHOLD = 0.5


class DataApiRetriever(BaseRetriever):
    data_api_url: str
    data_api_key: str
    filtro: str | None = None
    max_results: int = DEFAULT_MAX_RESULTS
    score_threshold: float = DEFAULT_SCORE_THRESHOLD
    campos_output: List[str] | None = None
    format_return: bool = False
    rerank: bool = False
    batch_size: Optional[int] = None

    # TODO: invoke chama batch()[0] sem guard clause — IndexError se batch retornar lista vazia.
    # TODO: adicionar type hints em input, config e retorno.
    def invoke(self, input, config=None, **kwargs):  # pylint: disable=unused-argument
        return self.batch([input])[0]

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,  # pylint: disable=unused-argument
    ) -> list[Document]:
        return self.batch([query])[0]

    def batch(self, inputs: list[str], **kwargs) -> list[list[Document]]:  # pylint: disable=unused-argument
        url = urljoin(self.data_api_url, "retriever/batch")
        headers = {"X-API-Key": self.data_api_key}

        payload = {
            "queries": inputs,
            "filter_expr": self.filtro,
            "max_results": self.max_results,
            "score_threshold": self.score_threshold,
            "output_fields": self.campos_output,
            "rerank": self.rerank or False,
            "format_as_document": self.format_return or False,
        }

        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response_data = response.json()

        if not self.format_return:
            return response_data

        return [
            [
                Document(
                    page_content=p["entity"].get("informacoes_para_embeddings"),
                    metadata={"id": p["id"], "ean": p["entity"].get("ean")},
                )
                for p in result
            ]
            for result in response_data
        ]

    def iterate(self, query: str) -> list[list[Document]]:
        """retorna uma lista de Recomendacao para o cache"""
        url = urljoin(self.data_api_url, "retriever/iterate")
        headers = {"X-API-Key": self.data_api_key}
        payload = {
            "query": query,
            "filter_expr": self.filtro,
            "max_results": self.max_results,
            "batch_size": self.batch_size,
            "score_threshold": self.score_threshold,
            "output_fields": self.campos_output,
            "with_content": self.format_return or False,
        }

        response = requests.post(url, headers=headers, json=payload, timeout=600)
        response.raise_for_status()

        try:
            response_data = response.json()
        except JSONDecodeError:
            print(
                f"Falha ao decodificar JSON da URL: {response.url}\nStatus Code: {response.status_code}\nConteúdo da Resposta: {response.text[:200]}"
            )
            return []

        if not self.format_return:
            return response_data

        return [
            Document(
                id=result['metadata'].get("id"),
                page_content=result.get('page_content'),
                score=result['metadata'].get("score", 0.0),
                rerank_score=result['metadata'].get("relevance_score"),
                metadata=result['metadata'],
            )
            for result in response_data
        ]


def get_data_api_retriever(
    data_api_url: str,
    data_api_key: str,
    filtro: str = None,
    max_results: int = DEFAULT_MAX_RESULTS,
    score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    campos_output: List[str] = None,
    format_return: bool = False,
    rerank: bool = False,
) -> DataApiRetriever:
    return DataApiRetriever(
        data_api_url=data_api_url,
        data_api_key=data_api_key,
        filtro=filtro,
        max_results=max_results,
        score_threshold=score_threshold,
        campos_output=campos_output,
        format_return=format_return,
        rerank=rerank,
    )
