from typing import List

from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from openai import OpenAI
from pymilvus import MilvusClient

# MILVUS_COLLECTION_NAME = "produtos"
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-large"
DEFAULT_EMBEDDING_MODEL_DIMENSION = 1536
DEFAULT_MAX_RESULTS = 5
DEFAULT_SCORE_THRESHOLD = 0.5
DEFAULT_SEARCH_VECTOR_FIELD = "vector"


class MilvusRetriever(BaseRetriever):
    openai_client: OpenAI
    milvus_client: MilvusClient
    filtro: str | None = None
    campos_output: List[str] | None = None
    max_results: int = DEFAULT_MAX_RESULTS
    score_threshold: float = DEFAULT_SCORE_THRESHOLD
    format_return: bool = True
    # TODO: Search type

    @classmethod
    def create(
        cls,
        openai_organization,
        openai_api_key,
        milvus_uri,
        milvus_token,
        *args,
        **kwargs,
    ):
        openai_client = OpenAI(organization=openai_organization, api_key=openai_api_key)

        milvus_client = MilvusClient(uri=milvus_uri, token=milvus_token)

        return cls(
            *args, **kwargs, openai_client=openai_client, milvus_client=milvus_client
        )

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,  # pylint: disable=unused-argument
    ) -> List[Document]:
        embeddings = self.get_embeddings([query])[0]
        response = self.search_milvus([embeddings])

        return response[0]

    def batch(
        self,
        inputs,
        config=None,  # pylint: disable=unused-argument
        *,
        return_exceptions: bool = False,  # pylint: disable=unused-argument
        **kwargs,  # pylint: disable=unused-argument
    ):
        embeddings = self.get_embeddings(inputs)
        response = self.search_milvus(embeddings)

        return response

    def get_embeddings(self, texts):
        texts = [t.replace("\n", " ") for t in texts if isinstance(t, str)]

        response = self.openai_client.embeddings.create(
            input=texts,
            model=DEFAULT_EMBEDDING_MODEL,
            dimensions=DEFAULT_EMBEDDING_MODEL_DIMENSION,
        )

        return [d.embedding for d in response.data]

    def search_milvus(self, vectors):
        response = self.milvus_client.search(
            collection_name="produtos",
            anns_field=DEFAULT_SEARCH_VECTOR_FIELD,
            data=vectors,
            limit=self.max_results,
            output_fields=self.campos_output
            or ["nome", "ean", "informacoes_para_embeddings"],
            filter=self.filtro,
            search_params={"params": {"radius": self.score_threshold}},
        )

        if not self.format_return:
            return list(response)

        return [
            [
                Document(
                    page_content=p["entity"].get("informacoes_para_embeddings"),
                    metadata={"id": p["id"], **p["entity"]},
                )
                for p in r
            ]
            for r in response
        ]


def get_milvus_retriever(
    openai_organization: str,
    openai_api_key: str,
    milvus_uri: str,
    milvus_token: str,
    filtro: str = None,
    max_results: int = DEFAULT_MAX_RESULTS,
    score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    campos_output: List[str] = None,
    format_return: bool = True,
) -> MilvusRetriever:
    return MilvusRetriever.create(
        openai_organization,
        openai_api_key,
        milvus_uri,
        milvus_token,
        filtro=filtro,
        max_results=max_results,
        score_threshold=score_threshold,
        campos_output=campos_output,
        format_return=format_return,
    )
