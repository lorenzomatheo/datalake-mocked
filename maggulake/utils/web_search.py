import random
import time
from typing import Literal

import requests
from agno.agent import Agent
from agno.models.google import Gemini
from langchain_community.tools import DuckDuckGoSearchResults
from langchain_community.utilities import DuckDuckGoSearchAPIWrapper
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from tavily import TavilyClient

from maggulake.llm.agno_tracer import TracedAgent
from maggulake.llm.vertex import setup_vertex_ai_credentials

# TODO: incluir possibilidade de salvar resultados da busca em uma tabela de cache.
# Oferecer possibilidade de consultar no cache antes de fazer a busca na web.


class BuscaWeb:
    def __init__(self, spark: SparkSession, region: str = "br-pt"):
        self.spark = spark
        self.region = region
        self._inicializar_motores_busca()

    def _limpar_consulta(self, query: str):
        if not query or not query.strip():
            return None
        return query.strip('"')

    def _inicializar_motores_busca(self):
        try:
            dbutils = DBUtils(self.spark)

            PROJECT_ID, LOCATION = setup_vertex_ai_credentials(self.spark)

            # Gemini with Vertex AI
            GEMINI_MODEL = 'gemini-2.5-flash-lite'
            self.web_search_agent = TracedAgent(
                Agent(
                    model=Gemini(
                        id=GEMINI_MODEL,
                        vertexai=True,
                        project_id=PROJECT_ID,
                        location=LOCATION,
                        search=True,
                        include_thoughts=False,
                    ),
                    reasoning=False,
                    description="Você é um agente de busca na web que ajuda usuários a encontrar informações diretas sobre produtos.",
                    instructions=[
                        """
                    # [OBJETIVO]
                    "Seu objetivo é responder perguntas sobre produtos, buscando informações primariamente em sites farmacêuticos, bulas online ou e-commerces de farmácias em português.",

                    # [LIMITE CRITICO] e priorização
                    "Responda da maneira mais objetiva e concisa possível, fornecendo APENAS uma resposta,sem adicionar frases, contexto ou raciocinio."

                    # [EXEMPLO]
                    "Siga estritamente o formato dos exemplos abaixo. Não inclua 'Pergunta:' ou 'Resposta:' na sua saída final, apenas o valor da resposta.",
                    "Exemplo 1:\n- Pergunta do usuário: Qual o fabricante do Roacutan?\n- Resposta: Roche",
                    "Exemplo 2:\n- Pergunta do usuário: Qual a marca do ean 7909843946212 ?\n- Resposta: Havaianas",

                    # [EM CASO DE AUSÊNCIA]
                    "Se a informação não for encontrada de forma clara e direta na busca, retorne estritamente a palavra `null`.",
                    """
                    ],
                    markdown=False,
                ),
                name="agno-web-search",
            )

            # DuckDuckGo
            duckduckgo_wrapper = DuckDuckGoSearchAPIWrapper(
                backend="auto", region=self.region
            )
            self.duckduckgo_search = DuckDuckGoSearchResults(
                api_wrapper=duckduckgo_wrapper, source="text", max_results=5
            )

            # Tavily
            tavily_api_key = dbutils.secrets.get(
                scope="llms", key="TAVILY_API_KEY_PROD"
            )
            self.tavily_client = TavilyClient(tavily_api_key)

        except Exception as e:
            raise ValueError(f"Falha ao inicializar motores de busca: {str(e)}")

    def buscar_tavily(self, query: str) -> str:
        consulta = self._limpar_consulta(query)
        if consulta is None:
            return ""

        try:
            results = self.tavily_client.search(
                query=consulta, search_depth="basic", max_results=3, timeout=5
            ).get('results', [])

            return str(results)
        except requests.RequestException as e:
            error_message = str(e).lower()

            # Verifica se é rate limit
            if (
                "rate limit" in error_message
                or "429" in error_message
                or "too many requests" in error_message
            ):
                # Espera 1 minuto pois o rate limit é 1000 RPM https://docs.tavily.com/documentation/rate-limits
                time.sleep(60)

            # Retorna erro para cair no fallback do ddg
            raise ValueError(f"Falha na busca Tavily: {error_message}")

    def buscar_duckduckgo(self, query: str) -> str:
        consulta = self._limpar_consulta(query)
        if consulta is None:
            return ""

        max_retries = 3
        base_delay = 2

        for attempt in range(max_retries + 1):
            try:
                return self.duckduckgo_search.invoke(consulta)
            except requests.RequestException as e:
                error_message = str(e).lower()
                is_rate_limit = any(
                    phrase in error_message
                    for phrase in ["ratelimit", "rate limit", "202 ratelimit"]
                )

                if not is_rate_limit or attempt == max_retries:
                    return ""

                delay = base_delay * (2**attempt) * (0.5 + random.random())
                time.sleep(delay)

        return ""

    def buscar_na_internet(
        self, query: str, motor_preferido: Literal["gemini", "tavily"] = "gemini"
    ) -> str:
        # pylint: disable=too-many-return-statements
        consulta = self._limpar_consulta(query)
        if consulta is None:
            return ""

        if motor_preferido.lower() == "gemini":
            try:
                gemini_results = self.web_search_agent.run(consulta).content

                # Se o Gemini não retornar resultados, usar DuckDuckGo como fallback
                if gemini_results == "[]" or not gemini_results:
                    return self.buscar_duckduckgo(consulta)

                return gemini_results

            except ValueError:
                return self.buscar_duckduckgo(consulta)

        if motor_preferido.lower() == "tavily":
            try:
                tavily_results = self.buscar_tavily(consulta)

                # Se o Tavily não retornar resultados, usar DuckDuckGo como fallback
                if tavily_results == "[]" or not tavily_results:
                    return self.buscar_duckduckgo(consulta)

                return tavily_results

            except ValueError:
                return self.buscar_duckduckgo(consulta)
        else:
            try:
                ddg_results = self.buscar_duckduckgo(consulta)

                # Se o DuckDuckGo não retornar resultados úteis, usar Tavily como fallback
                if not ddg_results or ddg_results == "No good search result found":
                    return self.buscar_tavily(consulta)

                return ddg_results

            except ValueError:
                return self.buscar_tavily(consulta)


# Exemplo de uso
# web_search_client = BuscaWeb(spark)
# web_search_client.buscar_na_internet("Por que o céu é azul?")
