import logging

import requests

from maggulake.integrations.tools import calcula_frase_media_por_embeddings

logger = logging.getLogger(__name__)


class BigDataCorpClient:
    PRODUTOS_ENDPOINT = "produtos"

    def __init__(self, url: str, token: str) -> None:
        self.address = url
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": token,
        }

    def procura_info_produto_por_ean(self, ean: str) -> dict:
        """
        Busca os dados do produto da API BigDataCorp usando o EAN fornecido.
        Retorna um dicionário com informações do produto
        """
        payload = {"Datasets": "basic_data", "q": f"ean{{{ean}}}"}

        response = None
        try:
            response = requests.post(
                self.address + self.PRODUTOS_ENDPOINT,
                headers=self.headers,
                json=payload,
                timeout=5,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as error_message:
            if (
                isinstance(error_message, requests.exceptions.HTTPError)
                and error_message.response is not None
                and error_message.response.status_code == 429
            ):
                raise
            logger.error(
                "BigDataCorpClient.procura_info_produto_por_ean - erro: %s",
                error_message,
                extra={
                    "ean": ean,
                    "duracao_segundos": (
                        response.elapsed.total_seconds() if response else None
                    ),
                },
            )
            return {}

        if not response.text:
            logger.error(
                "BigDataCorpClient.procura_info_produto_por_ean - resposta vazia",
                extra={
                    "ean": ean,
                    "duracao_segundos": response.elapsed.total_seconds(),
                },
            )
            return {}

        data = response.json()
        result = data.get("Result", [])

        if result:
            product_data = result[0].get("BasicData", {})

            nomes = list(self._get_items(product_data, "title"))
            descricoes = list(self._get_items(product_data, "description"))

            melhor_nome = calcula_frase_media_por_embeddings(nomes)
            melhor_nome_idx = nomes.index(melhor_nome)

            try:
                melhor_descricao = descricoes[melhor_nome_idx]
            except IndexError:
                melhor_descricao = calcula_frase_media_por_embeddings(descricoes)

            logger.info(
                "BigDataCorpClient.procura_info_produto_por_ean - EAN encontrado",
                extra={
                    "ean": ean,
                    "duracao_segundos": response.elapsed.total_seconds(),
                    "melhor_nome": melhor_nome,
                    "melhor_descricao": melhor_descricao,
                    "nomes": nomes,
                    "descricoes": descricoes,
                },
            )
            return {
                'ean': ean,
                'nome': melhor_nome,
                'descricao': melhor_descricao,
            }

        logger.error(
            "BigDataCorpClient.procura_info_produto_por_ean - EAN não encontrado",
            extra={
                "ean": ean,
                "duracao_segundos": response.elapsed.total_seconds(),
            },
        )
        return {}

    def _get_items(self, d: dict, k: str = "title"):
        if not isinstance(d, dict):
            return
        for key, value in d.items():
            if key.lower() == k.lower():
                yield value
            if isinstance(value, dict):
                yield from self._get_items(value)
            elif isinstance(value, list):
                for item in value:
                    yield from self._get_items(item)
