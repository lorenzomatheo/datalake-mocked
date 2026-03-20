import logging

import requests

logger = logging.getLogger(__name__)


class BluesoftClient:
    PRODUTOS_ENDPOINT = "gtins/"

    def __init__(self, url: str, token: str) -> None:
        self.address = url
        self.headers = {
            "User-Agent": "Cosmos-API-Request",
            "Content-Type": "application/json",
            "X-Cosmos-Token": token,
        }

    def procura_info_produto_por_ean(self, ean: str) -> dict:
        try:
            response = requests.get(
                self.address + self.PRODUTOS_ENDPOINT + ean,
                headers=self.headers,
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
                "BluesoftClient.procura_info_produto_por_ean - erro: %s",
                error_message,
                extra={"ean": ean},
            )
            return {}

        if not response.text:
            logger.debug(
                "BluesoftClient.procura_info_produto_por_ean - resposta vazia",
                extra={"ean": ean},
            )
            return {}

        data = response.json().get("description", [])

        if data:
            logger.info(
                "BluesoftClient.procura_info_produto_por_ean - EAN encontrado",
                extra={"ean": ean, "nome": data},
            )
            return {
                "ean": ean,
                "nome": data,
                "descricao": data,
            }

        logger.debug(
            "BluesoftClient.procura_info_produto_por_ean - EAN não encontrado",
            extra={"ean": ean},
        )
        return {}
