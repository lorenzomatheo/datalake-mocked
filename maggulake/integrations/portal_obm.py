import json
import logging

import requests

logger = logging.getLogger(__name__)


class PortalObmClient:
    ENDPOINT = "ampp"

    def __init__(self, url: str, token: str) -> None:
        self.address = url
        self.token = token
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def procura_info_produto_por_ean(self, ean: str) -> dict:
        params = {"api_token": self.token, "where": json.dumps({"NU_EAN13": ean})}
        try:
            response = requests.get(
                self.address + self.ENDPOINT,
                headers=self.headers,
                params=params,
                timeout=10,
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
                "PortalObmClient.procura_info_produto_por_ean - erro: %s",
                error_message,
                extra={"ean": ean},
            )
            return {}

        if not response.text or not response.text.strip():
            logger.debug(
                "PortalObmClient.procura_info_produto_por_ean - resposta vazia",
                extra={"ean": ean},
            )
            return {}

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            logger.warning(
                "PortalObmClient.procura_info_produto_por_ean"
                " - resposta não é JSON válido",
                extra={"ean": ean, "response_text": response.text[:200]},
            )
            return {}

        if data:
            nome = data[0].get("NO_NM")
            if nome:
                logger.info(
                    "PortalObmClient.procura_info_produto_por_ean - EAN encontrado",
                    extra={"ean": ean, "nome": nome},
                )
                return {"ean": ean, "nome": nome, "descricao": nome}

        logger.debug(
            "PortalObmClient.procura_info_produto_por_ean - EAN não encontrado",
            extra={"ean": ean},
        )
        return {}
