import time
from typing import Any, Literal

import requests
from requests.exceptions import RequestException

from maggulake.integrations.customerx.models.contact_customerx import (
    ContactCustomerXDTO,
)
from maggulake.integrations.customerx.models.customer import CustomerXCustomerDTO
from maggulake.integrations.customerx.models.customerx_group import (
    CustomerXGroupDTO,
)


class CustomerXClient:
    def __init__(
        self,
        api_token: str,
        cx_environment: Literal["sandbox", "production"] = "sandbox",
        max_retries: int = 3,
        rate_limit_buffer: int = 5,
    ):
        self.api_token = api_token
        self.cx_environment = cx_environment
        self.max_retries = max_retries
        self.rate_limit_buffer = rate_limit_buffer

        self.headers = {
            "Authorization": api_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @property
    def base_url(self) -> str:
        if self.cx_environment == "production":
            return "https://api.customerx.com.br"
        return "https://sandbox.api.customerx.com.br"

    def _check_rate_limit(self, response: requests.Response) -> None:
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_time = response.headers.get("X-RateLimit-Reset")

        if remaining and int(remaining) < self.rate_limit_buffer:
            if reset_time:
                wait_time = int(reset_time) - int(time.time())
                if wait_time > 0:
                    print(
                        f"⏸️  Rate limit approaching. Waiting {wait_time}s until reset..."
                    )
                    time.sleep(wait_time + 1)
            else:
                # Default wait if no reset time provided
                print("⏸️  Rate limit approaching. Waiting 60s...")
                time.sleep(60)

    def _make_request(
        self,
        method: str,
        endpoint: str,
        json_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    json=json_data,
                    params=params,
                    timeout=60,
                )

                # Check rate limits
                self._check_rate_limit(response)

                # Raise for 4xx/5xx status codes
                response.raise_for_status()

                return response

            except RequestException as e:
                # Try to extract error details from response
                error_details = ""
                if hasattr(e, 'response') and e.response is not None:
                    try:
                        error_json = e.response.json()
                        error_details = f" - Details: {error_json}"
                    except (ValueError, AttributeError, KeyError):
                        # ValueError: invalid JSON
                        # AttributeError: no json() method
                        # KeyError: unexpected response structure
                        error_details = f" - Response text: {e.response.text[:500]}"

                    # Não fazer retry em erros 4xx (client errors) - são erros de payload/validação
                    # que não serão resolvidos tentando novamente
                    # Exemplos: 400 Bad Request, 422 Unprocessable Entity, 404 Not Found
                    if hasattr(e.response, 'status_code'):
                        try:
                            status_code = int(e.response.status_code)
                            if 400 <= status_code < 500:
                                print(
                                    f"❌ Request failed with client error {status_code}: {e}{error_details}"
                                )
                                raise
                        except (ValueError, TypeError):
                            # Se status_code não for um número válido, continua com retry normal
                            pass

                if attempt < self.max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    print(
                        f"⚠️  Request failed (attempt {attempt + 1}/{self.max_retries}): {e}{error_details}"
                    )
                    print(f"   Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(
                        f"❌ Request failed after {self.max_retries} attempts: {e}{error_details}"
                    )
                    raise

        # This should never be reached due to raise above, but makes pylint happy
        raise RequestException("Request failed after all retries")

    def fetch_all_customers(
        self,
        validacao_estrita: bool = True,
        customers_per_page: int = 20,
    ) -> list[CustomerXCustomerDTO]:
        """Busca todos os clientes da API CustomerX.

        Args:
            validacao_estrita: Se True, exige que todos os clientes tenham postgres_uuid.
                              Se False, permite clientes sem postgres_uuid
                              (útil para operações de limpeza/reset onde podem existir
                              clientes criados manualmente)
            customers_per_page: Quantidade de clientes por página (máx 20 segundo docs CX)
        """
        todos_clientes: list[CustomerXCustomerDTO] = []
        pagina = 1

        modo_validacao = (
            "com validação estrita" if validacao_estrita else "sem validação"
        )
        print(f"📥 Buscando clientes do CustomerX ({modo_validacao})...")

        while True:
            response = self._make_request(
                method="GET",
                endpoint="/api/v1/clients",
                params={"page": pagina, "per_page": customers_per_page},
            )

            clientes_raw = response.json()
            clientes = [
                CustomerXCustomerDTO.from_api_response(
                    c, validacao_estrita=validacao_estrita
                )
                for c in clientes_raw
            ]
            todos_clientes.extend(clientes)

            # Check pagination headers
            total_paginas = int(response.headers.get("X-Pages", 1))
            pagina_atual = int(response.headers.get("X-Page", pagina))
            total_registros = int(response.headers.get("X-Total", 0))

            print(
                f"   Página {pagina_atual}/{total_paginas} - "
                f"{len(clientes)} clientes "
                f"(Total: {len(todos_clientes)}/{total_registros})"
            )

            if pagina_atual >= total_paginas:
                break

            pagina += 1

        print(f"✅ Total de {len(todos_clientes)} clientes encontrados")
        return todos_clientes

    def criar_cliente_com_payload(
        self,
        payload: dict[str, Any],
        debug: bool = False,
    ) -> dict[str, Any]:
        """Cria um cliente (rede ou loja) no CustomerX usando payload da API.

        Use este método com payloads gerados por ContaDTO.to_customerx_payload()
        ou LojaDTO.to_customerx_payload().

        Args:
            payload: Dicionário com campos da API:
                - company_name (str): Nome da empresa
                - date_register (str): Data no formato DD/MM/AAAA
                - external_id_client (str): UUID do cliente
                - trading_name (str, opcional): Nome fantasia
                - parent_company (bool, opcional): Se é matriz
                - custom_attributes (list, opcional): Atributos customizados
            debug: Se True, imprime payload antes de enviar

        Returns:
            Resposta da API com o cliente criado
        """
        if debug:
            print(f"   📋 Payload: {payload}")

        response = self._make_request(
            method="POST",
            endpoint="/api/v1/clients",
            json_data=payload,
        )

        return response.json()

    def associar_filiais(
        self,
        id_curto_cx_matriz: str,
        ids_curtos_cx_filiais: list[str],
    ) -> dict[str, Any]:
        """Associa filiais a uma matriz usando os external_id_client da API.

        IMPORTANTE: Use os valores de id_curto_cx (external_id_client) retornados
        pela API, NÃO os UUIDs do Postgres diretamente!

        Args:
            id_curto_cx_matriz: external_id_client da matriz retornado pela API
            ids_curtos_cx_filiais: Lista de external_id_client das filiais
        """
        payload = {
            "external_id_client": id_curto_cx_matriz,
            "branch_clients_external_ids": ids_curtos_cx_filiais,
        }

        response = self._make_request(
            method="PUT",
            endpoint=f"/api/v1/clients/{id_curto_cx_matriz}/branch_clients",
            json_data=payload,
        )

        return response.json()

    def fetch_all_contacts(
        self, contacts_per_page: int = 20
    ) -> list[ContactCustomerXDTO]:
        """Busca todos os contatos da API do CustomerX com paginação."""
        all_contacts = []
        page = 1

        while True:
            response = self._make_request(
                method="GET",
                endpoint="/api/v1/contacts",
                params={"page": page, "per_page": contacts_per_page},
            )

            contacts = response.json()
            all_contacts.extend(
                [ContactCustomerXDTO.from_api_response(c) for c in contacts]
            )

            # Check pagination headers
            total_pages = int(response.headers.get("X-Pages", 1))
            current_page = int(response.headers.get("X-Page", page))
            total_records = int(response.headers.get("X-Total", 0))

            print(
                f"   Página {current_page}/{total_pages} - "
                f"{len(contacts)} contatos "
                f"(Total: {len(all_contacts)}/{total_records})"
            )

            if current_page >= total_pages:
                break

            page += 1

        print(f"✅ Total de {len(all_contacts)} contatos encontrados")

        return all_contacts

    def criar_contato(
        self, payload: dict[str, Any], debug: bool = False
    ) -> dict[str, Any]:
        """Cria um novo contato na API do CustomerX.

        Use este método com payloads gerados por ContactDTO.to_customerx_payload().

        Args:
            payload: Dicionário com campos da API:
                - name (str): Nome do contato
                - type_contact (str): Tipo do contato ('Dono', 'Gerente')
                - external_id_client (str): UUID da conta/loja
                - email (str, opcional): Email do contato
                - document (str, opcional): CPF do contato (11 dígitos)
                - phones (list, opcional): Lista de telefones no formato:
                    [{"number": "(XX) XXXXX-XXXX", "is_default": True, "ddi": "+55"}]
            debug: Se True, imprime payload antes de enviar

        Returns:
            Resposta da API com o contato criado
        """
        if debug:
            print(f"   📋 Payload: {payload}")

        response = self._make_request(
            method="POST",
            endpoint="/api/v1/contacts",
            json_data=payload,
        )

        return response.json()

    def atualizar_contato(
        self, contact_id: str, payload: dict[str, Any], debug: bool = False
    ) -> dict[str, Any]:
        """Atualiza um contato existente na API do CustomerX.

        Use este método para atualizar contatos já criados, por exemplo, para
        substituir emails fake (loja_{uuid}@maggu.ai) por emails reais quando
        gerentes cadastrarem seus dados.

        Args:
            contact_id: ID interno do CustomerX do contato a atualizar (campo 'id' da API)
            payload: Dicionário com campos da API a atualizar:
                - name (str, opcional): Nome do contato
                - type_contact (str, opcional): Tipo do contato ('Dono', 'Gerente')
                - email (str, opcional): Email do contato
                - document (str, opcional): CPF do contato (11 dígitos)
                - phones (list, opcional): Lista de telefones no formato:
                    [{"number": "(XX) XXXXX-XXXX", "is_default": True, "ddi": "+55"}]
                - external_id_client (str, opcional): UUID da conta/loja
                - custom_attributes (list, opcional): Atributos customizados
            debug: Se True, imprime payload antes de enviar

        Returns:
            Resposta da API com o contato atualizado

        Note:
            A API CustomerX usa PUT para atualizações. Nenhum campo é obrigatório.
            Apenas os campos incluídos no payload serão atualizados. Campos com
            valores vazios ou nulos serão atualizados para vazio/nulo.
        """
        if debug:
            print(f"   📋 Payload de atualização: {payload}")

        response = self._make_request(
            method="PUT",
            endpoint="/api/v1/contacts/",
            params={"id": contact_id},
            json_data=payload,
        )

        return response.json()

    def fetch_all_groups(self, groups_per_page: int = 20) -> list[CustomerXGroupDTO]:
        """Busca todos os grupos da API CustomerX.

        Args:
            groups_per_page: Quantidade de grupos por página (máx 20 segundo docs CX)

        Returns:
            Lista de CustomerXGroupDTO com dados dos grupos
        """
        all_groups: list[CustomerXGroupDTO] = []
        page = 1

        print("📥 Buscando grupos do CustomerX...")

        while True:
            response = self._make_request(
                method="GET",
                endpoint="/api/v1/groups",
                params={"page": page, "per_page": groups_per_page},
            )

            data = response.json()

            # A API de grupos pode retornar lista diretamente ou dict com 'data'
            if isinstance(data, list):
                groups_data = data
            else:
                groups_data = data.get("data", [])

            if not groups_data:
                break

            # Converte dicionários para DTOs
            groups = [CustomerXGroupDTO.from_api_response(g) for g in groups_data]
            all_groups.extend(groups)

            # Verifica se há mais páginas (apenas se data for dict)
            if isinstance(data, dict):
                total_pages = data.get("last_page", page)
                if page >= total_pages:
                    break
            else:
                # Se retorna lista direta e está vazia, não há mais páginas
                if len(groups_data) < groups_per_page:
                    break

            page += 1

        print(f"   ✅ Total de grupos recuperados: {len(all_groups)}")
        return all_groups

    def criar_grupo(
        self, payload: dict[str, Any], debug: bool = False
    ) -> dict[str, Any]:
        """Cria um novo grupo na API do CustomerX.

        Use este método com payloads gerados por GroupDTO.to_customerx_payload().

        Args:
            payload: Dicionário com campos da API:
                - external_id (str): Identificador externo único
                - description (str): Nome/descrição do grupo
                - status (bool): Grupo ativo (True) ou inativo (False)
            debug: Se True, imprime payload antes de enviar

        Returns:
            Resposta da API com o grupo criado
        """
        if debug:
            print(f"   📋 Payload: {payload}")

        response = self._make_request(
            method="POST",
            endpoint="/api/v1/groups",
            json_data=payload,
        )

        return response.json()

    def atualizar_cliente(
        self,
        external_id_client: str,
        payload: dict[str, Any],
        debug: bool = False,
    ) -> dict[str, Any]:
        """Atualiza um cliente (matriz ou filial) existente no CustomerX.

        Use este método para atualizar clientes já criados quando houver alterações
        no Postgres (nome, custom_attributes, etc).

        Args:
            external_id_client: ID do CustomerX
            payload: Dicionário com campos da API a atualizar:
                - company_name (str, opcional): Nome da empresa
                - trading_name (str, opcional): Nome fantasia
                - custom_attributes (list, opcional): Atributos customizados
                Outros campos permitidos pela API CustomerX
            debug: Se True, imprime payload antes de enviar

        Returns:
            Resposta da API com o cliente atualizado

        Note:
            A API CustomerX usa PUT para atualizações. Apenas os campos incluídos
            no payload serão atualizados.
        """
        if debug:
            print(f"   📋 Payload de atualização: {payload}")

        response = self._make_request(
            method="PUT",
            endpoint=f"/api/v1/clients/{external_id_client}",
            json_data=payload,
        )

        return response.json()

    def atualizar_grupo(
        self,
        group_id: str | int,
        payload: dict[str, Any],
        debug: bool = False,
    ) -> dict[str, Any]:
        """Atualiza um grupo existente no CustomerX.

        Use este método para atualizar grupos já criados quando houver alterações
        no Postgres.

        Args:
            group_id: ID do grupo no CustomerX ou external_id
            payload: Dicionário com campos da API a atualizar:
                - description (str, opcional): Nome/descrição do grupo
                - status (bool, opcional): Grupo ativo (True) ou inativo (False)
            debug: Se True, imprime payload antes de enviar

        Returns:
            Resposta da API com o grupo atualizado

        Note:
            A API CustomerX usa PUT para atualizações de grupos.
        """
        if debug:
            print(f"   📋 Payload de atualização: {payload}")

        response = self._make_request(
            method="PUT",
            endpoint=f"/api/v1/groups/{str(group_id)}",
            json_data=payload,
        )

        return response.json()
