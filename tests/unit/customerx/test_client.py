"""
Testes para o CustomerXClient.
"""

from unittest.mock import Mock, patch

import pytest
from requests.exceptions import RequestException

from maggulake.integrations.customerx.client import CustomerXClient
from maggulake.integrations.customerx.models.customer import (
    CustomerXCustomerDTO,
)


class TestCustomerXClientRateLimit:
    """Testes para gerenciamento de rate limit."""

    @patch("time.sleep")
    @patch("time.time")
    def test_check_rate_limit_aguarda_quando_proximo_limite(
        self, mock_time, mock_sleep, customerx_client
    ):
        """Testa que aguarda quando próximo ao limite."""
        mock_time.return_value = 1000  # Tempo atual

        response = Mock()
        response.headers = {
            "X-RateLimit-Remaining": "3",  # Menor que buffer (5)
            "X-RateLimit-Reset": "1060",  # Reset em 60 segundos
        }

        customerx_client._check_rate_limit(response)  # pylint: disable=protected-access

        # Deve aguardar 61 segundos (60 + 1)
        mock_sleep.assert_called_once_with(61)

    @patch("time.sleep")
    def test_check_rate_limit_nao_aguarda_quando_acima_buffer(
        self, mock_sleep, customerx_client
    ):
        """Testa que não aguarda quando acima do buffer."""
        response = Mock()
        response.headers = {
            "X-RateLimit-Remaining": "10",  # Maior que buffer
            "X-RateLimit-Reset": "1000",
        }

        customerx_client._check_rate_limit(response)  # pylint: disable=protected-access

        mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_check_rate_limit_aguarda_default_sem_reset_time(
        self, mock_sleep, customerx_client
    ):
        """Testa aguarda tempo padrão quando não há reset time."""
        response = Mock()
        response.headers = {
            "X-RateLimit-Remaining": "2",  # Menor que buffer
            # Sem X-RateLimit-Reset
        }

        customerx_client._check_rate_limit(response)  # pylint: disable=protected-access

        mock_sleep.assert_called_once_with(60)

    def test_check_rate_limit_sem_headers(self, customerx_client):
        """Testa que não falha sem headers de rate limit."""
        response = Mock()
        response.headers = {}

        # Não deve lançar exceção
        customerx_client._check_rate_limit(response)  # pylint: disable=protected-access


class TestCustomerXClientMakeRequest:
    """Testes para o método _make_request."""

    @patch("requests.request")
    def test_make_request_sucesso(self, mock_request, customerx_client_with_retries):
        """Testa request bem-sucedido."""
        mock_response = Mock()
        mock_response.headers = {}
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        result = customerx_client_with_retries._make_request(  # pylint: disable=protected-access
            method="GET", endpoint="/api/v1/test"
        )

        assert result == mock_response
        mock_request.assert_called_once_with(
            method="GET",
            url="https://api.customerx.com.br/api/v1/test",
            headers=customerx_client_with_retries.headers,
            json=None,
            params=None,
            timeout=60,
        )

    @patch("requests.request")
    def test_make_request_com_json_e_params(
        self, mock_request, customerx_client_with_retries
    ):
        """Testa request com JSON data e params."""
        mock_response = Mock()
        mock_response.headers = {}
        mock_request.return_value = mock_response

        json_data = {"key": "value"}
        params = {"page": 1}

        customerx_client_with_retries._make_request(  # pylint: disable=protected-access
            method="POST", endpoint="/api/v1/test", json_data=json_data, params=params
        )

        mock_request.assert_called_once()
        call_kwargs = mock_request.call_args.kwargs
        assert call_kwargs["json"] == json_data
        assert call_kwargs["params"] == params

    @patch("time.sleep")
    @patch("requests.request")
    def test_make_request_retry_com_backoff_exponencial(
        self, mock_request, mock_sleep, customerx_client_with_retries
    ):
        """Testa retry com backoff exponencial."""
        # Primeira tentativa falha, segunda sucede
        mock_response_fail = Mock()
        mock_response_fail.headers = {}  # Adicionar headers vazios
        mock_response_fail.raise_for_status.side_effect = RequestException("Error")
        mock_response_fail.text = "Error details"

        mock_response_success = Mock()
        mock_response_success.headers = {}

        mock_request.side_effect = [mock_response_fail, mock_response_success]

        result = customerx_client_with_retries._make_request(  # pylint: disable=protected-access
            method="GET", endpoint="/test"
        )

        assert result == mock_response_success
        assert mock_request.call_count == 2
        # Backoff exponencial: 2^0 = 1 segundo na primeira falha
        mock_sleep.assert_called_once_with(1)

    @patch("time.sleep")
    @patch("requests.request")
    def test_make_request_falha_apos_max_retries(
        self, mock_request, mock_sleep, customerx_client_with_retries
    ):
        """Testa que levanta exceção após esgotar tentativas."""
        mock_response = Mock()
        mock_response.headers = {}  # Adicionar headers vazios
        mock_response.raise_for_status.side_effect = RequestException(
            "Persistent error"
        )
        mock_response.text = "Error"
        mock_request.return_value = mock_response

        with pytest.raises(RequestException):
            customerx_client_with_retries._make_request(  # pylint: disable=protected-access
                method="GET", endpoint="/test"
            )

        # 3 tentativas (max_retries)
        assert mock_request.call_count == 3
        # 2 sleeps (entre tentativas 1-2 e 2-3)
        assert mock_sleep.call_count == 2

    @patch("requests.request")
    def test_make_request_extrai_detalhes_erro_json(
        self, mock_request, customerx_client_with_retries
    ):
        """Testa extração de detalhes de erro do JSON."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = RequestException("API Error")
        mock_response.json.return_value = {"error": "Detailed error message"}

        mock_exception = RequestException("API Error")
        mock_exception.response = mock_response
        mock_request.side_effect = mock_exception

        with pytest.raises(RequestException):
            customerx_client_with_retries._make_request(  # pylint: disable=protected-access
                method="GET", endpoint="/test"
            )


class TestCustomerXClientFetchAllCustomers:
    """Testes para fetch_all_customers."""

    @patch.object(CustomerXClient, "_make_request")
    def test_fetch_all_customers_pagina_unica(
        self, mock_make_request, customerx_client
    ):
        """Testa busca com uma única página."""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "id": 1,
                "company_name": "Cliente 1",
                "custom_attributes": [{"name": "postgres_uuid", "value": "uuid-1"}],
            },
            {
                "id": 2,
                "company_name": "Cliente 2",
                "custom_attributes": [{"name": "postgres_uuid", "value": "uuid-2"}],
            },
        ]
        mock_response.headers = {"X-Pages": "1", "X-Page": "1", "X-Total": "2"}
        mock_make_request.return_value = mock_response

        clientes = customerx_client.fetch_all_customers()

        assert len(clientes) == 2
        assert all(isinstance(c, CustomerXCustomerDTO) for c in clientes)
        assert clientes[0].id_customerx == 1
        assert clientes[1].id_customerx == 2

        mock_make_request.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/clients",
            params={"page": 1, "per_page": 20},
        )

    @patch.object(CustomerXClient, "_make_request")
    def test_fetch_all_customers_multiplas_paginas(
        self, mock_make_request, customerx_client
    ):
        """Testa busca com múltiplas páginas."""
        # Página 1
        mock_response_1 = Mock()
        mock_response_1.json.return_value = [
            {
                "id": 1,
                "company_name": "Cliente 1",
                "custom_attributes": [{"name": "postgres_uuid", "value": "uuid-1"}],
            }
        ]
        mock_response_1.headers = {"X-Pages": "2", "X-Page": "1", "X-Total": "2"}

        # Página 2
        mock_response_2 = Mock()
        mock_response_2.json.return_value = [
            {
                "id": 2,
                "company_name": "Cliente 2",
                "custom_attributes": [{"name": "postgres_uuid", "value": "uuid-2"}],
            }
        ]
        mock_response_2.headers = {"X-Pages": "2", "X-Page": "2", "X-Total": "2"}

        mock_make_request.side_effect = [mock_response_1, mock_response_2]

        clientes = customerx_client.fetch_all_customers()

        assert len(clientes) == 2
        assert mock_make_request.call_count == 2

    @patch.object(CustomerXClient, "_make_request")
    def test_fetch_all_customers_validacao_estrita(
        self, mock_make_request, customerx_client
    ):
        """Testa que validação estrita é passada para o DTO."""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "id": 1,
                "company_name": "Cliente sem UUID",
                "custom_attributes": [],  # Sem postgres_uuid
            }
        ]
        mock_response.headers = {"X-Pages": "1", "X-Page": "1", "X-Total": "1"}
        mock_make_request.return_value = mock_response

        # Com validação estrita, deve levantar erro
        with pytest.raises(ValueError, match="postgres_uuid"):
            customerx_client.fetch_all_customers(validacao_estrita=True)

    @patch.object(CustomerXClient, "_make_request")
    def test_fetch_all_customers_validacao_permissiva(
        self, mock_make_request, customerx_client
    ):
        """Testa que validação permissiva aceita clientes sem UUID."""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "id": 1,
                "company_name": "Cliente Manual",
                "custom_attributes": [],
            }
        ]
        mock_response.headers = {"X-Pages": "1", "X-Page": "1", "X-Total": "1"}
        mock_make_request.return_value = mock_response

        clientes = customerx_client.fetch_all_customers(validacao_estrita=False)

        assert len(clientes) == 1
        assert clientes[0].postgres_uuid is None

    @patch.object(CustomerXClient, "_make_request")
    def test_fetch_all_customers_propaga_excecao(
        self, mock_make_request, customerx_client
    ):
        """Testa que exceções são propagadas."""
        mock_make_request.side_effect = RequestException("Network error")

        with pytest.raises(RequestException, match="Network error"):
            customerx_client.fetch_all_customers()


class TestCustomerXClientCriarCliente:
    """Testes para criar_cliente_com_payload."""

    @patch.object(CustomerXClient, "_make_request")
    def test_criar_cliente_com_payload_sucesso(
        self, mock_make_request, customerx_client
    ):
        """Testa criação bem-sucedida de cliente."""
        payload = {
            "company_name": "Nova Empresa",
            "date_register": "01/01/2024",
            "external_id_client": "test-uuid-123",
        }

        mock_response = Mock()
        mock_response.json.return_value = {"id": 999, "company_name": "Nova Empresa"}
        mock_make_request.return_value = mock_response

        result = customerx_client.criar_cliente_com_payload(payload)

        assert result["id"] == 999
        mock_make_request.assert_called_once_with(
            method="POST", endpoint="/api/v1/clients", json_data=payload
        )

    @patch.object(CustomerXClient, "_make_request")
    def test_criar_cliente_com_debug(self, mock_make_request, customerx_client, capsys):
        """Testa modo debug imprime payload."""
        payload = {"company_name": "Test"}

        mock_response = Mock()
        mock_response.json.return_value = {"id": 1}
        mock_make_request.return_value = mock_response

        customerx_client.criar_cliente_com_payload(payload, debug=True)

        captured = capsys.readouterr()
        assert "Payload" in captured.out
        assert "company_name" in captured.out


class TestCustomerXClientAssociarFiliais:
    """Testes para associar_filiais."""

    @patch.object(CustomerXClient, "_make_request")
    def test_associar_filiais_sucesso(self, mock_make_request, customerx_client):
        """Testa associação bem-sucedida de filiais."""
        id_matriz = "abc123"
        ids_filiais = ["def456", "ghi789"]

        mock_response = Mock()
        mock_response.json.return_value = {"success": True}
        mock_make_request.return_value = mock_response

        result = customerx_client.associar_filiais(id_matriz, ids_filiais)

        assert result["success"] is True

        # Verificar chamada
        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args

        assert call_args.kwargs["method"] == "PUT"
        assert call_args.kwargs["endpoint"] == "/api/v1/clients/abc123/branch_clients"

        payload = call_args.kwargs["json_data"]
        assert payload["external_id_client"] == "abc123"
        assert payload["branch_clients_external_ids"] == ["def456", "ghi789"]

    @patch.object(CustomerXClient, "_make_request")
    def test_associar_filiais_lista_vazia(self, mock_make_request, customerx_client):
        """Testa associação com lista vazia de filiais."""
        mock_response = Mock()
        mock_response.json.return_value = {"success": True}
        mock_make_request.return_value = mock_response

        result = customerx_client.associar_filiais("abc123", [])

        assert result["success"] is True
        payload = mock_make_request.call_args.kwargs["json_data"]
        assert payload["branch_clients_external_ids"] == []
