"""
Testes para o módulo de reset de sandbox CustomerX.
"""

from unittest.mock import MagicMock, patch

import pytest

from maggulake.integrations.customerx.reset import (
    CustomerXSandboxReset,
)

from .conftest import make_customer_dto


class TestCustomerXSandboxResetInit:
    """Testes para inicialização do CustomerXSandboxReset."""

    def test_init_sandbox_succeeds(self, mock_customerx_client):
        """Testa que inicialização em sandbox funciona corretamente."""
        reset_manager = CustomerXSandboxReset(
            client=mock_customerx_client, environment="sandbox"
        )

        assert reset_manager.environment == "sandbox"
        assert reset_manager.client == mock_customerx_client
        assert reset_manager.request_interval_seconds == 1  # default

    def test_init_custom_interval(self, mock_customerx_client):
        """Testa configuração de intervalo customizado."""
        reset_manager = CustomerXSandboxReset(
            client=mock_customerx_client,
            environment="sandbox",
            request_interval_seconds=5,
        )

        assert reset_manager.request_interval_seconds == 5

    def test_init_zero_interval(self, mock_customerx_client):
        """Testa que permite intervalo zero (útil para testes)."""
        reset_manager = CustomerXSandboxReset(
            client=mock_customerx_client,
            environment="sandbox",
            request_interval_seconds=0,
        )

        assert reset_manager.request_interval_seconds == 0


class TestCustomerXSandboxResetDeleteOneCustomer:
    """Testes para delete_one_customer."""

    def test_delete_one_customer_success(
        self, reset_manager, mock_customerx_client, capsys
    ):
        """Testa deleção bem-sucedida de um cliente."""
        mock_response = MagicMock()
        mock_customerx_client._make_request.return_value = mock_response  # pylint: disable=protected-access

        reset_manager.delete_one_customer(customer_id=123, customer_name="Teste Ltda")

        # Verificar chamada ao cliente
        mock_customerx_client._make_request.assert_called_once_with(  # pylint: disable=protected-access
            method="DELETE", endpoint="/api/v1/clients/123"
        )

        # Verificar output
        captured = capsys.readouterr()
        assert "✅ Deletado" in captured.out
        assert "[123]" in captured.out
        assert "Teste Ltda" in captured.out

    def test_delete_one_customer_failure(self, reset_manager, mock_customerx_client):
        """Testa que falha na deleção propaga exceção."""
        mock_customerx_client._make_request.side_effect = Exception(  # pylint: disable=protected-access
            "Delete failed"
        )

        with pytest.raises(Exception, match="Delete failed"):
            reset_manager.delete_one_customer(customer_id=123, customer_name="Teste")

    def test_delete_one_customer_com_nome_especial(
        self, reset_manager, mock_customerx_client, capsys
    ):
        """Testa deleção com nome contendo caracteres especiais."""
        mock_response = MagicMock()
        mock_customerx_client._make_request.return_value = mock_response  # pylint: disable=protected-access

        reset_manager.delete_one_customer(
            customer_id=456, customer_name="Farmácia São José & Cia"
        )

        captured = capsys.readouterr()
        assert "Farmácia São José & Cia" in captured.out


class TestCustomerXSandboxResetResetEnvironment:
    """Testes para reset_environment."""

    @patch("time.sleep")
    def test_reset_environment_success(
        self, _mock_sleep, reset_manager, mock_customerx_client
    ):
        """Testa reset completo com sucesso."""
        customer1 = make_customer_dto(
            nome_empresa="Empresa A", nome_fantasia="Empresa A Ltda"
        )
        customer2 = make_customer_dto(
            id_customerx=2,
            postgres_uuid="uuid-2",
            external_id_client="def456",
            nome_empresa="Empresa B",
            nome_fantasia="Empresa B Ltda",
        )

        mock_customerx_client.fetch_all_customers.return_value = [
            customer1,
            customer2,
        ]
        mock_customerx_client.fetch_all_contacts.return_value = []
        mock_customerx_client.fetch_all_groups.return_value = []

        # Mock da deleção
        mock_delete_response = MagicMock()
        mock_customerx_client._make_request.return_value = mock_delete_response  # pylint: disable=protected-access

        # Executar
        result = reset_manager.reset_environment(debug=False)

        # Verificar nova estrutura
        assert result["customers"]["total"] == 2
        assert result["customers"]["deleted"] == 2
        assert result["customers"]["failed"] == 0
        assert result["contacts"]["total"] == 0
        assert result["groups"]["total"] == 0

        # Verificar que tentou deletar ambos
        assert mock_customerx_client._make_request.call_count == 2  # pylint: disable=protected-access
        mock_customerx_client.fetch_all_customers.assert_called_once_with(
            validacao_estrita=False
        )

    def test_reset_environment_empty_sandbox(
        self, reset_manager, mock_customerx_client
    ):
        """Testa reset quando sandbox já está limpo."""
        mock_customerx_client.fetch_all_customers.return_value = []
        mock_customerx_client.fetch_all_contacts.return_value = []
        mock_customerx_client.fetch_all_groups.return_value = []

        result = reset_manager.reset_environment(debug=False)

        assert result["customers"]["total"] == 0
        assert result["customers"]["deleted"] == 0
        assert result["customers"]["failed"] == 0
        assert result["contacts"]["total"] == 0
        assert result["groups"]["total"] == 0

        mock_customerx_client.fetch_all_customers.assert_called_once_with(
            validacao_estrita=False
        )
        mock_customerx_client._make_request.assert_not_called()  # pylint: disable=protected-access

    @patch("time.sleep")
    def test_reset_environment_customer_without_id(
        self, _mock_sleep, reset_manager, mock_customerx_client
    ):
        """Testa que clientes sem ID são contados como falha."""
        customer = make_customer_dto(
            id_customerx=None,
            postgres_uuid=None,
            external_id_client=None,
            nome_empresa="Empresa Sem ID",
        )

        mock_customerx_client.fetch_all_customers.return_value = [customer]
        mock_customerx_client.fetch_all_contacts.return_value = []
        mock_customerx_client.fetch_all_groups.return_value = []

        result = reset_manager.reset_environment(debug=False)

        assert result["customers"]["total"] == 1
        assert result["customers"]["deleted"] == 0
        assert result["customers"]["failed"] == 1

        # Não deve tentar deletar cliente sem ID
        mock_customerx_client._make_request.assert_not_called()  # pylint: disable=protected-access

    @patch("time.sleep")
    def test_reset_environment_debug_mode(
        self, _mock_sleep, reset_manager, mock_customerx_client, capsys
    ):
        """Testa modo debug imprime progresso."""
        customer = make_customer_dto(
            postgres_uuid="uuid",
            external_id_client="abc",
            nome_empresa="Teste",
        )

        mock_customerx_client.fetch_all_customers.return_value = [customer]
        mock_customerx_client.fetch_all_contacts.return_value = []
        mock_customerx_client.fetch_all_groups.return_value = []
        mock_customerx_client._make_request.return_value = MagicMock()  # pylint: disable=protected-access

        reset_manager.reset_environment(debug=True)

        captured = capsys.readouterr()
        assert "RESET DE SANDBOX" in captured.out

    def test_reset_environment_empty_debug(
        self, reset_manager, mock_customerx_client, capsys
    ):
        """Testa mensagem quando sandbox já está limpo (debug)."""
        mock_customerx_client.fetch_all_customers.return_value = []
        mock_customerx_client.fetch_all_contacts.return_value = []
        mock_customerx_client.fetch_all_groups.return_value = []

        reset_manager.reset_environment(debug=True)

        captured = capsys.readouterr()
        assert "🏁 Reset de sandbox concluído!" in captured.out

    @patch("time.sleep")
    def test_reset_environment_respeita_intervalo(
        self, mock_sleep, mock_customerx_client
    ):
        """Testa que respeita o intervalo configurado entre requests."""
        # Configurar com intervalo de 2 segundos
        reset_manager = CustomerXSandboxReset(
            client=mock_customerx_client,
            environment="sandbox",
            request_interval_seconds=2,
        )

        customers = [
            make_customer_dto(
                id_customerx=i,
                postgres_uuid=f"uuid-{i}",
                external_id_client=f"id-{i}",
                nome_empresa=f"Cliente {i}",
            )
            for i in range(1, 4)
        ]

        mock_customerx_client.fetch_all_customers.return_value = customers
        mock_customerx_client.fetch_all_contacts.return_value = []
        mock_customerx_client.fetch_all_groups.return_value = []
        mock_customerx_client._make_request.return_value = MagicMock()  # pylint: disable=protected-access

        reset_manager.reset_environment(debug=False)

        # Deve ter chamado sleep 3 vezes (uma para cada cliente)
        assert mock_sleep.call_count == 3
        # Cada chamada deve ser com 2 segundos
        for call in mock_sleep.call_args_list:
            assert call[0][0] == 2


class TestCustomerXSandboxResetEdgeCases:
    """Testes para casos extremos e edge cases."""

    @patch("time.sleep")
    def test_reset_environment_nome_empresa_none(
        self, _mock_sleep, reset_manager, mock_customerx_client, capsys
    ):
        """Testa comportamento quando nome_empresa é None."""
        customer = make_customer_dto(
            postgres_uuid="uuid",
            external_id_client="abc",
            nome_empresa=None,
        )

        mock_customerx_client.fetch_all_customers.return_value = [customer]
        mock_customerx_client.fetch_all_contacts.return_value = []
        mock_customerx_client.fetch_all_groups.return_value = []
        mock_customerx_client._make_request.return_value = MagicMock()  # pylint: disable=protected-access

        reset_manager.reset_environment(debug=True)

        captured = capsys.readouterr()
        assert "Sem Nome" in captured.out  # Deve usar fallback
