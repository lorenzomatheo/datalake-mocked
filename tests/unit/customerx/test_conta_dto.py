"""Testes unitários para ContaDTO"""

from datetime import datetime

from maggulake.integrations.customerx.models.conta import ContaDTO


class TestContaDTOToCustomerXPayload:
    """Testes para ContaDTO.to_customerx_payload()"""

    def test_to_customerx_payload_completo(self):
        """Testa geração de payload completo para criar rede/conta no CustomerX"""
        conta = ContaDTO(
            postgres_uuid="550e8400-e29b-41d4-a716-446655440000",
            nome="Rede Farmácias Exemplo",
            criado_em=datetime(2023, 6, 15, 10, 30, 0),
        )

        payload = conta.to_customerx_payload()

        # Verificar campos principais
        assert payload["company_name"] == "Rede Farmácias Exemplo"
        assert payload["trading_name"] == "Rede Farmácias Exemplo"
        assert payload["date_register"] == "15/06/2023"
        assert payload["external_id_client"] == "550e8400-e29b-41d4-a716-446655440000"
        assert payload["parent_company"] is True

        # Verificar custom_attributes
        custom_attrs = payload["custom_attributes"]
        assert len(custom_attrs) == 1

        postgres_uuid_attr = next(
            attr for attr in custom_attrs if attr["external_id"] == "postgres_uuid"
        )
        assert postgres_uuid_attr["value"] == "550e8400-e29b-41d4-a716-446655440000"
        assert postgres_uuid_attr["description"] == "PostgreSQL UUID"

    def test_to_customerx_payload_formatacao_data(self):
        """Testa formatação correta de diferentes datas"""
        conta = ContaDTO(
            postgres_uuid="test-uuid",
            nome="Teste",
            criado_em=datetime(2024, 1, 5, 14, 20, 30),
        )

        payload = conta.to_customerx_payload()
        assert payload["date_register"] == "05/01/2024"
