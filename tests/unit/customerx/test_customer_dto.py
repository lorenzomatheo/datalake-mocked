"""Testes unitários para CustomerXCustomerDTO"""

import pytest

from maggulake.integrations.customerx.models.customer import (
    CustomerXCustomerDTO,
)


class TestCustomerXCustomerDTOFromApiResponse:
    """Testes para CustomerXCustomerDTO.from_api_response()"""

    def test_from_api_response_completo_com_matriz(self):
        """Testa conversão de resposta completa da API com dados de matriz"""
        api_data = {
            "id": 12345,
            "company_name": "Farmácia Exemplo Ltda",
            "trading_name": "Farmácia Exemplo",
            "external_id_client": "abc123",
            "custom_attributes": [
                {
                    "label": "postgres_uuid",
                    "name": "postgres_uuid",
                    "value": "550e8400-e29b-41d4-a716-446655440000",
                },
            ],
            "parent_client": {
                "is_parent_client": True,
                "parent_client": None,
            },
        }

        dto = CustomerXCustomerDTO.from_api_response(api_data)

        assert dto.id_customerx == 12345
        assert dto.postgres_uuid == "550e8400-e29b-41d4-a716-446655440000"
        assert dto.external_id_client == "abc123"
        assert dto.nome_empresa == "Farmácia Exemplo Ltda"
        assert dto.nome_fantasia == "Farmácia Exemplo"
        assert dto.eh_matriz is True
        assert dto.id_customerx_matriz is None
        assert dto.campos_customizados is not None

    def test_from_api_response_filial_com_matriz(self):
        """Testa conversão de filial com referência à matriz"""
        api_data = {
            "id": 67890,
            "company_name": "Loja Centro",
            "trading_name": "Loja Centro",
            "external_id_client": "def456",
            "custom_attributes": [
                {
                    "name": "postgres_uuid",
                    "value": "660e8400-e29b-41d4-a716-446655440001",
                }
            ],
            "parent_client": {
                "is_parent_client": False,
                "parent_client": {"id": 12345, "company_name": "Matriz"},
            },
        }

        dto = CustomerXCustomerDTO.from_api_response(api_data)

        assert dto.id_customerx == 67890
        assert dto.postgres_uuid == "660e8400-e29b-41d4-a716-446655440001"
        assert dto.external_id_client == "def456"
        assert dto.eh_matriz is False
        assert dto.id_customerx_matriz == 12345

    def test_from_api_response_sem_postgres_uuid_validacao_estrita(self):
        """Testa que validação estrita falha sem postgres_uuid"""
        api_data = {
            "id": 99999,
            "company_name": "Cliente Manual",
            "external_id_client": "xyz999",
            "custom_attributes": [],
        }

        with pytest.raises(ValueError) as exc_info:
            CustomerXCustomerDTO.from_api_response(api_data, validacao_estrita=True)

        assert "ERRO CRÍTICO" in str(exc_info.value)
        assert "postgres_uuid" in str(exc_info.value)
        assert "99999" in str(exc_info.value)

    def test_from_api_response_sem_postgres_uuid_validacao_permissiva(self):
        """Testa que validação permissiva permite clientes sem postgres_uuid"""
        api_data = {
            "id": 99999,
            "company_name": "Cliente Manual",
            "external_id_client": "xyz999",
            "custom_attributes": [],
        }

        dto = CustomerXCustomerDTO.from_api_response(api_data, validacao_estrita=False)

        assert dto.id_customerx == 99999
        assert dto.postgres_uuid is None
        assert dto.external_id_client == "xyz999"
        assert dto.nome_empresa == "Cliente Manual"

    def test_from_api_response_campos_opcionais_vazios(self):
        """Testa comportamento com campos opcionais ausentes"""
        api_data = {
            "id": 11111,
            "custom_attributes": [{"name": "postgres_uuid", "value": "test-uuid-123"}],
        }

        dto = CustomerXCustomerDTO.from_api_response(api_data)

        assert dto.id_customerx == 11111
        assert dto.postgres_uuid == "test-uuid-123"
        assert dto.nome_empresa is None
        assert dto.nome_fantasia is None
        assert dto.external_id_client is None
        assert dto.eh_matriz is False
        assert dto.id_customerx_matriz is None

    def test_from_api_response_parent_client_none(self):
        """Testa quando parent_client é None"""
        api_data = {
            "id": 22222,
            "company_name": "Teste",
            "parent_client": None,
            "custom_attributes": [{"name": "postgres_uuid", "value": "test-uuid"}],
        }

        dto = CustomerXCustomerDTO.from_api_response(api_data)

        assert dto.eh_matriz is False
        assert dto.id_customerx_matriz is None
