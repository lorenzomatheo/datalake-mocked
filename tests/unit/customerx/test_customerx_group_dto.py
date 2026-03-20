"""Testes para CustomerXGroupDTO."""

from datetime import datetime

import pytest

from maggulake.integrations.customerx.models.customerx_group import (
    CustomerXGroupDTO,
)


class TestCustomerXGroupDTO:
    """Testes para criação e manipulação de CustomerXGroupDTO."""

    def test_from_api_response_completo(self):
        """Testa conversão de resposta completa da API."""
        api_data = {
            "id": 123,
            "external_id": "uuid-conta-abc",
            "description": "Rede Farmácias ABC",
            "status": True,
            "created_at": "2024-01-15T10:30:00Z",
            "updated_at": "2024-01-20T15:45:00Z",
        }

        group = CustomerXGroupDTO.from_api_response(api_data)

        assert group.id == 123
        assert group.external_id == "uuid-conta-abc"
        assert group.description == "Rede Farmácias ABC"
        assert group.status is True
        assert isinstance(group.created_at, datetime)
        assert isinstance(group.updated_at, datetime)

    def test_from_api_response_sem_timestamps(self):
        """Testa conversão quando timestamps não estão presentes."""
        api_data = {
            "id": 456,
            "external_id": "uuid-conta-xyz",
            "description": "Rede XYZ",
            "status": False,
        }

        group = CustomerXGroupDTO.from_api_response(api_data)

        assert group.id == 456
        assert group.external_id == "uuid-conta-xyz"
        assert group.description == "Rede XYZ"
        assert group.status is False
        assert group.created_at is None
        assert group.updated_at is None

    def test_from_api_response_campos_obrigatorios_faltando(self):
        """Testa que erro é levantado quando campos obrigatórios faltam."""
        api_data_incompleto = {
            "external_id": "uuid-conta-abc",
            "description": "Rede ABC",
            "status": True,
        }

        with pytest.raises(KeyError):
            CustomerXGroupDTO.from_api_response(api_data_incompleto)

    def test_repr(self):
        """Testa representação do objeto."""
        group = CustomerXGroupDTO(
            id=123,
            external_id="uuid-conta-abc",
            description="Rede ABC",
            status=True,
        )

        repr_str = repr(group)

        assert "CustomerXGroupDTO" in repr_str
        assert "id=123" in repr_str
        assert "external_id='uuid-conta-abc'" in repr_str
        assert "description='Rede ABC'" in repr_str
        assert "status=True" in repr_str

    def test_timestamp_parsing_with_offset(self):
        """Testa parsing de timestamp com timezone offset."""
        api_data = {
            "id": 999,
            "external_id": "uuid-test",
            "description": "Test",
            "status": True,
            "created_at": "2024-01-15T10:30:00-03:00",
            "updated_at": "2024-01-20T15:45:00-03:00",
        }

        group = CustomerXGroupDTO.from_api_response(api_data)

        assert isinstance(group.created_at, datetime)
        assert isinstance(group.updated_at, datetime)

    def test_acesso_atributos(self):
        """Testa que atributos podem ser acessados diretamente (sem .get())."""
        api_data = {
            "id": 100,
            "external_id": "uuid-abc",
            "description": "Rede Teste",
            "status": True,
        }

        group = CustomerXGroupDTO.from_api_response(api_data)

        # Acesso direto deve funcionar
        assert group.id == 100
        assert group.external_id == "uuid-abc"
        assert group.description == "Rede Teste"
        assert group.status is True

        # Não deve ter método .get() como dicionário
        assert not hasattr(group, "get")
