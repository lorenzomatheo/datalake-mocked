"""Fixtures compartilhados para testes do módulo CustomerX."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from maggulake.integrations.customerx.client import CustomerXClient
from maggulake.integrations.customerx.models.customer import (
    CustomerXCustomerDTO,
)
from maggulake.integrations.customerx.reset import (
    CustomerXSandboxReset,
)


@pytest.fixture
def customerx_client() -> CustomerXClient:
    """Cliente CustomerX real com token de teste (default, sem retries extras)."""
    return CustomerXClient(api_token="test-token")


@pytest.fixture
def customerx_client_with_retries() -> CustomerXClient:
    """Cliente CustomerX com retries e ambiente production."""
    return CustomerXClient(
        api_token="test-token", max_retries=3, cx_environment="production"
    )


@pytest.fixture
def mock_customerx_client() -> MagicMock:
    """Cliente CustomerX mockado (MagicMock com spec)."""
    client = MagicMock(spec=CustomerXClient)
    client.base_url = "https://sandbox.api.customerx.com.br"
    return client


@pytest.fixture
def reset_manager(mock_customerx_client: MagicMock) -> CustomerXSandboxReset:
    """Gerenciador de reset com intervalo zero (para testes rápidos)."""
    return CustomerXSandboxReset(
        client=mock_customerx_client,
        environment="sandbox",
        request_interval_seconds=0,
    )


def make_customer_dto(**kwargs: object) -> CustomerXCustomerDTO:
    """Factory function para CustomerXCustomerDTO com defaults sensatos."""
    defaults: dict[str, object] = {
        "id_customerx": 1,
        "postgres_uuid": "uuid-1",
        "external_id_client": "abc123",
        "nome_empresa": "Empresa Teste",
        "nome_fantasia": None,
        "eh_matriz": False,
        "id_customerx_matriz": None,
    }
    defaults.update(kwargs)
    return CustomerXCustomerDTO(**defaults)


@pytest.fixture
def customer_dto_factory() -> Callable[..., CustomerXCustomerDTO]:
    """Fixture que retorna a factory function para criar DTOs."""
    return make_customer_dto
