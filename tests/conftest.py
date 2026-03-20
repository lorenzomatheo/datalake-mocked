"""Fixtures compartilhados para todos os testes."""

import pytest

from maggulake.environment.settings import Settings, Stage


@pytest.fixture
def mock_settings() -> Settings:
    """Settings com valores de teste para ambiente staging."""
    return Settings(
        stage=Stage.STAGING,
        name="test",
        name_short="test",
        catalog="test_catalog",
        bucket="test_bucket",
        postgres_host="localhost",
        postgres_replica_host=None,
        postgres_port=5432,
        postgres_database="test_db",
        postgres_user="test_user",
        postgres_password="test_password",
        openai_organization="test_org",
        openai_api_key="test_key",
        milvus_uri="http://localhost:19530",
        milvus_token="test_token",
        data_api_url="http://localhost:8000",
        data_api_key="test_api_key",
        gemini_api_key="test_gemini_key",
        bigdatacorp_address="test_address",
        bigdatacorp_token="test_token",
        bluesoft_address="test_bluesoft_address",
        bluesoft_token="test_bluesoft_token",
        portal_obm_address="test_portal_obm_address",
        portal_obm_token="test_portal_obm_token",
        postgres_schema="test_postgres_schema",
        databricks_views_schema="test_databricks_views_schema",
    )
