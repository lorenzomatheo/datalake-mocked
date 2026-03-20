"""Testes unitários para LojaDTO"""

from datetime import datetime

from maggulake.integrations.customerx.models.loja import LojaDTO


class TestLojaDTOToCustomerXPayload:
    """Testes para LojaDTO.to_customerx_payload()"""

    def test_to_customerx_payload_completo(self):
        """Testa geração de payload completo com todos os campos"""
        loja = LojaDTO(
            postgres_uuid="660e8400-e29b-41d4-a716-446655440001",
            nome="Loja Centro",
            conta_postgres_uuid="550e8400-e29b-41d4-a716-446655440000",
            cnpj="12.345.678/0001-99",
            status="Ativa",
            endereco="Rua Exemplo, 123",
            tamanho_loja="Grande",
            cidade="São Paulo",
            estado="SP",
            erp="SAP",
            codigo_de_seis_digitos="123456",
            criado_em=datetime(2023, 8, 20, 15, 0, 0),
            atualizado_em=datetime(2023, 9, 10, 10, 0, 0),
        )

        payload = loja.to_customerx_payload()

        # Campos obrigatórios
        assert payload["company_name"] == "Loja Centro"
        assert payload["trading_name"] == "Loja Centro"
        assert payload["date_register"] == "20/08/2023"
        assert payload["external_id_client"] == "660e8400-e29b-41d4-a716-446655440001"
        assert payload["cnpj_cpf"] == "12.345.678/0001-99"

        # Custom attributes obrigatórios
        custom_attrs = payload["custom_attributes"]
        assert any(
            attr["external_id"] == "postgres_uuid"
            and attr["value"] == "660e8400-e29b-41d4-a716-446655440001"
            for attr in custom_attrs
        )

        # Custom attributes opcionais
        assert any(
            attr["external_id"] == "status" and attr["value"] == "Ativa"
            for attr in custom_attrs
        )
        assert any(
            attr["external_id"] == "cidade" and attr["value"] == "São Paulo"
            for attr in custom_attrs
        )

    def test_to_customerx_payload_campos_opcionais_vazios(self):
        """Testa que campos opcionais None não aparecem no payload"""
        loja = LojaDTO(
            postgres_uuid="test-uuid",
            nome="Loja Teste",
            conta_postgres_uuid="conta-uuid",
            cnpj=None,
            status="Ativa",
            endereco=None,
            tamanho_loja=None,
            cidade=None,
            estado=None,
            erp=None,
            codigo_de_seis_digitos=None,
            criado_em=datetime(2023, 1, 1),
        )

        payload = loja.to_customerx_payload()
        custom_attrs = payload["custom_attributes"]

        # Verificar que apenas campos obrigatórios + status aparecem
        external_ids = [attr["external_id"] for attr in custom_attrs]

        assert "postgres_uuid" in external_ids
        assert "status" in external_ids  # Status tem valor

        # Campos None não devem aparecer
        assert "cnpj" not in external_ids
        assert "endereco" not in external_ids
        assert "cidade" not in external_ids

    def test_data_registro_formatada_property(self):
        """Testa property de formatação de data"""
        loja = LojaDTO(
            postgres_uuid="test",
            nome="Test",
            conta_postgres_uuid="test",
            cnpj=None,
            status="test",
            endereco=None,
            tamanho_loja=None,
            cidade=None,
            estado=None,
            erp=None,
            codigo_de_seis_digitos=None,
            criado_em=datetime(2024, 12, 25, 8, 30, 0),
        )

        assert loja.data_registro_formatada == "25/12/2024"
