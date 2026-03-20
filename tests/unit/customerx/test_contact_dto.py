"""Testes unitários para ContactDTO"""

from maggulake.integrations.customerx.models.contact import ContactDTO


class TestContactDTOFromDonoRow:
    """Testes para ContactDTO.from_dono_row()"""

    def test_from_dono_row_completo(self):
        """Testa criação de DTO com todos os campos válidos"""
        row = {
            "nome": "João da Silva",
            "telefone": "11987654321",
            "email": "joao@example.com",
            "cpf": "12345678901",
            "conta_id": "uuid-conta-1",
            "created_at": "2024-01-01",
        }

        dto = ContactDTO.from_dono_row(row)

        assert dto is not None
        assert dto.nome == "João da Silva"
        assert dto.telefone == "11987654321"
        assert dto.email == "joao@example.com"
        assert dto.cpf == "12345678901"
        assert dto.external_id_client == "uuid-conta-1"
        assert dto.type_contact == "Dono"

    def test_from_dono_row_sem_cpf(self):
        """Testa criação sem CPF (campo opcional)"""
        row = {
            "id": "uuid-dono-2",
            "nome": "Maria Santos",
            "telefone": "21987654321",
            "email": "maria@example.com",
            "cpf": None,
            "conta_id": "uuid-conta-2",
            "created_at": "2024-01-01",
        }

        dto = ContactDTO.from_dono_row(row)

        assert dto is not None
        assert dto.nome == "Maria Santos"
        assert dto.cpf is None

    def test_from_dono_row_sem_email(self):
        """Testa que contato sem email é rejeitado (API exige email obrigatório)"""
        row = {
            "id": "uuid-dono-3",
            "nome": "Pedro Oliveira",
            "telefone": "31987654321",
            "email": None,
            "cpf": "98765432100",
            "conta_id": "uuid-conta-3",
            "created_at": "2024-01-01",
        }

        dto = ContactDTO.from_dono_row(row)

        # Email é obrigatório - contato sem email deve ser rejeitado
        assert dto is None

    def test_from_dono_row_sem_nome_retorna_none(self):
        """Testa que retorna None se nome for ausente"""
        row = {
            "id": "uuid-dono-4",
            "nome": None,
            "telefone": "41987654321",
            "email": "teste@example.com",
            "cpf": "11111111111",
            "conta_id": "uuid-conta-4",
            "created_at": "2024-01-01",
        }

        dto = ContactDTO.from_dono_row(row)

        assert dto is None

    def test_from_dono_row_nome_vazio_retorna_none(self):
        """Testa que retorna None se nome for string vazia"""
        row = {
            "id": "uuid-dono-5",
            "nome": "   ",
            "telefone": "51987654321",
            "email": "teste@example.com",
            "cpf": "22222222222",
            "conta_id": "uuid-conta-5",
            "created_at": "2024-01-01",
        }

        dto = ContactDTO.from_dono_row(row)

        assert dto is None

    def test_from_dono_row_nome_com_espacos_extras(self):
        """Testa que remove espaços extras do nome"""
        row = {
            "id": "uuid-dono-6",
            "nome": "  Ana Paula  ",
            "telefone": "61987654321",
            "email": "ana@example.com",
            "cpf": "33333333333",
            "conta_id": "uuid-conta-6",
            "created_at": "2024-01-01",
        }

        dto = ContactDTO.from_dono_row(row)

        assert dto is not None
        assert dto.nome == "Ana Paula"

    def test_from_dono_row_email_invalido(self):
        """Testa que dono com email inválido é rejeitado"""
        row = {
            "id": "uuid-dono-7",
            "nome": "Carlos Silva",
            "telefone": "11987654321",
            "email": "email-sem-arroba",  # Email inválido
            "cpf": "12345678901",
            "conta_id": "uuid-conta-7",
            "created_at": "2024-01-01",
        }

        dto = ContactDTO.from_dono_row(row)

        # Email inválido deve rejeitar o contato
        assert dto is None


class TestContactDTOFromAtendenteLoja:
    """Testes para ContactDTO.from_atendente_loja_row()"""

    def test_from_atendente_loja_row_com_email_valido(self):
        """Testa criação de DTO de atendente com email válido"""
        row = {
            "nome_gerente": "João Atendente",
            "telefone_gerente": "11987654321",
            "email_gerente": "joao@example.com",
            "loja_id": "uuid-loja-1",
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is not None
        assert dto.nome == "João Atendente"
        assert dto.telefone == "11987654321"
        assert dto.email == "joao@example.com"
        assert dto.cpf is None  # Atendentes não têm CPF
        assert dto.external_id_client == "uuid-loja-1"
        assert dto.type_contact == "Gerente"

    def test_from_atendente_loja_row_sem_email_usa_fallback(self):
        """Testa que sem email usa fallback loja_{uuid}@maggu.ai"""
        row = {
            "nome_gerente": "Maria Atendente",
            "telefone_gerente": "21987654321",
            "email_gerente": None,  # Sem email
            "loja_id": "uuid-loja-2",
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is not None
        assert dto.nome == "Maria Atendente"
        # Deve gerar email fallback
        assert dto.email == "loja_uuid-loja-2@maggu.ai"
        assert dto.external_id_client == "uuid-loja-2"

    def test_from_atendente_loja_row_email_invalido_usa_fallback(self):
        """Testa que email inválido usa fallback"""
        row = {
            "nome_gerente": "Pedro Atendente",
            "telefone_gerente": "31987654321",
            "email_gerente": "email-sem-arroba",  # Email inválido
            "loja_id": "uuid-loja-3",
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is not None
        assert dto.nome == "Pedro Atendente"
        # Email inválido deve acionar fallback
        assert dto.email == "loja_uuid-loja-3@maggu.ai"

    def test_from_atendente_loja_row_sem_telefone_usa_fallback_email(self):
        """Testa criação sem telefone mas com fallback de email"""
        row = {
            "nome_gerente": "Ana Atendente",
            "telefone_gerente": None,  # Sem telefone
            "email_gerente": None,  # Sem email
            "loja_id": "uuid-loja-4",
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is not None
        assert dto.nome == "Ana Atendente"
        assert dto.telefone is None
        # Deve ter email fallback mesmo sem telefone
        assert dto.email == "loja_uuid-loja-4@maggu.ai"

    def test_from_atendente_loja_row_sem_nome_retorna_none(self):
        """Testa que retorna None se nome for ausente"""
        row = {
            "nome_gerente": None,  # Sem nome
            "telefone_gerente": "41987654321",
            "email_gerente": "teste@example.com",
            "loja_id": "uuid-loja-5",
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is None

    def test_from_atendente_loja_row_nome_vazio_retorna_none(self):
        """Testa que retorna None se nome for string vazia"""
        row = {
            "nome_gerente": "   ",  # Nome vazio
            "telefone_gerente": "51987654321",
            "email_gerente": "teste@example.com",
            "loja_id": "uuid-loja-6",
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is None

    def test_from_atendente_loja_row_sem_loja_id_retorna_none(self):
        """Testa que retorna None se loja_id for ausente"""
        row = {
            "nome_gerente": "Carlos Atendente",
            "telefone_gerente": "61987654321",
            "email_gerente": "carlos@example.com",
            "loja_id": None,  # Sem loja_id
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is None

    def test_from_atendente_loja_row_nome_com_espacos_extras(self):
        """Testa que remove espaços extras do nome"""
        row = {
            "nome_gerente": "  Roberto Atendente  ",  # Com espaços extras
            "telefone_gerente": "71987654321",
            "email_gerente": None,
            "loja_id": "uuid-loja-8",
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is not None
        assert dto.nome == "Roberto Atendente"  # Sem espaços extras
        assert dto.email == "loja_uuid-loja-8@maggu.ai"

    def test_from_atendente_loja_row_prioriza_email_real_sobre_fallback(self):
        """Testa que email real é usado quando disponível ao invés do fallback"""
        row = {
            "atendente_id": "uuid-atendente-9",
            "nome_gerente": "Fernanda Atendente",
            "telefone_gerente": "81987654321",
            "email_gerente": "fernanda@example.com",  # Email real válido
            "loja_id": "uuid-loja-9",
            "data_cadastro_gerente": "2024-01-01",
        }

        dto = ContactDTO.from_atendente_loja_row(row)

        assert dto is not None
        # Deve usar email real, não fallback
        assert dto.email == "fernanda@example.com"
        assert not dto.email.endswith("@maggu.ai")


class TestContactDTOToCustomerXPayload:
    """Testes para ContactDTO.to_customerx_payload()"""

    def test_payload_dono_completo(self):
        """Testa geração de payload com todos os campos válidos"""
        dto = ContactDTO(
            postgres_uuid="uuid-1",
            nome="José Silva",
            telefone="11987654321",
            email="jose@example.com",
            cpf="12345678901",
            external_id_client="uuid-conta-1",
            type_contact="Dono",
        )

        payload = dto.to_customerx_payload()

        assert payload["name"] == "José Silva"
        assert payload["type_contact"] == "Dono"
        assert payload["external_id_client"] == "uuid-conta-1"
        assert payload["document"] == "12345678901"
        assert payload["email"] == "jose@example.com"
        assert "phones" in payload
        assert len(payload["phones"]) == 1
        assert payload["phones"][0]["ddi"] == "+55"
        assert payload["phones"][0]["is_default"] is True

    def test_payload_cpf_invalido_omitido(self):
        """Testa que CPF inválido é omitido do payload"""
        dto = ContactDTO(
            postgres_uuid="uuid-2",
            nome="Maria Silva",
            telefone="11987654321",
            email="maria@example.com",
            cpf="123",  # CPF inválido (menos de 11 dígitos)
            external_id_client="uuid-conta-2",
            type_contact="Dono",
        )

        payload = dto.to_customerx_payload()

        assert "document" not in payload

    def test_payload_cpf_com_formatacao_limpo(self):
        """Testa que CPF formatado é limpo corretamente"""
        dto = ContactDTO(
            postgres_uuid="uuid-3",
            nome="Pedro Santos",
            telefone="11987654321",
            email="pedro@example.com",
            cpf="123.456.789-01",
            external_id_client="uuid-conta-3",
            type_contact="Dono",
        )

        payload = dto.to_customerx_payload()

        assert payload["document"] == "12345678901"

    def test_payload_email_invalido_omitido(self):
        """Testa que email inválido é omitido"""
        dto = ContactDTO(
            postgres_uuid="uuid-4",
            nome="Ana Costa",
            telefone="11987654321",
            email="email-invalido",  # Sem @
            cpf="12345678901",
            external_id_client="uuid-conta-4",
            type_contact="Dono",
        )

        payload = dto.to_customerx_payload()

        assert "email" not in payload

    def test_payload_telefone_formatado(self):
        """Testa formatação correta do telefone"""
        dto = ContactDTO(
            postgres_uuid="uuid-5",
            nome="Luiz Alves",
            telefone="11987654321",
            email="luiz@example.com",
            cpf="12345678901",
            external_id_client="uuid-conta-5",
            type_contact="Dono",
        )

        payload = dto.to_customerx_payload()

        # standardize_phone_number deve formatar para (XX) XXXXX-XXXX
        assert "phones" in payload
        phone = payload["phones"][0]
        assert phone["number"] == "(11) 98765-4321"
        assert phone["is_default"] is True
        assert phone["ddi"] == "+55"

    def test_payload_sem_telefone(self):
        """Testa que payload funciona sem telefone"""
        dto = ContactDTO(
            postgres_uuid="uuid-6",
            nome="Ricardo Lima",
            telefone=None,
            email="ricardo@example.com",
            cpf="12345678901",
            external_id_client="uuid-conta-6",
            type_contact="Dono",
        )

        payload = dto.to_customerx_payload()

        assert "phones" not in payload
        assert payload["name"] == "Ricardo Lima"

    def test_payload_gerente_sem_cpf_email(self):
        """Testa payload de gerente sem CPF e email"""
        dto = ContactDTO(
            postgres_uuid="uuid-7",
            nome="Gerente Teste",
            telefone="11987654321",
            email=None,
            cpf=None,
            external_id_client="uuid-loja-1",
            type_contact="Gerente",
        )

        payload = dto.to_customerx_payload()

        assert payload["name"] == "Gerente Teste"
        assert payload["type_contact"] == "Gerente"
        assert "document" not in payload
        assert "email" not in payload
        assert "phones" in payload

    def test_payload_campos_obrigatorios(self):
        """Testa que campos obrigatórios sempre estão presentes"""
        dto = ContactDTO(
            postgres_uuid="uuid-8",
            nome="Mínimo Campos",
            telefone=None,
            email=None,
            cpf=None,
            external_id_client="uuid-conta-8",
            type_contact="Dono",
        )

        payload = dto.to_customerx_payload()

        # Campos obrigatórios da API
        assert "name" in payload
        assert "type_contact" in payload
        assert "external_id_client" in payload
        assert payload["name"] == "Mínimo Campos"
        assert payload["type_contact"] == "Dono"
        assert payload["external_id_client"] == "uuid-conta-8"
