import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from maggulake.ativacao.customerx.constants import (
    FAKE_CONTACT_EMAIL_PATTERN,
    FAKE_CONTACT_NAME,
    FAKE_CONTACT_TYPE,
)
from maggulake.utils.strings import (
    limpa_cnpj,
    standardize_phone_number,
    validate_email_return_none_if_fail,
)


@dataclass
class ContactDTO:
    """DTO para representar um Contato (Dono, Gerente ou Fake) para envio à API CustomerX.

    Contatos podem ser:
    - Donos de Contas/Redes: Tabela contas_donoconta
    - Gerentes de Lojas: JOIN atendentes_atendenteloja (cargo GERENTE)
    - Fake: Contato placeholder para clientes sem contato real vinculado

    IMPORTANTE:
    - Um contato só é válido se tiver nome
    - Email é obrigatório pela API CustomerX:
      - Donos: devem ter email válido no banco
      - Gerentes: usa email_atendente ou fallback loja_{uuid}@maggu.ai
      - Fake: usa fake_{external_id}@maggu.ai
    - Telefone e CPF são opcionais
    """

    postgres_uuid: str | None
    """UUID do contato no Postgres (36 chars)"""

    nome: str
    """Nome do contato (obrigatório)"""

    external_id_client: str
    """UUID da conta (para donos) ou loja (para gerentes) no CustomerX"""

    type_contact: str
    """Tipo do contato: 'Dono', 'Gerente' ou 'Fake'"""

    telefone: str | None = None
    """Telefone do contato"""

    email: str | None = None
    """Email do contato"""

    cpf: str | None = None
    """CPF do contato (apenas donos têm CPF no banco)"""

    criado_em: datetime | None = None
    """Data de criação do contato"""

    @classmethod
    def from_dono_row(cls, row: dict[str, Any]) -> "ContactDTO | None":
        """Cria ContactDTO a partir de uma linha da tabela contas_donoconta.

        Args:
            row: Dicionário com campos: id, nome, telefone, email, cpf, conta_id, created_at

        Returns:
            ContactDTO se nome e email forem válidos, None caso contrário

        NOTE: A API CustomerX exige email como campo obrigatório. Donos sem email
        válido são rejeitados (retorna None) para evitar erro 422 na API.
        """
        nome = row.get("nome")
        if not nome or not isinstance(nome, str) or not nome.strip():
            return None

        # Validação de email obrigatório (exigência da API CustomerX)
        email = row.get("email")
        email_valido = validate_email_return_none_if_fail(email)
        if not email_valido:
            # Rejeita contato sem email válido
            return None

        return cls(
            postgres_uuid=None,  # não necessario
            nome=nome.strip(),
            telefone=row.get("telefone"),
            email=email_valido,
            cpf=row.get("cpf"),
            external_id_client=str(row["conta_id"]),
            type_contact="Dono",
            criado_em=row.get("created_at"),
        )

    @classmethod
    def from_atendente_loja_row(cls, row: dict[str, Any]) -> "ContactDTO | None":
        """Cria ContactDTO para gerente de loja a partir de JOIN atendentes_atendenteloja.

        Args:
            row: Dicionário retornado por PostgresAdapter.get_gerentes_lojas() com campos:
                atendente_id, nome_gerente, telefone_gerente, email_gerente,
                loja_id, data_cadastro_gerente

        Returns:
            ContactDTO se nome for válido, None caso contrário

        NOTE: Email é obrigatório pela API. Se email_gerente for inválido/ausente,
        gera fallback loja_{loja_id}@maggu.ai para satisfazer requisito da API.
        """
        nome = row.get("nome_gerente")
        if not nome or not isinstance(nome, str) or not nome.strip():
            return None

        loja_id = row.get("loja_id")
        if not loja_id:
            return None

        # Tentar usar email real se existir e for válido
        email = row.get("email_gerente")
        email_valido = validate_email_return_none_if_fail(email)

        # Fallback: Se não houver email válido, gerar email padrão
        if not email_valido:
            email_valido = f"loja_{str(loja_id)}@maggu.ai"

        return cls(
            postgres_uuid=None,  # não necessario
            nome=nome.strip(),
            telefone=row.get("telefone_gerente"),
            email=email_valido,
            cpf=None,  # Atendentes de loja não têm CPF
            external_id_client=str(loja_id),
            type_contact="Gerente",
            criado_em=row.get("data_cadastro_gerente"),
        )

    @classmethod
    def from_fake_client(
        cls, external_id: str, client_name: str | None = None
    ) -> "ContactDTO":
        """Cria ContactDTO fake/placeholder para um cliente sem contato real.

        Garante que todo cliente no CustomerX tenha pelo menos um contato
        vinculado, requisito para que as automações funcionem.

        O contato fake pode ser substituído posteriormente por um contato
        real (dono ou gerente) via atualiza_contatos.

        Args:
            external_id: UUID da conta ou loja no Postgres (usado como
                external_id_client no CustomerX)
            client_name: Nome do cliente para compor o nome do contato.
                Se None, usa apenas o nome padrão.

        Returns:
            ContactDTO com tipo 'Fake' e email determinístico
        """
        nome = (
            f"{FAKE_CONTACT_NAME} - {client_name}" if client_name else FAKE_CONTACT_NAME
        )
        email = FAKE_CONTACT_EMAIL_PATTERN.format(external_id=external_id)

        return cls(
            postgres_uuid=None,
            nome=nome,
            external_id_client=str(external_id),
            type_contact=FAKE_CONTACT_TYPE,
            email=email,
        )

    def _validate_and_format_phone(self) -> str | None:
        """Valida e formata telefone, retornando None se inválido.

        Verifica:
        - Se o telefone pode ser formatado
        - Se não é uma sequência de zeros (ex: 00000000000)

        Returns:
            Telefone formatado no padrão (XX) XXXXX-XXXX ou None se inválido
        """
        if not self.telefone:
            return None

        try:
            telefone_formatado = standardize_phone_number(self.telefone)

            # Se o telefone for uma sequencia de 0s, não utiliza
            if telefone_formatado:
                apenas_digitos = re.sub(r"\D", "", telefone_formatado)
                if set(apenas_digitos) == {"0"}:
                    return None

            return telefone_formatado
        except (ValueError, TypeError):
            # Se falhar na formatação, retorna None
            return None

    def to_customerx_payload(self) -> dict[str, Any]:
        """Gera payload para criar um contato no CustomerX.

        Valida e formata:
        - CPF: limpa e omite se inválido
        - Email: valida e omite se inválido
        - Telefone: padroniza para formato (XX) XXXXX-XXXX

        NOTE: A API CustomerX exige email como campo OBRIGATÓRIO. Contatos sem email
        válido devem ser rejeitados antes de chamar este método (ver from_dono_row() e
        from_atendente_loja_row()). Se o email for None ou inválido aqui, será omitido do
        payload e a API retornará erro 422.

        Returns:
            Dict com payload para POST /api/v1/contacts
        """
        payload: dict[str, Any] = {
            "name": self.nome,
            "type_contact": self.type_contact,
            "external_id_client": self.external_id_client,
        }

        # Validação e formatação de CPF
        if self.cpf:
            cpf_limpo = limpa_cnpj(self.cpf)
            # CPF válido tem 11 dígitos numéricos após limpeza
            if cpf_limpo and len(cpf_limpo) == 11 and cpf_limpo.isdigit():
                payload["document"] = cpf_limpo

        # Validação de email
        email_valido = validate_email_return_none_if_fail(self.email)
        if email_valido:
            payload["email"] = email_valido

        # Formatação de telefone
        telefone_formatado = self._validate_and_format_phone()
        if telefone_formatado:
            payload["phones"] = [
                {
                    "number": telefone_formatado,
                    "is_default": True,
                    "ddi": "+55",
                }
            ]

        return payload

    def to_update_payload(self) -> dict[str, Any]:
        """Gera payload para atualizar um contato no CustomerX.

        Retorna apenas campos modificáveis, excluindo campos imutáveis como:
        - external_id_client (não pode ser alterado após criação)

        NOTE: A API CustomerX exige email como campo obrigatório.
        """
        payload: dict[str, Any] = {
            "name": self.nome,
            "type_contact": self.type_contact,
        }

        # Validação e formatação de CPF
        if self.cpf:
            cpf_limpo = limpa_cnpj(self.cpf)
            # CPF válido tem 11 dígitos numéricos após limpeza
            if cpf_limpo and len(cpf_limpo) == 11 and cpf_limpo.isdigit():
                payload["document"] = cpf_limpo

        # Validação de email
        email_valido = validate_email_return_none_if_fail(self.email)
        if email_valido:
            payload["email"] = email_valido

        # Formatação de telefone
        telefone_formatado = self._validate_and_format_phone()
        if telefone_formatado:
            payload["phones"] = [
                {
                    "number": telefone_formatado,
                    "is_default": True,
                    "ddi": "+55",
                }
            ]

        return payload
