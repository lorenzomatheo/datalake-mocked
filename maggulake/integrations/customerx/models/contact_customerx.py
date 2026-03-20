from dataclasses import dataclass


@dataclass
class ContactCustomerXDTO:
    """DTO para representar um contato retornado pela API do CustomerX.

    Contatos podem ser donos (de contas/redes) ou gerentes (de lojas).
    """

    id_customerx: int
    """ID interno auto-gerado pela API do CustomerX"""

    nome: str | None
    """Nome do contato"""

    tipo_contato: str | None
    """Tipo do contato: 'Dono' ou 'Gerente'"""

    email: str | None
    """Email do contato"""

    telefones: list[dict[str, str]] | None = None
    """Lista de telefones do contato no formato [{"number": "(XX) XXXXX-XXXX", "is_default": True, "ddi": "+55"}]"""

    cpf: str | None = None
    """CPF do contato (11 dígitos)"""

    external_id_client: str | None = None
    """UUID da conta (para donos) ou loja (para gerentes) no CustomerX"""

    cliente: dict | None = None
    """Dados do cliente associado ao contato"""

    @classmethod
    def from_api_response(cls, data: dict) -> "ContactCustomerXDTO":
        external_id_client = None
        cliente_info = data.get("client")
        if cliente_info:
            external_id_client = cliente_info.get("external_id_client")

        return cls(
            id_customerx=data.get("id"),
            nome=data.get("name"),
            tipo_contato=data.get("type_contact"),
            email=data.get("email"),
            telefones=data.get("phones"),
            cpf=data.get("document"),
            external_id_client=external_id_client,
            cliente=cliente_info,
        )
