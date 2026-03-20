from dataclasses import dataclass


@dataclass
class GroupDTO:
    """DTO para representar um Grupo no CustomerX.

    Cada grupo representa uma rede/conta, permitindo filtrar
    e organizar clientes facilmente no CustomerX.

    Campos:
        external_id: Identificador externo único (usamos conta postgres_uuid)
        description: Nome/descrição do grupo (nome da rede)
        status: Indica se o grupo está ativo (True) ou inativo (False)
    """

    external_id: str
    """Identificador externo único (conta postgres_uuid)"""

    description: str
    """Nome/descrição do grupo (nome da rede)"""

    status: bool = True
    """Grupo ativo (True) ou inativo (False)"""

    def to_customerx_payload(self) -> dict[str, str | bool]:
        """Gera payload para criar um grupo no CustomerX.

        Returns:
            Dicionário com campos: external_id, description, status
        """
        return {
            "external_id": self.external_id,
            "description": self.description,
            "status": self.status,
        }

    @classmethod
    def from_customerx_response(cls, data: dict) -> "GroupDTO":
        """Cria um objeto GroupDTO a partir da resposta da API CustomerX.

        Args:
            data: Dicionário com resposta da API contendo id, external_id,
                  description, status, created_at, updated_at

        Returns:
            Objeto GroupDTO com dados parseados
        """
        return cls(
            external_id=data["external_id"],
            description=data["description"],
            status=data["status"],
        )
