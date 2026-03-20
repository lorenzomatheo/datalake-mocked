from dataclasses import dataclass
from datetime import datetime


@dataclass
class CustomerXGroupDTO:
    """DTO para representar um grupo retornado pela API do CustomerX.

    Este DTO representa a resposta completa da API de grupos,
    incluindo campos gerados pelo CustomerX (id, timestamps, etc).

    Diferente do GroupDTO (usado para criar payloads), este DTO
    é usado para trabalhar com dados já existentes na API.
    """

    id: int
    """ID interno auto-gerado pela API do CustomerX"""

    external_id: str
    """Identificador externo único (conta postgres_uuid)"""

    description: str
    """Nome/descrição do grupo (nome da rede)"""

    status: bool
    """Grupo ativo (True) ou inativo (False)"""

    created_at: datetime | None = None
    """Data de criação do grupo no CustomerX"""

    updated_at: datetime | None = None
    """Data da última atualização do grupo no CustomerX"""

    @classmethod
    def from_api_response(cls, data: dict) -> "CustomerXGroupDTO":
        """Converte resposta da API CustomerX para DTO.

        Args:
            data: Dict com dados do grupo da API contendo:
                - id (int): ID interno do CustomerX
                - external_id (str): Identificador externo
                - description (str): Nome/descrição do grupo
                - status (bool): Status ativo/inativo
                - created_at (str, opcional): Timestamp de criação
                - updated_at (str, opcional): Timestamp de atualização
        """
        # Parse timestamps se presentes
        created_at = None
        if data.get("created_at"):
            created_at = datetime.fromisoformat(
                data["created_at"].replace("Z", "+00:00")
            )

        updated_at = None
        if data.get("updated_at"):
            updated_at = datetime.fromisoformat(
                data["updated_at"].replace("Z", "+00:00")
            )

        return cls(
            id=data["id"],
            external_id=data["external_id"],
            description=data["description"],
            status=data["status"],
            created_at=created_at,
            updated_at=updated_at,
        )

    def __repr__(self) -> str:
        """Representação legível do objeto."""
        return (
            f"CustomerXGroupDTO(id={self.id}, "
            f"external_id='{self.external_id}', "
            f"description='{self.description}', "
            f"status={self.status})"
        )
