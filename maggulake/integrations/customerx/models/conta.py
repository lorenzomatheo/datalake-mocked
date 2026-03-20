from dataclasses import dataclass
from datetime import datetime


@dataclass
class ContaDTO:
    """DTO para representar uma Conta/Rede do Postgres para envio à API CustomerX.

    A conta será criada como 'matriz' (parent_company=True) no CustomerX.

    Campos:
        postgres_uuid: UUID da conta no Postgres (36 chars)
        nome: Nome da conta/rede
        criado_em: Data de criação
    """

    postgres_uuid: str
    """UUID da conta no Postgres (36 chars)"""

    nome: str
    """Nome da conta/rede"""

    criado_em: datetime
    """Data de criação da conta"""

    estado: str | None = None
    """Estado/UF da rede (calculado a partir das lojas)"""

    status: str | None = None
    """Status da conta/rede (ex: ativo, inativo)"""

    def to_customerx_payload(
        self, external_id_group: str | None = None
    ) -> dict[str, str | bool | list]:
        """Gera payload para criar uma rede (matriz) no CustomerX.

        NOTE: Antigamente o UUID era truncado, mas agora é preservado completo
        tanto em external_id_client quanto em custom_attributes.postgres_uuid para
        garantir integridade dos dados.
        """
        payload = {
            "company_name": self.nome,
            "date_register": self.criado_em.strftime("%d/%m/%Y"),
            "trading_name": self.nome,
            "external_id_client": self.postgres_uuid,
            "parent_company": True,  # Marca como matriz
            "state": self.estado,
            "custom_attributes": [
                {
                    "external_id": "postgres_uuid",
                    "description": "PostgreSQL UUID",
                    "value": self.postgres_uuid,  # Preservado completo (36 chars)
                },
            ],
        }

        # Adiciona campos opcionais apenas se tiverem valor
        campos_opcionais = {
            "status": ("Status", self.status),
        }

        for chave, (descricao, valor) in campos_opcionais.items():
            if valor:
                payload["custom_attributes"].append(
                    {
                        "external_id": chave,
                        "description": descricao,
                        "value": str(valor),
                    }
                )

        if external_id_group:
            payload["external_id_group"] = external_id_group

        return payload

    def to_update_payload(self) -> dict[str, str | list]:
        """Gera payload para atualizar uma conta (matriz) no CustomerX.

        Retorna apenas campos modificáveis, excluindo campos imutáveis como:
        - external_id_client (chave primária)
        - date_register (data de criação)
        - parent_company (tipo não pode mudar)
        - external_id_group (relacionamento não é atualizado por este endpoint)
        """
        payload: dict[str, str | list] = {
            "company_name": self.nome,
            "trading_name": self.nome,
            "custom_attributes": [
                {
                    "label": "postgres_uuid",
                    "value": self.postgres_uuid,
                },
            ],
        }

        campos_opcionais = {
            "status": self.status,
            "estado": self.estado,
        }

        for label, value in campos_opcionais.items():
            if value:
                payload["custom_attributes"].append({"label": label, "value": value})

        return payload

    @classmethod
    def from_postgres_row(cls, row: dict, estado: str | None = None) -> "ContaDTO":
        return cls(
            postgres_uuid=str(row["id"]),
            nome=row["name"],
            criado_em=row["created_at"],
            status=row["status"],
            estado=estado,
        )
