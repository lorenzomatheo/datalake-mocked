from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class LojaDTO:
    """DTO para representar uma Loja do Postgres para envio à API CustomerX.

    A loja será criada como 'filial' no CustomerX e posteriormente
    associada à sua conta/matriz via endpoint de branch_clients.

    NOTA: O UUID completo é preservado tanto em external_id_client quanto em
    custom_attributes.postgres_uuid para garantir integridade dos dados.
    """

    postgres_uuid: str
    """UUID da loja no Postgres (36 chars)"""

    nome: str
    """Nome da loja"""

    conta_postgres_uuid: str
    """UUID da Conta pai no Postgres (36 chars)"""

    cnpj: str | None
    status: str
    endereco: str | None
    tamanho_loja: str | None
    cidade: str | None
    estado: str | None
    erp: str | None
    codigo_de_seis_digitos: str | None

    criado_em: datetime
    """Data de criação da loja"""

    atualizado_em: datetime | None = None
    """Data da última atualização"""

    @property
    def data_registro_formatada(self) -> str:
        """Formata criado_em como DD/MM/AAAA para a API do CustomerX."""
        return self.criado_em.strftime("%d/%m/%Y")

    def to_customerx_payload(
        self, external_id_group: str | None = None
    ) -> dict[str, Any]:
        """Gera payload para criar uma loja (filial) no CustomerX.

        NOTA: O UUID completo é preservado tanto em external_id_client quanto em
        custom_attributes.postgres_uuid para garantir integridade dos dados.
        """
        payload: dict[str, Any] = {
            "company_name": self.nome,
            "trading_name": self.nome,
            "cnpj_cpf": self.cnpj,
            "date_register": self.data_registro_formatada,
            "parent_company": False,  # Loja é filial, não matriz
            "external_id_client": self.postgres_uuid,
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
            "endereco": ("Endereço", self.endereco),
            "cidade": ("Cidade", self.cidade),
            "estado": ("Estado", self.estado),
            "tamanho_loja": ("Tamanho da Loja", self.tamanho_loja),
            "erp": ("ERP", self.erp),
            "codigo_de_seis_digitos": (
                "Código de 6 Dígitos",
                self.codigo_de_seis_digitos,
            ),
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

    def to_update_payload(self) -> dict[str, Any]:
        """Gera payload para atualizar uma loja (filial) no CustomerX.

        Retorna apenas campos modificáveis, excluindo campos imutáveis como:
        - external_id_client (chave primária)
        - date_register (data de criação)
        - parent_company (tipo não pode mudar)
        - external_id_group (relacionamento não é atualizado por este endpoint)
        """
        payload: dict[str, Any] = {
            "company_name": self.nome,
            "trading_name": self.nome,
            "custom_attributes": [
                {
                    "label": "postgres_uuid",
                    "value": self.postgres_uuid,
                },
            ],
        }

        # Adicionar campos opcionais apenas se tiverem valor
        campos_opcionais = {
            "cnpj": self.cnpj,
            "status": self.status,
            "endereco": self.endereco,
            "cidade": self.cidade,
            "estado": self.estado,
            "tamanho_loja": self.tamanho_loja,
            "erp": self.erp,
            "codigo_de_seis_digitos": self.codigo_de_seis_digitos,
        }

        for label, value in campos_opcionais.items():
            if value:
                payload["custom_attributes"].append({"label": label, "value": value})

        return payload

    @classmethod
    def from_postgres_row(cls, row: dict) -> "LojaDTO":
        """Cria um objeto LojaDTO a partir de um dicionário (linha do Postgres)."""
        return cls(
            postgres_uuid=str(row["id"]),
            nome=row["name"],
            conta_postgres_uuid=str(row["conta_id"]),
            cnpj=row.get("cnpj"),
            status=row["status"],
            endereco=row.get("endereco"),
            tamanho_loja=row.get("tamanho_loja"),
            cidade=row.get("cidade"),
            estado=row.get("estado"),
            erp=row.get("erp"),
            codigo_de_seis_digitos=row.get("codigo_de_seis_digitos"),
            criado_em=row["created_at"],
            atualizado_em=row.get("updated_at"),
        )
