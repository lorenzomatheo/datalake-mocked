from dataclasses import dataclass


@dataclass
class CustomerXCustomerDTO:
    """DTO para representar um cliente retornado pela API do CustomerX.

    Este DTO contém 3 tipos diferentes de ID que não devem ser confundidos:

    - id_customerx: ID interno auto-gerado pela API do CustomerX (int)
    - postgres_uuid: UUID completo (36 chars) do nosso banco Postgres
    - external_id_client: external_id_client retornado pela API (usado para associações)

    O postgres_uuid é preservado via custom_attributes para garantir
    integridade e rastreabilidade dos dados entre sistemas.
    """

    id_customerx: int
    """ID interno auto-gerado pela API do CustomerX"""

    postgres_uuid: str | None
    """UUID completo (36 chars) do Postgres - vem de custom_attributes.postgres_uuid"""

    external_id_client: str | None
    """external_id_client retornado pela API - usado para associações matriz/filial"""

    nome_empresa: str | None
    """Nome da empresa (company_name na API)"""

    nome_fantasia: str | None
    """Nome fantasia (trading_name na API)"""

    eh_matriz: bool = False
    """True se for rede/matriz, False se for loja/filial"""

    id_customerx_matriz: int | None = None
    """ID CustomerX da matriz (para filiais)"""

    campos_customizados: dict[str, str] | None = None
    """Campos customizados retornados pela API"""

    @classmethod
    def from_api_response(
        cls, data: dict, validacao_estrita: bool = True
    ) -> "CustomerXCustomerDTO":
        """Converte resposta da API CustomerX para DTO.

        Args:
            data: Dict com dados do cliente da API
            validacao_estrita: Se True, exige que postgres_uuid exista.
                              Se False, permite clientes sem postgres_uuid
                              (útil para operações de limpeza/reset)

        Returns:
            CustomerXCustomerDTO instance

        Raises:
            ValueError: Se validacao_estrita=True e postgres_uuid não estiver presente
        """
        # Buscar UUID completo dos custom_attributes (campo postgres_uuid)
        custom_attrs = data.get("custom_attributes", [])
        postgres_uuid = None

        for attr in custom_attrs:
            if (
                attr.get("label") == "postgres_uuid"
                or attr.get("name") == "postgres_uuid"
            ):
                postgres_uuid = attr.get("value")
                break

        if not postgres_uuid:
            cx_id = data.get("id")
            nome = data.get("company_name")

            # VALIDAÇÃO CONDICIONAL: Em modo strict, TODOS os clientes DEVEM ter postgres_uuid
            if validacao_estrita:
                raise ValueError(
                    f"❌ ERRO CRÍTICO: Cliente CustomerX ID {cx_id} ('{nome}') "
                    f"não possui 'postgres_uuid' nos custom_attributes! "
                    f"Todos os clientes DEVEM ter este campo para sincronização."
                )
            # Se nao estiver em modo estrito, apenas logar aviso
            print(
                f"⚠️ AVISO: Cliente CustomerX ID {cx_id} ('{nome}') "
                f"não possui 'postgres_uuid' nos custom_attributes. "
                f"Isto é permitido em modo não estrito."
            )

        # O campo parent_client pode conter info se é matriz e quem é a matriz pai
        parent_client = data.get("parent_client", {})
        eh_matriz = (
            parent_client.get("is_parent_client", False) if parent_client else False
        )
        parent_info = parent_client.get("parent_client") if parent_client else None
        id_matriz = parent_info.get("id") if parent_info else None

        return cls(
            id_customerx=data.get("id"),
            postgres_uuid=postgres_uuid,
            external_id_client=data.get("external_id_client"),
            nome_empresa=data.get("company_name"),
            nome_fantasia=data.get("trading_name"),
            eh_matriz=eh_matriz,
            id_customerx_matriz=id_matriz,
            campos_customizados=data.get("custom_attributes"),
        )
