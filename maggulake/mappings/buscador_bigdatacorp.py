from maggulake.environment.tables import Table
from maggulake.mappings.buscador_base import BuscadorBase
from maggulake.schemas.produtos_bigdatacorp import schema


class BuscadorBigDataCorp(BuscadorBase):
    tabela = Table.produtos_bigdatacorp
    schema_tabela = schema
    fonte = "BigDataCorp"

    def __init__(self, env):
        self.client = env.bigdatacorp_client
        super().__init__(env)
