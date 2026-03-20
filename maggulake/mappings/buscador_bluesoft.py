from maggulake.environment.tables import Table
from maggulake.mappings.buscador_base import BuscadorBase
from maggulake.schemas.produtos_bluesoft import schema


class BuscadorBluesoft(BuscadorBase):
    tabela = Table.produtos_bluesoft
    schema_tabela = schema
    fonte = "Bluesoft"

    def __init__(self, env):
        self.client = env.bluesoft_client
        super().__init__(env)
