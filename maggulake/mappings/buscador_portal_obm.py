from maggulake.environment.tables import Table
from maggulake.mappings.buscador_base import BuscadorBase
from maggulake.schemas.produtos_portal_obm import schema


class BuscadorPortalObm(BuscadorBase):
    tabela = Table.produtos_portal_obm
    schema_tabela = schema
    fonte = "PortalOBM"

    def __init__(self, env):
        self.client = env.portal_obm_client
        super().__init__(env)
