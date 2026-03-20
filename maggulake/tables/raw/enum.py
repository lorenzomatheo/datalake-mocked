from enum import Enum

from maggulake.tables.raw.anvisa import AnvisaTable
from maggulake.tables.raw.anvisa_medicamentos_precos import (
    AnvisaMedicamentosPrecosTable,
)
from maggulake.tables.raw.anvisa_medicamentos_restricoes import (
    AnvisaMedicamentosRestricoesTable,
)
from maggulake.tables.raw.consulta_remedios_apresentacoes import (
    ConsultaRemediosApresentacoesTable,
)
from maggulake.tables.raw.consulta_remedios_marcas import ConsultaRemediosMarcasTable
from maggulake.tables.raw.farmarcas import FarmarcasTable
from maggulake.tables.raw.iqvia import IqviaTable
from maggulake.tables.raw.minas_mais import MinasMaisTable
from maggulake.tables.raw.produtos import ProdutosTable
from maggulake.tables.raw.rd import RdTable
from maggulake.tables.raw.sara import SaraTable


class Raw(Enum):
    produtos = ProdutosTable
    anvisa = AnvisaTable
    anvisa_medicamentos_precos = AnvisaMedicamentosPrecosTable
    anvisa_medicamentos_restricoes = AnvisaMedicamentosRestricoesTable
    consulta_remedios_apresentacoes = ConsultaRemediosApresentacoesTable
    consulta_remedios_marcas = ConsultaRemediosMarcasTable
    farmarcas = FarmarcasTable
    iqvia = IqviaTable
    minas_mais = MinasMaisTable
    rd = RdTable
    sara = SaraTable
