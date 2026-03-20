from .enriquece_alimentos_suplementos import (
    EnriquecimentoAlimentosSuplementos,
    enriquece_alimentos_suplementos_schema,
)
from .enriquece_forma_volume_quantidade import EnriquecimentoFormaVolumeQuantidade
from .enriquece_llm_judge import (
    QualidadeEnriquecimento,
    TriagemRapida,
    field_evaluation_schema,
    qualidade_enriquecimento_schema,
)
from .enriquece_marca_fabricante import MarcaFabricanteResponse
from .enriquece_materiais_saude import (
    EnriquecimentoMateriaisSaude,
    enriquece_materiais_saude_schema,
)
from .enriquece_perfumaria_cuidados import (
    EnriquecimentoPerfumariaCuidados,
    enriquece_perfumaria_cuidados_schema,
)
from .enriquece_power_phrase import (
    EnriquecimentoPowerPhrase,
    ResultadoPowerPhrase,
    power_phrase_produtos_schema,
)
from .enriquece_produtos_animais import (
    EnriquecimentoProdutosAnimais,
    enriquece_produtos_animais_schema,
)
from .enriquece_produtos_casa import (
    EnriquecimentoProdutosCasa,
    enriquece_produtos_casa_schema,
)
from .enriquece_produtos_info_bula import EnriquecimentoInfoBula
from .enriquece_produtos_nao_medicamentos import EnriquecimentoProdutosNaoMedicamentos
from .enriquece_tags import EnriquecimentoTags
from .enriquece_tags_clientes import EnriquecimentoTagsCliente

__all__ = [
    # Schemas por categoria
    "EnriquecimentoAlimentosSuplementos",
    "EnriquecimentoMateriaisSaude",
    "EnriquecimentoPerfumariaCuidados",
    "EnriquecimentoProdutosAnimais",
    "EnriquecimentoProdutosCasa",
    # Schemas existentes
    "EnriquecimentoFormaVolumeQuantidade",
    "EnriquecimentoInfoBula",
    "EnriquecimentoPowerPhrase",
    "EnriquecimentoProdutosNaoMedicamentos",
    "EnriquecimentoTags",
    "EnriquecimentoTagsCliente",
    "MarcaFabricanteResponse",
    "QualidadeEnriquecimento",
    "TriagemRapida",
    "ResultadoPowerPhrase",
    # Schemas PySpark por categoria
    "enriquece_alimentos_suplementos_schema",
    "enriquece_materiais_saude_schema",
    "enriquece_perfumaria_cuidados_schema",
    "enriquece_produtos_animais_schema",
    "enriquece_produtos_casa_schema",
    # Schemas PySpark existentes
    "field_evaluation_schema",
    "power_phrase_produtos_schema",
    "qualidade_enriquecimento_schema",
]
