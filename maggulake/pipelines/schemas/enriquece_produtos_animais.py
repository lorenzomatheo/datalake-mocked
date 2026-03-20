"""
Schema Pydantic para enriquecimento de produtos da categoria "Produtos para Animais".

Meso categorias incluídas:
- Medicamentos (Antiparasitário)
- Higiene (Shampoo, Tapete Higiênico)
"""

from textwrap import dedent
from typing import Optional

import pyspark.sql.types as T
from pydantic import BaseModel, ConfigDict, Field


class EnriquecimentoProdutosAnimais(BaseModel):
    """Schema para enriquecimento de Produtos para Animais."""

    model_config = ConfigDict(use_enum_values=True)

    ean: str = Field(description="Seleciona o EAN do produto sem modificação.")

    nome_comercial: str = Field(
        description=dedent("""\
        [OBJETIVO]: Criar um nome comercial padronizado do produto com no máximo 60 caracteres.
        [REGRAS]:
        1. Siga EXATAMENTE esta estrutura: [Tipo] [Marca] [Espécie/Porte] [Quantidade].
        2. Incluir faixa de peso do animal para medicamentos.
        [EXEMPLOS]:
        - 'Antipulgas Frontline Plus Cães 10-20kg'
        - 'Antiparasitário Bravecto Cães 4,5-10kg'
        - 'Shampoo Pet Clean Pelos Claros 500ml'
        - 'Tapete Higiênico Petix Super Premium 80x60cm 30un'
        - 'Vermífugo Drontal Plus Cães 10kg 2 Comprimidos'
        [O QUE NÃO FAZER]: Não invente informações. Não exceda 60 caracteres.
        """),
    )

    principio_ativo: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Identificar o ingrediente ativo para medicamentos veterinários.
        [REGRAS]:
        1. Para produtos de higiene (shampoo, tapete), retorne null.
        2. Para medicamentos, listar princípios ativos com dosagem.
        [EXEMPLOS]:
        - Antipulgas Frontline: 'Fipronil 9,8mg + S-Metopreno 8,8mg'
        - Vermífugo Drontal: 'Febantel 150mg + Pirantel 50mg + Praziquantel 50mg'
        - Bravecto: 'Fluralaner 250mg'
        - Shampoo: null
        - Tapete Higiênico: null
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    dosagem: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair dosagem e concentração para medicamentos.
        [EXEMPLOS]:
        - '1 pipeta de 1,34ml para cães de 10-20kg'
        - '1 comprimido para cada 10kg de peso do animal'
        - '1 comprimido mastigável a cada 12 semanas'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    volume_quantidade: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair a apresentação comercial do produto.
        [EXEMPLOS]:
        - '1 pipeta'
        - '2 comprimidos'
        - '500ml'
        - '30 unidades 80x60cm'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    indicacao: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever para qual animal/situação o produto é indicado.
        [REGRAS]:
        1. SEMPRE especificar espécie (cão, gato, ambos).
        2. Para medicamentos, incluir faixa de peso e duração do efeito.
        3. Máximo 200 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Antipulgas: 'Cães de 10 a 20kg. Controle de pulgas e carrapatos por 30 dias.'
        - Vermífugo: 'Cães adultos. Tratamento e prevenção de vermes intestinais.'
        - Bravecto: 'Cães de 4,5 a 10kg. Proteção contra pulgas e carrapatos por 12 semanas.'
        - Shampoo: 'Cães e gatos de pelagem clara. Realça brilho e cor natural.'
        - Tapete Higiênico: 'Treinamento de cães filhotes. Super absorção por 12 horas.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    instrucoes_de_uso: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como aplicar/usar o produto.
        [REGRAS]:
        1. Para medicamentos, incluir orientações de aplicação.
        2. Máximo 300 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Antipulgas Pipeta: 'Aplicar 1 pipeta na nuca do animal, afastando os pelos, diretamente na pele. Não banhar 48h antes e após.'
        - Vermífugo: 'Administrar diretamente na boca ou misturar ao alimento. Dose única.'
        - Bravecto: 'Oferecer junto com ou após a refeição para melhor absorção.'
        - Shampoo: 'Molhar o animal, aplicar o shampoo massageando, deixar agir 3min e enxaguar.'
        - Tapete: 'Posicionar em local de fácil acesso. Trocar quando saturado.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    contraindicacoes: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Indicar quando NÃO usar o produto.
        [EXEMPLOS]:
        - Antipulgas: 'Não usar em filhotes com menos de 8 semanas ou animais debilitados.'
        - Vermífugo: 'Não usar em fêmeas gestantes ou lactantes sem orientação veterinária.'
        - Bravecto: 'Não usar em cães com menos de 2kg ou 8 semanas de idade.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    advertencias_e_precaucoes: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair alertas de segurança.
        [EXEMPLOS]:
        - 'Manter fora do alcance de crianças e outros animais.'
        - 'Lavar as mãos após aplicação.'
        - 'Evitar contato com olhos e mucosas.'
        - 'Em caso de ingestão acidental, procurar veterinário.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    condicoes_de_armazenamento: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como armazenar o produto.
        [EXEMPLOS]:
        - 'Manter em local fresco e seco, ao abrigo da luz.'
        - 'Conservar na embalagem original.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    validade_apos_abertura: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Indicar prazo de validade após abertura.
        [EXEMPLOS]:
        - 'Usar imediatamente após abertura da pipeta.'
        - 'Shampoo: 12 meses após aberto.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )


# Schema PySpark para criação de tabela Delta
enriquece_produtos_animais_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("nome_comercial", T.StringType(), True),
        T.StructField("principio_ativo", T.StringType(), True),
        T.StructField("dosagem", T.StringType(), True),
        T.StructField("volume_quantidade", T.StringType(), True),
        T.StructField("indicacao", T.StringType(), True),
        T.StructField("instrucoes_de_uso", T.StringType(), True),
        T.StructField("contraindicacoes", T.StringType(), True),
        T.StructField("advertencias_e_precaucoes", T.StringType(), True),
        T.StructField("condicoes_de_armazenamento", T.StringType(), True),
        T.StructField("validade_apos_abertura", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)

colunas_obrigatorias_produtos_animais = [
    "ean",
    "nome_comercial",
]

schema_enriquece_produtos_animais = """
    ean STRING NOT NULL,
    nome_comercial STRING,
    principio_ativo STRING,
    dosagem STRING,
    volume_quantidade STRING,
    indicacao STRING,
    instrucoes_de_uso STRING,
    contraindicacoes STRING,
    advertencias_e_precaucoes STRING,
    condicoes_de_armazenamento STRING,
    validade_apos_abertura STRING,
    atualizado_em TIMESTAMP
"""
