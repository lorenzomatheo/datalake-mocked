"""
Schema Pydantic para enriquecimento de produtos da categoria "Produtos para Casa".

Meso categorias incluídas:
- Limpeza (Sabão em Pó, Detergentes, Amaciantes, Inseticidas, Água Sanitária,
  Desinfetantes, Desengordurante, Álcool)
- Aromatização (Difusores, Óleos Essenciais, Bloqueadores de Odor, Velas)
- Utensílios (Esponjas, Pilhas, Baterias, Sacos, Refeição, Saboneteiras,
  Raquete Elétrica, Super Cola, Recipientes, Bolsa Térmica)
"""

from textwrap import dedent
from typing import Optional

import pyspark.sql.types as T
from pydantic import BaseModel, ConfigDict, Field


class EnriquecimentoProdutosCasa(BaseModel):
    """Schema para enriquecimento de Produtos para Casa."""

    model_config = ConfigDict(use_enum_values=True)

    ean: str = Field(description="Seleciona o EAN do produto sem modificação.")

    nome_comercial: str = Field(
        description=dedent("""\
        [OBJETIVO]: Criar um nome comercial padronizado do produto com no máximo 60 caracteres.
        [REGRAS]:
        1. Siga EXATAMENTE esta estrutura: [Tipo] [Marca] [Variante/Fragrância] [Quantidade].
        2. Utilize apenas informações presentes no texto-fonte.
        [EXEMPLOS]:
        - 'Desinfetante Pinho Sol Original 1L'
        - 'Detergente Ypê Neutro 500ml'
        - 'Inseticida SBP Multi Insetos 300ml'
        - 'Álcool Gel 70% Santa Cruz 500ml'
        - 'Amaciante Comfort Concentrado Intense 500ml'
        - 'Água Sanitária Ypê 1L'
        - 'Pilha Alcalina Duracell AA 4un'
        - 'Difusor de Ambiente Via Aroma Lavanda 250ml'
        [O QUE NÃO FAZER]: Não invente informações. Não exceda 60 caracteres.
        """),
    )

    volume_quantidade: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair a apresentação comercial do produto.
        [EXEMPLOS]:
        - '1L'
        - '500ml'
        - '300ml'
        - '4 unidades'
        - 'Refil 400ml'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    indicacao: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever para que superfícies/situações o produto é indicado.
        [REGRAS]:
        1. Foque no uso principal.
        2. Máximo 200 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Desinfetantes: 'Limpeza e desinfecção de pisos, azulejos e superfícies laváveis'
        - Detergentes: 'Limpeza de louças, panelas e utensílios de cozinha'
        - Amaciantes: 'Amaciamento e perfumação de roupas durante a lavagem'
        - Inseticidas: 'Eliminação de moscas, mosquitos, baratas e formigas'
        - Água Sanitária: 'Desinfecção de superfícies e clareamento de roupas'
        - Desengordurante: 'Remoção de gordura em fogões, coifas e superfícies'
        - Álcool: 'Higienização de mãos e superfícies. Ação bactericida.'
        - Difusores: 'Aromatização de ambientes. Fragrância duradoura.'
        - Velas: 'Aromatização e decoração de ambientes.'
        - Pilhas: 'Alimentação de controles remotos, brinquedos e dispositivos eletrônicos.'
        - Esponjas: 'Limpeza de louças e superfícies. Dupla face.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    instrucoes_de_uso: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como usar o produto corretamente.
        [REGRAS]:
        1. Incluir diluição quando aplicável.
        2. Máximo 300 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Desinfetantes: 'Diluir 1 tampa (50ml) em 1L de água. Aplicar com pano úmido.'
        - Detergentes: 'Aplicar diretamente na esponja ou diluir em água.'
        - Amaciantes: 'Adicionar no último enxágue ou no compartimento da máquina.'
        - Inseticidas: 'Borrifar diretamente sobre insetos ou em frestas, a 30cm de distância.'
        - Água Sanitária: 'Diluir 200ml em 10L de água para desinfecção.'
        - Álcool Gel: 'Aplicar nas mãos e espalhar até secar naturalmente.'
        - Difusores: 'Inserir as varetas no frasco. Virar a cada 3 dias.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    advertencias_e_precaucoes: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair alertas de segurança.
        [REGRAS]:
        1. Produtos químicos SEMPRE devem ter advertências.
        2. Incluir riscos de mistura com outros produtos.
        [EXEMPLOS]:
        - Desinfetantes: 'Manter fora do alcance de crianças. Não ingerir. Não misturar com água sanitária.'
        - Inseticidas: 'Não aplicar sobre pessoas, animais ou alimentos. Ventilar ambiente após uso. Inflamável.'
        - Água Sanitária: 'Não misturar com outros produtos. Libera gases tóxicos.'
        - Álcool: 'Inflamável. Manter longe de fontes de calor. Não ingerir.'
        - Pilhas: 'Não recarregar pilhas alcalinas. Não descartar no lixo comum.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )


# Schema PySpark para criação de tabela Delta
enriquece_produtos_casa_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("nome_comercial", T.StringType(), True),
        T.StructField("volume_quantidade", T.StringType(), True),
        T.StructField("indicacao", T.StringType(), True),
        T.StructField("instrucoes_de_uso", T.StringType(), True),
        T.StructField("advertencias_e_precaucoes", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)

colunas_obrigatorias_produtos_casa = [
    "ean",
    "nome_comercial",
]

schema_enriquece_produtos_casa = """
    ean STRING NOT NULL,
    nome_comercial STRING,
    volume_quantidade STRING,
    indicacao STRING,
    instrucoes_de_uso STRING,
    advertencias_e_precaucoes STRING,
    atualizado_em TIMESTAMP
"""
