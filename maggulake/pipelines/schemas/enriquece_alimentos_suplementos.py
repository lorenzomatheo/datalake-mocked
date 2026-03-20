"""
Schema Pydantic para enriquecimento de produtos da categoria "Alimentos e Suplementos".

Meso categorias incluídas:
- Fórmulas (Infantil, Adulto)
- Bebidas (Chás, Águas, Sucos, Energéticos, Refrigerantes, Repositor de Eletrólitos)
- Snacks (Salgadinhos, Biscoitos, Chocolates, Barras de Proteína, Balas, Barra de Cereais, etc.)
- Suplementos (Colágeno, Proteínas, Ômega, Creatina, Vitaminas, Minerais, Probióticos, etc.)
- Adoçantes
"""

from textwrap import dedent
from typing import Optional

import pyspark.sql.types as T
from pydantic import BaseModel, ConfigDict, Field

from maggulake.enums import FaixaEtaria


class EnriquecimentoAlimentosSuplementos(BaseModel):
    """Schema para enriquecimento de produtos de Alimentos e Suplementos."""

    model_config = ConfigDict(use_enum_values=True)

    ean: str = Field(description="Seleciona o EAN do produto sem modificação.")

    nome_comercial: str = Field(
        description=dedent("""\
        [OBJETIVO]: Criar um nome comercial padronizado do produto com no máximo 60 caracteres.
        [REGRAS]:
        1. Siga EXATAMENTE esta estrutura: [Tipo] [Marca] [Sabor/Variante] [Dosagem] [Quantidade].
        2. Utilize apenas informações presentes no texto-fonte.
        3. Se necessário, abrevie ou remova palavras menos importantes para respeitar o limite.
        [EXEMPLOS]:
        - 'Whey Protein Gold Standard Chocolate 907g'
        - 'Vitamina D3 2000UI Sundown 200 Cápsulas'
        - 'Barra de Proteína Nutrata Cookies 12un 60g'
        - 'Fórmula Infantil Aptamil Premium 1 800g'
        - 'Energético Red Bull Lata 250ml'
        - 'Chá Mate Leão Natural 25 Sachês'
        [O QUE NÃO FAZER]: Não invente informações. Não exceda 60 caracteres.
        """),
    )

    dosagem: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair a dosagem ou quantidade por porção/unidade.
        [REGRAS]:
        1. Incluir unidade de medida correta (mg, g, UI, mcg, ml).
        2. Especificar porção de referência.
        [EXEMPLOS]:
        - '2000UI por cápsula (50mcg)'
        - '24g de proteína por scoop (30g)'
        - '1000mg de colágeno por porção'
        - '500mg de vitamina C por comprimido'
        - 'Tomar 2 comprimidos = 1000mg de cálcio'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    idade_recomendada: FaixaEtaria = Field(
        description=dedent("""\
        [OBJETIVO]: Indicar a faixa etária para a qual o produto é recomendado.
        [OPÇÕES]: 'bebe', 'crianca', 'adolescente', 'jovem', 'adulto', 'idoso', 'todos'.
        [REGRAS]:
        - Fórmulas Infantis 0-6m, 6-12m → 'bebe'
        - Suplementos infantis → 'crianca'
        - Whey Protein, Creatina → geralmente 'adulto' ou 'jovem'
        - Suplementos para idosos/terceira idade → 'idoso'
        - Se não especificado → 'todos'
        [EXEMPLOS]:
        - Aptamil 1 (0-6 meses) → 'bebe'
        - Vitamina D Infantil Gotas → 'crianca'
        - Whey Protein Isolado → 'adulto'
        - Centrum Silver 50+ → 'idoso'
        """)
    )

    volume_quantidade: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair a apresentação comercial do produto.
        [EXEMPLOS]:
        - '200 cápsulas'
        - '907g (30 doses)'
        - '12 barras de 60g'
        - 'Lata 250ml'
        - '25 sachês'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    indicacao: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever para que ou quem o produto é indicado.
        [REGRAS]:
        1. Foque no benefício principal e público-alvo.
        2. Use linguagem clara. Máximo 200 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Fórmulas Infantis: 'Lactentes de 0 a 6 meses quando amamentação não é possível'
        - Energéticos: 'Aumento de energia e concentração. Pré-treino ou estudo.'
        - Repositores: 'Reposição de eletrólitos durante atividades físicas intensas'
        - Proteínas: 'Ganho de massa muscular. Recuperação pós-treino.'
        - Vitamina D: 'Suplementação diária de vitamina D. Saúde óssea e imunidade.'
        - Colágeno: 'Saúde da pele, cabelos, unhas e articulações.'
        - Probióticos: 'Equilíbrio da flora intestinal. Melhora da digestão.'
        - Adoçantes: 'Adoçar bebidas e alimentos sem açúcar. Diabéticos e dietas.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    instrucoes_de_uso: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como consumir o produto corretamente.
        [REGRAS]:
        1. Incluir frequência, quantidade e modo de preparo.
        2. Máximo de 300 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Vitaminas: 'Tomar 1 cápsula ao dia com uma refeição'
        - Proteínas: 'Diluir 1 scoop (30g) em 200ml de água ou leite. Consumir após treino.'
        - Fórmulas: 'Medida dosadora: 4,3g. Diluir conforme tabela de idade/peso.'
        - Colágeno: 'Diluir 10g em 200ml de água ou suco. Consumir em jejum.'
        - Energéticos: 'Consumir gelado. Não exceder 2 latas por dia.'
        - Probióticos: 'Tomar 1 cápsula ao dia em jejum ou conforme orientação.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    contraindicacoes: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Indicar quem NÃO deve consumir o produto.
        [REGRAS]:
        1. Listar grupos de risco (gestantes, lactantes, crianças, diabéticos, alérgicos).
        2. Incluir interações medicamentosas quando mencionadas.
        [EXEMPLOS]:
        - Suplementos: 'Gestantes, lactantes e crianças. Diabéticos devem consultar médico.'
        - Energéticos: 'Gestantes, lactantes, crianças. Não consumir mais de 400mg cafeína/dia.'
        - Proteínas: 'Pessoas com doenças renais sem orientação médica.'
        - Fórmulas Infantis: 'Não utilizar em crianças alérgicas à proteína do leite de vaca.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    advertencias_e_precaucoes: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair alertas de segurança adicionais.
        [EXEMPLOS]:
        - 'Este produto não é um medicamento.'
        - 'Não substitui uma alimentação equilibrada e hábitos de vida saudáveis.'
        - 'Consumir durante ou após as refeições para melhor absorção.'
        - 'Interromper o uso se sentir qualquer desconforto.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    condicoes_de_armazenamento: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como armazenar o produto.
        [EXEMPLOS]:
        - 'Manter em local fresco e seco, ao abrigo da luz.'
        - 'Após aberto, conservar em geladeira e consumir em até 30 dias.'
        - 'Manter a embalagem bem fechada.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    validade_apos_abertura: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Indicar prazo de validade após abertura.
        [EXEMPLOS]:
        - '30 dias após aberto'
        - 'Consumir em até 6 meses após abertura'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )


# Schema PySpark para criação de tabela Delta
enriquece_alimentos_suplementos_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("nome_comercial", T.StringType(), True),
        T.StructField("dosagem", T.StringType(), True),
        T.StructField("idade_recomendada", T.StringType(), True),
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

colunas_obrigatorias_alimentos_suplementos = [
    "ean",
    "nome_comercial",
    "idade_recomendada",
]

schema_enriquece_alimentos_suplementos = """
    ean STRING NOT NULL,
    nome_comercial STRING,
    dosagem STRING,
    idade_recomendada STRING,
    volume_quantidade STRING,
    indicacao STRING,
    instrucoes_de_uso STRING,
    contraindicacoes STRING,
    advertencias_e_precaucoes STRING,
    condicoes_de_armazenamento STRING,
    validade_apos_abertura STRING,
    atualizado_em TIMESTAMP
"""
