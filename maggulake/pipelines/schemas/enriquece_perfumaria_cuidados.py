"""
Schema Pydantic para enriquecimento de produtos da categoria "Perfumaria e Cuidados".

Meso categorias incluídas:
- Cuidado com a Pele
- Cuidado Capilar
- Cuidado Pessoal
- Higiene Pessoal
- Higiene Bucal
- Maquiagem
- Oftalmológicos
- Produtos para Unhas
- Infantil
- Acessórios
- Joias
- Cabelo
"""

from textwrap import dedent
from typing import Optional

import pyspark.sql.types as T
from pydantic import BaseModel, ConfigDict, Field

from maggulake.enums import FaixaEtaria, SexoRecomendado


class EnriquecimentoPerfumariaCuidados(BaseModel):
    """Schema para enriquecimento de produtos de Perfumaria e Cuidados."""

    model_config = ConfigDict(use_enum_values=True)

    ean: str = Field(description="Seleciona o EAN do produto sem modificação.")

    nome_comercial: str = Field(
        description=dedent("""\
        [OBJETIVO]: Criar um nome comercial padronizado do produto com no máximo 60 caracteres.
        [REGRAS]:
        1. Siga EXATAMENTE esta estrutura: [Tipo de Produto] [Marca] [Linha] [Característica] [Quantidade/Tamanho].
        2. Utilize apenas informações presentes no texto-fonte.
        3. Se necessário, abrevie ou remova palavras menos importantes para respeitar o limite de 60 caracteres.
        [EXEMPLOS]:
        - 'Protetor Solar Neutrogena Sun Fresh FPS70 120ml'
        - 'Shampoo Elseve L'Oréal Hidra Hialurônico 400ml'
        - 'Creme Dental Colgate Luminous White 70g'
        - 'Desodorante Rexona Clinical Aerosol 150ml'
        - 'Batom MAC Ruby Woo Matte Vermelho'
        [O QUE NÃO FAZER]: Não invente informações. Não exceda 60 caracteres.
        """),
    )

    idade_recomendada: FaixaEtaria = Field(
        description=dedent("""\
        [OBJETIVO]: Indicar a faixa etária para a qual o produto é recomendado.
        [OPÇÕES]: 'bebe', 'crianca', 'adolescente', 'jovem', 'adulto', 'idoso', 'todos'.
        [REGRAS]:
        - Use 'bebe' para fraldas, mamadeiras, chupetas, pomadas de assadura.
        - Use 'crianca' para shampoos infantis, produtos com personagens infantis.
        - Use 'adulto' para produtos de depilação, preservativos, cosméticos anti-idade.
        - Use 'todos' se não houver indicação explícita de faixa etária.
        [EXEMPLOS]:
        - Fralda Pampers → 'bebe'
        - Shampoo Infantil Johnson's → 'crianca'
        - Creme Anti-rugas → 'adulto'
        - Protetor Solar FPS50 → 'todos'
        """)
    )

    sexo_recomendado: SexoRecomendado = Field(
        description=dedent("""\
        [OBJETIVO]: Informar para qual sexo o produto é indicado.
        [OPÇÕES]: 'homem', 'mulher', 'todos'.
        [REGRAS]:
        - Use 'mulher' para absorventes, coletores menstruais, maquiagem específica.
        - Use 'homem' para lâminas de barba masculinas, desodorantes masculinos.
        - Use 'todos' se não houver indicação de sexo (maioria dos casos).
        [EXEMPLOS]:
        - Absorvente Always → 'mulher'
        - Aparelho Gillette Mach3 → 'homem'
        - Shampoo Anticaspa → 'todos'
        """)
    )

    volume_quantidade: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair a apresentação comercial do produto (volume, peso ou quantidade).
        [REGRAS]:
        1. Inclua unidade de medida (ml, g, un, etc.).
        2. Para kits, especifique quantidade de itens.
        [EXEMPLOS]:
        - '200ml'
        - '50g'
        - 'Kit 3 unidades 200ml cada'
        - '30 cápsulas'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    indicacao: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever para que ou quem o produto é indicado.
        [REGRAS]:
        1. Foque no benefício principal e tipo de usuário/situação.
        2. Use linguagem clara e direta.
        3. Máximo de 200 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Hidratantes: 'Hidratação profunda para peles secas e ressecadas'
        - Protetores Solares: 'Proteção solar diária FPS 70. Resistente à água por 80min.'
        - Shampoos: 'Cabelos secos, opacos e desidratados. Hidratação com ácido hialurônico.'
        - Anti-caspa: 'Controle da caspa e dermatite seborreica do couro cabeludo'
        - Coloração: 'Coloração permanente. Cobertura total de cabelos brancos.'
        - Desodorantes: 'Antitranspirante 48h proteção. Sem álcool.'
        - Fraldas: 'Bebês de 6 a 10kg. Proteção por até 12 horas.'
        - Absorventes: 'Fluxo intenso, proteção durante a noite'
        - Bases: 'Cobertura média, acabamento matte natural. Pele oleosa.'
        - Esmaltes: 'Cores vibrantes com longa duração. Secagem rápida.'
        - Chupetas: 'Bico ortodôntico para bebês de 0 a 6 meses.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    instrucoes_de_uso: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como usar o produto corretamente.
        [REGRAS]:
        1. Seja objetivo e prático.
        2. Inclua frequência de uso quando relevante.
        3. Máximo de 300 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Protetores Solares: 'Aplicar 15min antes da exposição solar. Reaplicar a cada 2h ou após mergulho.'
        - Shampoos: 'Aplicar nos cabelos molhados, massagear e enxaguar. Usar 2-3x por semana.'
        - Condicionadores: 'Aplicar após shampoo, deixar agir 2-3 minutos e enxaguar.'
        - Máscaras Capilares: 'Aplicar após shampoo, deixar agir 5-10 minutos, enxaguar. Usar 1-2x por semana.'
        - Coloração: 'Misturar creme e oxidante. Aplicar nos cabelos secos. Deixar agir 30-45 minutos.'
        - Depilação: 'Aplicar na área desejada, aguardar 5 minutos e remover com espátula.'
        - Demaquilantes: 'Aplicar com algodão e remover delicadamente. Remove maquiagem à prova d'água.'
        - Escovas de Dente: 'Escovar por 2 minutos, 3 vezes ao dia. Trocar a cada 3 meses.'
        - Fio Dental: 'Usar diariamente antes ou após a escovação. Deslizar suavemente entre os dentes.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    advertencias_e_precaucoes: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair alertas de segurança e precauções de uso.
        [REGRAS]:
        1. Foque em informações de segurança relevantes.
        2. Inclua alertas para grupos sensíveis (gestantes, crianças, alérgicos).
        [EXEMPLOS]:
        - Protetores Solares: 'Evitar contato com os olhos. Se ocorrer irritação, suspender o uso.'
        - Coloração: 'Fazer teste de alergia 48h antes. Não aplicar em sobrancelhas ou cílios.'
        - Repelentes: 'Não aplicar em mucosas ou ferimentos. Não usar em menores de 2 anos.'
        - Depilação: 'Não usar em pele irritada ou com ferimentos. Fazer teste de sensibilidade.'
        - Produtos Infantis: 'Uso sob supervisão de adultos. Verificar integridade antes do uso.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    condicoes_de_armazenamento: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como armazenar o produto corretamente.
        [EXEMPLOS]:
        - 'Manter em local fresco e seco, ao abrigo da luz solar.'
        - 'Conservar em temperatura ambiente (15-30°C).'
        - 'Manter a tampa fechada após o uso.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )


# Schema PySpark para criação de tabela Delta
enriquece_perfumaria_cuidados_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("nome_comercial", T.StringType(), True),
        T.StructField("idade_recomendada", T.StringType(), True),
        T.StructField("sexo_recomendado", T.StringType(), True),
        T.StructField("volume_quantidade", T.StringType(), True),
        T.StructField("indicacao", T.StringType(), True),
        T.StructField("instrucoes_de_uso", T.StringType(), True),
        T.StructField("advertencias_e_precaucoes", T.StringType(), True),
        T.StructField("condicoes_de_armazenamento", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)

colunas_obrigatorias_perfumaria_cuidados = [
    "ean",
    "nome_comercial",
    "idade_recomendada",
    "sexo_recomendado",
]

schema_enriquece_perfumaria_cuidados = """
    ean STRING NOT NULL,
    nome_comercial STRING,
    idade_recomendada STRING,
    sexo_recomendado STRING,
    volume_quantidade STRING,
    indicacao STRING,
    instrucoes_de_uso STRING,
    advertencias_e_precaucoes STRING,
    condicoes_de_armazenamento STRING,
    atualizado_em TIMESTAMP
"""
