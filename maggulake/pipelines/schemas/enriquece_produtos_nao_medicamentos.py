from textwrap import dedent

from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import types as T

from maggulake.enums import (
    FaixaEtaria,
    SexoRecomendado,
)

enriquece_nao_medicamentos_schema = T.StructType(
    [
        T.StructField("id", T.StringType(), True),
        T.StructField("ean", T.StringType(), True),
        T.StructField("nome_comercial", T.StringType(), True),
        T.StructField("idade_recomendada", T.StringType(), True),
        T.StructField("sexo_recomendado", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)


# TODO: o campo `descricao` sempre estara preenchido antes de chegar na LLM.
# Contudo, podemos incluir um campo `descricao` nesse schema e pedir para que a LLM reescreva a descricao do produto.
# Isso seria bem util para garantir que o texto esteja sempre num tom de voz, comprimento e conteudo padronizado.


# TODO: acredito que poderiamos incluir os campos `marca` e `fabricante` nesse schema.
# Nao vou fazer agora pois preciso avancar rapido. Mas deixo como recomendacao futura.
# Deveriamos incluir tanto nesse quanto no schema de medicamentos


class EnriquecimentoProdutosNaoMedicamentos(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    id: str = Field(description="Seleciona o Id do produto sem modificação.")
    ean: str = Field(description="Seleciona o Ean do produto sem modificação.")

    nome_comercial: str = Field(
        description=dedent("""\
        [OBJETIVO]: Criar um nome comercial padronizado do produto com no máximo 60 caracteres.
        [REGRAS]:
        1.  Siga EXATAMENTE esta estrutura: [Tipo de Produto] [Marca] [Linha] [Característica] [Quantidade/Tamanho].
        2.  Utilize apenas informações presentes no texto-fonte.
        3.  Se necessário, abrevie ou remova palavras menos importantes para respeitar o limite de 60 caracteres.
        [EXEMPLO]: 'Creme Dental Colgate Luminous White Lovers Vinho 70g'.
        [O QUE NÃO FAZER]: Não invente informações. Não exceda o limite de caracteres.
        """),
    )

    idade_recomendada: FaixaEtaria = Field(
        description=dedent("""\
        [OBJETIVO]: Indique a faixa etária para a qual o produto é recomendado, de acordo com a descrição do produto.
        [OPÇÕES DISPONÍVEIS]:
        - 'bebe': Para produtos destinados a bebês (ex: fraldas, mamadeiras, pomadas para assaduras).
        - 'crianca': Para produtos voltados para crianças (ex: brinquedos, produtos de higiene infantil).
        - 'adolescente': Para produtos indicados para adolescentes (ex: produtos para acne, suplementos para crescimento).
        - 'jovem': Para produtos voltados para jovens (ex: cosméticos, produtos esportivos).
        - 'adulto': Para produtos indicados para adultos (maiores de 18 anos, ex: preservativos, suplementos).
        - 'idoso': Para produtos com indicação específica para idosos (ex: suplementos para terceira idade, bengalas).
        - 'todos': Para produtos sem restrição etária específica (ex: escova de cabelo, água mineral).
        [EXEMPLOS]:
        - Use 'bebe' se o texto mencionar "bebês", "recém-nascidos" ou "0-2 anos".
        - Use 'crianca' se o texto mencionar "infantil", "crianças" ou "3-12 anos".
        - Use 'adolescente' se o texto mencionar "adolescentes" ou "13-17 anos".
        - Use 'jovem' se o texto mencionar "jovens" ou "18-25 anos".
        - Use 'adulto' se o texto especificar "+18" ou "adultos".
        - Use 'idoso' se o texto mencionar "idosos", "terceira idade" ou "acima de 60 anos".
        - Use 'todos' se não houver restrição etária mencionada.
        [REGRA]: Base-se apenas nas informações explícitas do texto sobre faixa etária. Caso nao haja informacao suficiente, use 'todos'.
        """)
    )

    sexo_recomendado: SexoRecomendado = Field(
        description=dedent("""\
        [OBJETIVO]: Informe para qual sexo o produto é indicado, se houver especificação no texto fornecido.
        [OPÇÕES DISPONÍVEIS]:
        - 'homem': Para produtos específicos masculinos (ex: cueca, lâmina de barbear masculina)
        - 'mulher': Para produtos específicos femininos (ex: absorvente, coletor menstrual)
        - 'todos': Para produtos sem restrição de sexo (maioria dos casos)
        [EXEMPLOS]:
        - Use 'mulher' para produtos ginecológicos ou absorventes.
        - Use 'homem' para produtos masculinos específicos.
        - Use 'todos' se não houver indicação de sexo.
        [REGRA]: Se não houver restrição mencionada na descricao ou nome do produto, use 'todos'.
        """)
    )


colunas_obrigatorios_enriquecimento_nao_medicamentos = [
    "id",  # TODO: temos que parar de enriquecer por id
    "ean",
    "nome_comercial",
    "idade_recomendada",
    "sexo_recomendado",
]
