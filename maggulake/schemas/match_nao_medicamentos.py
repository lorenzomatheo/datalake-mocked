"""
Schema para a tabela de enriquecimento match_nao_medicamentos.

Esta tabela armazena os resultados do processo de match de não medicamentos,
onde produtos sem informações são enriquecidos através de busca vetorial
por produtos similares que já possuem dados completos.

NOTE: Para não medicamentos, só faz sentido copiar categorias, pois outros atributos
(como marca, fabricante) não podem ser generalizados entre produtos diferentes.
"""

from dataclasses import dataclass

from pyspark.sql import types as T

# NOTE: incluir aqui somente colunas que podem ser enriquecidas via match, isto é,
# colunas que podem ser copiadas de outros produtos de nomes parecidos.
schema = """
    ean STRING NOT NULL,
    nome STRING NOT NULL,
    eh_medicamento BOOLEAN,
    categorias ARRAY<STRING>,
    match_nao_medicamentos_em TIMESTAMP NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""

# Lista de colunas para facilitar operações de select
columns = [
    "ean",
    "nome",
    "eh_medicamento",
    "categorias",
    "match_nao_medicamentos_em",
    "atualizado_em",
]


# Schema PySpark para facilitar criação de DataFrames vazios
pyspark_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), nullable=False),
        T.StructField("nome", T.StringType(), nullable=False),
        T.StructField("eh_medicamento", T.BooleanType(), nullable=True),
        T.StructField("categorias", T.ArrayType(T.StringType()), nullable=True),
        T.StructField("match_nao_medicamentos_em", T.TimestampType(), nullable=False),
        T.StructField("atualizado_em", T.TimestampType(), nullable=False),
    ]
)


# Schema para os resultados do match (colunas temporárias com sufixo _match)
schema_match_result = T.StructType(
    [
        T.StructField("nome", T.StringType()),
        T.StructField("eh_medicamento_match", T.BooleanType()),
        T.StructField("categorias_match", T.ArrayType(T.StringType())),
    ]
)


@dataclass
class NaoMedicamentoRetrieverResponse:
    """
    Response do retriever para não medicamentos.

    NOTE: Removidos "marca" e "fabricante" pois nao sao genericos o suficiente.
    Isso gerava o problema de transformar qualquer "creme dental" em "marca: oral-b"
    Manter aqui somente aquilo que realmente pode ser extrapolado de um produto
    para outro de nome parecido.
    """

    nome: str
    eh_medicamento: bool | None
    categorias: list[str]

    def to_tuple(self) -> tuple:
        return (
            self.nome,
            self.eh_medicamento,
            self.categorias,
        )
