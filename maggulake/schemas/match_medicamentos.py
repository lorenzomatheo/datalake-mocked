"""
Schema para a tabela de enriquecimento match_medicamentos.

Esta tabela armazena os resultados do processo de match de medicamentos,
onde produtos sem informações são enriquecidos através de busca vetorial
por produtos similares que já possuem dados completos.
"""

from dataclasses import dataclass

from pyspark.sql import types as T

# NOTE: incluir aqui somente colunas que podem ser enriquecidas via match, isto é,
# colunas que podem ser copiadas de outros produtos de nomes parecidos. Não incluir
# marca, fabricante ou similares pois estes dados não podem ser copiados de outros
# produtos.
schema = """
    ean STRING NOT NULL,
    nome STRING NOT NULL,
    principio_ativo STRING,
    eh_medicamento BOOLEAN,
    eh_tarjado BOOLEAN,
    tipo_de_receita_completo STRING,
    eh_controlado BOOLEAN,
    tipo_medicamento STRING,
    especialidades ARRAY<STRING>,
    classes_terapeuticas ARRAY<STRING>,
    categorias ARRAY<STRING>,
    match_medicamentos_em TIMESTAMP NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""

# Lista de colunas para facilitar operações de select
columns = [
    "ean",
    "nome",
    "principio_ativo",
    "eh_medicamento",
    "eh_tarjado",
    "tipo_de_receita_completo",
    "eh_controlado",
    "tipo_medicamento",
    "especialidades",
    "classes_terapeuticas",
    "categorias",
    "match_medicamentos_em",
    "atualizado_em",
]


# Schema para os resultados do match (colunas temporárias com sufixo _match)
schema_match_result = T.StructType(
    [
        T.StructField("nome", T.StringType()),
        T.StructField("principio_ativo_match", T.StringType()),
        T.StructField("eh_medicamento_match", T.BooleanType()),
        T.StructField("eh_tarjado_match", T.BooleanType()),
        T.StructField("tipo_medicamento_match", T.StringType()),
        T.StructField("tipo_de_receita_completo_match", T.StringType()),
        T.StructField("eh_controlado_match", T.BooleanType()),
        T.StructField("especialidades_match", T.ArrayType(T.StringType())),
        T.StructField("classes_terapeuticas_match", T.ArrayType(T.StringType())),
        T.StructField("categorias_match", T.ArrayType(T.StringType())),
    ]
)


@dataclass
class MedicamentoRetrieverResponse:
    # NOTE: Removidos "marca" e "fabricante" pois nao sao genericos o suficiente.
    # Isso gerava o problema de transformar qualquer "creme dental" em "marca: oral-b"
    # Manter aqui somente aquilo que realmente pode ser extrapolado de um produto
    # para outro de nome parecido.

    nome: str
    principio_ativo: str | None
    eh_medicamento: bool | None
    eh_tarjado: bool | None
    eh_controlado: bool | None
    tipo_de_receita_completo: str | None
    tipo_medicamento: str | None
    especialidades: list[str]
    classes_terapeuticas: list[str]
    categorias: list[str]

    def to_tuple(self) -> tuple:
        return (
            self.nome,
            self.principio_ativo,
            self.eh_medicamento,
            self.eh_tarjado,
            self.tipo_medicamento,
            self.tipo_de_receita_completo,
            self.eh_controlado,
            self.especialidades,
            self.classes_terapeuticas,
            self.categorias,
        )
