import pyspark.sql.types as T
from pydantic import BaseModel, Field


class SintomasProduto(BaseModel):
    """Schema de output do LLM para a fase de sintomas.

    O LLM recebe nome + descrição de um produto medicamento e retorna
    o EAN do produto e uma lista de sintomas separados por '|'.
    """

    ean: str = Field(
        description="EAN do produto exatamente como fornecido na entrada, sem nenhuma modificação."
    )
    resposta: str = Field(
        description=(
            "Lista de até 10 sintomas ou problemas de saúde que o produto é indicado para tratar "
            "ou aliviar, separados pelo caractere '|'. "
            "Cada termo deve ser um sintoma específico escrito no singular. "
            "Exemplos: 'dor de cabeça|febre|inflamação' ou 'ressecamento|coceira|sensibilidade da pele'. "
            "Se o produto não tiver função médica ou de saúde, responda apenas 'N/A'."
        )
    )


class CategoriasPorSintoma(BaseModel):
    """Schema de output do LLM para a fase de categorias complementares.

    O LLM recebe um sintoma e retorna categorias de produtos farmacêuticos
    não medicamentosos que o cliente pode comprar para tratar aquele sintoma.
    """

    resposta: str = Field(
        description=(
            "Lista de até 5 tipos de produtos farmacêuticos NÃO medicamentosos que um cliente "
            "pode querer comprar em uma farmácia para auxiliar no tratamento ou alívio do sintoma "
            "informado. Separados pelo caractere '|'. "
            "Exemplos: 'termômetro|compressa|soro fisiológico' ou 'shampoo|tônico capilar|vitamina'. "
            "Exclua remédios e medicamentos da lista. "
            "Se não houver produtos complementares aplicáveis, responda apenas 'N/A'."
        )
    )


# ── Schemas PySpark para criação das tabelas Delta ─────────────────────────────

sintomas_pyspark_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("resposta", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)

categorias_sintomas_pyspark_schema = T.StructType(
    [
        T.StructField("sintoma", T.StringType(), False),
        T.StructField("resposta", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)

indicacoes_categorias_pyspark_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField(
            "categorias_de_complementares_por_indicacao",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("indicacao", T.StringType(), True),
                        T.StructField("categorias", T.ArrayType(T.StringType()), True),
                    ]
                )
            ),
            True,
        ),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)

# ── DDL strings para env.create_table_if_not_exists ───────────────────────────

schema_sintomas = """
    ean STRING NOT NULL,
    resposta STRING,
    atualizado_em TIMESTAMP
"""

schema_categorias_sintomas = """
    sintoma STRING NOT NULL,
    resposta STRING,
    atualizado_em TIMESTAMP
"""

schema_indicacoes_categorias = """
    ean STRING NOT NULL,
    categorias_de_complementares_por_indicacao ARRAY<STRUCT<indicacao: STRING, categorias: ARRAY<STRING>>>,
    atualizado_em TIMESTAMP
"""
