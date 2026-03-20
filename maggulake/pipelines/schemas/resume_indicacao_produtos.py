from dataclasses import dataclass
from datetime import datetime
from textwrap import dedent

import pyspark.sql.types as T
from pydantic import BaseModel, Field

resume_indicacao_produtos_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("descricao_curta", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)


@dataclass
class ResultadoResumeIndicacao:
    ean: str
    descricao_curta: str | None
    atualizado_em: datetime

    def to_dict(self) -> dict:
        return {
            "ean": self.ean,
            "descricao_curta": self.descricao_curta,
            "atualizado_em": self.atualizado_em,
        }


class ResumeIndicacaoProdutos(BaseModel):
    ean: str = Field(description="Código EAN do produto sem modificação.")
    descricao_curta: str = Field(
        description=dedent("""
        [OBJETIVO]: Criar uma descrição curta e objetiva das indicações de uso do produto.

        [REGRAS]:
        1. Comece a frase com "O produto [NOME] é indicado para..."
        2. Seja conciso e objetivo, com no máximo 200 caracteres
        3. Foque nas principais indicações de uso
        4. Use linguagem clara e acessível ao consumidor final
        5. Se o produto for um medicamento, mencione as principais condições tratadas
        6. Se não for medicamento, descreva sua principal finalidade

        [EXEMPLOS]:
        - "O produto Dipirona Sódica 500mg é indicado para o alívio da dor e febre."
        - "O produto Bepantol Derma é indicado para hidratação e regeneração da pele."
        - "O produto Termômetro Digital é indicado para medição rápida e precisa da temperatura corporal."

        [O QUE NÃO FAZER]:
        - Não inclua informações sobre dosagem
        - Não mencione contraindicações
        - Não use termos técnicos complexos
        - Não exceda o limite de caracteres

        [EM CASO DE AUSÊNCIA]: Se não houver informações suficientes, retorne uma descrição genérica baseada no nome do produto.
        """)
    )
