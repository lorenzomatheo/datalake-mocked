from dataclasses import dataclass
from datetime import datetime

import pyspark.sql.types as T
from pydantic import BaseModel, Field

power_phrase_produtos_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("power_phrase", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)


@dataclass
class ResultadoPowerPhrase:
    ean: str
    power_phrase: str | None
    atualizado_em: datetime

    def to_dict(self) -> dict:
        return {
            "ean": self.ean,
            "power_phrase": self.power_phrase,
            "atualizado_em": self.atualizado_em,
        }


class EnriquecimentoPowerPhrase(BaseModel):
    power_phrase: str = Field(
        description=(
            "Identifique qual é a unique selling proposition do produto, ou seja, "
            "uma descrição sobre o que torna o produto diferente da sua concorrência. "
            "A frase deve ser sucinta e não deve ultrapassar 100 caracteres."
        ),
    )
