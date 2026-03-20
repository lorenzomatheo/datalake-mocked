from dataclasses import dataclass
from datetime import datetime

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass
class ErroEnvioLog:
    id: str
    id_atendente: int
    username_atendente: str
    numero_destinatario: str
    data_hora_erro: datetime
    eh_somente_teste: bool
    tipo_erro: str
    subtipo_erro: str


erros_envio_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("id_atendente", IntegerType(), False),
        StructField("username_atendente", StringType(), True),
        StructField("numero_destinatario", StringType(), True),
        StructField("data_hora_erro", TimestampType(), False),
        StructField("eh_somente_teste", BooleanType(), False),
        StructField("tipo_erro", StringType(), True),
        StructField("subtipo_erro", StringType(), True),
    ]
)
