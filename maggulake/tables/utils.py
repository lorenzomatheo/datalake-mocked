from typing import Callable

from pyspark.sql import DataFrame


class ValidationError(Exception):
    pass


def tabela_facil(
    cls,
    validate: Callable[[DataFrame], DataFrame | None] = lambda x: None,
    force_schema: bool = True,
):
    """Usar com parcimônia. Bom para gerar uma tabela para depois validar."""

    class Maroto(cls):
        def validate(self, new: DataFrame) -> DataFrame | None:
            return validate(new)

    Maroto.force_schema = force_schema

    return Maroto
