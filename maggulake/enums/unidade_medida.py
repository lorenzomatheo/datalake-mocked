from .extended_enum import ExtendedEnum


class UnidadeMedida(ExtendedEnum):
    # massa
    GRAMAS = "g"

    # Volume
    ML = "ml"
    LITRO = "l"

    # Outros
    UNIDADE = "unidade"
    COMPRIMIDO = "comprimido"
    CAPSULA = "capsula"
