from .extended_enum import ExtendedEnum


class SexoRecomendado(ExtendedEnum):
    HOMEM = "homem"
    MULHER = "mulher"
    TODOS = "todos"


class SexoCliente(ExtendedEnum):
    MASCULINO = "masculino"
    FEMININO = "feminino"
