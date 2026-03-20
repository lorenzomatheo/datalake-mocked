from .extended_enum import ExtendedEnum


class FaixaEtaria(ExtendedEnum):
    BEBE = "bebe"
    CRIANCA = "crianca"
    ADOLESCENTE = "adolescente"
    JOVEM = "jovem"
    ADULTO = "adulto"
    IDOSO = "idoso"
    TODOS = "todos"


class FaixaEtariaSimplificado(ExtendedEnum):
    # NOTE: mantido para retrocompatibilidade, porem nao estamos mais utilizando
    # Preferir o FaixaEtaria pois tem maior granularidade
    # TODO: remover esse enum num futuro proximo, caso realmente nao seja mais atualizado.
    CRIANCA = "crianca"
    ADULTO = "adulto"
    IDOSO = "idoso"
    TODOS = "todos"
