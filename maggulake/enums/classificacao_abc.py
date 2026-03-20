from .extended_enum import ExtendedEnum


class ClassificacaoABC(ExtendedEnum):
    """
    O método ABC é uma técnica de classificação de itens baseada em sua
    importância relativa, geralmente medida pelo valor de consumo ou impacto
    no estoque. Primeiro, os itens são ranqueados conforme esse critério, e
    depois classificados em categorias (A, B ou C) de acordo com a distribuição
    acumulada percentual.
    """

    A = "A"  # 0-20%
    B = "B"  # 20-50%
    C = "C"  # 50-100%
