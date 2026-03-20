from collections import Counter
from datetime import date

from maggulake.enums import FaixaEtaria


def calculate_mode(numbers):
    # Conta a ocorrência de cada número
    count = Counter(numbers)
    # Encontra o(s) número(s) mais comum(ns)
    mode = count.most_common(1)
    if mode:
        return mode[0][0]
    else:
        return None


def assign_quintile(
    value: float, quintiles: list[float], ascending: bool = True
) -> int:
    """Assign a quintile to a given value.

    Parameters
    ----------
    value : float
        The value to assign the quintile to.
    quintiles : list[float]
        The quintiles to use for the assignment. This should have 4 elements,
        representing the 20th, 40th, 60th and 80th percentiles.
    ascending : bool, optional
        Whether the quintiles are in ascending order or not. If True, the
        quintiles will be in ascending order, if False, they will be in
        descending order. The default is True.

    Returns
    -------
    int
        The quintile number of the given value.
    """
    for i, q in enumerate(quintiles):
        if value <= q:
            return (i + 1) if ascending else (len(quintiles) + 1 - i)
    return (len(quintiles) + 1) if ascending else 1


def calcula_idade(data_nascimento: date) -> int | None:
    """Função para calcular a idade com base na data de nascimento"""

    if not data_nascimento:
        return None

    hoje = date.today()
    return (
        hoje.year
        - data_nascimento.year
        - ((hoje.month, hoje.day) < (data_nascimento.month, data_nascimento.day))
    )


def map_faixa_etaria(
    idade: int | float,
) -> str | None:
    """Mapeamento de faixa etária"""
    # pylint: disable=too-many-return-statements
    if idade is None or idade < 0:
        return None

    match idade:
        case _ if idade < 2:
            return FaixaEtaria.BEBE.value
        case _ if idade < 13:
            return FaixaEtaria.CRIANCA.value
        case _ if idade < 18:
            return FaixaEtaria.ADOLESCENTE.value
        case _ if idade < 25:
            return FaixaEtaria.JOVEM.value
        case _ if idade < 60:
            return FaixaEtaria.ADULTO.value
        case _ if idade >= 60:
            return FaixaEtaria.IDOSO.value
    return None
