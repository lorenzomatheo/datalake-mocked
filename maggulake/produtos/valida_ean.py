# https://docs.guinzo.com.br/pt/ferramentas/validador-ean13

from typing import Any


def valida_ean(ean: Any) -> bool:
    if not isinstance(ean, str):
        return False

    ean = ean.strip()

    if not ean or len(ean) > 13 or not ean.isdigit() or ean.startswith("99"):
        return False

    ean = ean.rjust(13, "0")
    digitos = [int(i) for i in ean]
    validador = digitos[-1]

    soma = sum(a * b for a, b in zip(digitos[:-1], [1, 3] * 6))
    resto = soma % 10
    calculado = 0 if resto == 0 else 10 - resto
    return calculado == validador
