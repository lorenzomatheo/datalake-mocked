from pyspark.sql import functions as F


def mapear_nivel_para_status(nivel: str) -> str | None:
    return {
        "nenhum": "nenhum",
        "nivel-1": "esquenta",
        "nivel-2": "iniciante",
        "nivel-3": "intermediario",
        "nivel-4": "avancado",
        "nivel-5": "lenda_viva",
    }.get(nivel, None)


def mapear_status_para_nivel(status: str) -> int:
    # TODO: erro silencioso aqui se usar o -1

    return {
        "nenhum": 0,
        "esquenta": 1,
        "iniciante": 2,
        "intermediario": 3,
        "avancado": 4,
        "lenda_viva": 5,
    }.get(status, -1)


def mapear_status_para_label(coluna_status):
    # Converte o status interno para um label legível.
    return (
        F.when(coluna_status == "esquenta", "Esquenta")
        .when(coluna_status == "iniciante", "Iniciante")
        .when(coluna_status == "intermediario", "Intermediário")
        .when(coluna_status == "avancado", "Avançado")
        .when(coluna_status == "lenda_viva", "Lenda Viva")
        .otherwise(coluna_status)
    )
