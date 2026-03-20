"""Schema para a tabela de resultados de regex de idade/sexo para não-medicamentos."""

schema = """
    ean STRING NOT NULL,
    idade_recomendada STRING,
    sexo_recomendado STRING,
    atualizado_em TIMESTAMP NOT NULL
"""
