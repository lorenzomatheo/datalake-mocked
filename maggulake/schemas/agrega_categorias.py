"""
Schema para a tabela categorias_cascata_agregado.

Esta tabela armazena as categorias agregadas dos produtos após o processo
de categorização em cascata, com uma linha por EAN contendo todas as categorias
desse EAN.
"""

schema = """
    ean STRING NOT NULL,
    categorias ARRAY<STRING>,
    eh_medicamento BOOLEAN,
    atualizado_em TIMESTAMP NOT NULL
"""
