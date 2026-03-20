"""
Schema para a tabela de correções manuais da coluna eh_medicamento.

Esta tabela armazena produtos que tiveram a coluna eh_medicamento corrigida
manualmente através de expressões ou listas hardcoded de nomes.
"""

schema = """
    ean STRING,
    eh_medicamento BOOLEAN NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""
