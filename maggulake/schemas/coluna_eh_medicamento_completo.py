"""
Schema para a tabela que consolida a coluna eh_medicamento de múltiplas fontes.

Esta tabela armazena o resultado final da determinação se um produto é medicamento ou não,
considerando categorias cascata, correções manuais e dados da camada standard.
"""

schema = """
    ean STRING,
    eh_medicamento BOOLEAN,
    fonte STRING NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""
