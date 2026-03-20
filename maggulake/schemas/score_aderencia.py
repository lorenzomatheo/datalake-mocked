"""Schema para tabela de score de aderência de produtos (agregado por EAN)."""

schema = """
    ean STRING NOT NULL,
    qtde_lojas INT NOT NULL,
    score_aderencia DOUBLE NOT NULL,
    score_estoque DOUBLE NOT NULL,
    score_volume DOUBLE NOT NULL,
    score_preco DOUBLE NOT NULL,
    score_margem DOUBLE NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""
