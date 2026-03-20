# schema produto ids das lojas que vão receber recomendações seguindo as regras do script `cria_ids_lojas_com_produtos`

schema = """
    ean STRING,
    lojas_com_produto STRING,
    ids_lojas_com_produto ARRAY<STRING>,
    gerado_em TIMESTAMP NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""
