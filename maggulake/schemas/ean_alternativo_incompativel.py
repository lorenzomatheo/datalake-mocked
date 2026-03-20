"""
Tabela auxiliar que registra os pares (ean_principal, ean_alternativo)
removidos da base final porque o EAN alternativo possui marca e/ou
fabricante diferente do EAN principal
"""

schema = """
    ean_principal STRING NOT NULL,
    ean_alternativo STRING
"""
