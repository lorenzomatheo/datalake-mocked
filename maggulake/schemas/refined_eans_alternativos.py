"""
Schema para a tabela refined_eans_alternativos.

Esta tabela armazena o mapeamento de EANs para seus EANs alternativos,
utilizado para consolidar produtos equivalentes no pipeline de enriquecimento.
"""

schema = """
    ean STRING NOT NULL,
    eans_alternativos ARRAY<STRING>,
    status STRING
"""
