schema = """
    ean STRING,
    fonte STRING,
    quality_score DOUBLE,
    completeness_score DOUBLE,
    compliance_score DOUBLE,
    qtd_total_colunas_avaliadas INT,
    qtd_colunas_preenchidas INT,
    qtd_colunas_em_compliance INT,
    colunas_fora_compliance ARRAY<STRING>,
    colunas_sem_informacao ARRAY<STRING>,
    quality_tier STRING,
    gerado_em TIMESTAMP
"""
