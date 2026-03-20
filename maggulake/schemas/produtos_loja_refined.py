schema = """
    id STRING NOT NULL,
    id_produto STRING NOT NULL,
    codigo_externo STRING,
    ean STRING NOT NULL,
    eans_alternativos ARRAY<STRING>,
    ean_conta_loja STRING, -- Separados por |
    conta STRING NOT NULL,
    loja STRING NOT NULL,
    tenant STRING NOT NULL,
    loja_id STRING NOT NULL,
    codigo_loja STRING NOT NULL,
    preco_venda DOUBLE,
    preco_venda_desconto DOUBLE,
    custo_compra DOUBLE,
    estoque_unid INTEGER,
    permite_desconto BOOLEAN,
    desconto_percentual_maximo DOUBLE,
    preco_pmc DOUBLE,
    grupo STRING,
    grupos STRING,
    subgrupo STRING,
    categoria STRING,
    eh_produto_foco BOOLEAN,
    -- TODO: remover maggu coins.
    maggu_coins_venda_organica INTEGER,
    maggu_coins_venda_agregada INTEGER,
    maggu_coins_ads_substituicao INTEGER,
    maggu_coins_ads_agregada INTEGER,

    -- Informacoes refined
    margem_media DOUBLE,
    demanda_media_diaria DOUBLE,
    dias_de_estoque DOUBLE,
    classificacao_demanda_abc STRING,  -- Tambem conhecido como "Giro"
    classificacao_margem_abc STRING,

    json_data STRING,
    gerado_em TIMESTAMP NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""
