schema = """
    -- Private Key
    loja_id STRING,

    -- Cadastrais
    nome_loja STRING,
    cnpj STRING,
    conta_loja STRING,
    codigo_loja STRING,
    ativo BOOLEAN,  -- TODO: vamos trocar para "status" depois
    
    -- Metricas 
    quantidade_lvlup LONG,
    pontos_roxos_totais LONG,
    moedas_totais LONG,
    tamanho_faturamento_medio_mensal DOUBLE,
    ticket_medio_mensal DOUBLE,
    quantidade_atendimentos_medio_mensal DOUBLE,
    quantidade_atendentes LONG,
    quantidade_atendimentos_total LONG,
    faturamento_medio_mensal DOUBLE,
    horario_de_pico INT,
    percentual_rx DOUBLE,
    percentual_otc DOUBLE,
    primeira_cesta_fechada_em DATE,
    tempo_de_uso INT,
    
    -- Localizacao
    latitude DOUBLE,
    longitude DOUBLE,
    cidade STRING,
    estado STRING,

    -- Cluster
    tamanho_loja STRING,

    -- Timestamps
    created_at TIMESTAMP
"""
