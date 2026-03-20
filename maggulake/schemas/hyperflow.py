# -- Cadastro --

schema_hyperflow_cadastros_sheets = """
    -- Espelho normalizado e deduplicado do Google Sheets de cadastros do Hyperflow.

    -- Informações da loja
    codigo_loja STRING,

    -- Informações do atendente
    atendente_id INT,
    username STRING,
    codigo_externo STRING,
    nome_completo STRING,

    -- Datas e controle
    confirmou_chave_em TIMESTAMP,
    data_cadastro_hyperflow TIMESTAMP,
    id_usuario_unificado STRING,
    chave_deduplicacao STRING,
    atualizado_em TIMESTAMP
"""

schema_hyperflow_cadastros_base = """
    -- NOTE: Cada linha representa um par atendente x loja elegível.

    -- Informações da loja
    id_loja STRING,
    nome_loja STRING,
    codigo_de_seis_digitos STRING,
    status_loja STRING,
    cnpj_loja STRING,
    erp STRING,

    -- Informações da rede
    nome_rede STRING,

    -- Informações do atendente
    codigo_externo_atendente STRING,
    id_atendente INT,
    username STRING,
    status_ativacao STRING,
    deve_receber_comunicacoes_no_whatsapp BOOLEAN,
    nome_atendente STRING,
    telefone_atendente STRING,

    -- Informações adicionais
    passou_pelo_hyperflow BOOLEAN,
    ativo_ultimos_30d BOOLEAN,
    data_cadastro_hyperflow TIMESTAMP,
    atualizado_em TIMESTAMP
"""

schema_hyperflow_nsm = """
    -- Snapshot
    data_snapshot TIMESTAMP,

    -- Métricas de cadastro e atividade
    total_cadastros_hyperflow INT,
    total_ativos_30d INT,
    cadastros_em_ativos_30d INT, -- Atendentes únicos que passaram no Hyperflow e tiveram atividade nos últimos 30 dias.

    -- Cálculos
    nsm_percentual FLOAT, -- Percentual em escala 0-100: (cadastros_em_ativos_30d / total_ativos_30d) * 100.
    total_atendentes_base INT, -- Total de atendentes únicos elegíveis na base global.
    atualizado_em TIMESTAMP
"""

schema_hyperflow_cadastros_por_loja = """
    -- Informações da loja
    codigo_de_seis_digitos STRING,
    nome_loja STRING,
    nome_rede STRING,
    erp STRING,
    status_loja STRING,

    -- Cálculos
    total_atendentes_elegiveis LONG, -- Total de atendentes únicos elegíveis na loja.
    total_com_hyperflow INT,  -- Total de atendentes únicos da loja com cadastro no Hyperflow.
    total_ativos_30d INT,  -- Total de atendentes únicos da loja com atividade nos últimos 30 dias.
    taxa_adesao_percentual DOUBLE,  -- Percentual da loja: (total_com_hyperflow / total_atendentes_elegiveis) * 100
    atualizado_em TIMESTAMP
"""

# -- Régua de Comunicação --

schema_hyperflow_regua_alcance = """
    -- Snapshot
    data_snapshot TIMESTAMP,

    -- Métricas de alcance
    total_atendentes_alcancados INT,
    total_elegiveis_opt_in INT,
    total_cadastrados INT,

    -- Cálculos
    percentual_alcance FLOAT,
    atualizado_em TIMESTAMP
"""

schema_hyperflow_regua_opt_out = """
    id_atendente INT,
    username STRING,
    nome_atendente STRING,
    telefone_atendente STRING,
    data_cadastro TIMESTAMP,

    -- Informações da loja
    id_loja STRING,
    nome_loja STRING,
    nome_rede STRING,

    -- INformações do opt-out
    mensagens_recebidas_antes_opt_out INT,
    ultima_mensagem_antes_opt_out TIMESTAMP,
    atualizado_em TIMESTAMP
"""
