schema = """
    id STRING NOT NULL,
    conta STRING NOT NULL,
    conta_id STRING NOT NULL,
    cod_externo STRING,

    cpf_cnpj STRING,
    eh_pessoa_fisica BOOLEAN,
    eh_pessoa_juridica BOOLEAN,
    nome_completo STRING,
    nome_social STRING,
    data_nascimento DATE,
    idade INT,
    faixa_etaria STRING,
    sexo STRING,

    email STRING,
    celular STRING,
    telefone_alt STRING,

    enderecos ARRAY<STRUCT<
        cep STRING,
        logradouro STRING,
        numero STRING,
        complemento STRING,
        bairro STRING,
        cidade STRING,
        estado STRING
    >>,

    endereco_completo STRING,

    autorizacao_comunicacao BOOLEAN,

    data_primeira_compra DATE,
    data_ultima_compra DATE,
    total_de_compras INT,  -- total de compras realizadas
    total_de_cestas INT,  -- total de cestas criadas aqui na maggu
    compras ARRAY<STRING>,
    ticket_medio FLOAT,
    media_itens_por_cesta FLOAT,
    pref_dia_semana STRING,
    pref_dia_mes INT,
    pref_hora INT,
    intervalo_medio_entre_compras INT,
    cluster_rfm STRING,
    loja_mais_frequente STRING,
    pct_canal_loja_fisica FLOAT,
    pct_canal_tele_entrega FLOAT,
    pct_canal_e_commerce FLOAT,
    pct_canal_outros_canais FLOAT,
    canal_preferido STRING,

    pct_pgto_cartao FLOAT,
    pct_pgto_pix FLOAT,
    pct_pgto_dinheiro FLOAT,
    pct_pgto_crediario FLOAT,
    metodo_pgto_preferido STRING,

    pct_produtos_impulsionados FLOAT,

    pct_itens_medicamentos FLOAT,
    pct_itens_medicamentos_genericos FLOAT,
    pct_itens_medicamentos_referencia FLOAT,
    pct_itens_medicamentos_similar FLOAT,
    pct_itens_medicamentos_similar_intercambiavel FLOAT,

    formas_farmaceuticas_preferidas ARRAY<STRING>,

    pct_venda_agregada FLOAT,
    pct_venda_organica FLOAT,
    pct_venda_substituicao FLOAT,

    pct_pequeno FLOAT,
    pct_medio FLOAT,
    pct_grande FLOAT,
    tamanho_produto_preferido STRING,

    doencas_cronicas ARRAY<STRING>,
    doencas_agudas ARRAY<STRING>,

    medicamentos_tarjados ARRAY<STRING>,
    medicamentos_controle ARRAY<STRING>,

    ja_comprou_medicamento BOOLEAN,
    
    fonte STRING,
    criado_em TIMESTAMP NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""
