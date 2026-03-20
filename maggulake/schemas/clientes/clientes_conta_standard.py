schema = """
    id STRING NOT NULL,
    conta STRING NOT NULL,
    conta_id STRING NOT NULL,
    cod_externo STRING,

    cpf_cnpj STRING,
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

    fonte STRING,
    criado_em TIMESTAMP NOT NULL,
    atualizado_em TIMESTAMP NOT NULL
"""
