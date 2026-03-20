from enum import Enum


class Table(Enum):
    # Produtos:
    produtos_refined = "refined.produtos_refined"
    produtos_standard = "standard.produtos_standard"
    produtos_raw = "raw.produtos_raw"
    produtos_loja_refined = "refined.produtos_loja_refined"
    ids_lojas_com_produto = "refined.ids_lojas_com_produto"
    produtos_em_processamento = "refined._produtos_em_processamento"
    produtos_com_embeddings = "refined.produtos_com_embeddings"
    principios_ativos_padronizados = "refined.principios_ativos_padronizados"
    produtos_genericos = "raw.produtos_genericos"
    produtos_bluesoft = "raw.produtos_bluesoft"
    produtos_bigdatacorp = "raw.produtos_bigdatacorp"
    produtos_portal_obm = "raw.produtos_portal_obm"
    bulas_medicamentos = "standard.bulas_medicamentos"
    medicamentos_anvisa = "raw.medicamentos_anvisa"
    lmip_anvisa = "raw.lmip_anvisa"
    ean_alternativo_incompativel = "refined.ean_alternativo_incompativel"
    intercambiaveis = "refined.intercambiaveis"

    # Enriquecimento Produtos:
    extract_product_info = "enriquecimento.gpt_extract_product_info"
    extract_product_info_regex = "enriquecimento.gpt_extract_product_info_regex"
    extract_product_info_nao_medicamentos = (
        "enriquecimento.gpt_extract_product_info_nao_medicamentos"
    )
    extract_product_info_nao_medicamentos_regex = (
        "enriquecimento.gpt_extract_product_info_nao_medicamentos_regex"
    )
    # Enriquecimento por macro categoria (não-medicamentos)
    extract_product_info_perfumaria = (
        "enriquecimento.gpt_extract_product_info_perfumaria"
    )
    extract_product_info_suplementos = (
        "enriquecimento.gpt_extract_product_info_suplementos"
    )
    extract_product_info_materiais_saude = (
        "enriquecimento.gpt_extract_product_info_materiais_saude"
    )
    extract_product_info_produtos_casa = (
        "enriquecimento.gpt_extract_product_info_produtos_casa"
    )
    extract_product_info_produtos_animais = (
        "enriquecimento.gpt_extract_product_info_produtos_animais"
    )
    enriquecimento_tags_produtos = "enriquecimento.gpt_tags_produtos"
    embeddings = "enriquecimento.embeddings"
    power_phrase_produtos = "enriquecimento.power_phrase_produtos"
    enriquecimento_websearch = "enriquecimento.enriquecimento_websearch"
    gpt_judge_evaluator_product = "enriquecimento.gpt_judge_evaluator_product"
    coluna_eh_medicamento_completo = "enriquecimento.coluna_eh_medicamento_completo"
    coluna_eh_medicamento_correcoes_manuais = (
        "enriquecimento.coluna_eh_medicamento_correcoes_manuais"
    )
    match_medicamentos = "enriquecimento.match_medicamentos"
    match_nao_medicamentos = "enriquecimento.match_nao_medicamentos"
    categorias_cascata = "enriquecimento.categorias_cascata"
    categorias_cascata_agregado = "enriquecimento.categorias_cascata_agregado"
    refined_eans_alternativos = "enriquecimento.refined_eans_alternativos"
    gpt_descricoes_curtas_produtos = "enriquecimento.gpt_descricoes_curtas_produtos"
    produtos_nome_descricao_curta = "enriquecimento.produtos_nome_descricao_curta"
    produtos_nome_descricao_externos = "enriquecimento.produtos_nome_descricao_externos"
    mescla_tags_similaridade = "enriquecimento.mescla_tags_similaridade"
    # Sintomas e categorias complementares
    sintomas = "enriquecimento.sintomas"
    categorias_sintomas = "enriquecimento.categorias_sintomas"
    indicacoes_categorias = "enriquecimento.indicacoes_categorias"

    # Enriquecimento - Clientes
    clientes_conta_refined = "refined.clientes_conta_refined"
    clientes_conta_standard = "standard.clientes_conta_standard"

    # Utils:
    medicamentos_forma_volume_quantidade = "utils.medicamentos_forma_volume_quantidade"
    enum_principio_ativo_medicamentos = "utils.enum_principio_ativo_medicamentos"
    eans_processados_sara = "utils.eans_processados_sara"
    eans_processados_guia_da_farmacia = "utils.eans_processados_guia_da_farmacia"
    enums_completo = "utils.enums_completo"

    # Views:
    view_cestas_de_compras = "analytics.view_cestas_de_compras"
    view_usuarios = "analytics.view_usuarios"
    view_vendas = "analytics.view_vendas"
    view_vendas_eans_campanhas = "analytics.view_vendas_eans_campanhas"
    view_vendas_substitutos = "analytics.view_vendas_substitutos"
    view_data_quality_clientes_conta = "analytics.view_data_quality_clientes_conta"
    view_data_quality_produtos = "analytics.view_data_quality_produtos"
    view_data_quality_produtos_campanha_concorrente_analise_detalhada = (
        "analytics.view_data_quality_produtos_campanha_concorrente_analise_detalhada"
    )

    # Chat
    chat_milvus_payload_with_embeddings = (
        "raw.chat_milvus_production_payload_with_embeddings"
    )

    # Analytics
    score_qualidade_produto = "analytics.score_qualidade_produto"
    score_qualidade_produto_latest = "analytics.score_qualidade_produto_latest"
    score_aderencia = "analytics.score_aderencia"
    eval_puro_alternativos_recomendacoes_historico = (
        "analytics.eval_puro_alternativos_recomendacoes_historico"
    )
    eval_puro_alternativos_recomendacoes_detalhes = (
        "analytics.eval_puro_alternativos_recomendacoes_detalhes"
    )

    # Hyperflow — Raw
    regua_atendente_mensagens_enviadas = "raw.regua_atendente_mensagens_enviadas"
    regua_atendente_mensagens_enviadas_erros = (
        "raw.regua_atendente_mensagens_enviadas_erros"
    )

    # Hyperflow — Analytics: Cadastro
    hyperflow_cadastros_sheets = "analytics.hyperflow_cadastros_sheets"
    hyperflow_cadastros_base = "analytics.hyperflow_cadastros_base"
    hyperflow_nsm = "analytics.hyperflow_nsm"
    hyperflow_cadastros_por_loja = "analytics.hyperflow_cadastros_por_loja"

    # Hyperflow — Analytics: Régua de Comunicação
    hyperflow_regua_alcance = "analytics.hyperflow_regua_alcance"
    hyperflow_regua_opt_out = "analytics.hyperflow_regua_opt_out"


class CopilotTable(Enum):  # São as tabelas do BigQuery!!
    campanhas = 'campanhas_campanha'
    campanhas_produto = 'campanhas_campanhaproduto'
    campanhas_produto_concorrentes = 'campanhas_campanhaproduto_produtos_concorrentes'
    campanhas_lojas_especificas = 'campanhas_campanha_lojas_especificas'
    campanhas_todas_as_lojas_das_contas = 'campanhas_campanha_todas_as_lojas_das_contas'
    cesta_de_compras_item = 'cesta_de_compras_item'
    extratodepontos = 'gameficacao_extratodepontos'
    contas_loja = 'contas_loja'
    contas_conta = 'contas_conta'
    produtos_loja = "produtos_produtoloja"
    produtos = "produtos_produtov2"
    vendas = "vendas_venda"
    vendas_item = "vendas_item"
    moedas_por_produto = "moedas_por_produto"
    atendente_loja_apuracao_atendente = "atendente_loja_apuracao_atendente"


class BigqueryView(Enum):
    agregado_em_missao = "agregado_total_de_vendas_de_produtos_em_missao_por_semana"
    agregado_por_missao = "agregado_total_de_produtos_por_missao_por_semana"


class BigqueryGold(Enum):
    view_saude_recomendacoes = "view_f_saude_recomendacao"
    view_recomendacoes = "recomendacoes"
    view_vendas_apos_primeira_sala_maggu = "f_vendas_apos_primeira_sala_maggu"


class BigqueryViewPbi(Enum):
    d_produto_loja_campanha = "d_produto_loja_campanha"


class HiveMetastoreTable(Enum):
    # Essas tabelas estao no catalogo do hive_metastore por ser mais fácil de puxar pelo PowerBI
    disponibilidade_estoque_por_marca = (
        "hive_metastore.pbi.disponibilidade_estoque_por_marca"
    )
