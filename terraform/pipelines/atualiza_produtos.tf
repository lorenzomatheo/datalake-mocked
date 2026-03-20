locals {
  atualiza_produtos_tasks = [
    {
      task_key    = "cria_produtos_standard"
      path        = "1_raw_2_standard/maggu/cria_produtos_standard"
      clear_cache = "true"
    },
    # LMIP - Lista de Medicamentos Isentos de Prescrição (ANVISA)
    {
      task_key = "atualiza_lmip_anvisa"
      path     = "0_any_2_raw/anvisa/anvisa_lmip"
    },
    {
      task_key   = "enriquece_produto_loja"
      depends_on = ["cria_produtos_standard"]
      path       = "2_standard_2_refined/produto_loja/enriquece_produto_loja"
      base_parameters = {
        debug = "false",
      }
    },
    {
      task_key   = "adiciona_correcoes_manuais_pre_enriquecimento"
      depends_on = ["cria_produtos_standard"]
      path       = "2_standard_2_refined/adiciona_correcoes_manuais_pre_enriquecimento"
    },
    {
      task_key   = "cria_ids_lojas_com_produtos"
      depends_on = ["cria_produtos_standard"]
      path       = "2_standard_2_refined/cria_ids_lojas_com_produtos"
    },
    {
      task_key = "agrega_enriquecimentos"
      depends_on = [
        "adiciona_correcoes_manuais_pre_enriquecimento",
        "cria_ids_lojas_com_produtos"
      ]
      path = "2_standard_2_refined/agrega_enriquecimentos"
    },
    {
      task_key   = "padroniza_principio_ativo"
      depends_on = ["agrega_enriquecimentos"]
      path       = "2_standard_2_refined/padroniza_principio_ativo"
    },
    {
      task_key   = "junta_informacoes_anvisa"
      depends_on = ["padroniza_principio_ativo"]
      path       = "2_standard_2_refined/junta_informacoes_anvisa"
    },
    {
      task_key   = "padroniza_receita_e_tarja"
      depends_on = ["junta_informacoes_anvisa"]
      path       = "2_standard_2_refined/padroniza_receita_e_tarja"
    },
    {
      task_key   = "define_eh_controlado"
      depends_on = ["padroniza_receita_e_tarja", "atualiza_lmip_anvisa"]
      path       = "2_standard_2_refined/define_eh_controlado"
    },
    {
      task_key   = "enriquece_tamanho_produtos"
      depends_on = ["define_eh_controlado"]
      path       = "2_standard_2_refined/enriquece_tamanho_produtos"
    },
    {
      task_key   = "agrega_info_iqvia"
      depends_on = ["enriquece_tamanho_produtos"]
      path       = "2_standard_2_refined/agrega_info_iqvia"
    },
    {
      task_key   = "padroniza_produtos_foco_industria"
      depends_on = ["agrega_info_iqvia"]
      path       = "2_standard_2_refined/padroniza_produtos_foco_industria"
    },
    {
      task_key   = "padroniza_categorias"
      depends_on = ["padroniza_produtos_foco_industria"]
      path       = "2_standard_2_refined/enriquecimento/categorias/padroniza_categorias_e_tags"
    },
    {
      task_key   = "gera_recomendacoes_por_categorias"
      depends_on = ["padroniza_categorias"]
      path       = "2_standard_2_refined/enriquecimento/categorias/atualiza_categorias_recomendacoes"
    },
    {
      task_key   = "adiciona_correcoes_manuais"
      depends_on = ["gera_recomendacoes_por_categorias"]
      path       = "2_standard_2_refined/adiciona_correcoes_manuais"
    },
    {
      task_key   = "add_info_intercambiaveis"
      depends_on = ["adiciona_correcoes_manuais"]
      path       = "2_standard_2_refined/add_info_intercambiaveis"
    },
    {
      task_key   = "completar_idade_e_sexo"
      depends_on = ["add_info_intercambiaveis"]
      path       = "2_standard_2_refined/enriquecimento/completar_idade_e_sexo"
    },
    {
      task_key   = "completar_categorias"
      depends_on = ["completar_idade_e_sexo"]
      path       = "2_standard_2_refined/enriquecimento/categorias/completar_categorias"
    },
    {
      task_key   = "fuzzy_matching_produtos_em_processamento"
      depends_on = ["completar_categorias"]
      path       = "2_standard_2_refined/fuzzy_matching_produtos_em_processamento"
    },
    {
      task_key   = "calcula_score_qualidade_produtos"
      depends_on = ["fuzzy_matching_produtos_em_processamento"]
      path       = "3_analytics/qualidade/calcula_score_qualidade_produtos"
    },
    {
      task_key   = "salva_tabela_refined"
      depends_on = ["calcula_score_qualidade_produtos"]
      path       = "2_standard_2_refined/salva_tabela_refined"
    },
    {
      task_key   = "calcula_kpis_qualidade"
      depends_on = ["salva_tabela_refined"]
      path       = "3_analytics/qualidade/valida_qualidade_dados_produtos"
    },
    {
      task_key   = "valida_compliance_otc"
      depends_on = ["salva_tabela_refined"]
      path       = "3_analytics/qualidade/valida_compliance_otc"
    },
    {
      task_key   = "atualiza_produtos_copilot"
      depends_on = ["salva_tabela_refined"]
      path       = "3_analytics/to_postgres/atualiza_produtos_copilot"
    },
    {
      task_key   = "gera_embeddings_produtos"
      depends_on = ["salva_tabela_refined"]
      path       = "2_standard_2_refined/gera_embeddings_produtos"
    },
    {
      task_key   = "salva_embeddings_s3_milvus"
      depends_on = ["gera_embeddings_produtos"]
      path       = "2_standard_2_refined/salva_embeddings_s3_milvus"
    },
    {
      task_key   = "eval_recomendacoes"
      depends_on = ["salva_embeddings_s3_milvus"]
      path       = "evals/eval_recomendacoes"
    },
    {
      task_key   = "eval_puro_alternativos_recomendacoes"
      depends_on = ["salva_embeddings_s3_milvus"]
      path       = "evals/eval_puro_alternativos_recomendacoes"
      base_parameters = {
        tamanho_amostra = "100"
      }
    },
    {
      task_key   = "avisa_discord"
      depends_on = ["salva_embeddings_s3_milvus", "atualiza_produtos_copilot", "eval_recomendacoes", "eval_puro_alternativos_recomendacoes"]
      path       = "misc/avisa_sucesso_workflow_discord"
    }
  ]
}

data "databricks_node_type" "atualiza_produtos_staging" {
  local_disk    = true
  min_memory_gb = 32
}

data "databricks_node_type" "atualiza_produtos_producao" {
  local_disk    = true
  min_memory_gb = 64
  min_cores     = 8
}

module "tudo_producao" {
  source          = "./pipeline"
  name            = "atualiza_produtos_producao"
  parameters      = { "stage" = "prod", "environment" = "production", "debug" = "false" }
  schedule        = "33 0 18 * * ?" # Every Day at 18:00
  notify          = true
  timeout_seconds = 21600 # 6 horas
  maven_libraries = ["com.amazon.deequ:deequ:2.0.7-spark-3.5"]
  tasks           = local.atualiza_produtos_tasks
  driver_node_id  = data.databricks_node_type.atualiza_produtos_producao.id
  worker_node_id  = data.databricks_node_type.atualiza_produtos_producao.id
}

module "tudo_staging" {
  source          = "./pipeline"
  name            = "atualiza_produtos_staging"
  parameters      = { "stage" = "dev", "environment" = "staging", "debug" = "false" }
  schedule        = "33 0 18 * * ?" # Every Day at 18:00
  notify          = true
  timeout_seconds = 14400 # 4 horas
  maven_libraries = ["com.amazon.deequ:deequ:2.0.7-spark-3.5"]
  tasks           = local.atualiza_produtos_tasks
  driver_node_id  = data.databricks_node_type.atualiza_produtos_staging.id
  worker_node_id  = data.databricks_node_type.atualiza_produtos_staging.id
}
