data "databricks_node_type" "enriquece_produtos_producao" {
  local_disk    = true
  min_memory_gb = 64
  min_cores     = 8
}

data "databricks_node_type" "enriquece_produtos_staging" {
  local_disk    = true
  min_memory_gb = 32
}

locals {
  enriquece_produtos_tasks = [
    # ==================== FASE 1: Categorias ==================================
    {
      task_key   = "categoriza_produtos_em_cascata"
      depends_on = []
      path       = "2_standard_2_refined/enriquecimento/categorias/categoriza_produtos_em_cascata"
      base_parameters = {
        debug        = "false"
        max_produtos = "5000"
        batch_size   = "500"
        llm_provider = "gemini_vertex"
        model_size   = "SMALL"
      }
    },
    {
      task_key   = "agrega_categorias"
      depends_on = ["categoriza_produtos_em_cascata"]
      path       = "2_standard_2_refined/enriquecimento/categorias/agrega_categorias"
      base_parameters = {
        debug = "false"
      }
    },
    # ==================== FASE 2: Define eh_medicamento =======================
    {
      task_key   = "define_eh_medicamento_hardcoded"
      depends_on = []
      path       = "2_standard_2_refined/enriquecimento/define_eh_medicamento_hardcoded"
      base_parameters = {
        debug = "false"
      }
    },
    {
      task_key   = "consolida_coluna_eh_medicamento"
      depends_on = ["define_eh_medicamento_hardcoded", "agrega_categorias"]
      path       = "2_standard_2_refined/enriquecimento/consolida_coluna_eh_medicamento"
      base_parameters = {
        debug = "false"
      }
    },
    # ==================== FASE 3: Matches =====================================
    {
      task_key   = "match_medicamentos"
      depends_on = ["consolida_coluna_eh_medicamento"]
      path       = "2_standard_2_refined/enriquecimento/match_medicamentos"
      base_parameters = {
        debug        = "false"
        max_products = "30000"
      }
    },
    {
      task_key   = "match_nao_medicamentos"
      depends_on = ["consolida_coluna_eh_medicamento"]
      path       = "2_standard_2_refined/enriquecimento/match_nao_medicamentos"
      base_parameters = {
        debug        = "false"
        max_products = "30000"
      }
    },
    {
      task_key   = "match_tags"
      depends_on = []
      path       = "2_standard_2_refined/enriquecimento/match_tags"
      base_parameters = {
        debug        = "false"
        max_products = "3000"
        batch_size   = "1000"
      }
    },
    {
      task_key   = "mescla_tags_por_similaridade"
      depends_on = ["match_tags"]
      path       = "2_standard_2_refined/enriquecimento/mescla_tags_por_similaridade"
      base_parameters = {
        debug = "false"
      }
    },
    # ==================== FASE 4: Enriquecimentos LLM =========================
    {
      task_key   = "gera_sintomas_categorias"
      depends_on = []
      path       = "2_standard_2_refined/enriquecimento/categorias/enriquece_sintomas_e_categorias_complementares"
      base_parameters = {
        max_products = "30000"
      }
    },
    {
      task_key   = "enriquece_com_info_bula"
      depends_on = ["gera_sintomas_categorias", "consolida_coluna_eh_medicamento"]
      path       = "2_standard_2_refined/enriquecimento/gpt_enriquece_produtos_info_bula"
      base_parameters = {
        max_products = "30000",
        llm_provider = "gemini_vertex"
      }
    },
    {
      task_key   = "complementa_nome_descricao_produtos"
      depends_on = ["consolida_coluna_eh_medicamento"]
      path       = "2_standard_2_refined/complementa_nome_descricao_produtos"
    },
    # {
    #   task_key   = "enriquece_produtos_nao_medicamentos"
    #   depends_on = ["complementa_nome_descricao_produtos"]
    #   path       = "2_standard_2_refined/enriquecimento/gpt_enriquece_produtos_nao_medicamentos"
    #   base_parameters = {
    #     max_products = "30000"
    #   }
    # },
    # Novas tasks de enriquecimento por categoria
    {
      task_key   = "enriquece_produtos_perfumaria"
      depends_on = ["complementa_nome_descricao_produtos"]
      path       = "2_standard_2_refined/enriquecimento/enriquece_produtos_perfumaria"
      base_parameters = {
        max_products = "30000",
        llm_provider = "gemini_vertex"
      }
    },
    {
      task_key   = "enriquece_produtos_alimentos"
      depends_on = ["complementa_nome_descricao_produtos"]
      path       = "2_standard_2_refined/enriquecimento/enriquece_produtos_alimentos"
      base_parameters = {
        max_products = "30000",
        llm_provider = "gemini_vertex"
      }
    },
    {
      task_key   = "enriquece_produtos_materiais_saude"
      depends_on = ["complementa_nome_descricao_produtos"]
      path       = "2_standard_2_refined/enriquecimento/enriquece_produtos_materiais_saude"
      base_parameters = {
        max_products = "30000",
        llm_provider = "gemini_vertex"
      }
    },
    {
      task_key   = "enriquece_produtos_casa"
      depends_on = ["complementa_nome_descricao_produtos"]
      path       = "2_standard_2_refined/enriquecimento/enriquece_produtos_casa"
      base_parameters = {
        max_products = "30000",
        llm_provider = "gemini_vertex"
      }
    },
    {
      task_key   = "enriquece_produtos_animais"
      depends_on = ["complementa_nome_descricao_produtos"]
      path       = "2_standard_2_refined/enriquecimento/enriquece_produtos_animais"
      base_parameters = {
        max_products = "30000",
        llm_provider = "gemini_vertex"
      }
    },
    {
      task_key   = "gera_indicacoes"
      depends_on = []
      path       = "2_standard_2_refined/enriquecimento/gpt_resume_indicacao_produtos"
      parameters = {
        "batch_size"    = "200",
        "max_products"  = "30000",
        "timeout_batch" = "600", # seconds
        "llm_provider"  = "gemini_vertex"
      }
    },
    {
      task_key   = "gera_tags_produtos"
      depends_on = ["mescla_tags_por_similaridade"]
      path       = "2_standard_2_refined/enriquecimento/gpt_gera_tags_produtos"
      base_parameters = {
        max_products    = "30000"
        refazer_tags    = "false" # Refaz as tags que estiverem fora de compliance
        timeout_batches = "1800"  # em segundos
        batch_size      = "500"
      }
    },
    {
      task_key   = "gera_power_phrase_produtos"
      depends_on = [] # Totalmente standalone
      path       = "2_standard_2_refined/enriquecimento/gera_power_phrase_produtos"
      base_parameters = {
        max_products = "30000"
        debug        = "true"
      }
    },
    {
      task_key   = "enriquece_marca_e_fabricante"
      depends_on = []
      path       = "2_standard_2_refined/enriquecimento/enriquece_marca_fabricante"
      base_parameters = {
        max_products = "10000"
      }
    },
    # ==================== FASE 5: Enriquecimentos Complementares ==============
    {
      task_key   = "medicamentos_forma_volume_quantidade"
      depends_on = []
      path       = "2_standard_2_refined/enriquecimento/medicamentos_forma_volume_quantidade"
      base_parameters = {
        max_products = "30000"
      }
    },
    {
      task_key   = "llm_judge_produtos_campanha"
      depends_on = []
      path       = "2_standard_2_refined/enriquecimento/llm_as_judge"
      base_parameters = {
        debug                       = "false"
        max_products                = "2000" # Hj tá rodando 500 em 1 hora
        refazer_eans_qualidade_ruim = "true"
      }
    },
    {
      task_key   = "mescla_eans_similares"
      depends_on = ["enriquece_marca_e_fabricante"]
      path       = "2_standard_2_refined/mescla_eans_similares"
      base_parameters = {
        debug = "false"
      }
    },
    {
      task_key   = "valida_eans_alternativos"
      depends_on = ["mescla_eans_similares"]
      path       = "3_analytics/qualidade/valida_produtos_eans_alternativos"
    },
  ]
}

module "enriquece_produtos_producao" {
  source          = "./pipeline"
  name            = "enriquece_produtos_producao"
  schedule        = "0 0 0 * * ?" # Daily at midnight
  notify          = true
  timeout_seconds = 64800 # 18 horas
  parameters      = { "stage" = "prod", "environment" = "production" }
  driver_node_id  = data.databricks_node_type.enriquece_produtos_producao.id
  worker_node_id  = data.databricks_node_type.enriquece_produtos_producao.id
  tasks           = local.enriquece_produtos_tasks
}

module "enriquece_produtos_staging" {
  source          = "./pipeline"
  name            = "enriquece_produtos_staging"
  notify          = false
  timeout_seconds = 10800 # 3 horas
  # max_products sobrescreve o limite global de produtos para smoke test em staging
  parameters     = { "stage" = "dev", "environment" = "staging", "max_products" = "500" }
  driver_node_id = data.databricks_node_type.enriquece_produtos_staging.id
  worker_node_id = data.databricks_node_type.enriquece_produtos_staging.id
  tasks          = local.enriquece_produtos_tasks
}
