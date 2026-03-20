data "databricks_node_type" "cluster_16gb" {
  local_disk    = true
  min_memory_gb = 16
}

data "databricks_node_type" "update_processa_raw" {
  local_disk    = true
  min_memory_gb = 16
  min_cores     = 8
}

module "update_processa_raw" {
  source         = "./pipeline"
  name           = "update_processa_raw"
  parameters     = { "stage" = "prod", "environment" = "production" }
  schedule       = "0 23 2 ? * 7" # (2:23 AM every Saturday)
  notify         = true
  driver_node_id = data.databricks_node_type.update_processa_raw.id
  worker_node_id = data.databricks_node_type.update_processa_raw.id
  tasks = [
    {
      task_key = "atualiza_fontes_da_verdade"
      path     = "0_any_2_raw/maggu/atualiza_fontes_da_verdade"
    },
    {
      task_key   = "atualiza_produtos_raw"
      path       = "0_any_2_raw/maggu/atualiza_produtos_raw"
      depends_on = ["atualiza_fontes_da_verdade"]
    },
    {
      task_key   = "extrai_texto_bulas_cr"
      path       = "1_raw_2_standard/consulta_remedios/extrai_texto_bulas_cr"
      depends_on = ["atualiza_produtos_raw"]
    },
    {
      task_key = "processa_anvisa"
      path     = "1_raw_2_standard/maggu/cria_produtos_anvisa"
    },
  ]
}

module "scrape_guia_da_farmacia" {
  source          = "./pipeline"
  name            = "scrape_guia_da_farmacia"
  schedule        = "0 0 2 ? * 1" # Every Sunday at 2:00 AM
  notify          = true
  timeout_seconds = 86400 # 24 horas
  parameters      = { "stage" = "prod" }
  tasks = [
    {
      task_key = "scrape_bula"
      path     = "0_any_2_raw/guia_da_farmacia/scrape_guia_da_farmacia"
      base_parameters = {
        max_products = "10000"
      }
    },
    {
      task_key   = "scrape_intercambiaveis"
      depends_on = []
      path       = "0_any_2_raw/guia_da_farmacia/scrape_guia_da_farmacia_intercambiaveis"
      base_parameters = {
        max_products = "10000"
      }
    },
    {
      task_key   = "extrai_texto_bulas_guia_farmacia"
      path       = "1_raw_2_standard/guia_da_farmacia/extrai_texto_bulas_guia_farmacia"
      depends_on = ["scrape_bula"]
    }
  ]
}

module "cria_dev" {
  source         = "./pipeline"
  name           = "atualiza_dev"
  schedule       = "32 0 4 ? * Mon"
  driver_node_id = data.databricks_node_type.cluster_16gb.id
  worker_node_id = data.databricks_node_type.cluster_16gb.id
  tasks = [
    {
      task_key        = "define_produtos"
      depends_on      = []
      path            = "misc/dev/define_produtos_ambiente_dev"
      base_parameters = {}
    },
    {
      task_key        = "gera_fixtures"
      depends_on      = ["define_produtos"]
      path            = "misc/dev/gera_fixtures_dev"
      base_parameters = {}
    },
    {
      task_key        = "atualiza_milvus"
      depends_on      = ["define_produtos"]
      path            = "misc/dev/atualiza_milvus_dev"
      base_parameters = {}
    },
  ]
}
# Desabilitado por não estar sendo utilizado 27.02.2026
module "atualiza_embeddings_conversas_chat" {
  source = "./pipeline"
  name   = "atualiza_embeddings_conversas_chat"
  # schedule        = "0 0 6 * * ?" # (6 AM every day)
  notify          = true
  timeout_seconds = 3600 # 1 hora
  parameters      = { "stage" = "prod", "environment" = "production" }
  tasks = [
    {
      task_key   = "cria_delta_table_perguntas_respostas_chat_agno"
      depends_on = []
      path       = "1_raw_2_standard/chat/cria_delta_table_perguntas_respostas_chat_agno"
    },
    {
      task_key   = "gera_fine_tuning_dataset_chat"
      depends_on = ["cria_delta_table_perguntas_respostas_chat_agno"]
      path       = "3_analytics/gera_fine_tuning_dataset_chat"
    },
    {
      task_key   = "gera_embeddings_conversas_chat"
      depends_on = ["cria_delta_table_perguntas_respostas_chat_agno"]
      path       = "1_raw_2_standard/chat/gera_embeddings_conversas_chat"
    },
    {
      task_key   = "atualiza_collection_conversas_chat"
      depends_on = ["gera_embeddings_conversas_chat"]
      path       = "2_standard_2_refined/atualiza_collection_conversas_chat"
      base_parameters = {
        recria_collection = "true"
      }
    }
  ]
}

module "atualiza_agregado_de_vendas_de_produtos_em_missao" {
  source          = "./pipeline"
  name            = "atualiza_agregado_de_vendas_de_produtos_em_missao"
  parameters      = { "environment" = "production" }
  schedule        = "0 0 6 * * ?" # (6 AM every day)
  notify          = true
  timeout_seconds = 3600 # 1 hora
  tasks = [
    {
      task_key   = "atualiza_agregado_de_vendas_de_produtos_em_missao"
      depends_on = []
      path       = "3_analytics/to_postgres/atualiza_agregado_de_vendas_de_produtos_em_missao"
    },
  ]
}

module "cria_destaques_semana_mini_maggu" {
  source = "./pipeline"

  name        = "cria_destaques_semana_mini_maggu"
  description = "Cria destaques da rodada anterior para exibição na aba destaques na mini maggu"

  schedule        = "0 0 4 ? * 2" # Every Monday at 4:00 AM
  notify          = true
  timeout_seconds = 3600

  tasks = [
    {
      task_key = "cria_destaques_semana_mini_maggu"
      path     = "3_analytics/to_postgres/cria_destaques_semana_mini_maggu"
      email_notifications = {
        on_failure                = ["carlos.silva@maggu.ai"]
        no_alert_for_skipped_runs = true
      }
    }
  ]
}

module "enriquece_dados_lojas" {
  source          = "./pipeline"
  name            = "adiciona_uf_lojas_via_bigdatacorp"
  description     = "Enriquece lojas com dados de estado via BigDataCorp API"
  schedule        = "0 0 2 * * ?" # Diariamente as 2 AM
  notify          = true
  timeout_seconds = 3600 # 1 hora
  parameters      = { "stage" = "prod", "environment" = "production" }

  tasks = [
    {
      task_key = "adiciona_uf_lojas_via_bigdatacorp"
      path     = "2_standard_2_refined/enriquecimento/adiciona_uf_lojas_via_bigdatacorp"
    }
  ]
}
