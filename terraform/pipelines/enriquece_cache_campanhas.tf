locals {
  cache_campanhas_tasks = [
    {
      task_key = "match_ads_substitutos"
      path     = "2_standard_2_refined/enriquecimento/match_ads_substitutos"
    },
    {
      task_key   = "atualiza_cache_recomendacoes_campanha"
      depends_on = ["match_ads_substitutos"]
      path       = "3_analytics/to_postgres/atualiza_cache_recomendacoes_campanha"
    }
  ]
}

data "databricks_node_type" "cache_campanhas" {
  local_disk    = true
  min_memory_gb = 16
  min_cores     = 8
}

module "cache_campanhas" {
  source          = "./pipeline"
  name            = "enriquece_cache_campanhas"
  schedule        = "0 10 3 ? * 2" # Segunda-feira às 03:10
  notify          = true
  timeout_seconds = 90000 # 25 horas
  parameters      = { "stage" = "prod", "environment" = "production" }
  tasks           = local.cache_campanhas_tasks
  driver_node_id  = data.databricks_node_type.cache_campanhas.id
  worker_node_id  = data.databricks_node_type.cache_campanhas.id
  min_num_workers = 1
  max_num_workers = 1 # Deixei poucos pra nao paralelizar demais e sobrecarregar o data api
}
