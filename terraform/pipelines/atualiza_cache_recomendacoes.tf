locals {
  tasks_atualiza_cache_recomendacoes = [
    {
      task_key = "atualiza_cache_recomendacoes"
      path     = "3_analytics/to_postgres/atualiza_cache_recomendacoes"
      base_parameters = {
        debug = "false"
      }
    }
  ]
}

data "databricks_node_type" "atualiza_cache_recomendacoes" {
  local_disk    = true
  min_memory_gb = 8
  min_cores     = 4
}

module "atualiza_cache_recomendacoes" {
  source = "./pipeline"
  name   = "atualiza_cache_recomendacoes"
  # TODO: schedule comentado — documentar por que está desabilitado ou reativar.
  # schedule        = "0 0 23 * * ?" # Daily at 11 PM
  notify          = true
  timeout_seconds = 14400 # 4 horas
  parameters      = { "stage" = "prod", "environment" = "production" }
  driver_node_id  = data.databricks_node_type.atualiza_cache_recomendacoes.id
  worker_node_id  = data.databricks_node_type.atualiza_cache_recomendacoes.id
  tasks           = local.tasks_atualiza_cache_recomendacoes
}
