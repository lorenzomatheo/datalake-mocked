locals {
  tasks_atualiza_status_contas = [
    {
      task_key = "atualiza_status_contas"
      path     = "3_analytics/to_postgres/atualiza_status_contas"
      base_parameters = {
        debug = "false"
      }
    }
  ]
}

data "databricks_node_type" "atualiza_status_contas" {
  local_disk    = true
  min_memory_gb = 4
  min_cores     = 2
}

module "atualiza_status_contas" {
  source          = "../pipeline"
  name            = "atualiza_status_contas"
  description     = "Atualiza status das contas baseado no status das lojas"
  schedule        = "0 0 0 * * ?" # Daily at midnight
  notify          = true
  timeout_seconds = 7200 # 2 horas
  parameters      = { "stage" = "prod" }
  driver_node_id  = data.databricks_node_type.atualiza_status_contas.id
  worker_node_id  = data.databricks_node_type.atualiza_status_contas.id
  tasks           = local.tasks_atualiza_status_contas
}
