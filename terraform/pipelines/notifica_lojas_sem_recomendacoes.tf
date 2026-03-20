data "databricks_node_type" "small_node" {
  local_disk    = true
  min_memory_gb = 8
  min_cores     = 4
}

module "notifica_lojas_sem_recomendacoes" {
  source          = "./pipeline"
  name            = "notifica_lojas_sem_recomendacoes"
  parameters      = { "stage" = "prod", "environment" = "production" }
  schedule        = "0 0 23 ? * SUN *" # Weekly on Sundays at 11:00 PM
  notify          = true
  timeout_seconds = 3600 # 1 hour
  tasks = [
    {
      task_key = "notifica_lojas_sem_recomendacoes"
      path     = "misc/notifica_lojas_sem_recomendacoes"
    }
  ]
  driver_node_id = data.databricks_node_type.small_node.id
  worker_node_id = data.databricks_node_type.small_node.id
}
