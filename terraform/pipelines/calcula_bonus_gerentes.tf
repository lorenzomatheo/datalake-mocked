locals {
  calcular_bonus_gerentes_tasks = [
    {
      task_key = "calcular_bonus_gerentes"
      path     = "apuracao/calcular_bonus_gerentes"
    }
  ]
}

data "databricks_node_type" "calcular_bonus_gerentes" {
  local_disk    = true
  min_memory_gb = 8
  min_cores     = 2
  category      = "Memory Optimized"
}

module "calcular_bonus_gerentes" {
  source          = "./pipeline"
  name            = "calcular_bonus_gerentes"
  schedule        = "0 0 8 ? * 4" # Toda quarta-feira às 8:00 AM
  notify          = true
  timeout_seconds = 3600 # 1 hora
  parameters      = { "environment" = "production" }
  driver_node_id  = data.databricks_node_type.calcular_bonus_gerentes.id
  worker_node_id  = data.databricks_node_type.calcular_bonus_gerentes.id
  tasks           = local.calcular_bonus_gerentes_tasks
}

