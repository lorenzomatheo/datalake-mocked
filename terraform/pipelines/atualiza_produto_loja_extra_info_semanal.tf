data "databricks_node_type" "produto_loja_extra_info" {
  local_disk    = true
  min_memory_gb = 32
}

module "atualiza_produto_loja_extra_info_producao" {
  source          = "./pipeline"
  name            = "atualiza_produto_loja_extra_info_producao"
  parameters      = { "stage" = "prod", "environment" = "production" }
  schedule        = "0 0 4 ? * SUN" # Sundays at 4:00 AM
  notify          = true
  timeout_seconds = 28800 # 8 hours (current script needs 6+, optimization to be done later)
  tasks = [
    {
      task_key = "atualiza_produto_loja_extra_info"
      path     = "3_analytics/to_postgres/atualiza_produto_loja_extra_info"
    }
  ]
  driver_node_id = data.databricks_node_type.produto_loja_extra_info.id
  worker_node_id = data.databricks_node_type.produto_loja_extra_info.id
}
