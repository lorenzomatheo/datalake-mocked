data "databricks_node_type" "puxa_dados_postgres" {
  local_disk    = true
  min_memory_gb = 64
  min_cores     = 16
  category      = "Memory Optimized"
}

module "puxa_dados_estoque_postgres" {
  source          = "./pipeline"
  name            = "sync_dados_estoque_postgres"
  schedule        = "0 0 8 * * ?"
  notify          = true
  timeout_seconds = 3600
  parameters      = { "stage" = "prod", "environment" = "production" }
  driver_node_id  = data.databricks_node_type.puxa_dados_postgres.id
  worker_node_id  = data.databricks_node_type.puxa_dados_postgres.id
  tasks = [
    {
      task_key = "sobe_estoque_pro_s3"
      path     = "3_analytics/from_postgres/sobe_estoque_pro_s3"
      base_parameters = {
        debug          = "false"
        somente_testes = "false"
      }
    },
    {
      task_key = "historico_cobertura_estoque"
      path     = "3_analytics/powerbi/historico-disponibilidade-estoque-por-marca"
    }
  ]
}

module "puxa_dados_conversoes" {
  source          = "./pipeline"
  name            = "sync_dados_conversoes"
  schedule        = "0 30 6 ? * *"
  notify          = true
  timeout_seconds = 18000 # 5 horas
  driver_node_id  = data.databricks_node_type.puxa_dados_postgres.id
  worker_node_id  = data.databricks_node_type.puxa_dados_postgres.id
  tasks = [
    {
      task_key = "sobe_conversoes"
      path     = "3_analytics/to_postgres/salva_conversoes_postgres"
    }
  ]
}
