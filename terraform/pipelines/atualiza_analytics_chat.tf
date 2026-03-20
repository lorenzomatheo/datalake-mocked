data "databricks_node_type" "atualiza_analytics_chat" {
  local_disk    = true
  min_memory_gb = 64
  min_cores     = 16
  category      = "Memory Optimized"
}

module "sobe_analise_chat_s3" {
  source          = "./pipeline"
  name            = "atualiza_analytics_chat"
  schedule        = "0 0 8 ? * MON"
  notify          = true
  timeout_seconds = 3600
  parameters      = { "stage" = "prod", "environment" = "production" }
  driver_node_id  = data.databricks_node_type.atualiza_analytics_chat.id
  worker_node_id  = data.databricks_node_type.atualiza_analytics_chat.id

  tasks = [
    {
      task_key = "sobe_analise_chat_s3"
      path     = "3_analytics/powerbi/sobe_analise_chat_s3"
      base_parameters = {
        debug       = "false"
        stage       = "prod"
        environment = "production"
      }
    }
  ]
}
