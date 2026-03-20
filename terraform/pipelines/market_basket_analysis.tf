data "databricks_node_type" "market_basket_analysis" {
  local_disk    = true
  min_memory_gb = 32
  min_cores     = 8
}

module "enriquece_market_basket_analysis" {
  source          = "./pipeline"
  name            = "enriquece_market_basket_analysis"
  description     = "Geração de regras de associação de produtos e categorias"
  schedule        = "0 0 3 * * ?" # Daily at 3 AM
  notify          = true
  timeout_seconds = 7200 # 2 hours
  parameters      = { "stage" = "prod", "environment" = "production" }
  driver_node_id  = data.databricks_node_type.market_basket_analysis.id
  worker_node_id  = data.databricks_node_type.market_basket_analysis.id
  tasks = [
    {
      task_key = "market_basket_products"
      path     = "3_analytics/market_basket_analysis"
      base_parameters = {
        debug          = "false"
        min_support    = "0.01"
        min_confidence = "0.05"
        dias_analise   = "180"
        auto_ajuste    = "ligado"
      }
    },
    {
      task_key   = "market_basket_categories"
      depends_on = ["market_basket_products"]
      path       = "3_analytics/market_basket_analysis_categories"
      base_parameters = {
        debug          = "false"
        min_support    = "0.01"
        min_confidence = "0.05"
        dias_analise   = "180"
        auto_ajuste    = "ligado"
      }
    },
    {
      task_key        = "atualiza_regras_associacao_postgres"
      depends_on      = ["market_basket_products"]
      path            = "3_analytics/to_postgres/atualiza_regras_associacao"
      base_parameters = {}
    }
  ]
}
