module "desliga_model_serving_endpoints" {
  source          = "./pipeline"
  name            = "infra_desliga_model_serving_endpoints"
  schedule        = "0 0 23 * * ?" # (11 PM)
  notify          = true
  timeout_seconds = 3600 # 1 hora
  tasks = [
    {
      task_key   = "desliga_model_serving_endpoints"
      depends_on = []
      path       = "misc/update_model_serving_endpoints"
      base_parameters = {
        stage                = "production"
        enable_scale_to_zero = "true"
      }
    }
  ]
}

module "liga_model_serving_endpoints" {
  source          = "./pipeline"
  name            = "infra_liga_model_serving_endpoints"
  schedule        = "0 0 7 * * ?" # (7 AM)
  notify          = true
  timeout_seconds = 3600 # 1 hora
  tasks = [
    {
      task_key   = "liga_model_serving_endpoints"
      depends_on = []
      path       = "misc/update_model_serving_endpoints"
      base_parameters = {
        stage                = "production"
        enable_scale_to_zero = "false"
      }
    }
  ]
}
