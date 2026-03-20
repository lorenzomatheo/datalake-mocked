module "atualiza_produtos_anvisa_prod" {
  source     = "./pipeline"
  name       = "atualiza_produtos_anvisa_prod"
  schedule   = "0 0 7 ? * 4#2"
  notify     = true
  parameters = { "environment" = "production" }
  tasks = [
    {
      task_key = "processa_intercambiaveis_anvisa"
      path     = "2_standard_2_refined/process_intercambiaveis_anvisa"
    },
    {
      task_key   = "cria_sync_no_copilot"
      depends_on = ["processa_intercambiaveis_anvisa"]
      path       = "3_analytics/to_postgres/update_maggu_poc_intercambiaveis"
    }
  ]
}

module "atualiza_produtos_anvisa_staging" {
  source     = "./pipeline"
  name       = "atualiza_produtos_anvisa_staging"
  parameters = { "environment" = "staging" }
  tasks = [
    {
      task_key = "processa_intercambiaveis_dev"
      path     = "2_standard_2_refined/process_intercambiaveis_anvisa"
    },
    {
      task_key   = "cria_sync_pro_copilot"
      depends_on = ["processa_intercambiaveis_dev"]
      path       = "3_analytics/to_postgres/update_maggu_poc_intercambiaveis"
    }
  ]
}
