module "regua_atendente_subiu_nivel" {
  source          = "../pipeline"
  name            = "regua_atendente_subiu_nivel"
  description     = "Envia mensagens de notificando evolução de nível para atendentes"
  schedule        = "0 0 9-17 * * ?" # a cada hora entre 9h e 17h, todos os dias
  notify          = true
  timeout_seconds = 3600 # 1 hora
  tasks = [
    {
      task_key = "subiu_nivel"
      path     = "ativacao/hyperflow/reguas/atendente/subiu_nivel"
      base_parameters = {
        somente_testes = "False"
        debug          = "False"
        environment    = "production"
      }
      email_notifications = {
        on_failure                = ["reslley@maggu.ai"]
        no_alert_for_skipped_runs = true
      }
    }
  ]
}
