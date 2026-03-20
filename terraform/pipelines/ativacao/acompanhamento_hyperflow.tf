module "acompanhamento_hyperflow" {
  source          = "../pipeline"
  name            = "acompanhamento_hyperflow"
  description     = "Analisa cadastro de atendentes via Hyperflow e persiste tabelas Delta para dashboards"
  schedule        = "0 0 3 * * ?" # diariamente às 3h (America/Sao_Paulo)
  notify          = true
  timeout_seconds = 3600 # 1 hora
  tasks = [
    {
      task_key = "acompanhamento_hyperflow"
      path     = "ativacao/hyperflow/acompanhamento_hyperflow"
      base_parameters = {
        debug       = "false"
        environment = "production"
      }
      email_notifications = {
        on_failure                = ["reslley@maggu.ai"]
        no_alert_for_skipped_runs = true
      }
    },
    {
      task_key   = "analytics_regua_hyperflow"
      depends_on = ["acompanhamento_hyperflow"]
      path       = "ativacao/hyperflow/analytics_regua_hyperflow"
      base_parameters = {
        debug       = "false"
        environment = "production"
      }
      email_notifications = {
        on_failure                = ["reslley@maggu.ai"]
        no_alert_for_skipped_runs = true
      }
    }
  ]
}
