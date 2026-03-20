data "databricks_node_type" "sync_customerx" {
  local_disk    = true
  min_memory_gb = 4 # deixei bem pequeno pois os notebooks não são pesados, mas pode aumentar se der erro
}

module "sync_customerx_create" {
  source          = "../pipeline"
  name            = "sync_customerx_create"
  description     = "Cria novos registros (contas/lojas/contatos) do Postgres para CustomerX"
  schedule        = "0 0 8-20 ? * *" # Every hour from 8am to 8pm daily
  notify          = true
  timeout_seconds = 3300 # 55 minutos, pra não ter 2 execuções concomitantes
  parameters = {
    "stage"             = "prod"
    "cx_environment"    = "production"
    "dry_run"           = "false"
    "environment"       = "production"
    "validacao_estrita" = "true" # Se false, ignora o erro de "cliente sem postgres_uuid"
  }
  driver_node_id = data.databricks_node_type.sync_customerx.id
  worker_node_id = data.databricks_node_type.sync_customerx.id
  tasks = [
    {
      task_key = "cria_grupos"
      path     = "ativacao/customerx/cria_grupos"
    },
    {
      task_key   = "cria_matrizes_filiais"
      depends_on = ["cria_grupos"]
      path       = "ativacao/customerx/cria_matrizes_filiais"
    },
    {
      task_key   = "associa_matrizes_filiais"
      depends_on = ["cria_matrizes_filiais"]
      path       = "ativacao/customerx/associa_matrizes_filiais"
    },
    {
      task_key   = "cria_contatos"
      depends_on = ["associa_matrizes_filiais"]
      path       = "ativacao/customerx/cria_contatos"
    },
    {
      task_key   = "atualiza_emails_contatos"
      depends_on = ["cria_contatos"]
      path       = "ativacao/customerx/atualiza_emails_contatos"
    }
  ]
}
module "sync_customerx_update" {
  source          = "../pipeline"
  name            = "sync_customerx_update"
  description     = "Atualiza registros modificados no Postgres para CustomerX"
  schedule        = "0 0 0 ? * *" # Daily: midnight
  notify          = true
  timeout_seconds = 36000 # 10 horas
  parameters = {
    "stage"             = "prod"
    "cx_environment"    = "production"
    "dry_run"           = "false"
    "environment"       = "production"
    "run_all"           = "false" # Se true, força atualizar todos os registros independente da data de atualizacao
    "validacao_estrita" = "true"  # Se false, ignora o erro de "cliente sem postgres_uuid"
  }
  driver_node_id = data.databricks_node_type.sync_customerx.id
  worker_node_id = data.databricks_node_type.sync_customerx.id
  tasks = [
    {
      task_key = "atualiza_grupos"
      path     = "ativacao/customerx/atualiza_grupos"
    },
    {
      task_key   = "atualiza_matrizes_filiais"
      depends_on = ["atualiza_grupos"]
      path       = "ativacao/customerx/atualiza_matrizes_filiais"
    },
    {
      task_key   = "atualiza_contatos"
      depends_on = ["atualiza_matrizes_filiais"]
      path       = "ativacao/customerx/atualiza_contatos"
    },
    {
      task_key   = "detecta_clientes_orfaos"
      depends_on = ["atualiza_contatos"]
      path       = "ativacao/customerx/detecta_clientes_orfaos"
    }
  ]
}
