locals {
  job_cluster_key      = "cluster_do_pipeline_${var.name}"
  cluster_availability = var.name == "atualiza_produtos_producao" || var.name == "enriquece_produtos" ? "ON_DEMAND" : "SPOT_WITH_FALLBACK"
  tasks_map = {
    for task in var.tasks :
    task.task_key => task
  }
  sorted_tasks_keys = sort(keys(local.tasks_map))
  sorted_tasks      = [for task_key in local.sorted_tasks_keys : local.tasks_map[task_key]]
}

data "databricks_current_user" "me" {}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  spark_version     = "3.5"
  scala             = "2.12"
}

data "databricks_node_type" "driver" {
  local_disk    = true
  min_memory_gb = 8
  min_cores     = 4
}

data "databricks_node_type" "worker" {
  local_disk    = true
  min_memory_gb = 4
  min_cores     = 4
}

resource "databricks_job" "this" {
  name            = var.name
  description     = var.description
  timeout_seconds = var.timeout_seconds

  dynamic "parameter" {
    for_each = var.parameters
    content {
      name    = parameter.key
      default = parameter.value
    }
  }

  dynamic "schedule" {
    for_each = var.schedule != null ? ["apply"] : []
    content {
      quartz_cron_expression = var.schedule
      timezone_id            = "America/Sao_Paulo"
    }
  }

  dynamic "email_notifications" {
    for_each = var.notify ? ["apply"] : []
    content {
      on_failure = ["reslley@maggu.ai"]
    }
  }

  dynamic "webhook_notifications" {
    for_each = var.name == "atualiza_produtos_producao" ? ["apply"] : []
    content {
      on_success {
        id = "28e376cc-97e3-43c0-b139-39429be0836f"
      }
    }
  }

  job_cluster {
    job_cluster_key = local.job_cluster_key
    new_cluster {
      kind                = "CLASSIC_PREVIEW"
      spark_version       = data.databricks_spark_version.latest_lts.id
      driver_node_type_id = coalesce(var.driver_node_id, data.databricks_node_type.driver.id)
      node_type_id        = coalesce(var.worker_node_id, data.databricks_node_type.worker.id)
      enable_elastic_disk = true

      custom_tags = {
        billing_group = "datalake"
      }

      aws_attributes {
        instance_profile_arn   = "arn:aws:iam::727927907560:instance-profile/maggu-datalake"
        availability           = local.cluster_availability
        spot_bid_price_percent = local.cluster_availability == "SPOT_WITH_FALLBACK" ? 100 : null
        first_on_demand        = 1
      }

      autoscale {
        min_workers = var.min_num_workers
        max_workers = var.max_num_workers
      }

      docker_image {
        url = "ghcr.io/maggu-ai/datalake:standard"
        basic_auth {
          username = "{{secrets/github/username}}"
          password = "{{secrets/github/password}}"
        }
      }
    }
  }

  git_source {
    url      = "https://github.com/maggu-ai/datalake.git"
    provider = "gitHub"
    branch   = var.branch
  }

  dynamic "task" {
    for_each = local.sorted_tasks
    content {
      task_key        = task.value.task_key
      job_cluster_key = local.job_cluster_key

      dynamic "depends_on" {
        for_each = toset(task.value.depends_on)
        content {
          task_key = depends_on.value
        }
      }

      notebook_task {
        source          = var.use_local_workspace ? "WORKSPACE" : "GIT"
        notebook_path   = var.use_local_workspace ? format("%s/%s", var.workspace_path, task.value.path) : task.value.path
        base_parameters = task.value.base_parameters
      }

      dynamic "library" {
        for_each = var.maven_libraries
        content {
          maven {
            coordinates = library.value
          }
        }
      }
    }
  }
}
