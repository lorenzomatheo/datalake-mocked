terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_group" "developers" {
  display_name = "developers"
}

data "databricks_user" "users" {
  for_each = data.databricks_group.developers.users
  user_id  = each.value
}

data "databricks_node_type" "smallest" {
  local_disk            = true
  category              = "Memory Optimized"
  min_memory_gb         = 32
  photon_driver_capable = true
  is_io_cache_enabled   = true
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  spark_version     = "3.5"
  scala             = "2.12"
}

resource "databricks_cluster" "personal_cluster" {
  for_each                = data.databricks_user.users
  cluster_name            = "${each.value.display_name}'s Default Cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = "r6id.xlarge"
  runtime_engine          = "STANDARD"
  enable_elastic_disk     = true
  autotermination_minutes = 20
  num_workers             = 0
  data_security_mode      = "SINGLE_USER"
  single_user_name        = each.value.user_name
  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*, 4]"
  }
  spark_env_vars = {
    "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
  }
  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
  no_wait = true

  docker_image {
    url = "ghcr.io/maggu-ai/datalake:standard"
    basic_auth {
      username = "{{secrets/github/username}}"
      password = "{{secrets/github/password}}"
    }
  }

  aws_attributes {
    instance_profile_arn   = "arn:aws:iam::727927907560:instance-profile/maggu-datalake"
    availability           = "SPOT_WITH_FALLBACK"
    spot_bid_price_percent = 100
    first_on_demand        = 1
    ebs_volume_count       = 0
    zone_id                = "auto"
  }

  library {
    maven {
      coordinates = "com.amazon.deequ:deequ:2.0.7-spark-3.5"
    }
  }
}

output "users" {
  value = data.databricks_user.users
}
