terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    aws = {
      source = "hashicorp/aws"
    }
  }

  backend "s3" {
    bucket = "terraform.maggu.ai"
    key    = "infraestrutura/terraform/databricks"
    region = "us-east-1"
  }
}

provider "databricks" {
  host = "https://dbc-25297b38-ec1c.cloud.databricks.com"
}

provider "aws" {
  region = "us-east-1"
}

data "databricks_current_user" "me" {}
data "databricks_spark_version" "latest" {
  spark_version = "3.5"
  scala         = "2.12"
}
data "databricks_node_type" "smallest" {
  local_disk = true
}

locals {
  instance_profile_arn = "arn:aws:iam::727927907560:instance-profile/maggu-datalake"
}

module "aws" {
  source = "./aws"
}

module "pipelines" {
  source = "./pipelines"
}

module "ativacao" {
  source = "./pipelines/ativacao"
}

module "clusters" {
  source = "./clusters"
}

resource "databricks_sql_endpoint" "sql_endpoint" {
  name             = "SQL Endpoint"
  cluster_size     = "2X-Small"
  max_num_clusters = 1
  enable_photon    = false
  warehouse_type   = "CLASSIC"
  auto_stop_mins   = 10

  tags {
    custom_tags {
      key   = "billing_group"
      value = "datalake"
    }
    custom_tags {
      key   = "origem"
      value = "sql_warehouse"
    }
  }
}
