variable "name" {
  type = string
}

variable "description" {
  type    = string
  default = ""
}

variable "branch" {
  type    = string
  default = "main"
}

variable "use_local_workspace" {
  type    = bool
  default = false
}

variable "workspace_path" {
  type    = string
  default = "/Workspace/Repos/marcos@maggu.ai/datalake"
}

variable "schedule" {
  type    = string
  default = null
}

variable "notify" {
  type    = bool
  default = false
}

variable "timeout_seconds" {
  description = "Job timeout in seconds"
  type        = number
  default     = 28800 # 8 hours default
}

variable "maven_libraries" {
  type    = list(string)
  default = []
}

variable "parameters" {
  type    = map(string)
  default = {}
}

variable "tasks" {
  type = list(
    object({
      task_type       = optional(string, "notebook")
      task_key        = string
      depends_on      = optional(list(string), [])
      path            = string
      base_parameters = optional(map(string), {})
    })
  )
}

variable "driver_node_id" {
  type    = string
  default = null
}

variable "worker_node_id" {
  type    = string
  default = null
}

variable "min_num_workers" {
  description = "Minimum number of workers for the cluster."
  type        = number
  default     = 1
}

variable "max_num_workers" {
  description = "Maximum number of workers for the cluster." # Obs.: em teoria colocar 0 aqui para single node, mas fiz isso e nao deu muito certo.
  type        = number
  default     = 4
}
