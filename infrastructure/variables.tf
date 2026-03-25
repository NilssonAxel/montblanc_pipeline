variable "databricks_host" {
  description = "The Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "A Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "environments" {
  description = "The environments to provision schemas for"
  type        = list(string)
  default     = ["dev", "test", "prod"]
}

variable "create_monitors" {
  description = "Set to true after the first pipeline run to enable Lakehouse Monitoring"
  type        = bool
  default     = false
}
