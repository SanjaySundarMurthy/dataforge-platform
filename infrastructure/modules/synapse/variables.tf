variable "project_name" { type = string }
variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "suffix" { type = string }
variable "data_lake_id" { type = string }
variable "gold_filesystem_id" {
  description = "ADLS Gen2 filesystem resource ID for the gold container"
  type        = string
}
variable "sql_admin_password" {
  type      = string
  sensitive = true
}
variable "log_analytics_workspace_id" {
  type    = string
  default = null
}
variable "tags" { type = map(string) }
