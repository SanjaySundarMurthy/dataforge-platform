variable "project_name" { type = string }
variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "key_vault_id" { type = string }
variable "data_lake_id" { type = string }
variable "data_lake_dfs_endpoint" {
  description = "Primary DFS endpoint for the Data Lake storage account"
  type        = string
}
variable "log_analytics_workspace_id" {
  type    = string
  default = null
}
variable "tags" { type = map(string) }
