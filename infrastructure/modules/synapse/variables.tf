variable "project_name" { type = string }
variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "suffix" { type = string }
variable "data_lake_id" { type = string }
variable "data_lake_url" { type = string }
variable "subnet_id" { type = string }
variable "sql_admin_password" {
  type      = string
  sensitive = true
}
variable "tags" { type = map(string) }
