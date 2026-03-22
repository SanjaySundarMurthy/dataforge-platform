variable "project_name" { type = string }
variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "vnet_id" { type = string }
variable "private_subnet_name" { type = string }
variable "public_subnet_name" { type = string }
variable "private_nsg_association_id" {
  description = "NSG association ID for Databricks private subnet"
  type        = string
}
variable "public_nsg_association_id" {
  description = "NSG association ID for Databricks public subnet"
  type        = string
}
variable "log_analytics_workspace_id" {
  type    = string
  default = null
}
variable "tags" { type = map(string) }
