variable "project_name" { type = string }
variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "alert_email" {
  description = "Email address for alert notifications"
  type        = string
  default     = ""
}
variable "retention_in_days" {
  description = "Log Analytics retention in days"
  type        = number
  default     = 90
}
variable "tags" { type = map(string) }
