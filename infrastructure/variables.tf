# ============================================================
# DataForge Platform - Input Variables
# ============================================================

# ── General ──────────────────────────────────────────────────
variable "project_name" {
  description = "Name of the project, used for resource naming"
  type        = string
  default     = "dataforge"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus2"
}

# ── Networking ───────────────────────────────────────────────
variable "vnet_address_space" {
  description = "Address space for the Virtual Network"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

# ── AKS ──────────────────────────────────────────────────────
variable "aks_node_count" {
  description = "Number of nodes in the AKS default node pool"
  type        = number
  default     = 3
}

variable "aks_node_vm_size" {
  description = "VM size for AKS nodes"
  type        = string
  default     = "Standard_D4s_v3"
}

# ── Synapse ──────────────────────────────────────────────────
variable "synapse_sql_admin_password" {
  description = "SQL administrator password for Synapse"
  type        = string
  sensitive   = true
}
