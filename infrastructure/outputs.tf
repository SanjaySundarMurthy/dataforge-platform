# ============================================================
# DataForge Platform - Outputs
# ============================================================

# ── Networking ───────────────────────────────────────────────
output "vnet_id" {
  description = "Virtual Network ID"
  value       = module.networking.vnet_id
}

# ── Data Lake ────────────────────────────────────────────────
output "data_lake_name" {
  description = "Data Lake Storage Account name"
  value       = module.data_lake.storage_account_name
}

output "data_lake_primary_endpoint" {
  description = "Data Lake primary DFS endpoint"
  value       = module.data_lake.primary_dfs_endpoint
}

# ── Data Factory ─────────────────────────────────────────────
output "data_factory_name" {
  description = "Azure Data Factory name"
  value       = module.data_factory.data_factory_name
}

output "data_factory_id" {
  description = "Azure Data Factory ID"
  value       = module.data_factory.data_factory_id
}

# ── Databricks ───────────────────────────────────────────────
output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks.workspace_url
}

# ── Synapse ──────────────────────────────────────────────────
output "synapse_workspace_name" {
  description = "Synapse workspace name"
  value       = module.synapse.workspace_name
}

output "synapse_sql_endpoint" {
  description = "Synapse SQL endpoint"
  value       = module.synapse.sql_endpoint
}

# ── AKS ──────────────────────────────────────────────────────
output "aks_cluster_name" {
  description = "AKS cluster name"
  value       = module.aks.cluster_name
}

output "aks_kube_config" {
  description = "AKS kubeconfig (sensitive)"
  value       = module.aks.kube_config
  sensitive   = true
}

# ── Container Registry ───────────────────────────────────────
output "acr_login_server" {
  description = "ACR login server URL"
  value       = module.container_registry.login_server
}

# ── Key Vault ────────────────────────────────────────────────
output "key_vault_uri" {
  description = "Key Vault URI"
  value       = module.key_vault.vault_uri
}

# ── Monitoring ───────────────────────────────────────────────
output "log_analytics_workspace_id" {
  description = "Log Analytics Workspace ID"
  value       = module.monitoring.log_analytics_workspace_id
}
