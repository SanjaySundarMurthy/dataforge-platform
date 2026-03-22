# ============================================================
# Module: Azure Data Factory
# ============================================================

resource "azurerm_data_factory" "main" {
  name                = "adf-${var.project_name}-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name

  identity {
    type = "SystemAssigned"
  }

  managed_virtual_network_enabled = true

  global_parameter {
    name  = "environment"
    type  = "String"
    value = var.environment
  }

  global_parameter {
    name  = "project_name"
    type  = "String"
    value = var.project_name
  }

  tags = var.tags
}

# ── Managed Private Endpoint to Data Lake ────────────────────
resource "azurerm_data_factory_managed_private_endpoint" "data_lake" {
  name               = "mpe-datalake"
  data_factory_id    = azurerm_data_factory.main.id
  target_resource_id = var.data_lake_id
  subresource_name   = "dfs"
}

# ── Managed Private Endpoint to Key Vault ────────────────────
resource "azurerm_data_factory_managed_private_endpoint" "key_vault" {
  name               = "mpe-keyvault"
  data_factory_id    = azurerm_data_factory.main.id
  target_resource_id = var.key_vault_id
  subresource_name   = "vault"
}

# ── Linked Service: Data Lake ────────────────────────────────
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "data_lake" {
  name                 = "ls_adls_datalake"
  data_factory_id      = azurerm_data_factory.main.id
  use_managed_identity = true
  url                  = "https://${var.project_name}datalake.dfs.core.windows.net"
}

# ── Linked Service: Key Vault ────────────────────────────────
resource "azurerm_data_factory_linked_service_key_vault" "key_vault" {
  name            = "ls_kv_secrets"
  data_factory_id = azurerm_data_factory.main.id
  key_vault_id    = var.key_vault_id
}

# ── Integration Runtime ──────────────────────────────────────
resource "azurerm_data_factory_integration_runtime_azure" "auto_resolve" {
  name                    = "ir-auto-resolve"
  data_factory_id         = azurerm_data_factory.main.id
  location                = "AutoResolve"
  virtual_network_enabled = true
}

# ── Diagnostic Settings ──────────────────────────────────────
resource "azurerm_monitor_diagnostic_setting" "adf" {
  name               = "diag-adf"
  target_resource_id = azurerm_data_factory.main.id

  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category = "PipelineRuns"
  }
  enabled_log {
    category = "ActivityRuns"
  }
  enabled_log {
    category = "TriggerRuns"
  }

  metric {
    category = "AllMetrics"
  }

  count = var.log_analytics_workspace_id != null ? 1 : 0
}
