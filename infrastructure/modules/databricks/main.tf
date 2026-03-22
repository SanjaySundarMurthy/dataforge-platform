# ============================================================
# Module: Databricks Workspace
# ============================================================

resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-${var.project_name}-${var.environment}"
  resource_group_name         = var.resource_group_name
  location                    = var.location
  sku                         = "premium"
  managed_resource_group_name = "rg-${var.project_name}-databricks-${var.environment}"

  custom_parameters {
    no_public_ip                                         = true
    virtual_network_id                                   = var.vnet_id
    private_subnet_name                                  = var.private_subnet_name
    public_subnet_name                                   = var.public_subnet_name
    private_subnet_network_security_group_association_id  = var.private_nsg_association_id
    public_subnet_network_security_group_association_id   = var.public_nsg_association_id
  }

  tags = var.tags
}

# ── Diagnostic Settings ──────────────────────────────────────
resource "azurerm_monitor_diagnostic_setting" "databricks" {
  count                      = var.log_analytics_workspace_id != null ? 1 : 0
  name                       = "diag-${azurerm_databricks_workspace.main.name}"
  target_resource_id         = azurerm_databricks_workspace.main.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category = "dbfs"
  }
  enabled_log {
    category = "clusters"
  }
  enabled_log {
    category = "accounts"
  }
  enabled_log {
    category = "jobs"
  }
  enabled_log {
    category = "notebook"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}
