# ============================================================
# Module: Synapse Analytics (Data Warehouse)
# ============================================================

resource "azurerm_synapse_workspace" "main" {
  name                                 = "syn-${var.project_name}-${var.environment}-${var.suffix}"
  resource_group_name                  = var.resource_group_name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = var.gold_filesystem_id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = var.sql_admin_password

  # ── Enterprise security ─────────────────────────────────────
  managed_virtual_network_enabled      = true
  public_network_access_enabled        = false
  data_exfiltration_protection_enabled = true

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# ── Dedicated SQL Pool ──────────────────────────────────────
resource "azurerm_synapse_sql_pool" "warehouse" {
  name                 = "sqldw"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  sku_name             = "DW100c"
  create_mode          = "Default"
  storage_account_type = "GRS"
  geo_backup_policy_enabled = true

  tags = var.tags
}

# ── Spark Pool ───────────────────────────────────────────────
resource "azurerm_synapse_spark_pool" "etl" {
  name                 = "sparkpool"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"
  node_count           = 3

  auto_scale {
    min_node_count = 3
    max_node_count = 10
  }

  auto_pause {
    delay_in_minutes = 15
  }

  library_requirement {
    content  = <<-EOT
      delta-spark==2.4.0
      great-expectations==0.18.0
    EOT
    filename = "requirements.txt"
  }

  spark_config {
    content  = <<-EOT
      spark.shuffle.service.enabled true
      spark.dynamicAllocation.enabled true
    EOT
    filename = "spark-defaults.conf"
  }

  tags = var.tags
}

# ── Firewall Rule (allow Azure services) ─────────────────────
resource "azurerm_synapse_firewall_rule" "allow_azure" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

# ── Managed Private Endpoint to Data Lake ─────────────────────
resource "azurerm_synapse_managed_private_endpoint" "datalake" {
  name                 = "datalake-pe"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  target_resource_id   = var.data_lake_id
  subresource_name     = "dfs"
}

# ── Diagnostic Settings ──────────────────────────────────────
resource "azurerm_monitor_diagnostic_setting" "synapse" {
  count                      = var.log_analytics_workspace_id != null ? 1 : 0
  name                       = "diag-${azurerm_synapse_workspace.main.name}"
  target_resource_id         = azurerm_synapse_workspace.main.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category_group = "allLogs"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}
