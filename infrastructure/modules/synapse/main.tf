# ============================================================
# Module: Synapse Analytics (Data Warehouse)
# ============================================================

resource "azurerm_synapse_workspace" "main" {
  name                                 = "syn-${var.project_name}-${var.environment}-${var.suffix}"
  resource_group_name                  = var.resource_group_name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = "${var.data_lake_id}/blobServices/default/containers/gold"
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = var.sql_admin_password

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
  storage_account_type = "LRS"

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

  tags = var.tags
}

# ── Firewall Rule (allow Azure services) ─────────────────────
resource "azurerm_synapse_firewall_rule" "allow_azure" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}
