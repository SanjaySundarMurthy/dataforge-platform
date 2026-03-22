# ============================================================
# Module: Monitoring — Log Analytics + Application Insights
# ============================================================

resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${var.project_name}-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "PerGB2018"
  retention_in_days   = 30

  tags = var.tags
}

resource "azurerm_application_insights" "main" {
  name                = "appi-${var.project_name}-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "other"

  tags = var.tags
}

# ── Action Group for Alerts ──────────────────────────────────
resource "azurerm_monitor_action_group" "critical" {
  name                = "ag-${var.project_name}-critical"
  resource_group_name = var.resource_group_name
  short_name          = "dfcritical"

  email_receiver {
    name          = "admin"
    email_address = "admin@example.com"
  }

  tags = var.tags
}

# ── Alert: Pipeline Failure ──────────────────────────────────
resource "azurerm_monitor_metric_alert" "pipeline_failure" {
  name                = "alert-pipeline-failure-${var.environment}"
  resource_group_name = var.resource_group_name
  scopes              = [azurerm_application_insights.main.id]
  description         = "Alert when data pipeline failures exceed threshold"
  severity            = 1
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.Insights/components"
    metric_name      = "exceptions/count"
    aggregation      = "Count"
    operator         = "GreaterThan"
    threshold        = 5
  }

  action {
    action_group_id = azurerm_monitor_action_group.critical.id
  }

  tags = var.tags
}
