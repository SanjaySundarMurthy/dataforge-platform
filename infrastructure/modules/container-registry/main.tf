# ============================================================
# Module: Container Registry
# ============================================================

resource "azurerm_container_registry" "main" {
  name                          = "acr${var.project_name}${var.environment}${var.suffix}"
  resource_group_name           = var.resource_group_name
  location                      = var.location
  sku                           = "Premium"
  admin_enabled                 = false
  public_network_access_enabled = true
  zone_redundancy_enabled       = true

  retention_policy {
    days    = 30
    enabled = true
  }

  tags = var.tags
}

# ── AKS → ACR Pull Permission ───────────────────────────────
resource "azurerm_role_assignment" "aks_acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = var.aks_principal_id
}
