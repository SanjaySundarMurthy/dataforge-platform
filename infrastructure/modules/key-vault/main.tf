# ============================================================
# Module: Key Vault
# ============================================================

resource "azurerm_key_vault" "main" {
  name                       = "kv-${var.project_name}-${var.environment}-${var.suffix}"
  location                   = var.location
  resource_group_name        = var.resource_group_name
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = true
  enable_rbac_authorization  = true

  network_acls {
    bypass                     = "AzureServices"
    default_action             = "Deny"
    virtual_network_subnet_ids = [var.subnet_id]
  }

  tags = var.tags
}

# ── RBAC: Current user as Key Vault Admin ────────────────────
resource "azurerm_role_assignment" "kv_admin" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = var.object_id
}
