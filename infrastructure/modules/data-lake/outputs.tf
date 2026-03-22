output "storage_account_id" { value = azurerm_storage_account.data_lake.id }
output "storage_account_name" { value = azurerm_storage_account.data_lake.name }
output "primary_dfs_endpoint" { value = azurerm_storage_account.data_lake.primary_dfs_endpoint }
output "primary_access_key" {
  value     = azurerm_storage_account.data_lake.primary_access_key
  sensitive = true
}
output "gold_filesystem_id" { value = azurerm_storage_data_lake_gen2_filesystem.gold.id }
