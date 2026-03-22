output "workspace_name" { value = azurerm_synapse_workspace.main.name }
output "workspace_id" { value = azurerm_synapse_workspace.main.id }
output "sql_endpoint" { value = azurerm_synapse_workspace.main.connectivity_endpoints["sql"] }
output "dev_endpoint" { value = azurerm_synapse_workspace.main.connectivity_endpoints["dev"] }
output "identity_principal_id" { value = azurerm_synapse_workspace.main.identity[0].principal_id }
output "sql_pool_id" { value = azurerm_synapse_sql_pool.warehouse.id }
output "spark_pool_id" { value = azurerm_synapse_spark_pool.etl.id }
