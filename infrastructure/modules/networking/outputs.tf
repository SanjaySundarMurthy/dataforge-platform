output "vnet_id" { value = azurerm_virtual_network.main.id }
output "vnet_name" { value = azurerm_virtual_network.main.name }
output "aks_subnet_id" { value = azurerm_subnet.aks.id }
output "data_subnet_id" { value = azurerm_subnet.data.id }
output "private_endpoints_subnet_id" { value = azurerm_subnet.private_endpoints.id }
output "databricks_private_subnet_name" { value = azurerm_subnet.databricks_private.name }
output "databricks_public_subnet_name" { value = azurerm_subnet.databricks_public.name }
output "databricks_nsg_id" { value = azurerm_network_security_group.databricks.id }
