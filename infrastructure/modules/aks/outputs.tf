output "cluster_id" { value = azurerm_kubernetes_cluster.main.id }
output "cluster_name" { value = azurerm_kubernetes_cluster.main.name }
output "cluster_fqdn" { value = azurerm_kubernetes_cluster.main.fqdn }
output "kube_config" {
  value     = azurerm_kubernetes_cluster.main.kube_config_raw
  sensitive = true
}
output "kubelet_identity_object_id" {
  value = azurerm_kubernetes_cluster.main.kubelet_identity[0].object_id
}
output "node_resource_group" { value = azurerm_kubernetes_cluster.main.node_resource_group }
output "oidc_issuer_url" { value = azurerm_kubernetes_cluster.main.oidc_issuer_url }
output "key_vault_secrets_provider_identity" {
  value = try(azurerm_kubernetes_cluster.main.key_vault_secrets_provider[0].secret_identity[0].object_id, null)
}
