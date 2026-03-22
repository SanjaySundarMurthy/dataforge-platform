# ============================================================
# DataForge Platform - Terraform Environment: Dev
# ============================================================

project_name = "dataforge"
environment  = "dev"
location     = "eastus2"

# Networking
vnet_address_space = ["10.0.0.0/16"]

# AKS
aks_node_count  = 2
aks_node_vm_size = "Standard_D2s_v3"
