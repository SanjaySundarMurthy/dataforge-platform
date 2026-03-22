# ============================================================
# DataForge Platform - Terraform Environment: Production
# ============================================================

project_name = "dataforge"
environment  = "prod"
location     = "eastus2"

# Networking
vnet_address_space = ["10.0.0.0/16"]

# AKS
aks_node_count  = 3
aks_node_vm_size = "Standard_D4s_v3"
