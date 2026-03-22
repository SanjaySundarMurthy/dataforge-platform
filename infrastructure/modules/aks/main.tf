# ============================================================
# Module: AKS Cluster
# ============================================================

resource "azurerm_kubernetes_cluster" "main" {
  name                = "aks-${var.project_name}-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  dns_prefix          = "${var.project_name}-${var.environment}"
  kubernetes_version  = "1.29"

  # ── Workload identity & OIDC ────────────────────────────────
  oidc_issuer_enabled       = true
  workload_identity_enabled = true

  # ── Auto-upgrade to latest stable patches ───────────────────
  automatic_channel_upgrade = "patch"

  default_node_pool {
    name                = "system"
    node_count          = var.node_count
    vm_size             = var.node_vm_size
    vnet_subnet_id      = var.subnet_id
    os_disk_size_gb     = 128
    max_pods            = 50
    enable_auto_scaling = true
    min_count           = 1
    max_count           = 5
    zones               = ["1", "2", "3"]

    upgrade_settings {
      max_surge = "33%"
    }
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin    = "azure"
    network_policy    = "calico"
    load_balancer_sku = "standard"
    service_cidr      = "10.1.0.0/16"
    dns_service_ip    = "10.1.0.10"
  }

  # ── OMS Agent (only when Log Analytics ID is provided) ──────
  dynamic "oms_agent" {
    for_each = var.log_analytics_workspace_id != null ? [1] : []
    content {
      log_analytics_workspace_id = var.log_analytics_workspace_id
    }
  }

  # ── Key Vault Secrets Provider ──────────────────────────────
  key_vault_secrets_provider {
    secret_rotation_enabled  = true
    secret_rotation_interval = "2m"
  }

  azure_active_directory_role_based_access_control {
    azure_rbac_enabled = true
    managed            = true
  }

  auto_scaler_profile {
    balance_similar_node_groups = true
    scale_down_delay_after_add  = "10m"
  }

  # ── Maintenance Window (Tue/Wed nights) ─────────────────────
  maintenance_window {
    allowed {
      day   = "Tuesday"
      hours = [21, 22, 23]
    }
    allowed {
      day   = "Wednesday"
      hours = [0, 1, 2, 3]
    }
  }

  tags = var.tags

  lifecycle {
    ignore_changes = [default_node_pool[0].node_count]
  }
}

# ── Worker Node Pool ─────────────────────────────────────────
resource "azurerm_kubernetes_cluster_node_pool" "worker" {
  name                  = "worker"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  vm_size               = "Standard_D8s_v3"
  node_count            = 0
  enable_auto_scaling   = true
  min_count             = 0
  max_count             = 10
  vnet_subnet_id        = var.subnet_id
  os_disk_size_gb       = 256
  max_pods              = 50
  priority              = "Spot"
  eviction_policy       = "Delete"
  spot_max_price        = -1

  node_labels = {
    "workload" = "spark"
  }

  node_taints = [
    "workload=spark:NoSchedule"
  ]

  lifecycle {
    ignore_changes = [node_count]
  }

  tags = var.tags
}
