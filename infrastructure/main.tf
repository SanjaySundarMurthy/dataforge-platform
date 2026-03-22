# ============================================================
# DataForge Platform - Root Terraform Configuration
# ============================================================
# This is the main entry point for deploying all Azure
# infrastructure required by the DataForge platform.
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-dataforge-tfstate"
    storage_account_name = "stadataforgetfstate"
    container_name       = "tfstate"
    key                  = "dataforge.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

provider "azuread" {}

# ── Data Sources ─────────────────────────────────────────────
data "azurerm_client_config" "current" {}

# ── Random Suffix ────────────────────────────────────────────
resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

# ── Resource Group ───────────────────────────────────────────
resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location

  tags = local.common_tags
}

# ── Locals ───────────────────────────────────────────────────
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Repository  = "dataforge-platform"
  }
  suffix = random_string.suffix.result
}

# ════════════════════════════════════════════════════════════
# Module: Networking
# ════════════════════════════════════════════════════════════
module "networking" {
  source = "./modules/networking"

  project_name        = var.project_name
  environment         = var.environment
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = var.vnet_address_space
  tags                = local.common_tags
}

# ════════════════════════════════════════════════════════════
# Module: Key Vault
# ════════════════════════════════════════════════════════════
module "key_vault" {
  source = "./modules/key-vault"

  project_name        = var.project_name
  environment         = var.environment
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  suffix              = local.suffix
  tenant_id           = data.azurerm_client_config.current.tenant_id
  object_id           = data.azurerm_client_config.current.object_id
  subnet_id           = module.networking.private_endpoints_subnet_id
  tags                = local.common_tags
}

# ════════════════════════════════════════════════════════════
# Module: Data Lake (ADLS Gen2)
# ════════════════════════════════════════════════════════════
module "data_lake" {
  source = "./modules/data-lake"

  project_name        = var.project_name
  environment         = var.environment
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  suffix              = local.suffix
  subnet_id           = module.networking.data_subnet_id
  tags                = local.common_tags
}

# ════════════════════════════════════════════════════════════
# Module: Data Factory
# ════════════════════════════════════════════════════════════
module "data_factory" {
  source = "./modules/data-factory"

  project_name        = var.project_name
  environment         = var.environment
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = module.networking.data_subnet_id
  key_vault_id        = module.key_vault.key_vault_id
  data_lake_id        = module.data_lake.storage_account_id
  tags                = local.common_tags
}

# ════════════════════════════════════════════════════════════
# Module: Databricks
# ════════════════════════════════════════════════════════════
module "databricks" {
  source = "./modules/databricks"

  project_name         = var.project_name
  environment          = var.environment
  location             = azurerm_resource_group.main.location
  resource_group_name  = azurerm_resource_group.main.name
  vnet_id              = module.networking.vnet_id
  private_subnet_name  = module.networking.databricks_private_subnet_name
  public_subnet_name   = module.networking.databricks_public_subnet_name
  nsg_id               = module.networking.databricks_nsg_id
  tags                 = local.common_tags
}

# ════════════════════════════════════════════════════════════
# Module: Synapse Analytics (Data Warehouse)
# ════════════════════════════════════════════════════════════
module "synapse" {
  source = "./modules/synapse"

  project_name               = var.project_name
  environment                = var.environment
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  suffix                     = local.suffix
  data_lake_id               = module.data_lake.storage_account_id
  gold_filesystem_id         = module.data_lake.gold_filesystem_id
  sql_admin_password         = var.synapse_sql_admin_password
  log_analytics_workspace_id = module.monitoring.log_analytics_workspace_id
  tags                       = local.common_tags
}

# ════════════════════════════════════════════════════════════
# Module: AKS Cluster
# ════════════════════════════════════════════════════════════
module "aks" {
  source = "./modules/aks"

  project_name               = var.project_name
  environment                = var.environment
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  subnet_id                  = module.networking.aks_subnet_id
  node_count                 = var.aks_node_count
  node_vm_size               = var.aks_node_vm_size
  log_analytics_workspace_id = module.monitoring.log_analytics_workspace_id
  tags                       = local.common_tags
}

# ════════════════════════════════════════════════════════════
# Module: Container Registry
# ════════════════════════════════════════════════════════════
module "container_registry" {
  source = "./modules/container-registry"

  project_name        = var.project_name
  environment         = var.environment
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  suffix              = local.suffix
  aks_principal_id    = module.aks.kubelet_identity_object_id
  tags                = local.common_tags
}

# ════════════════════════════════════════════════════════════
# Module: Monitoring
# ════════════════════════════════════════════════════════════
module "monitoring" {
  source = "./modules/monitoring"

  project_name        = var.project_name
  environment         = var.environment
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.common_tags
}
