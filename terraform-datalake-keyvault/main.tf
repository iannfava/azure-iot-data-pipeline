terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
  skip_provider_registration = true
}

# Resource Group: ja existe, so referenciamos
data "azurerm_resource_group" "meu_rg" {
  name = "gr_blueprint_azure_curso"
}

# Precisamos do tenant/subscription atual pro Key Vault
data "azurerm_client_config" "atual" {}

# ---------- Data Lake (Storage Account Gen2 + Container) ----------
resource "azurerm_storage_account" "datalake" {
  name                     = "dltfianfava01"
  resource_group_name      = data.azurerm_resource_group.meu_rg.name
  location                 = data.azurerm_resource_group.meu_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  # Isso transforma um Storage Account comum em Data Lake Gen2
  is_hns_enabled = true
}

resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# ---------- Key Vault ----------
resource "azurerm_key_vault" "kv" {
  name                = "kv-tf-ianfava01"
  resource_group_name = data.azurerm_resource_group.meu_rg.name
  location            = data.azurerm_resource_group.meu_rg.location
  tenant_id           = data.azurerm_client_config.atual.tenant_id
  sku_name            = "standard"
}

resource "azurerm_key_vault_access_policy" "minha_conta" {
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = data.azurerm_client_config.atual.tenant_id
  object_id    = data.azurerm_client_config.atual.object_id
  secret_permissions = ["Get", "List", "Set", "Delete"]
}

# ---------- Outputs ----------
output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "datalake_primary_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}