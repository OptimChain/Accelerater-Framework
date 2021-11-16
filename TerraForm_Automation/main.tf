locals {
  naming = {
    location = {
      "westus2" = "wus"
    }
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "this" {
  name = format("rg-%s-%s-%s",
  local.naming.location[var.location], var.environment, var.project)

  location = var.location
}

resource "azurerm_key_vault" "this" {
  name = format("kv-%s-%s-%s",
  local.naming.location[var.location], var.environment, var.project)

  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  enabled_for_disk_encryption = false
  enable_rbac_authorization   = false
  soft_delete_enabled         = true
  soft_delete_retention_days  = 90
  purge_protection_enabled    = false

  sku_name = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get",
    ]

    secret_permissions = [
      "Get",
      "Set",
      "Delete",
      "Recover",
      "Purge"
    ]
    storage_permissions = [
      "Get",
    ]
  }
  network_acls {
  default_action = "Allow"
  bypass         = "AzureServices"
  ip_rules = []
  }
}

resource "azurerm_databricks_workspace" "databricks1" {
  name = format("dbs-%s-%s-%s-%s",
  local.naming.location[var.location], var.environment, var.bk_name1, var.project)

  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = var.databricks_sku
  managed_resource_group_name = format("databricks-%s-%s-%s-%s-managedgroup",
  local.naming.location[var.location], var.environment, var.bk_name1, var.project)
  custom_parameters {
    virtual_network_id  = azurerm_virtual_network.vNetbk1.id
    public_subnet_name  = azurerm_subnet.public1.name
    private_subnet_name = azurerm_subnet.private1.name
    public_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.public1.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private1.id
  }

  depends_on = [
    azurerm_subnet_network_security_group_association.public1,
    azurerm_subnet_network_security_group_association.private1,
  ]
  tags = {
        environment = var.environment
        "Group Name" = "Data and Analytics"
        "Budget Code" = ""
        "Project ID" = ""
        "Deployed By" = ""
        "SL" = "1 High Priority"
        "Privacy Level" = "1 Confidential"
        "Internal Owner" = ""
        "Support Contact" = ""
  }
}

resource "azurerm_databricks_workspace" "databricks2" {
  name = format("dbs-%s-%s-%s-%s-managedgroup",
  local.naming.location[var.location], var.environment, var.bk_name2, var.project)

  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = var.databricks_sku
  managed_resource_group_name = format("databricks-%s-%s-%s-%s-managedgroup",
  local.naming.location[var.location], var.environment, var.bk_name2, var.project)
  custom_parameters {
    virtual_network_id  = azurerm_virtual_network.vNetbk2.id
    public_subnet_name  = azurerm_subnet.public2.name
    private_subnet_name = azurerm_subnet.private2.name
    public_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.public2.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private2.id
  }

  depends_on = [
    azurerm_subnet_network_security_group_association.public2,
    azurerm_subnet_network_security_group_association.private2,
  ]
  tags = {
        environment = var.environment
        "Group Name" = "Data and Analytics"
        "Budget Code" = ""
        "Project ID" = ""
        "Deployed By" = ""
        "SL" = "1 High Priority"
        "Privacy Level" = "1 Confidential"
        "Internal Owner" = ""
        "Support Contact" = ""
  }
}

