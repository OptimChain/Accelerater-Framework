// data "azurerm_resource_group" "this" {
//   name     = var.rg_name
// }

resource "azurerm_storage_account" "datalake" {
  name                     = var.storageaccountname
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = var.account_tier
  account_replication_type = var.account_replication_type
  enable_https_traffic_only = var.enable_https_traffic_only
  account_kind             = var.account_kind
  is_hns_enabled           = var.is_hns_enabled
  tags = {
        "Group Name" = "Data Analytics"
        "Budget Code" = ""
        "Project ID" = ""
        "Deployed By" = "var.deployedby"
        "SL" = "1 High Priority"
        "PL" = "1 Confidential"
        "Internal Owner" = ""
        "Support Contact" = ""
  }
}
