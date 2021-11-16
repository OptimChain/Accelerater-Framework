resource "azurerm_mssql_server" "dbserver" {
  name                         = var.dbsname
  resource_group_name          = azurerm_resource_group.this.name
  location                     = azurerm_resource_group.this.location
  version                      = "12.0"
  administrator_login          = "4dm1n157r470r"
  administrator_login_password = "4-v3ry-53cr37-p455w0rd"
  tags = {
        environment = var.environment
        "Group Name" = "Data and Analytics",
        "Budget Code" = "",
        "Project ID" = "",
        "Deployed By" = "",
        "SL" = "1 High Priority",
        "Privacy Level" = "1 Confidential",
        "Internal Owner" = "",
        "Support Contact" = ""
  }
}

resource "azurerm_mssql_server_transparent_data_encryption" "dbserver" {
  server_id = azurerm_mssql_server.dbserver.id
}

resource "azurerm_mssql_database" "database" {
  name                = var.dbname
  server_id           = azurerm_mssql_server.dbserver.id
  collation           = "SQL_Latin1_General_CP1_CI_AS"
  //license_type        = "LicenseIncluded"
  read_replica_count  = 0
  max_size_gb         = 250
  read_scale          = false
  sku_name            = "S3"
  zone_redundant      = false

  short_term_retention_policy {
    retention_days = 7
  }
  threat_detection_policy {
    disabled_alerts            = []
    email_account_admins       = "Enabled"
    email_addresses            = []
  }
}

resource "azurerm_mssql_database" "dwdatabase" {
  name                = var.dwname
  server_id           = azurerm_mssql_server.dbserver.id
  collation           = "SQL_Latin1_General_CP1_CI_AS"
  license_type        = "LicenseIncluded"
  read_replica_count  = 0
  max_size_gb         = 250
  read_scale          = false
  sku_name            = "DW100c"
  zone_redundant      = false

  // short_term_retention_policy {
  //   retention_days = 7
  // }
//   threat_detection_policy {
//     disabled_alerts            = []
//     email_account_admins       = "Enabled"
//     email_addresses            = []
//   }
}