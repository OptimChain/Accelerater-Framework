# Configure KeyVault

resource "azurerm_key_vault_secret" "ADLSName" {
  name         = "ADLSName"
  value        = var.ADLSName
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "ADLSStorageAccountKey" {
  name         = "ADLSStorageAccountKey"
  value        = var.ADLSStorageAccountKey
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "ADLSTenantId" {
  name         = "ADLSTenantId"
  value        = var.ADLSTenantId
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLFrameworkConnectionString" {
  name         = "SQLFrameworkConnectionString"
  value        = var.SQLFrameworkConnectionString
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLFrameworkServerName" {
  name         = "SQLFrameworkServerName"
  value        = var.SQLFrameworkServerName
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLFrameworkDatabaseName" {
  name         = "SQLFrameworkDatabaseName"
  value        = var.SQLFrameworkDatabaseName
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLFrameworkPassword" {
  name         = "SQLFrameworkPassword"
  value        = var.SQLFrameworkPassword
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLFrameworkUserName" {
  name         = "SQLFrameworkUserName"
  value        = var.SQLFrameworkUserName
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLDWServerName" {
  name         = "SQLDWServerName"
  value        = var.SQLDWServerName
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLDWDatabaseName" {
  name         = "SQLDWDatabaseName"
  value        = var.SQLDWDatabaseName
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLDWPassword" {
  name         = "SQLDWPassword"
  value        = var.SQLDWPassword
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "SQLDWUserName" {
  name         = "SQLDWUserName"
  value        = var.SQLDWUserName
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "DatabricksFrameworkAdminToken" {
  name         = "DatabricksFrameworkAdminToken"
  value        = var.DatabricksFrameworkAdminToken
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "DatabricksDataAdminToken" {
  name         = "DatabricksDataAdminToken"
  value        = var.DatabricksDataAdminToken
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "AdminADGroupID" {
  name         = "AdminADGroupID"
  value        = var.AdminADGroupID
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "ContributorADGroupID" {
  name         = "ContributorADGroupID"
  value        = var.ContributorADGroupID
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "PublisherADGroupID" {
  name         = "PublisherADGroupID"
  value        = var.PublisherADGroupID
  key_vault_id = azurerm_key_vault.this.id
}

resource "azurerm_key_vault_secret" "ReaderADGroupID" {
  name         = "ReaderADGroupID"
  value        = var.ReaderADGroupID
  key_vault_id = azurerm_key_vault.this.id
}