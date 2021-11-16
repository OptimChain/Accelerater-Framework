resource "azurerm_storage_data_lake_gen2_filesystem" "datalake" {
  name               = lower(var.ContainerName)
  storage_account_id = azurerm_storage_account.datalake.id

  // properties = {
  //   hello = "aGVsbG8="
  // }
}

// resource "azurerm_storage_data_lake_gen2_filesystem" "datalake" {
//   name = format("fs%s%s%s",
//   local.naming.location[var.location], var.environment, var.project)
//   storage_account_id = azurerm_storage_account.datalake.id
// }

resource "azurerm_storage_data_lake_gen2_path" "BadRecords" {
  path               = "BadRecords"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // ace {
  //   scope = "default"
  //   type = "app"
  //   id = azuread_application.admindisplayname.application_id
  //   permissions  = "rwx"
  // }
}

resource "azurerm_storage_data_lake_gen2_path" "Landing" {
  path               = "Landing"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}

resource "azurerm_storage_data_lake_gen2_path" "Raw" {
  path               = "Raw"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}

resource "azurerm_storage_data_lake_gen2_path" "Query" {
  path               = "Query"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}

resource "azurerm_storage_data_lake_gen2_path" "CurrentState" {
  path               = "Query/CurrentState"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}

resource "azurerm_storage_data_lake_gen2_path" "Enriched" {
  path               = "Query/Enriched"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}

resource "azurerm_storage_data_lake_gen2_path" "Sandbox" {
  path               = "Query/Sandbox"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}

resource "azurerm_storage_data_lake_gen2_path" "Schemas" {
  path               = "Schemas"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}

resource "azurerm_storage_data_lake_gen2_path" "Summary" {
  path               = "Summary"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}

resource "azurerm_storage_data_lake_gen2_path" "Export" {
  path               = "Summary/Export"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.datalake.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
  // depends_on = [
  //   azurerm_storage_data_lake_gen2_filesystem.datalake.name
  // ]
}