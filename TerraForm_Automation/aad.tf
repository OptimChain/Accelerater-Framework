resource "random_password" "service_principal" {
  length  = 16
  special = true
}

data "azuread_client_config" "current" {}

resource "azuread_application" "admindisplayname" {
  display_name = "${var.ApplicationPrefix}-${var.ContainerName}-admin"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "admindisplayname" {
  application_id               = azuread_application.admindisplayname.application_id
}

resource "azuread_application" "contributedisplayname" {
  display_name = "${var.ApplicationPrefix}-${var.ContainerName}-contribute"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_application" "publishdisplayname" {
  display_name = "${var.ApplicationPrefix}-${var.ContainerName}-publish"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_application" "readdisplayname" {
  display_name = "${var.ApplicationPrefix}-${var.ContainerName}-read"
  owners       = [data.azuread_client_config.current.object_id]
}

// resource "azuread_service_principal" "admindisplayname" {
//   application_id               = azuread_application.admindisplayname.application_id
// }

resource "azuread_service_principal" "contributedisplayname" {
  application_id               = azuread_application.contributedisplayname.application_id
}

resource "azuread_service_principal" "publishdisplayname" {
  application_id               = azuread_application.publishdisplayname.application_id
}

resource "azuread_service_principal" "readdisplayname" {
  application_id               = azuread_application.readdisplayname.application_id
}

resource "azuread_group" "ad_group_admin" {
  display_name = "${var.GroupPrefix}-${var.ContainerName}-Admin"
}

resource "azuread_group" "ad_group_contribute" {
  display_name = "${var.GroupPrefix}-${var.ContainerName}-Contribute"
}

resource "azuread_group" "ad_group_publish" {
  display_name = "${var.GroupPrefix}-${var.ContainerName}-Publish"
}

resource "azuread_group" "ad_group_read" {
  display_name = "${var.GroupPrefix}-${var.ContainerName}-Read'"
}

// resource "azurerm_role_assignment" "ad_group_admin" {
//   scope                = azurerm_storage_account.datalake.id
//   role_definition_name = "Storage Blob Data Contributor"
//   principal_id         = azuread_application.admindisplayname.application_id
// }

// resource "azurerm_role_assignment" "ad_group_contribute" {
//   scope                = azurerm_storage_account.datalake.id
//   role_definition_name = "Storage Blob Data Contributor"
//   principal_id         = azuread_application.contributedisplayname.application_id
// }

// resource "azurerm_role_assignment" "ad_group_publish" {
//   scope                = azurerm_storage_account.datalake.id
//   role_definition_name = "Storage Blob Data Contributor"
//   principal_id         = azuread_application.publishdisplayname.application_id
// }


