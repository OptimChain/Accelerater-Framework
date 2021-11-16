variable "location" {
  type        = string
  description = "(Optional) The location for resource deployment"
  default     = "westus2"
}

variable "environment" {
  type        = string
  description = "(Required) Three character environment name"

  validation {
    condition     = length(var.environment) <= 3
    error_message = "Err: Environment cannot be longer than three characters."
  }
}

variable "project" {
  type        = string
  description = "(Required) The project name"
}

// variable "databricks_sku" {
//   type        = string
//   description = <<EOT
//     (Optional) The SKU to use for the databricks instance"
//     Default: standard
// EOT

//   validation {
//     condition     = can(regex("standard|premium|trial", var.databricks_sku))
//     error_message = "Err: Valid options are 'standard', 'premium' or 'trial'."
//   }
// }

variable "databricks_sku" {
  type        = string
  description = "(Required) The project name"
}

variable "bk_name1" {
  type        = string
  description   = "Data bricks name1"
}

variable "bk_name2" {
  type        = string
  description   = "databricks name2"
}

variable "ContainerName" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "ApplicationPrefix" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "GroupPrefix" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "EnvironmentSuffix1" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "EnvironmentSuffix2" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "storageaccountname" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "account_tier" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "account_kind" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "is_hns_enabled" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "account_replication_type" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "enable_https_traffic_only" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "vnetCidr" {
  type        = string
  description   = "Cidr range for the vnet."
}

variable "privateSubnetCidr" {
  type        = string
  description   = "Cidr range for the private subnet."
}

variable "publicSubnetCidr" {
  type        = string
  description   = "Cidr range for the public subnet.."
}

variable "controlPlaneIp" {
  type        = string
  description   = "Cidr range for the public subnet.."
}

variable "webappIp" {
  type        = string
  description   = "Cidr range for the public subnet.."
}

variable "ADLSName" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "ADLSStorageAccountKey" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "ADLSTenantId" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLFrameworkConnectionString" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLFrameworkServerName" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLFrameworkDatabaseName" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLFrameworkPassword" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLFrameworkUserName" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLDWServerName" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLDWDatabaseName" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLDWPassword" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "SQLDWUserName" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "DatabricksFrameworkAdminToken" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "DatabricksDataAdminToken" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

// variable "DatabricksDataAdminToken" {
//   type        = string
//   description   = "Group Prefix for Azure AD and Groups."
// }

variable "AdminADGroupID" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "ContributorADGroupID" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "PublisherADGroupID" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "ReaderADGroupID" {
  type        = string
  description   = "Group Prefix for Azure AD and Groups."
}

variable "dbsname" {
  type = string
  description   = "dbs name"
}

variable "dwname" {
  type = string
  description   = "dw name"
}

variable "dbname" {
  type = string
  description   = "db name"
}

variable "transparentDataEncryption" {
  type = string
}

variable "vulnerabilityAssessments" {
  type = string
}

variable "dw_database" {
  type = string
}