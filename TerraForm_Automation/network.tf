resource "azurerm_virtual_network" "vNetbk1" {
  name = format("vnet1-%s-%s-%s-%s",
  local.naming.location[var.location], var.environment, var.bk_name1,var.project)

  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name

  address_space = [var.vnetCidr]
}

resource "azurerm_virtual_network" "vNetbk2" {
  name = format("vnet1-%s-%s-%s-%s",
  local.naming.location[var.location], var.environment, var.bk_name2, var.project)

  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name

  address_space = [var.vnetCidr]
}

resource "azurerm_subnet" "private1" {
  name = format("sn-%s-%s-%s-%s-priv",
  local.naming.location[var.location], var.environment, var.bk_name1, var.project)

  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.vNetbk1.name
  address_prefixes     = [var.privateSubnetCidr]

  delegation {
    name = "databricks-delegation"

    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "public1" {
  name = format("sn-%s-%s-%s-%s-pub",
  local.naming.location[var.location], var.environment, var.bk_name1, var.project)

  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.vNetbk1.name
  address_prefixes     = [var.publicSubnetCidr]

  delegation {
    name = "databricks-delegation"

    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "private2" {
  name = format("sn-%s-%s-%s-%s-priv",
  local.naming.location[var.location], var.environment, var.bk_name2, var.project)

  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.vNetbk2.name
  address_prefixes     = [var.privateSubnetCidr]

  delegation {
    name = "databricks-delegation"

    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "public2" {
  name = format("sn-%s-%s-%s-%s-pub",
  local.naming.location[var.location], var.environment, var.bk_name2, var.project)

  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.vNetbk2.name
  address_prefixes     = [var.publicSubnetCidr]

  delegation {
    name = "databricks-delegation"

    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

locals { 
nsgrules = {

    worker-to-worker = {
      name                       = "databricks-worker-to-worker-inbound"
      priority                   = 200
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "*"
      source_port_range          = "*"
      destination_port_range     = "*"
      source_address_prefix      = "VirtualNetwork"
      destination_address_prefix = "*"
    }
    control-plane-ssh = {
      name                       = "databricks-control-plane-ssh"
      priority                   = 106
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "*"
      source_port_range          = "*"
      destination_port_range     = "22"
      source_address_prefix      = var.controlPlaneIp
      destination_address_prefix = "*"
    }
    databricks-control-plane-worker-proxy = {
      name                       = "databricks-control-plane-worker-proxy"
      priority                   = 110
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "*"
      source_port_range          = "*"
      destination_port_range     = "5557"
      source_address_prefix      = var.controlPlaneIp
      destination_address_prefix = "*"
    }
    databricks-worker-to-webapp = {
      name                       = "databricks-worker-to-webapp"
      priority                   = 105
      direction                  = "Outbound"
      access                     = "Allow"
      protocol                   = "*"
      source_port_range          = "*"
      destination_port_range     = "*"
      source_address_prefix      = var.webappIp
      destination_address_prefix = "*"
    }
    databricks-worker-to-sql = {
      name                       = "databricks-worker-to-sql"
      priority                   = 110
      direction                  = "Outbound"
      access                     = "Allow"
      protocol                   = "*"
      source_port_range          = "*"
      destination_port_range     = "*"
      source_address_prefix      = "*"
      destination_address_prefix = "sql"
    }
    databricks-worker-to-storage = {
      name                       = "databricks-worker-to-storage"
      priority                   = 120
      direction                  = "Outbound"
      access                     = "Allow"
      protocol                   = "*"
      source_port_range          = "*"
      destination_port_range     = "*"
      source_address_prefix      = "*"
      destination_address_prefix = "Storage"
    }
    databricks-worker-to-worker-outbound = {
      name                       = "databricks-worker-to-worker-outbound"
      priority                   = 130
      direction                  = "Outbound"
      access                     = "Allow"
      protocol                   = "*"
      source_port_range          = "*"
      destination_port_range     = "*"
      source_address_prefix      = "*"
      destination_address_prefix = "VirtualNetwork"
    }
    databricks-worker-to-worker-outbound = {
      name                       = "databricks-worker-to-any"
      priority                   = 140
      direction                  = "Outbound"
      access                     = "Allow"
      protocol                   = "*"
      source_port_range          = "*"
      destination_port_range     = "*"
      source_address_prefix      = "*"
      destination_address_prefix = "VirtualNetwork"
    }
}
}

resource "azurerm_network_security_group" "vnet1" {
  name = format("nsg-%s-%s-%s-%s-vnet1",
  local.naming.location[var.location], var.environment, var.bk_name1, var.project)

  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
}

resource "azurerm_network_security_group" "vnet2" {
  name = format("nsg-%s-%s-%s-%s-vnet2",
  local.naming.location[var.location], var.environment, var.bk_name2, var.project)

  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
}

resource "azurerm_subnet_network_security_group_association" "private1" {
  subnet_id                 = azurerm_subnet.private1.id
  network_security_group_id = azurerm_network_security_group.vnet1.id
}

resource "azurerm_subnet_network_security_group_association" "public1" {
  subnet_id                 = azurerm_subnet.public1.id
  network_security_group_id = azurerm_network_security_group.vnet1.id
}

resource "azurerm_subnet_network_security_group_association" "private2" {
  subnet_id                 = azurerm_subnet.private2.id
  network_security_group_id = azurerm_network_security_group.vnet2.id
}

resource "azurerm_subnet_network_security_group_association" "public2" {
  subnet_id                 = azurerm_subnet.public2.id
  network_security_group_id = azurerm_network_security_group.vnet2.id
}