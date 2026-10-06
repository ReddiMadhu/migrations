terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }
}

variable "resource_group_name" {
  type        = string
  description = "Resource group name for Cosmos DB"
  default     = "rg-sas2pyspark"
}

variable "location" {
  type        = string
  description = "Azure region"
  default     = "eastus"
}

variable "cosmos_account_name" {
  type        = string
  description = "Cosmos DB account name"
  default     = "sas2pyspark-cosmos"
}

variable "database_name" {
  type        = string
  description = "Cosmos DB SQL database name"
  default     = "sas2pyspark"
}

resource "azurerm_cosmosdb_account" "main" {
  name                = var.cosmos_account_name
  location            = var.location
  resource_group_name = var.resource_group_name
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  capabilities {
    name = "EnableServerless"
  }

  consistency_policy {
    consistency_level = "Session"
  }

  geo_location {
    location          = var.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_sql_database" "db" {
  name                = var.database_name
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.main.name
}

resource "azurerm_cosmosdb_sql_container" "checkpoints" {
  name                = "langgraph_checkpoints"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.main.name
  database_name       = azurerm_cosmosdb_sql_database.db.name
  partition_key_paths = ["/thread_id"]
}

resource "azurerm_cosmosdb_sql_container" "sessions" {
  name                = "sessions"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.main.name
  database_name       = azurerm_cosmosdb_sql_database.db.name
  partition_key_paths = ["/session_id"]
}

output "cosmos_endpoint" {
  value       = azurerm_cosmosdb_account.main.endpoint
  description = "Cosmos DB account endpoint"
}
