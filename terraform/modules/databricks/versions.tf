terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }
}
