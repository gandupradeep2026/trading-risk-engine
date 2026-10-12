terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }

  backend "gcs" {
    # Backend configuration passed dynamically via CLI or backend-config
    # bucket = "tf-state-trading-risk-<project_id>"
    # prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# ROOT NETWORK MODULE INSTANTIATION
# -----------------------------------------------------------------------------
module "network" {
  source = "./modules/network"

  project_id                = var.project_id
  region                    = var.region
  environment               = var.environment
  network_name              = var.network_name
  ingestion_subnet_cidr     = var.ingestion_subnet_cidr
  streaming_subnet_cidr     = var.streaming_subnet_cidr
  data_services_subnet_cidr = var.data_services_subnet_cidr
  enable_flow_logs          = var.enable_flow_logs
}
