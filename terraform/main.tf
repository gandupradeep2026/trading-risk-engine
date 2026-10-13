terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
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
# 1. NETWORK MODULE
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

# -----------------------------------------------------------------------------
# 2. STORAGE (GCS DATA LAKE) MODULE
# -----------------------------------------------------------------------------
module "storage" {
  source = "./modules/storage"

  project_id                  = var.project_id
  region                      = var.region
  environment                 = var.environment
  force_destroy               = var.force_destroy
  raw_archive_retention_days  = var.raw_archive_retention_days
  raw_archive_expiration_days = var.raw_archive_expiration_days
}

# -----------------------------------------------------------------------------
# 3. CLOUD SQL (POSTGRESQL OPERATIONAL ALERTS) MODULE
# -----------------------------------------------------------------------------
module "cloudsql" {
  source = "./modules/cloudsql"

  project_id       = var.project_id
  region           = var.region
  environment      = var.environment
  network_id       = module.network.network_id
  psa_dependency   = module.network.private_service_access_connection
  database_version = var.database_version
  tier             = var.db_tier
  disk_size_gb     = var.db_disk_size_gb
  database_name    = var.database_name
  db_username      = var.db_username
}

# -----------------------------------------------------------------------------
# 4. PUBSUB & IAM MODULE
# -----------------------------------------------------------------------------
module "pubsub" {
  source = "./modules/pubsub"

  project_id                 = var.project_id
  environment                = var.environment
  message_retention_duration = var.pubsub_message_retention_duration
  ack_deadline_seconds       = var.pubsub_ack_deadline_seconds
  max_delivery_attempts      = var.pubsub_max_delivery_attempts
}

# -----------------------------------------------------------------------------
# 5. COMPUTE ENGINE INGESTION WORKER MODULE
# -----------------------------------------------------------------------------
module "compute" {
  source = "./modules/compute"

  project_id             = var.project_id
  region                 = var.region
  zone                   = var.compute_zone
  environment            = var.environment
  machine_type           = var.compute_machine_type
  subnetwork_id          = module.network.ingestion_subnet_id
  service_account_email  = module.pubsub.ingestion_service_account_email
  boot_disk_size_gb      = var.compute_disk_size_gb
  worker_container_image = var.worker_container_image
  pubsub_topic_id        = module.pubsub.trade_orders_topic_id
  market_feed_tickers    = var.market_feed_tickers
}

