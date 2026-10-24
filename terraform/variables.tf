variable "project_id" {
  description = "The GCP Project ID where resources will be provisioned"
  type        = string
}

variable "region" {
  description = "The primary GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# --- Network Module Variables ---
variable "network_name" {
  description = "Name identifier for the VPC"
  type        = string
  default     = "trading-risk-vpc"
}

variable "ingestion_subnet_cidr" {
  description = "CIDR block for Ingestion subnet"
  type        = string
  default     = "10.10.10.0/24"
}

variable "streaming_subnet_cidr" {
  description = "CIDR block for Streaming compute subnet"
  type        = string
  default     = "10.10.20.0/24"
}

variable "data_services_subnet_cidr" {
  description = "CIDR block for Data services subnet"
  type        = string
  default     = "10.10.30.0/24"
}

variable "enable_flow_logs" {
  description = "Enable VPC Flow Logs"
  type        = bool
  default     = true
}

# --- Storage Module Variables ---
variable "force_destroy" {
  description = "Allow deletion of non-empty GCS buckets"
  type        = bool
  default     = false
}

variable "raw_archive_retention_days" {
  description = "Days before transitioning raw market archive files to Nearline"
  type        = number
  default     = 30
}

variable "raw_archive_expiration_days" {
  description = "Days before deleting raw market archive files"
  type        = number
  default     = 365
}

# --- Cloud SQL Module Variables ---
variable "database_version" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "POSTGRES_15"
}

variable "db_tier" {
  description = "Machine tier for the master Cloud SQL instance"
  type        = string
  default     = "db-custom-2-7680"
}

variable "db_disk_size_gb" {
  description = "Allocated storage capacity in GB"
  type        = number
  default     = 50
}

variable "database_name" {
  description = "Initial operational database name for trading risk alerts"
  type        = string
  default     = "trading_risk_db"
}

variable "db_username" {
  description = "Primary application database user"
  type        = string
  default     = "risk_app_user"
}

# --- Pub/Sub Module Variables ---
variable "pubsub_message_retention_duration" {
  description = "Pub/Sub message retention duration"
  type        = string
  default     = "604800s"
}

variable "pubsub_ack_deadline_seconds" {
  description = "Pub/Sub ack deadline seconds"
  type        = number
  default     = 60
}

variable "pubsub_max_delivery_attempts" {
  description = "Pub/Sub max attempts before DLQ routing"
  type        = number
  default     = 5
}

# --- Compute Engine Module Variables ---
variable "compute_machine_type" {
  description = "Machine type for the ingestion worker VM"
  type        = string
  default     = "e2-standard-2"
}

variable "compute_zone" {
  description = "Zone for the ingestion worker VM"
  type        = string
  default     = "us-central1-a"
}

variable "compute_disk_size_gb" {
  description = "Boot disk size in GB for the ingestion worker VM"
  type        = number
  default     = 50
}

variable "worker_container_image" {
  description = "Container image URI for the trading ingestion worker"
  type        = string
  default     = "gcr.io/trading-risk-dev-001/ingestion-worker:latest"
}

variable "market_feed_tickers" {
  description = "Default ticker symbols ingested by the worker"
  type        = list(string)
  default     = ["AAPL", "GOOGL", "MSFT", "AMZN", "NVDA", "TSLA", "JPM", "GS"]
}

# --- Cloud Composer Module Variables ---
variable "composer_image_version" {
  description = "Cloud Composer image version"
  type        = string
  default     = "composer-2.9.11-airflow-2.9.3"
}

variable "composer_environment_size" {
  description = "Cloud Composer environment size (ENVIRONMENT_SIZE_SMALL, ENVIRONMENT_SIZE_MEDIUM, ENVIRONMENT_SIZE_LARGE)"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
}

variable "composer_master_ipv4_cidr" {
  description = "The /28 CIDR block for GKE master in Cloud Composer"
  type        = string
  default     = "172.16.0.0/28"
}

variable "composer_cloudsql_ipv4_cidr" {
  description = "The CIDR block for internal Cloud SQL in Cloud Composer"
  type        = string
  default     = "10.10.100.0/24"
}

variable "slack_webhook_secret_id" {
  description = "Secret Manager secret ID containing the Slack webhook URL"
  type        = string
  default     = "trading-risk-slack-webhook"
}

variable "pagerduty_routing_key_secret_id" {
  description = "Secret Manager secret ID containing the PagerDuty integration key"
  type        = string
  default     = "trading-risk-pagerduty-routing-key"
}

# --- Databricks Module Variables ---
variable "databricks_workspace_url" {
  description = "The Databricks workspace host URL"
  type        = string
  default     = "https://trading-risk-dev.cloud.databricks.com"
}

variable "databricks_account_id" {
  description = "Databricks GCP Account ID for Unity Catalog integration"
  type        = string
  default     = "00000000-0000-0000-0000-000000000000"
}

variable "databricks_cluster_node_type" {
  description = "GCP compute machine type for Databricks worker nodes"
  type        = string
  default     = "n2-standard-4"
}

variable "databricks_cluster_driver_node_type" {
  description = "GCP compute machine type for Databricks driver node"
  type        = string
  default     = "n2-standard-4"
}

variable "databricks_spark_version" {
  description = "Databricks Runtime version for the Lakehouse cluster"
  type        = string
  default     = "15.4.x-scala2.12"
}

variable "databricks_min_workers" {
  description = "Minimum number of worker nodes for cluster autoscaling"
  type        = number
  default     = 1
}

variable "databricks_max_workers" {
  description = "Maximum number of worker nodes for cluster autoscaling"
  type        = number
  default     = 8
}

variable "databricks_autotermination_minutes" {
  description = "Cluster inactivity duration in minutes before automated shutdown"
  type        = number
  default     = 20
}

variable "databricks_catalog_name" {
  description = "Unity Catalog name for the analytical Lakehouse"
  type        = string
  default     = "trading_risk"
}



