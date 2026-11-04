variable "project_id" {
  description = "The GCP Project ID where BigQuery resources will be provisioned"
  type        = string
}

variable "region" {
  description = "The primary GCP region or multi-region for BigQuery storage location"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "dataset_id" {
  description = "BigQuery dataset identifier (defaults to trading_risk_dwh_<environment> if null)"
  type        = string
  default     = null
}

variable "dataset_name" {
  description = "Friendly display name for the BigQuery dataset"
  type        = string
  default     = "Trading Risk Enterprise Data Warehouse"
}

variable "dataset_description" {
  description = "Descriptive summary of the BigQuery DWH dataset"
  type        = string
  default     = "Enterprise star schema analytical data warehouse for trading orders, market ticks, portfolio VaR, and risk analytics."
}

variable "delete_contents_on_destroy" {
  description = "Allow deletion of tables upon dataset destruction (set true in dev, false in prod)"
  type        = bool
  default     = true
}

variable "default_table_expiration_ms" {
  description = "Default table lifetime in milliseconds (null for permanent retention)"
  type        = number
  default     = null
}

variable "require_partition_filter" {
  description = "Enforce partition filtering in queries against fact tables to control query costs"
  type        = bool
  default     = true
}

variable "composer_service_account_email" {
  description = "IAM service account email for Cloud Composer Airflow workers"
  type        = string
  default     = ""
}

variable "dataflow_service_account_email" {
  description = "IAM service account email for Dataflow pipeline workers"
  type        = string
  default     = ""
}

variable "labels" {
  description = "Key-value resource labels applied to dataset and tables"
  type        = map(string)
  default     = {}
}

variable "mv_refresh_interval_ms" {
  description = "Refresh interval in milliseconds for BigQuery Materialized Views (default: 15 minutes / 900000ms)"
  type        = number
  default     = 900000
}

# -----------------------------------------------------------------------------
# Cloud SQL Federated Query Connection Variables (Day 25)
# -----------------------------------------------------------------------------
variable "enable_cloudsql_federation" {
  description = "Enable BigQuery external connection and federated views to Cloud SQL PostgreSQL"
  type        = bool
  default     = true
}

variable "cloudsql_connection_id" {
  description = "Connection ID for the BigQuery Cloud SQL external connection (defaults to conn_cloudsql_risk_db_<environment> if null)"
  type        = string
  default     = null
}

variable "cloudsql_instance_connection_name" {
  description = "Connection name of the Cloud SQL PostgreSQL instance (project:region:instance)"
  type        = string
  default     = ""
}

variable "cloudsql_database_name" {
  description = "Name of the Cloud SQL database for federated queries"
  type        = string
  default     = "trading_risk_db"
}

variable "cloudsql_user" {
  description = "Database user for BigQuery federated query authentication"
  type        = string
  default     = "risk_app_user"
}

variable "cloudsql_password" {
  description = "Database password for BigQuery federated query authentication"
  type        = string
  default     = ""
  sensitive   = true
}

