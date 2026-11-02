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
