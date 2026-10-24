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

variable "databricks_workspace_url" {
  description = "The Databricks workspace host URL (e.g., https://trading-risk-dev.cloud.databricks.com)"
  type        = string
  default     = "https://trading-risk-dev.cloud.databricks.com"
}

variable "databricks_account_id" {
  description = "Databricks GCP Account ID for Unity Catalog integration"
  type        = string
  default     = "00000000-0000-0000-0000-000000000000"
}

variable "cluster_node_type" {
  description = "GCP compute machine type for Databricks worker nodes"
  type        = string
  default     = "n2-standard-4"
}

variable "cluster_driver_node_type" {
  description = "GCP compute machine type for Databricks driver node"
  type        = string
  default     = "n2-standard-4"
}

variable "spark_version" {
  description = "Databricks Runtime version for the Lakehouse cluster"
  type        = string
  default     = "15.4.x-scala2.12"
}

variable "min_workers" {
  description = "Minimum number of worker nodes for cluster autoscaling"
  type        = number
  default     = 1
}

variable "max_workers" {
  description = "Maximum number of worker nodes for cluster autoscaling"
  type        = number
  default     = 8
}

variable "autotermination_minutes" {
  description = "Cluster inactivity duration in minutes before automated shutdown"
  type        = number
  default     = 20
}

variable "catalog_name" {
  description = "Unity Catalog name for the analytical Lakehouse"
  type        = string
  default     = "trading_risk"
}

variable "gcs_bronze_bucket_name" {
  description = "Name of the GCS Bronze Data Lake bucket"
  type        = string
}

variable "gcs_bronze_bucket_url" {
  description = "gs:// URI of the GCS Bronze Data Lake bucket"
  type        = string
}

variable "gcs_silver_bucket_name" {
  description = "Name of the GCS Silver Processed Data Lake bucket"
  type        = string
}

variable "gcs_silver_bucket_url" {
  description = "gs:// URI of the GCS Silver Processed Data Lake bucket"
  type        = string
}

variable "gcs_gold_bucket_name" {
  description = "Name of the GCS Gold Curated Data Lake bucket"
  type        = string
}

variable "gcs_gold_bucket_url" {
  description = "gs:// URI of the GCS Gold Curated Data Lake bucket"
  type        = string
}
