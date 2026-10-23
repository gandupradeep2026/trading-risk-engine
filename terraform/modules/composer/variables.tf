variable "project_id" {
  description = "The GCP Project ID where the Cloud Composer environment will be provisioned"
  type        = string
}

variable "region" {
  description = "The primary GCP region for Cloud Composer"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "composer_env_name" {
  description = "Custom name for the Cloud Composer environment. If null, computed from environment."
  type        = string
  default     = null
}

variable "composer_image_version" {
  description = "Cloud Composer and Apache Airflow image version"
  type        = string
  default     = "composer-2.9.11-airflow-2.9.3"
}

variable "composer_environment_size" {
  description = "Cloud Composer environment size (ENVIRONMENT_SIZE_SMALL, ENVIRONMENT_SIZE_MEDIUM, ENVIRONMENT_SIZE_LARGE)"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
}

variable "network_id" {
  description = "The VPC network self link or ID for Cloud Composer"
  type        = string
}

variable "subnetwork_id" {
  description = "The subnetwork self link or ID for Cloud Composer (Data Services subnet)"
  type        = string
}

variable "composer_master_ipv4_cidr" {
  description = "The /28 CIDR block for GKE master control plane in Cloud Composer private environment"
  type        = string
  default     = "172.16.0.0/28"
}

variable "composer_cloudsql_ipv4_cidr" {
  description = "The CIDR block for internal Cloud SQL in Cloud Composer private environment"
  type        = string
  default     = "10.10.100.0/24"
}

variable "bronze_bucket_name" {
  description = "GCS Bronze raw market data bucket name for Airflow pipeline reference"
  type        = string
}

variable "silver_bucket_name" {
  description = "GCS Silver processed market data bucket name for Airflow pipeline reference"
  type        = string
}

variable "gold_bucket_name" {
  description = "GCS Gold curated risk data bucket name for Airflow pipeline reference"
  type        = string
}

variable "cloudsql_instance_name" {
  description = "Cloud SQL PostgreSQL instance connection name for operational risk alerts"
  type        = string
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

# --- Workload Config Variables ---
variable "scheduler_cpu" {
  description = "CPU allocated to the Airflow scheduler"
  type        = number
  default     = 0.5
}

variable "scheduler_memory_gb" {
  description = "Memory allocated to the Airflow scheduler in GB"
  type        = number
  default     = 1.875
}

variable "scheduler_storage_gb" {
  description = "Storage allocated to the Airflow scheduler in GB"
  type        = number
  default     = 1.0
}

variable "scheduler_count" {
  description = "Number of Airflow schedulers to run"
  type        = number
  default     = 1
}

variable "web_server_cpu" {
  description = "CPU allocated to the Airflow web server"
  type        = number
  default     = 0.5
}

variable "web_server_memory_gb" {
  description = "Memory allocated to the Airflow web server in GB"
  type        = number
  default     = 1.875
}

variable "web_server_storage_gb" {
  description = "Storage allocated to the Airflow web server in GB"
  type        = number
  default     = 1.0
}

variable "worker_cpu" {
  description = "CPU allocated to Airflow workers"
  type        = number
  default     = 0.5
}

variable "worker_memory_gb" {
  description = "Memory allocated to Airflow workers in GB"
  type        = number
  default     = 1.875
}

variable "worker_storage_gb" {
  description = "Storage allocated to Airflow workers in GB"
  type        = number
  default     = 1.0
}

variable "worker_min_count" {
  description = "Minimum number of Airflow workers"
  type        = number
  default     = 1
}

variable "worker_max_count" {
  description = "Maximum number of Airflow workers"
  type        = number
  default     = 3
}

variable "triggerer_cpu" {
  description = "CPU allocated to Airflow triggerers"
  type        = number
  default     = 0.5
}

variable "triggerer_memory_gb" {
  description = "Memory allocated to Airflow triggerers in GB"
  type        = number
  default     = 0.5
}

variable "triggerer_count" {
  description = "Number of Airflow triggerers to run"
  type        = number
  default     = 1
}
