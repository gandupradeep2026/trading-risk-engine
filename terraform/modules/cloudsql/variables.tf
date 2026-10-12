variable "project_id" {
  description = "The Google Cloud Platform project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for the Cloud SQL instance"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "network_id" {
  description = "VPC network self link or ID for Private Service Access peering"
  type        = string
}

variable "psa_dependency" {
  description = "Explicit dependency on the Private Service Access connection"
  type        = any
  default     = null
}

variable "database_version" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "POSTGRES_15"
}

variable "tier" {
  description = "Machine tier for the master Cloud SQL instance"
  type        = string
  default     = "db-custom-2-7680"
}

variable "disk_size_gb" {
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
