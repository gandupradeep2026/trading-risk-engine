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
