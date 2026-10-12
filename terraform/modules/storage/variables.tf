variable "project_id" {
  description = "The Google Cloud Platform project ID"
  type        = string
}

variable "region" {
  description = "Primary GCP region for regional storage buckets"
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

variable "force_destroy" {
  description = "Allow deletion of non-empty buckets (set false for prod protection)"
  type        = bool
  default     = false
}

variable "raw_archive_retention_days" {
  description = "Days before transitioning raw market archive files to Nearline/Coldline storage"
  type        = number
  default     = 30
}

variable "raw_archive_expiration_days" {
  description = "Days before deleting raw market archive objects from cold storage"
  type        = number
  default     = 365
}
