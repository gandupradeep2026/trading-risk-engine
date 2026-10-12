variable "project_id" {
  description = "The Google Cloud Platform project ID"
  type        = string
}

variable "region" {
  description = "The primary GCP region for network resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "network_name" {
  description = "Name identifier for the Virtual Private Cloud (VPC)"
  type        = string
  default     = "trading-risk-vpc"
}

variable "ingestion_subnet_cidr" {
  description = "IPv4 CIDR block for the Ingestion subnet"
  type        = string
  default     = "10.10.10.0/24"
}

variable "streaming_subnet_cidr" {
  description = "IPv4 CIDR block for the Streaming compute subnet (Dataflow/Dataproc)"
  type        = string
  default     = "10.10.20.0/24"
}

variable "data_services_subnet_cidr" {
  description = "IPv4 CIDR block for Managed Data Services subnet (Cloud SQL, Composer)"
  type        = string
  default     = "10.10.30.0/24"
}

variable "enable_flow_logs" {
  description = "Flag to enable VPC Flow Logs across subnets"
  type        = bool
  default     = true
}
