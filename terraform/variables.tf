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
