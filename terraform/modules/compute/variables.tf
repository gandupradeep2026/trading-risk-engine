variable "project_id" {
  description = "The GCP Project ID where the VM will be deployed"
  type        = string
}

variable "region" {
  description = "The primary GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The GCP zone for the ingestion worker VM"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "instance_name" {
  description = "Custom name for the Compute Engine instance (defaults to vm-ingestion-worker-<env>)"
  type        = string
  default     = ""
}

variable "machine_type" {
  description = "Compute Engine machine type for the ingestion worker"
  type        = string
  default     = "e2-standard-2"
}

variable "subnetwork_id" {
  description = "The VPC subnetwork self_link or ID where the VM will be attached"
  type        = string
}

variable "service_account_email" {
  description = "The service account email attached to the Compute Engine VM"
  type        = string
}

variable "boot_disk_size_gb" {
  description = "Boot disk capacity in GB"
  type        = number
  default     = 50
}

variable "boot_disk_type" {
  description = "Persistent disk type (pd-standard, pd-balanced, pd-ssd)"
  type        = string
  default     = "pd-balanced"
}

variable "boot_disk_image" {
  description = "OS boot disk image family or specific image"
  type        = string
  default     = "debian-cloud/debian-12"
}

variable "network_tags" {
  description = "Network tags applied to the instance for firewall rule targeting"
  type        = list(string)
  default     = ["ingestion-worker", "bastion-access"]
}

variable "worker_container_image" {
  description = "Container image URI for the trading ingestion worker"
  type        = string
  default     = "gcr.io/trading-risk-dev-001/ingestion-worker:latest"
}

variable "pubsub_topic_id" {
  description = "The destination Pub/Sub topic for published trade orders"
  type        = string
  default     = ""
}

variable "market_feed_tickers" {
  description = "List of financial market ticker symbols ingested by this worker"
  type        = list(string)
  default     = ["AAPL", "GOOGL", "MSFT", "AMZN", "NVDA", "TSLA", "JPM", "GS"]
}

variable "labels" {
  description = "Additional resource labels"
  type        = map(string)
  default     = {}
}
