variable "project_id" {
  description = "The Google Cloud Platform project ID"
  type        = string
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

variable "message_retention_duration" {
  description = "Topic message retention duration (e.g. 604800s = 7 days)"
  type        = string
  default     = "604800s"
}

variable "ack_deadline_seconds" {
  description = "Subscription acknowledgment deadline in seconds"
  type        = number
  default     = 60
}

variable "max_delivery_attempts" {
  description = "Maximum delivery attempts before forwarding to dead-letter queue (DLQ)"
  type        = number
  default     = 5
}
