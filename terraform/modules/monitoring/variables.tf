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

variable "notification_slack_channel" {
  description = "Slack channel for operational risk alerts and monitoring incidents"
  type        = string
  default     = "#risk-alerts"
}

variable "notification_slack_dlq_channel" {
  description = "Slack channel specifically for Dead-Letter Queue (DLQ) alerts"
  type        = string
  default     = "#risk-dlq"
}

variable "slack_auth_token" {
  description = "OAuth token or webhook URL for Slack notification channel"
  type        = string
  default     = "xoxb-mock-token-for-terraform-validation"
  sensitive   = true
}

variable "notification_email" {
  description = "Email address for critical alert escalations"
  type        = string
  default     = "trading-risk-alerts@example.com"
}

variable "pagerduty_service_key" {
  description = "PagerDuty integration/service key for critical escalations"
  type        = string
  default     = "mock-pagerduty-service-key"
  sensitive   = true
}

variable "pubsub_lag_threshold_messages" {
  description = "Threshold for undelivered messages on primary trade orders subscription before triggering lag alert"
  type        = number
  default     = 10000
}

variable "pubsub_oldest_unacked_age_seconds" {
  description = "Threshold for oldest unacknowledged message age in seconds"
  type        = number
  default     = 300
}

variable "pubsub_dlq_threshold_messages" {
  description = "Threshold for messages published to the dead-letter topic"
  type        = number
  default     = 1
}

variable "dataflow_system_lag_threshold_seconds" {
  description = "Threshold for Dataflow streaming pipeline system lag in seconds"
  type        = number
  default     = 120
}

variable "cloudsql_cpu_threshold" {
  description = "Threshold for Cloud SQL CPU utilization (0.0 to 1.0)"
  type        = number
  default     = 0.80
}

variable "trade_orders_subscription_id" {
  description = "Resource ID of the primary trade orders Pub/Sub subscription"
  type        = string
  default     = ""
}

variable "trade_orders_dlq_topic_id" {
  description = "Resource ID of the trade orders dead-letter topic"
  type        = string
  default     = ""
}

variable "enable_alert_policies" {
  description = "Whether alerting policies are enabled in Cloud Monitoring"
  type        = bool
  default     = true
}
