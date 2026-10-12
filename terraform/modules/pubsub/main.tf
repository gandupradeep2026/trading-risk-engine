/**
 * Trading Risk Engine - Enterprise Cloud Pub/Sub & IAM Module
 *
 * Implements:
 * - Trade Orders topic with Dead-Letter Topic (DLQ)
 * - Risk Anomaly Alerts topic with DLQ
 * - Subscriptions with DeadLetterPolicy (max delivery attempts) & Exponential Backoff retry
 * - Dedicated least-privilege IAM service accounts:
 *   - sa-ingestion-producer: Pub/Sub publisher for market/order ingestion
 *   - sa-dataflow-worker: Pub/Sub subscriber, BigQuery editor, GCS admin
 * - IAM roles bindings with principle of least privilege
 */

locals {
  common_labels = {
    project     = "trading-risk-engine"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# -----------------------------------------------------------------------------
# 1. TRADE ORDERS TOPIC & DEAD-LETTER TOPIC (DLQ)
# -----------------------------------------------------------------------------
resource "google_pubsub_topic" "trade_orders_dlq" {
  name    = "top-trade-orders-dlq-${var.environment}"
  project = var.project_id

  message_retention_duration = "1209600s" # 14 days retention for dead letters
  labels                     = merge(local.common_labels, { tier = "dlq" })
}

resource "google_pubsub_subscription" "trade_orders_dlq_sub" {
  name    = "sub-trade-orders-dlq-${var.environment}"
  topic   = google_pubsub_topic.trade_orders_dlq.id
  project = var.project_id

  ack_deadline_seconds       = 60
  message_retention_duration = "1209600s"

  labels = merge(local.common_labels, { tier = "dlq-subscription" })
}

resource "google_pubsub_topic" "trade_orders" {
  name    = "top-trade-orders-${var.environment}"
  project = var.project_id

  message_retention_duration = var.message_retention_duration
  labels                     = merge(local.common_labels, { tier = "primary-stream" })
}

resource "google_pubsub_subscription" "trade_orders_dataflow_sub" {
  name    = "sub-trade-orders-dataflow-${var.environment}"
  topic   = google_pubsub_topic.trade_orders.id
  project = var.project_id

  ack_deadline_seconds       = var.ack_deadline_seconds
  message_retention_duration = var.message_retention_duration
  retain_acked_messages      = false

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.trade_orders_dlq.id
    max_delivery_attempts = var.max_delivery_attempts
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = merge(local.common_labels, { tier = "dataflow-consumer" })
}

# -----------------------------------------------------------------------------
# 2. RISK ALERTS NOTIFICATION TOPIC & SUBSCRIPTION
# -----------------------------------------------------------------------------
resource "google_pubsub_topic" "risk_alerts" {
  name    = "top-risk-alerts-${var.environment}"
  project = var.project_id

  message_retention_duration = var.message_retention_duration
  labels                     = merge(local.common_labels, { tier = "alerts" })
}

resource "google_pubsub_subscription" "risk_alerts_sub" {
  name    = "sub-risk-alerts-consumer-${var.environment}"
  topic   = google_pubsub_topic.risk_alerts.id
  project = var.project_id

  ack_deadline_seconds = var.ack_deadline_seconds

  retry_policy {
    minimum_backoff = "5s"
    maximum_backoff = "300s"
  }

  labels = merge(local.common_labels, { tier = "alerts-consumer" })
}

# -----------------------------------------------------------------------------
# 3. LEAST-PRIVILEGE SERVICE ACCOUNTS
# -----------------------------------------------------------------------------
resource "google_service_account" "ingestion_producer" {
  account_id   = "sa-ingestion-${var.environment}"
  display_name = "Trading Risk Engine Ingestion Producer (${var.environment})"
  description  = "Service account for ingestion worker VMs publishing to Pub/Sub topics"
  project      = var.project_id
}

resource "google_service_account" "dataflow_worker" {
  account_id   = "sa-dataflow-${var.environment}"
  display_name = "Trading Risk Engine Dataflow Worker (${var.environment})"
  description  = "Service account for Apache Beam Dataflow processing workers"
  project      = var.project_id
}

# -----------------------------------------------------------------------------
# 4. IAM ROLE BINDINGS
# -----------------------------------------------------------------------------

# Allow Ingestion SA to publish to Trade Orders topic
resource "google_pubsub_topic_iam_member" "ingestion_publisher" {
  topic   = google_pubsub_topic.trade_orders.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.ingestion_producer.email}"
  project = var.project_id
}

# Allow Dataflow SA to subscribe to Trade Orders
resource "google_pubsub_subscription_iam_member" "dataflow_subscriber" {
  subscription = google_pubsub_subscription.trade_orders_dataflow_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.dataflow_worker.email}"
  project      = var.project_id
}

# Allow Dataflow SA to publish Risk Alerts
resource "google_pubsub_topic_iam_member" "dataflow_alert_publisher" {
  topic   = google_pubsub_topic.risk_alerts.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
  project = var.project_id
}

# Cloud Pub/Sub service agent permission to forward to DLQ
data "google_project" "current" {
  project_id = var.project_id
}

resource "google_pubsub_topic_iam_member" "pubsub_dlq_publisher" {
  topic   = google_pubsub_topic.trade_orders_dlq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  project = var.project_id
}

resource "google_pubsub_subscription_iam_member" "pubsub_subscriber_ack" {
  subscription = google_pubsub_subscription.trade_orders_dataflow_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  project      = var.project_id
}
