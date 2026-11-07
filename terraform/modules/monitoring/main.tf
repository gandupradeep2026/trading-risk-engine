/**
 * Trading Risk Engine - Enterprise Cloud Monitoring & Alerting Module
 *
 * Implements:
 * - Google Cloud Monitoring notification channels (Slack Alerts, Slack DLQ, Email, PagerDuty)
 * - Metric-driven Alert Policies:
 *   1. Pub/Sub Subscription Undelivered Messages Lag (num_undelivered_messages)
 *   2. Pub/Sub Oldest Unacked Message Age (oldest_unacked_message_age)
 *   3. Dead-Letter Queue (DLQ) Message Spike (Immediate Slack notification)
 *   4. Dataflow Pipeline Streaming System Lag (system_lag)
 *   5. Cloud SQL PostgreSQL High CPU Utilization (cpu/utilization)
 * - Diagnostic runbook links, incident documentation, and automated notification routing
 */

locals {
  common_labels = {
    project     = "trading-risk-engine"
    environment = var.environment
    managed_by  = "terraform"
    tier        = "monitoring"
  }

  trade_orders_sub_name = var.trade_orders_subscription_id != "" ? var.trade_orders_subscription_id : "sub-trade-orders-dataflow-${var.environment}"
  trade_orders_dlq_name = var.trade_orders_dlq_topic_id != "" ? var.trade_orders_dlq_topic_id : "top-trade-orders-dlq-${var.environment}"
}

# -----------------------------------------------------------------------------
# 1. NOTIFICATION CHANNELS
# -----------------------------------------------------------------------------
resource "google_monitoring_notification_channel" "slack_alerts" {
  project      = var.project_id
  display_name = "Slack Risk Alerts (${var.environment})"
  type         = "slack"
  labels = {
    channel_name = var.notification_slack_channel
  }
  user_labels = local.common_labels

  sensitive_labels {
    auth_token = var.slack_auth_token
  }
}

resource "google_monitoring_notification_channel" "slack_dlq" {
  project      = var.project_id
  display_name = "Slack DLQ Alerts (${var.environment})"
  type         = "slack"
  labels = {
    channel_name = var.notification_slack_dlq_channel
  }
  user_labels = local.common_labels

  sensitive_labels {
    auth_token = var.slack_auth_token
  }
}

resource "google_monitoring_notification_channel" "email_alerts" {
  project      = var.project_id
  display_name = "Email Risk Engineering (${var.environment})"
  type         = "email"
  labels = {
    email_address = var.notification_email
  }
  user_labels = local.common_labels
}

resource "google_monitoring_notification_channel" "pagerduty_alerts" {
  project      = var.project_id
  display_name = "PagerDuty Trading Platform (${var.environment})"
  type         = "pagerduty"
  labels = {
    service_name = "trading-risk-${var.environment}"
  }
  user_labels = local.common_labels

  sensitive_labels {
    service_key = var.pagerduty_service_key
  }
}

# -----------------------------------------------------------------------------
# 2. ALERT POLICIES
# -----------------------------------------------------------------------------

# Alert 1: Pub/Sub Subscription Undelivered Messages Lag
resource "google_monitoring_alert_policy" "pubsub_subscription_lag" {
  project      = var.project_id
  display_name = "[${upper(var.environment)}] Pub/Sub Trade Orders Subscription Lag"
  combiner     = "OR"
  enabled      = var.enable_alert_policies
  user_labels  = merge(local.common_labels, { severity = "critical" })

  documentation {
    content   = "Pub/Sub subscription '${local.trade_orders_sub_name}' backlog has exceeded ${var.pubsub_lag_threshold_messages} messages or oldest unacknowledged message age > ${var.pubsub_oldest_unacked_age_seconds}s. Possible Dataflow worker crash or backpressure stall. Refer to: docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#pubsub-lag-remediation."
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Pub/Sub Undelivered Messages Exceeds Threshold"
    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.labels.subscription_id = \"${local.trade_orders_sub_name}\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = var.pubsub_lag_threshold_messages

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  conditions {
    display_name = "Pub/Sub Oldest Unacked Message Age Exceeds Threshold"
    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND metric.type = \"pubsub.googleapis.com/subscription/oldest_unacked_message_age\" AND resource.labels.subscription_id = \"${local.trade_orders_sub_name}\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = var.pubsub_oldest_unacked_age_seconds

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_MAX"
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.slack_alerts.name,
    google_monitoring_notification_channel.email_alerts.name,
  ]
}

# Alert 2: Dead-Letter Queue (DLQ) Poison Pill Activity (Immediate Slack Notification)
resource "google_monitoring_alert_policy" "pubsub_dlq_activity" {
  project      = var.project_id
  display_name = "[${upper(var.environment)}] Trade Orders DLQ Poison Pill Received"
  combiner     = "OR"
  enabled      = var.enable_alert_policies
  user_labels  = merge(local.common_labels, { severity = "critical", routing = "dlq" })

  documentation {
    content   = "One or more unparseable or rejected trade orders were routed to Dead-Letter Topic '${local.trade_orders_dlq_name}'. Immediate forensic triage required to diagnose invalid schema, crossed quote, or corrupted financial payload. Refer to: docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#dlq-triage-sop."
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Messages Sent to Dead-Letter Topic"
    condition_threshold {
      filter          = "resource.type = \"pubsub_topic\" AND metric.type = \"pubsub.googleapis.com/topic/send_request_count\" AND resource.labels.topic_id = \"${local.trade_orders_dlq_name}\""
      duration        = "60s"
      comparison      = "COMPARISON_GT"
      threshold_value = var.pubsub_dlq_threshold_messages - 1 # e.g. > 0

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.slack_dlq.name,
    google_monitoring_notification_channel.slack_alerts.name,
    google_monitoring_notification_channel.pagerduty_alerts.name,
  ]
}

# Alert 3: Dataflow Pipeline System Lag
resource "google_monitoring_alert_policy" "dataflow_system_lag" {
  project      = var.project_id
  display_name = "[${upper(var.environment)}] Cloud Dataflow System Lag"
  combiner     = "OR"
  enabled      = var.enable_alert_policies
  user_labels  = merge(local.common_labels, { severity = "warning" })

  documentation {
    content   = "Cloud Dataflow streaming pipeline system lag has exceeded ${var.dataflow_system_lag_threshold_seconds}s. Risk calculations may experience delayed event-time sliding windows. Check Dataflow autoscaling and worker throughput. Refer to: docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#dataflow-lag-remediation."
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Dataflow Pipeline System Lag Exceeds Threshold"
    condition_threshold {
      filter          = "resource.type = \"dataflow_job\" AND metric.type = \"dataflow.googleapis.com/job/system_lag\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = var.dataflow_system_lag_threshold_seconds

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_MAX"
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.slack_alerts.name,
    google_monitoring_notification_channel.email_alerts.name,
  ]
}

# Alert 4: Cloud SQL PostgreSQL High CPU Utilization
resource "google_monitoring_alert_policy" "cloudsql_cpu_utilization" {
  project      = var.project_id
  display_name = "[${upper(var.environment)}] Cloud SQL PostgreSQL High CPU"
  combiner     = "OR"
  enabled      = var.enable_alert_policies
  user_labels  = merge(local.common_labels, { severity = "warning" })

  documentation {
    content   = "Cloud SQL PostgreSQL instance CPU utilization has exceeded ${var.cloudsql_cpu_threshold * 100}%. Check active queries, connection pooling, and risk_alerts table index scans. Refer to: docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#cloudsql-cpu-remediation."
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "Cloud SQL CPU Utilization Exceeds Threshold"
    condition_threshold {
      filter          = "resource.type = \"cloudsql_database\" AND metric.type = \"cloudsql.googleapis.com/database/cpu/utilization\""
      duration        = "600s"
      comparison      = "COMPARISON_GT"
      threshold_value = var.cloudsql_cpu_threshold

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_MEAN"
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.slack_alerts.name,
    google_monitoring_notification_channel.email_alerts.name,
  ]
}
