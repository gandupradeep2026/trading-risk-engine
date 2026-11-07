output "slack_notification_channel_id" {
  description = "The resource ID of the Slack general risk alerts notification channel"
  value       = google_monitoring_notification_channel.slack_alerts.id
}

output "slack_dlq_notification_channel_id" {
  description = "The resource ID of the Slack Dead-Letter Queue (DLQ) notification channel"
  value       = google_monitoring_notification_channel.slack_dlq.id
}

output "email_notification_channel_id" {
  description = "The resource ID of the Email notification channel"
  value       = google_monitoring_notification_channel.email_alerts.id
}

output "pagerduty_notification_channel_id" {
  description = "The resource ID of the PagerDuty notification channel"
  value       = google_monitoring_notification_channel.pagerduty_alerts.id
}

output "pubsub_lag_alert_policy_id" {
  description = "The resource ID of the Pub/Sub subscription lag alert policy"
  value       = google_monitoring_alert_policy.pubsub_subscription_lag.id
}

output "pubsub_dlq_alert_policy_id" {
  description = "The resource ID of the Pub/Sub DLQ message spike alert policy"
  value       = google_monitoring_alert_policy.pubsub_dlq_activity.id
}

output "dataflow_system_lag_alert_policy_id" {
  description = "The resource ID of the Dataflow system lag alert policy"
  value       = google_monitoring_alert_policy.dataflow_system_lag.id
}

output "cloudsql_cpu_alert_policy_id" {
  description = "The resource ID of the Cloud SQL CPU utilization alert policy"
  value       = google_monitoring_alert_policy.cloudsql_cpu_utilization.id
}

output "alert_policy_ids" {
  description = "Map of all Cloud Monitoring alert policy IDs"
  value = {
    pubsub_lag   = google_monitoring_alert_policy.pubsub_subscription_lag.id
    pubsub_dlq   = google_monitoring_alert_policy.pubsub_dlq_activity.id
    dataflow_lag = google_monitoring_alert_policy.dataflow_system_lag.id
    cloudsql_cpu = google_monitoring_alert_policy.cloudsql_cpu_utilization.id
  }
}
