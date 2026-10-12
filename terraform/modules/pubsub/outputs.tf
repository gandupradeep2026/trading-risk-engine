output "trade_orders_topic_id" {
  description = "The ID of the primary trade orders Pub/Sub topic"
  value       = google_pubsub_topic.trade_orders.id
}

output "trade_orders_topic_name" {
  description = "The name of the primary trade orders Pub/Sub topic"
  value       = google_pubsub_topic.trade_orders.name
}

output "trade_orders_dlq_topic_id" {
  description = "The ID of the trade orders dead-letter topic"
  value       = google_pubsub_topic.trade_orders_dlq.id
}

output "trade_orders_dataflow_subscription_id" {
  description = "The ID of the Dataflow trade orders subscription"
  value       = google_pubsub_subscription.trade_orders_dataflow_sub.id
}

output "risk_alerts_topic_id" {
  description = "The ID of the risk alerts Pub/Sub topic"
  value       = google_pubsub_topic.risk_alerts.id
}

output "ingestion_service_account_email" {
  description = "The email address of the Ingestion producer service account"
  value       = google_service_account.ingestion_producer.email
}

output "dataflow_service_account_email" {
  description = "The email address of the Dataflow worker service account"
  value       = google_service_account.dataflow_worker.email
}
