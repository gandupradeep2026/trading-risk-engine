# Network Outputs
output "vpc_network_id" {
  description = "The ID of the provisioned VPC network"
  value       = module.network.network_id
}

output "vpc_network_name" {
  description = "The name of the provisioned VPC network"
  value       = module.network.network_name
}

output "ingestion_subnet_id" {
  description = "The ID of the Ingestion subnet"
  value       = module.network.ingestion_subnet_id
}

output "streaming_subnet_id" {
  description = "The ID of the Streaming compute subnet"
  value       = module.network.streaming_subnet_id
}

output "data_services_subnet_id" {
  description = "The ID of the Data Services subnet"
  value       = module.network.data_services_subnet_id
}

output "cloud_nat_name" {
  description = "The name of the Cloud NAT gateway"
  value       = module.network.nat_name
}

# Storage Outputs
output "bronze_bucket_name" {
  description = "Name of the raw/bronze GCS bucket"
  value       = module.storage.bronze_bucket_name
}

output "silver_bucket_name" {
  description = "Name of the silver processed GCS bucket"
  value       = module.storage.silver_bucket_name
}

output "gold_bucket_name" {
  description = "Name of the gold curated GCS bucket"
  value       = module.storage.gold_bucket_name
}

output "staging_bucket_name" {
  description = "Name of the Dataflow staging/temp GCS bucket"
  value       = module.storage.staging_bucket_name
}

# Cloud SQL Outputs
output "cloud_sql_instance_name" {
  description = "Name of the Cloud SQL PostgreSQL instance"
  value       = module.cloudsql.instance_name
}

output "cloud_sql_connection_name" {
  description = "Connection string identifier for the Cloud SQL instance"
  value       = module.cloudsql.instance_connection_name
}

output "cloud_sql_private_ip" {
  description = "Private IP address of the Cloud SQL PostgreSQL instance"
  value       = module.cloudsql.private_ip_address
}

output "cloud_sql_database" {
  description = "Operational database name for real-time risk alerts"
  value       = module.cloudsql.database_name
}

# Pub/Sub Outputs
output "trade_orders_topic_id" {
  description = "The ID of the trade orders Pub/Sub topic"
  value       = module.pubsub.trade_orders_topic_id
}

output "trade_orders_dlq_topic_id" {
  description = "The ID of the trade orders DLQ topic"
  value       = module.pubsub.trade_orders_dlq_topic_id
}

output "trade_orders_dataflow_subscription_id" {
  description = "The ID of the Dataflow trade orders subscription"
  value       = module.pubsub.trade_orders_dataflow_subscription_id
}

output "ingestion_service_account_email" {
  description = "The email of the Ingestion producer service account"
  value       = module.pubsub.ingestion_service_account_email
}

output "dataflow_service_account_email" {
  description = "The email of the Dataflow worker service account"
  value       = module.pubsub.dataflow_service_account_email
}

# Compute Outputs
output "ingestion_vm_id" {
  description = "The ID of the Ingestion worker VM"
  value       = module.compute.instance_id
}

output "ingestion_vm_name" {
  description = "The name of the Ingestion worker VM"
  value       = module.compute.instance_name
}

output "ingestion_vm_self_link" {
  description = "The URI of the Ingestion worker VM"
  value       = module.compute.instance_self_link
}

output "ingestion_vm_internal_ip" {
  description = "The internal RFC 1918 IP address of the Ingestion worker VM"
  value       = module.compute.internal_ip
}

