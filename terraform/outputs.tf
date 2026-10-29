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

# Cloud Composer Outputs
output "composer_environment_id" {
  description = "The ID of the Cloud Composer environment"
  value       = module.composer.composer_environment_id
}

output "composer_environment_name" {
  description = "The name of the Cloud Composer environment"
  value       = module.composer.composer_environment_name
}

output "composer_airflow_uri" {
  description = "The Web UI URI for Apache Airflow in Cloud Composer"
  value       = module.composer.airflow_uri
}

output "composer_dag_gcs_prefix" {
  description = "The Cloud Storage prefix for DAGs in Cloud Composer"
  value       = module.composer.dag_gcs_prefix
}

output "composer_service_account_email" {
  description = "The IAM service account email attached to Cloud Composer"
  value       = module.composer.composer_service_account_email
}

# Databricks Outputs
output "databricks_cluster_id" {
  description = "The ID of the Databricks Lakehouse Bronze cluster"
  value       = module.databricks.cluster_id
}

output "databricks_cluster_name" {
  description = "The name of the Databricks Lakehouse Bronze cluster"
  value       = module.databricks.cluster_name
}

output "databricks_bronze_job_id" {
  description = "The ID of the Databricks Bronze ingestion scheduled job"
  value       = module.databricks.job_id
}

output "databricks_catalog_name" {
  description = "The Unity Catalog name for the analytical Lakehouse"
  value       = module.databricks.catalog_name
}

output "databricks_bronze_schema" {
  description = "The Bronze Delta Lake schema name"
  value       = module.databricks.bronze_schema_name
}

output "databricks_storage_credential_id" {
  description = "The Unity Catalog GCS storage credential ID"
  value       = module.databricks.storage_credential_id
}

output "databricks_bronze_external_location_id" {
  description = "The Unity Catalog Bronze GCS external location ID"
  value       = module.databricks.bronze_external_location_id
}

output "databricks_silver_schema" {
  description = "The Silver Delta Lake schema name"
  value       = module.databricks.silver_schema_name
}

output "databricks_silver_job_id" {
  description = "The ID of the Databricks Silver cleansing scheduled job"
  value       = module.databricks.silver_job_id
}

output "databricks_silver_external_location_id" {
  description = "The Unity Catalog Silver GCS external location ID"
  value       = module.databricks.silver_external_location_id
}

output "databricks_gold_schema" {
  description = "The Gold Delta Lake schema name"
  value       = module.databricks.gold_schema_name
}

output "databricks_gold_job_id" {
  description = "The ID of the Databricks Gold VaR & Feature Store scheduled job"
  value       = module.databricks.gold_job_id
}

output "databricks_gold_external_location_id" {
  description = "The Unity Catalog Gold GCS external location ID"
  value       = module.databricks.gold_external_location_id
}



