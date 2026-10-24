output "cluster_id" {
  description = "The ID of the Databricks Lakehouse Bronze cluster"
  value       = databricks_cluster.bronze_lakehouse_cluster.id
}

output "cluster_name" {
  description = "The name of the Databricks Lakehouse Bronze cluster"
  value       = databricks_cluster.bronze_lakehouse_cluster.cluster_name
}

output "job_id" {
  description = "The ID of the Databricks Bronze ingestion scheduled job"
  value       = databricks_job.bronze_ingestion_job.id
}

output "catalog_name" {
  description = "The Unity Catalog name"
  value       = databricks_catalog.lakehouse_catalog.name
}

output "bronze_schema_name" {
  description = "The Bronze Delta Lake schema name"
  value       = databricks_schema.bronze_schema.name
}

output "storage_credential_id" {
  description = "The Unity Catalog GCS storage credential ID"
  value       = databricks_storage_credential.gcs_credential.id
}

output "bronze_external_location_id" {
  description = "The Unity Catalog Bronze GCS external location ID"
  value       = databricks_external_location.bronze_external_location.id
}
