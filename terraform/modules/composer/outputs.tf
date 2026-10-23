output "composer_environment_id" {
  description = "The ID of the provisioned Cloud Composer environment"
  value       = google_composer_environment.risk_composer.id
}

output "composer_environment_name" {
  description = "The name of the provisioned Cloud Composer environment"
  value       = google_composer_environment.risk_composer.name
}

output "airflow_uri" {
  description = "The URI of the Apache Airflow Web UI hosted in Cloud Composer"
  value       = google_composer_environment.risk_composer.config[0].airflow_uri
}

output "dag_gcs_prefix" {
  description = "The Cloud Storage prefix for DAGs in the Cloud Composer environment"
  value       = google_composer_environment.risk_composer.config[0].dag_gcs_prefix
}

output "composer_service_account_email" {
  description = "The IAM service account email attached to the Cloud Composer environment"
  value       = google_service_account.composer_sa.email
}
