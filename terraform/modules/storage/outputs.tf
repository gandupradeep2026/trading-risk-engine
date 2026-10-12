output "bronze_bucket_name" {
  description = "Name of the Raw / Bronze GCS bucket"
  value       = google_storage_bucket.bronze_raw.name
}

output "bronze_bucket_url" {
  description = "gs:// URI of the Raw / Bronze GCS bucket"
  value       = google_storage_bucket.bronze_raw.url
}

output "silver_bucket_name" {
  description = "Name of the Silver Processed GCS bucket"
  value       = google_storage_bucket.silver_processed.name
}

output "silver_bucket_url" {
  description = "gs:// URI of the Silver Processed GCS bucket"
  value       = google_storage_bucket.silver_processed.url
}

output "gold_bucket_name" {
  description = "Name of the Gold Curated GCS bucket"
  value       = google_storage_bucket.gold_curated.name
}

output "gold_bucket_url" {
  description = "gs:// URI of the Gold Curated GCS bucket"
  value       = google_storage_bucket.gold_curated.url
}

output "staging_bucket_name" {
  description = "Name of the Dataflow/Spark Staging & Temp GCS bucket"
  value       = google_storage_bucket.dataflow_staging.name
}

output "staging_bucket_url" {
  description = "gs:// URI of the Dataflow/Spark Staging & Temp GCS bucket"
  value       = google_storage_bucket.dataflow_staging.url
}
