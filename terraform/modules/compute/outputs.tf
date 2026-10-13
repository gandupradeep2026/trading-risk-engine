output "instance_id" {
  description = "The server-assigned unique identifier of the Compute Engine instance"
  value       = google_compute_instance.ingestion_worker.instance_id
}

output "instance_name" {
  description = "The name of the Compute Engine ingestion worker instance"
  value       = google_compute_instance.ingestion_worker.name
}

output "instance_self_link" {
  description = "The URI of the Compute Engine instance"
  value       = google_compute_instance.ingestion_worker.self_link
}

output "internal_ip" {
  description = "The internal RFC 1918 IP address of the ingestion worker"
  value       = google_compute_instance.ingestion_worker.network_interface[0].network_ip
}

output "service_account_email" {
  description = "The service account email attached to the instance"
  value       = google_compute_instance.ingestion_worker.service_account[0].email
}

output "zone" {
  description = "The GCP zone hosting the ingestion worker instance"
  value       = google_compute_instance.ingestion_worker.zone
}
