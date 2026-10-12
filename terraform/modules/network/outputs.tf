output "network_id" {
  description = "The ID of the VPC network"
  value       = google_compute_network.vpc.id
}

output "network_name" {
  description = "The name of the VPC network"
  value       = google_compute_network.vpc.name
}

output "network_self_link" {
  description = "The URI of the VPC network"
  value       = google_compute_network.vpc.self_link
}

output "ingestion_subnet_id" {
  description = "The ID of the Ingestion subnetwork"
  value       = google_compute_subnetwork.sb_ingestion.id
}

output "ingestion_subnet_name" {
  description = "The name of the Ingestion subnetwork"
  value       = google_compute_subnetwork.sb_ingestion.name
}

output "streaming_subnet_id" {
  description = "The ID of the Streaming subnetwork"
  value       = google_compute_subnetwork.sb_streaming.id
}

output "streaming_subnet_name" {
  description = "The name of the Streaming subnetwork"
  value       = google_compute_subnetwork.sb_streaming.name
}

output "data_services_subnet_id" {
  description = "The ID of the Data Services subnetwork"
  value       = google_compute_subnetwork.sb_data_services.id
}

output "data_services_subnet_name" {
  description = "The name of the Data Services subnetwork"
  value       = google_compute_subnetwork.sb_data_services.name
}

output "router_name" {
  description = "The name of the Cloud Router"
  value       = google_compute_router.router.name
}

output "nat_name" {
  description = "The name of the Cloud NAT gateway"
  value       = google_compute_router_nat.nat.name
}

output "private_service_access_connection" {
  description = "The private VPC service networking connection resource ID"
  value       = google_service_networking_connection.private_vpc_connection.id
}
