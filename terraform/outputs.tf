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
