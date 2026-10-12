output "instance_name" {
  description = "The name of the Cloud SQL PostgreSQL instance"
  value       = google_sql_database_instance.postgres.name
}

output "instance_connection_name" {
  description = "The connection name of the instance to be used in connection strings (project:region:instance)"
  value       = google_sql_database_instance.postgres.connection_name
}

output "private_ip_address" {
  description = "The first private IPv4 address assigned to the Cloud SQL instance"
  value       = google_sql_database_instance.postgres.private_ip_address
}

output "database_name" {
  description = "The name of the provisioned operational database"
  value       = google_sql_database.risk_db.name
}

output "db_username" {
  description = "Application username for the database"
  value       = google_sql_user.app_user.name
}

output "db_password" {
  description = "Generated password for the application database user"
  value       = random_password.db_password.result
  sensitive   = true
}
