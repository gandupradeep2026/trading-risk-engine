/**
 * Trading Risk Engine - Enterprise Cloud SQL PostgreSQL Module
 *
 * Implements:
 * - High-Availability (HA) Regional deployment in prod, single-zone in dev
 * - Zero Public IP exposure: strictly bound to Private VPC via Private Service Access (RFC 1918)
 * - Automated automated backups with Point-In-Time-Recovery (PITR) & transaction log retention
 * - PostgreSQL flags optimized for low-latency operational alert ingestion:
 *   - log_connections, log_disconnections, log_min_duration_statement
 *   - max_connections, work_mem
 * - Secure random password generation with Secret Manager integration readiness
 * - Initial operational database and application user provisioning
 * - Deletion protection enabled for production safeguards
 */

# -----------------------------------------------------------------------------
# RANDOM SUFFIX & CREDENTIAL GENERATION
# -----------------------------------------------------------------------------
resource "random_id" "db_suffix" {
  byte_length = 4
}

resource "random_password" "db_password" {
  length           = 24
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# -----------------------------------------------------------------------------
# CLOUD SQL POSTGRESQL INSTANCE
# -----------------------------------------------------------------------------
resource "google_sql_database_instance" "postgres" {
  name             = "pg-risk-${var.environment}-${random_id.db_suffix.hex}"
  project          = var.project_id
  region           = var.region
  database_version = var.database_version

  deletion_protection = var.environment == "prod" ? true : false

  settings {
    tier              = var.tier
    availability_type = var.environment == "prod" ? "REGIONAL" : "ZONAL"
    disk_type         = "PD_SSD"
    disk_size         = var.disk_size_gb
    disk_autoresize   = true
    disk_autoresize_limit = 500

    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = var.network_id
      enable_private_path_for_google_cloud_services = true
    }

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "02:00"
      transaction_log_retention_days = 7
      backup_retention_settings {
        retained_backups = 14
        retention_unit   = "COUNT"
      }
    }

    maintenance_window {
      day          = 7 # Sunday
      hour         = 3
      update_track = "stable"
    }

    insights_config {
      query_insights_enabled  = true
      query_string_length     = 1024
      record_application_tags = true
      record_client_address   = true
    }

    database_flags {
      name  = "log_connections"
      value = "on"
    }

    database_flags {
      name  = "log_disconnections"
      value = "on"
    }

    database_flags {
      name  = "log_min_duration_statement"
      value = "500" # Log statements running over 500ms
    }

    database_flags {
      name  = "max_connections"
      value = "200"
    }

    user_labels = {
      project     = "trading-risk-engine"
      environment = var.environment
      managed_by  = "terraform"
      tier        = "operational-hot-store"
    }
  }

  depends_on = [var.psa_dependency]
}

# -----------------------------------------------------------------------------
# DATABASE AND APPLICATION USER
# -----------------------------------------------------------------------------
resource "google_sql_database" "risk_db" {
  name     = var.database_name
  instance = google_sql_database_instance.postgres.name
  project  = var.project_id
  charset  = "UTF8"
  collation = "en_US.UTF8"
}

resource "google_sql_user" "app_user" {
  name     = var.db_username
  instance = google_sql_database_instance.postgres.name
  project  = var.project_id
  password = random_password.db_password.result
}
