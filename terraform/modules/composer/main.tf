locals {
  composer_name      = var.composer_env_name != null ? var.composer_env_name : "composer-trading-risk-${var.environment}"
  service_account_id = "sa-composer-${var.environment}"
}

# -----------------------------------------------------------------------------
# 1. LEAST-PRIVILEGE IAM SERVICE ACCOUNT FOR COMPOSER WORKER
# -----------------------------------------------------------------------------
resource "google_service_account" "composer_sa" {
  project      = var.project_id
  account_id   = local.service_account_id
  display_name = "Cloud Composer Service Account for Trading Risk (${var.environment})"
  description  = "Dedicated IAM service account executing Airflow DAGs across Dataproc, GCS, BigQuery, and Cloud SQL"
}

# Core Composer worker and orchestration IAM role bindings
resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Allow Composer v2 service agent to act as the worker service account
resource "google_service_account_iam_member" "composer_agent_user" {
  service_account_id = google_service_account.composer_sa.name
  role               = "roles/composer.ServiceAgentV2Ext"
  member             = "serviceAccount:${google_service_account.composer_sa.email}"
}

# -----------------------------------------------------------------------------
# 2. CLOUD COMPOSER 2 PRIVATE ENVIRONMENT
# -----------------------------------------------------------------------------
resource "google_composer_environment" "risk_composer" {
  name    = local.composer_name
  project = var.project_id
  region  = var.region

  config {
    software_config {
      image_version = var.composer_image_version

      airflow_config_overrides = {
        "core-dags_are_paused_at_creation" = "true"
        "core-load_examples"               = "false"
        "core-max_active_tasks_per_dag"    = "16"
        "core-max_active_runs_per_dag"     = "2"
        "core-dagbag_import_timeout"       = "120"
        "scheduler-min_file_process_interval" = "30"
        "webserver-expose_config"          = "false"
      }

      pypi_packages = {
        apache-airflow-providers-google     = ">=10.15.0"
        apache-airflow-providers-databricks = ">=6.5.0"
        slack-sdk                           = ">=3.27.0"
        pydantic                            = ">=2.6.0"
      }

      env_variables = {
        AIRFLOW_VAR_ENVIRONMENT                      = var.environment
        AIRFLOW_VAR_GCP_PROJECT                      = var.project_id
        AIRFLOW_VAR_GCS_BRONZE_BUCKET                = var.bronze_bucket_name
        AIRFLOW_VAR_GCS_SILVER_BUCKET                = var.silver_bucket_name
        AIRFLOW_VAR_GCS_GOLD_BUCKET                  = var.gold_bucket_name
        AIRFLOW_VAR_CLOUDSQL_INSTANCE                = var.cloudsql_instance_name
        AIRFLOW_VAR_SLACK_WEBHOOK_SECRET_ID          = var.slack_webhook_secret_id
        AIRFLOW_VAR_PAGERDUTY_ROUTING_KEY_SECRET_ID  = var.pagerduty_routing_key_secret_id
      }
    }

    node_config {
      network         = var.network_id
      subnetwork      = var.subnetwork_id
      service_account = google_service_account.composer_sa.email
      enable_ip_masq_agent = true
      tags            = ["trading-risk", "composer", "airflow"]
    }

    private_environment_config {
      enable_private_endpoint              = true
      enable_privately_used_public_ips     = false
      master_ipv4_cidr_block               = var.composer_master_ipv4_cidr
      cloud_sql_ipv4_cidr_block            = var.composer_cloudsql_ipv4_cidr
    }

    workload_config {
      scheduler {
        cpu        = var.scheduler_cpu
        memory_gb  = var.scheduler_memory_gb
        storage_gb = var.scheduler_storage_gb
        count      = var.scheduler_count
      }
      web_server {
        cpu        = var.web_server_cpu
        memory_gb  = var.web_server_memory_gb
        storage_gb = var.web_server_storage_gb
      }
      worker {
        cpu        = var.worker_cpu
        memory_gb  = var.worker_memory_gb
        storage_gb = var.worker_storage_gb
        min_count  = var.worker_min_count
        max_count  = var.worker_max_count
      }
      triggerer {
        cpu        = var.triggerer_cpu
        memory_gb  = var.triggerer_memory_gb
        count      = var.triggerer_count
      }
    }

    environment_size = var.composer_environment_size
  }

  depends_on = [
    google_project_iam_member.composer_worker,
    google_project_iam_member.dataproc_editor,
    google_project_iam_member.storage_admin,
    google_project_iam_member.bigquery_job_user,
    google_project_iam_member.bigquery_data_editor,
    google_project_iam_member.secret_accessor,
    google_project_iam_member.cloudsql_client,
  ]
}
