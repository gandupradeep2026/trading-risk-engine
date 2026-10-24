project_id                  = "trading-risk-dev-001"
region                      = "us-central1"
environment                 = "dev"
network_name                = "trading-risk-vpc"
ingestion_subnet_cidr       = "10.10.10.0/24"
streaming_subnet_cidr       = "10.10.20.0/24"
data_services_subnet_cidr   = "10.10.30.0/24"
enable_flow_logs            = true

# Storage
force_destroy               = true
raw_archive_retention_days  = 30
raw_archive_expiration_days = 365

# Cloud SQL
database_version            = "POSTGRES_15"
db_tier                     = "db-custom-2-7680"
db_disk_size_gb             = 50
database_name               = "trading_risk_db"
db_username                 = "risk_app_user"

# Pub/Sub
pubsub_message_retention_duration = "604800s"
pubsub_ack_deadline_seconds       = 60
pubsub_max_delivery_attempts      = 5

# Compute Engine
compute_machine_type   = "e2-standard-2"
compute_zone           = "us-central1-a"
compute_disk_size_gb   = 50
worker_container_image = "gcr.io/trading-risk-dev-001/ingestion-worker:latest"
market_feed_tickers    = ["AAPL", "GOOGL", "MSFT", "AMZN", "NVDA", "TSLA", "JPM", "GS"]

# Cloud Composer (Development Small Profile)
composer_image_version          = "composer-2.9.11-airflow-2.9.3"
composer_environment_size       = "ENVIRONMENT_SIZE_SMALL"
composer_master_ipv4_cidr       = "172.16.0.0/28"
composer_cloudsql_ipv4_cidr     = "10.10.100.0/24"
slack_webhook_secret_id         = "trading-risk-slack-webhook-dev"
pagerduty_routing_key_secret_id = "trading-risk-pagerduty-routing-key-dev"

# Databricks Lakehouse (Dev profile)
databricks_workspace_url            = "https://trading-risk-dev.cloud.databricks.com"
databricks_account_id               = "00000000-0000-0000-0000-000000000001"
databricks_cluster_node_type        = "n2-standard-4"
databricks_cluster_driver_node_type = "n2-standard-4"
databricks_spark_version            = "15.4.x-scala2.12"
databricks_min_workers              = 1
databricks_max_workers              = 4
databricks_autotermination_minutes  = 20
databricks_catalog_name             = "trading_risk"



