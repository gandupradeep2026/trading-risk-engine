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

