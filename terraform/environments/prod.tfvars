project_id                  = "trading-risk-prod-001"
region                      = "us-central1"
environment                 = "prod"
network_name                = "trading-risk-vpc"
ingestion_subnet_cidr       = "10.20.10.0/24"
streaming_subnet_cidr       = "10.20.20.0/24"
data_services_subnet_cidr   = "10.20.30.0/24"
enable_flow_logs            = true

# Storage (Production protection)
force_destroy               = false
raw_archive_retention_days  = 30
raw_archive_expiration_days = 730

# Cloud SQL (High availability production tier)
database_version            = "POSTGRES_15"
db_tier                     = "db-custom-8-30720"
db_disk_size_gb             = 200
database_name               = "trading_risk_db"
db_username                 = "risk_app_user"

# Pub/Sub (Production settings)
pubsub_message_retention_duration = "604800s"
pubsub_ack_deadline_seconds       = 60
pubsub_max_delivery_attempts      = 5

# Compute Engine (Production C2 compute-optimized tier for low latency)
compute_machine_type   = "c2-standard-4"
compute_zone           = "us-central1-a"
compute_disk_size_gb   = 100
worker_container_image = "gcr.io/trading-risk-prod-001/ingestion-worker:v1.0.0"
market_feed_tickers    = ["AAPL", "GOOGL", "MSFT", "AMZN", "NVDA", "TSLA", "JPM", "GS", "SPY", "QQQ"]

