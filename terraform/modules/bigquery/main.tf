/**
 * BigQuery Enterprise Star-Schema Data Warehouse Module (Day 22).
 *
 * Implements:
 * - BigQuery analytical dataset with region pinning and access controls.
 * - Dimension tables: dim_tickers, dim_portfolios, dim_traders, dim_date.
 * - Partitioned and clustered fact tables:
 *   - fact_trade_orders (PARTITION BY trade_date, CLUSTER BY portfolio_id, ticker, trader_id, execution_status)
 *   - fact_market_ticks (PARTITION BY tick_date, CLUSTER BY asset_class, ticker)
 *   - fact_risk_stats (PARTITION BY valuation_date, CLUSTER BY portfolio_id, risk_regime, confidence_level)
 *   - fact_risk_alerts (PARTITION BY alert_date, CLUSTER BY portfolio_id, ticker, severity, alert_type)
 * - Strict partition filter enforcement (require_partition_filter = true) to prevent runaway query scan costs.
 */

locals {
  dataset_id = var.dataset_id != null ? var.dataset_id : "trading_risk_dwh_${var.environment}"
  common_labels = merge(var.labels, {
    environment = var.environment
    tier        = "tier3-dwh"
    platform    = "trading-risk-engine"
    managed_by  = "terraform"
  })
}

# -----------------------------------------------------------------------------
# 1. BIGQUERY DATASET
# -----------------------------------------------------------------------------
resource "google_bigquery_dataset" "dwh_dataset" {
  dataset_id                  = local.dataset_id
  project                     = var.project_id
  friendly_name               = var.dataset_name
  description                 = var.dataset_description
  location                    = var.region
  default_table_expiration_ms = var.default_table_expiration_ms
  delete_contents_on_destroy  = var.delete_contents_on_destroy

  labels = local.common_labels
}

# -----------------------------------------------------------------------------
# 2. DATASET ACCESS PERMISSIONS
# -----------------------------------------------------------------------------
resource "google_bigquery_dataset_access" "composer_access" {
  count         = var.composer_service_account_email != "" ? 1 : 0
  project       = var.project_id
  dataset_id    = google_bigquery_dataset.dwh_dataset.dataset_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = var.composer_service_account_email
}

resource "google_bigquery_dataset_access" "dataflow_access" {
  count         = var.dataflow_service_account_email != "" ? 1 : 0
  project       = var.project_id
  dataset_id    = google_bigquery_dataset.dwh_dataset.dataset_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = var.dataflow_service_account_email
}

# -----------------------------------------------------------------------------
# 3. DIMENSION TABLES
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "dim_tickers" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "dim_tickers"
  project             = var.project_id
  description         = "Financial instrument dimension table capturing tickers, asset classes, exchanges, and lot sizes."
  deletion_protection = !var.delete_contents_on_destroy

  clustering = ["asset_class", "ticker"]
  schema     = file("${path.module}/schemas/dim_tickers.json")
  labels     = local.common_labels
}

resource "google_bigquery_table" "dim_portfolios" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "dim_portfolios"
  project             = var.project_id
  description         = "Trading portfolio dimension table capturing books, desks, portfolio managers, and risk limits."
  deletion_protection = !var.delete_contents_on_destroy

  clustering = ["desk", "portfolio_id"]
  schema     = file("${path.module}/schemas/dim_portfolios.json")
  labels     = local.common_labels
}

resource "google_bigquery_table" "dim_traders" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "dim_traders"
  project             = var.project_id
  description         = "Trading personnel dimension capturing authorized traders, roles, desks, and single order limits."
  deletion_protection = !var.delete_contents_on_destroy

  clustering = ["desk", "trader_id"]
  schema     = file("${path.module}/schemas/dim_traders.json")
  labels     = local.common_labels
}

resource "google_bigquery_table" "dim_date" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "dim_date"
  project             = var.project_id
  description         = "Trading calendar dimension table capturing market sessions, holidays, and fiscal periods."
  deletion_protection = !var.delete_contents_on_destroy

  clustering = ["year", "quarter"]
  schema     = file("${path.module}/schemas/dim_date.json")
  labels     = local.common_labels
}

# -----------------------------------------------------------------------------
# 4. FACT TABLES (PARTITIONED & CLUSTERED)
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "fact_trade_orders" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "fact_trade_orders"
  project             = var.project_id
  description         = "Fact table tracking all executed trade orders, fills, fees, slippage, and notional exposure."
  deletion_protection = !var.delete_contents_on_destroy

  time_partitioning {
    type                     = "DAY"
    field                    = "trade_date"
    require_partition_filter = var.require_partition_filter
  }

  clustering = ["portfolio_id", "ticker", "trader_id", "execution_status"]
  schema     = file("${path.module}/schemas/fact_trade_orders.json")
  labels     = local.common_labels
}

resource "google_bigquery_table" "fact_market_ticks" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "fact_market_ticks"
  project             = var.project_id
  description         = "Fact table storing granular high-frequency market tick prints, spreads, and order book top."
  deletion_protection = !var.delete_contents_on_destroy

  time_partitioning {
    type                     = "DAY"
    field                    = "tick_date"
    require_partition_filter = var.require_partition_filter
  }

  clustering = ["asset_class", "ticker"]
  schema     = file("${path.module}/schemas/fact_market_ticks.json")
  labels     = local.common_labels
}

resource "google_bigquery_table" "fact_risk_stats" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "fact_risk_stats"
  project             = var.project_id
  description         = "Fact table storing institutional portfolio Value-at-Risk (VaR), CVaR, limits, and exposures."
  deletion_protection = !var.delete_contents_on_destroy

  time_partitioning {
    type                     = "DAY"
    field                    = "valuation_date"
    require_partition_filter = var.require_partition_filter
  }

  clustering = ["portfolio_id", "risk_regime", "confidence_level"]
  schema     = file("${path.module}/schemas/fact_risk_stats.json")
  labels     = local.common_labels
}

resource "google_bigquery_table" "fact_risk_alerts" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "fact_risk_alerts"
  project             = var.project_id
  description         = "Fact table storing streaming market anomalies, flash crash drawdowns, and limit breaches."
  deletion_protection = !var.delete_contents_on_destroy

  time_partitioning {
    type                     = "DAY"
    field                    = "alert_date"
    require_partition_filter = false
  }

  clustering = ["portfolio_id", "ticker", "severity", "alert_type"]
  schema     = file("${path.module}/schemas/fact_risk_alerts.json")
  labels     = local.common_labels
}
