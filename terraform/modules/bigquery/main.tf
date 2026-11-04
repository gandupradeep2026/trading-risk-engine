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

# -----------------------------------------------------------------------------
# 5. MATERIALIZED VIEWS (REAL-TIME PERFORMANCE ANALYTICS - DAY 24)
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "mv_realtime_market_performance" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "mv_realtime_market_performance"
  project             = var.project_id
  description         = "Real-time materialized view aggregating intraday market ticks and microstructure metrics."
  deletion_protection = !var.delete_contents_on_destroy

  materialized_view {
    query               = "SELECT tick_date, asset_class, ticker, COUNT(*) AS tick_count, SUM(volume) AS total_volume, ROUND(SUM(last_price * volume), 4) AS gross_notional_usd, ROUND(AVG(relative_spread_bps), 2) AS avg_relative_spread_bps, MIN(bid_price) AS min_bid_price, MAX(ask_price) AS max_ask_price FROM `${var.project_id}.${local.dataset_id}.fact_market_ticks` GROUP BY tick_date, asset_class, ticker"
    enable_refresh      = true
    refresh_interval_ms = var.mv_refresh_interval_ms
  }

  clustering = ["asset_class", "ticker"]
  schema     = file("${path.module}/schemas/mv_realtime_market_performance.json")
  labels     = local.common_labels

  depends_on = [google_bigquery_table.fact_market_ticks]
}

resource "google_bigquery_table" "mv_realtime_trader_exposure" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "mv_realtime_trader_exposure"
  project             = var.project_id
  description         = "Real-time materialized view aggregating intraday trade executions, notional volume, and slippage."
  deletion_protection = !var.delete_contents_on_destroy

  materialized_view {
    query               = "SELECT trade_date, portfolio_id, ticker, trader_id, execution_status, COUNT(*) AS order_count, SUM(quantity) AS total_quantity, ROUND(SUM(notional_usd), 2) AS gross_notional_usd, ROUND(SUM(fee_usd), 2) AS total_fees_usd, ROUND(AVG(slippage_bps), 2) AS avg_slippage_bps, ROUND(MAX(notional_usd), 2) AS max_order_notional_usd FROM `${var.project_id}.${local.dataset_id}.fact_trade_orders` GROUP BY trade_date, portfolio_id, ticker, trader_id, execution_status"
    enable_refresh      = true
    refresh_interval_ms = var.mv_refresh_interval_ms
  }

  clustering = ["portfolio_id", "ticker", "trader_id"]
  schema     = file("${path.module}/schemas/mv_realtime_trader_exposure.json")
  labels     = local.common_labels

  depends_on = [google_bigquery_table.fact_trade_orders]
}

resource "google_bigquery_table" "mv_realtime_risk_alerts" {
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "mv_realtime_risk_alerts"
  project             = var.project_id
  description         = "Real-time materialized view aggregating streaming risk alerts and anomaly breach metrics."
  deletion_protection = !var.delete_contents_on_destroy

  materialized_view {
    query               = "SELECT alert_date, portfolio_id, severity, alert_type, COUNT(*) AS alert_count, ROUND(MAX(observed_value), 4) AS max_observed_value, ROUND(AVG(observed_value), 4) AS avg_observed_value, ROUND(AVG(threshold_value), 4) AS avg_threshold_value FROM `${var.project_id}.${local.dataset_id}.fact_risk_alerts` GROUP BY alert_date, portfolio_id, severity, alert_type"
    enable_refresh      = true
    refresh_interval_ms = var.mv_refresh_interval_ms
  }

  clustering = ["portfolio_id", "severity", "alert_type"]
  schema     = file("${path.module}/schemas/mv_realtime_risk_alerts.json")
  labels     = local.common_labels

  depends_on = [google_bigquery_table.fact_risk_alerts]
}

# -----------------------------------------------------------------------------
# 5. BIGQUERY CLOUD SQL EXTERNAL CONNECTION & IAM (DAY 25)
# -----------------------------------------------------------------------------
resource "google_bigquery_connection" "cloudsql_connection" {
  count         = var.enable_cloudsql_federation ? 1 : 0
  connection_id = var.cloudsql_connection_id != null ? var.cloudsql_connection_id : "conn_cloudsql_risk_db_${var.environment}"
  project       = var.project_id
  location      = var.region
  friendly_name = "Cloud SQL PostgreSQL Connection for Federated Queries (${var.environment})"
  description   = "External federated query connection from BigQuery to Cloud SQL PostgreSQL operational risk database."

  cloud_sql {
    instance_id = var.cloudsql_instance_connection_name
    database    = var.cloudsql_database_name
    type        = "POSTGRES"
    credential {
      username = var.cloudsql_user
      password = var.cloudsql_password
    }
  }
}

resource "google_project_iam_member" "bigquery_cloudsql_client" {
  count   = var.enable_cloudsql_federation && var.cloudsql_instance_connection_name != "" ? 1 : 0
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_bigquery_connection.cloudsql_connection[0].cloud_sql[0].service_account_id}"
}

# -----------------------------------------------------------------------------
# 6. BIGQUERY FEDERATED VIEWS OVER CLOUD SQL (DAY 25)
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "v_federated_risk_alerts" {
  count               = var.enable_cloudsql_federation ? 1 : 0
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "v_federated_risk_alerts"
  project             = var.project_id
  description         = "Real-time federated view querying operational risk_alerts from Cloud SQL via BigQuery external query connection."
  deletion_protection = !var.delete_contents_on_destroy

  view {
    query          = "SELECT alert_id, portfolio_id, ticker, asset_class, alert_type, severity, metric_name, threshold_value, observed_value, event_timestamp, created_at, status, payload_json FROM EXTERNAL_QUERY(\"${var.project_id}.${var.region}.${var.cloudsql_connection_id != null ? var.cloudsql_connection_id : "conn_cloudsql_risk_db_${var.environment}"}\", \"SELECT alert_id::text AS alert_id, portfolio_id, ticker, asset_class, alert_type, severity, metric_name, threshold_value::numeric AS threshold_value, observed_value::numeric AS observed_value, event_timestamp, created_at, status, payload_json::text AS payload_json FROM risk_alerts\")"
    use_legacy_sql = false
  }

  schema = file("${path.module}/schemas/v_federated_risk_alerts.json")
  labels = local.common_labels

  depends_on = [google_bigquery_connection.cloudsql_connection]
}

resource "google_bigquery_table" "v_federated_portfolio_limits" {
  count               = var.enable_cloudsql_federation ? 1 : 0
  dataset_id          = google_bigquery_dataset.dwh_dataset.dataset_id
  table_id            = "v_federated_portfolio_limits"
  project             = var.project_id
  description         = "Real-time federated view querying operational portfolio_limits from Cloud SQL via BigQuery external query connection."
  deletion_protection = !var.delete_contents_on_destroy

  view {
    query          = "SELECT limit_id, portfolio_id, asset_class, max_notional_usd, max_drawdown_pct, max_var_99_usd, is_active, updated_at FROM EXTERNAL_QUERY(\"${var.project_id}.${var.region}.${var.cloudsql_connection_id != null ? var.cloudsql_connection_id : "conn_cloudsql_risk_db_${var.environment}"}\", \"SELECT limit_id::text AS limit_id, portfolio_id, asset_class, max_notional_usd::numeric AS max_notional_usd, max_drawdown_pct::numeric AS max_drawdown_pct, max_var_99_usd::numeric AS max_var_99_usd, is_active, updated_at FROM portfolio_limits\")"
    use_legacy_sql = false
  }

  schema = file("${path.module}/schemas/v_federated_portfolio_limits.json")
  labels = local.common_labels

  depends_on = [google_bigquery_connection.cloudsql_connection]
}

