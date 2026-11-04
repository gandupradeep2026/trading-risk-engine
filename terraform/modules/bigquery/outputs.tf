output "dataset_id" {
  description = "The ID of the BigQuery analytical data warehouse dataset"
  value       = google_bigquery_dataset.dwh_dataset.dataset_id
}

output "dataset_name" {
  description = "Friendly name of the BigQuery dataset"
  value       = google_bigquery_dataset.dwh_dataset.friendly_name
}

output "dataset_location" {
  description = "Storage geographic location of the BigQuery dataset"
  value       = google_bigquery_dataset.dwh_dataset.location
}

output "table_ids" {
  description = "Map of logical table names to BigQuery table IDs"
  value = {
    dim_tickers      = google_bigquery_table.dim_tickers.table_id
    dim_portfolios   = google_bigquery_table.dim_portfolios.table_id
    dim_traders      = google_bigquery_table.dim_traders.table_id
    dim_date         = google_bigquery_table.dim_date.table_id
    fact_trade_orders = google_bigquery_table.fact_trade_orders.table_id
    fact_market_ticks = google_bigquery_table.fact_market_ticks.table_id
    fact_risk_stats  = google_bigquery_table.fact_risk_stats.table_id
    fact_risk_alerts = google_bigquery_table.fact_risk_alerts.table_id
  }
}

output "fact_trade_orders_table_id" {
  description = "The table ID for fact_trade_orders"
  value       = google_bigquery_table.fact_trade_orders.table_id
}

output "fact_market_ticks_table_id" {
  description = "The table ID for fact_market_ticks"
  value       = google_bigquery_table.fact_market_ticks.table_id
}

output "fact_risk_stats_table_id" {
  description = "The table ID for fact_risk_stats"
  value       = google_bigquery_table.fact_risk_stats.table_id
}

output "fact_risk_alerts_table_id" {
  description = "The table ID for fact_risk_alerts"
  value       = google_bigquery_table.fact_risk_alerts.table_id
}

output "dim_tickers_table_id" {
  description = "The table ID for dim_tickers"
  value       = google_bigquery_table.dim_tickers.table_id
}

output "dim_portfolios_table_id" {
  description = "The table ID for dim_portfolios"
  value       = google_bigquery_table.dim_portfolios.table_id
}

output "dim_traders_table_id" {
  description = "The table ID for dim_traders"
  value       = google_bigquery_table.dim_traders.table_id
}

output "dim_date_table_id" {
  description = "The table ID for dim_date"
  value       = google_bigquery_table.dim_date.table_id
}

output "mv_realtime_market_performance_id" {
  description = "The table ID for mv_realtime_market_performance"
  value       = google_bigquery_table.mv_realtime_market_performance.table_id
}

output "mv_realtime_trader_exposure_id" {
  description = "The table ID for mv_realtime_trader_exposure"
  value       = google_bigquery_table.mv_realtime_trader_exposure.table_id
}

output "mv_realtime_risk_alerts_id" {
  description = "The table ID for mv_realtime_risk_alerts"
  value       = google_bigquery_table.mv_realtime_risk_alerts.table_id
}

output "materialized_view_ids" {
  description = "Map of logical materialized view names to BigQuery table IDs"
  value = {
    mv_realtime_market_performance = google_bigquery_table.mv_realtime_market_performance.table_id
    mv_realtime_trader_exposure   = google_bigquery_table.mv_realtime_trader_exposure.table_id
    mv_realtime_risk_alerts       = google_bigquery_table.mv_realtime_risk_alerts.table_id
  }
}

# -----------------------------------------------------------------------------
# Cloud SQL External Connection Outputs (Day 25)
# -----------------------------------------------------------------------------
output "cloudsql_connection_id" {
  description = "The BigQuery connection ID for Cloud SQL federated queries"
  value       = var.enable_cloudsql_federation && length(google_bigquery_connection.cloudsql_connection) > 0 ? google_bigquery_connection.cloudsql_connection[0].connection_id : null
}

output "cloudsql_connection_name" {
  description = "The resource name of the BigQuery Cloud SQL connection"
  value       = var.enable_cloudsql_federation && length(google_bigquery_connection.cloudsql_connection) > 0 ? google_bigquery_connection.cloudsql_connection[0].name : null
}

output "cloudsql_connection_service_account" {
  description = "The service account email assigned to the BigQuery Cloud SQL connection"
  value       = var.enable_cloudsql_federation && length(google_bigquery_connection.cloudsql_connection) > 0 ? google_bigquery_connection.cloudsql_connection[0].cloud_sql[0].service_account_id : null
}

output "federated_view_ids" {
  description = "Map of logical federated view names to BigQuery table IDs"
  value = var.enable_cloudsql_federation ? {
    v_federated_risk_alerts      = length(google_bigquery_table.v_federated_risk_alerts) > 0 ? google_bigquery_table.v_federated_risk_alerts[0].table_id : null
    v_federated_portfolio_limits = length(google_bigquery_table.v_federated_portfolio_limits) > 0 ? google_bigquery_table.v_federated_portfolio_limits[0].table_id : null
  } : {}
}

