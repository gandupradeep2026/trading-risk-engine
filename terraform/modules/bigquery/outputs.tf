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
