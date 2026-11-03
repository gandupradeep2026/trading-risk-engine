-- =============================================================================
-- Master DDL Script: all_materialized_views.sql
-- Description: Deploys all BigQuery Materialized Views for Real-Time Market Performance Analytics
-- =============================================================================

-- 1. Real-Time Market Performance & Microstructure
CREATE MATERIALIZED VIEW IF NOT EXISTS `${project_id}.${dataset_id}.mv_realtime_market_performance`
CLUSTER BY asset_class, ticker
OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 15,
    description = "Real-time materialized view aggregating intraday market ticks and microstructure metrics."
) AS
SELECT
    tick_date,
    asset_class,
    ticker,
    COUNT(*) AS tick_count,
    SUM(volume) AS total_volume,
    ROUND(SUM(last_price * volume), 4) AS gross_notional_usd,
    ROUND(AVG(relative_spread_bps), 2) AS avg_relative_spread_bps,
    MIN(bid_price) AS min_bid_price,
    MAX(ask_price) AS max_ask_price
FROM `${project_id}.${dataset_id}.fact_market_ticks`
GROUP BY
    tick_date,
    asset_class,
    ticker;

-- 2. Real-Time Trader & Desk Exposure
CREATE MATERIALIZED VIEW IF NOT EXISTS `${project_id}.${dataset_id}.mv_realtime_trader_exposure`
CLUSTER BY portfolio_id, ticker, trader_id
OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 15,
    description = "Real-time materialized view aggregating intraday trade executions, notional volume, and slippage."
) AS
SELECT
    trade_date,
    portfolio_id,
    ticker,
    trader_id,
    execution_status,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(notional_usd), 2) AS gross_notional_usd,
    ROUND(SUM(fee_usd), 2) AS total_fees_usd,
    ROUND(AVG(slippage_bps), 2) AS avg_slippage_bps,
    ROUND(MAX(notional_usd), 2) AS max_order_notional_usd
FROM `${project_id}.${dataset_id}.fact_trade_orders`
GROUP BY
    trade_date,
    portfolio_id,
    ticker,
    trader_id,
    execution_status;

-- 3. Real-Time Risk Anomaly Alerts Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS `${project_id}.${dataset_id}.mv_realtime_risk_alerts`
CLUSTER BY portfolio_id, severity, alert_type
OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 15,
    description = "Real-time materialized view aggregating streaming risk alerts and anomaly breach metrics."
) AS
SELECT
    alert_date,
    portfolio_id,
    severity,
    alert_type,
    COUNT(*) AS alert_count,
    ROUND(MAX(observed_value), 4) AS max_observed_value,
    ROUND(AVG(observed_value), 4) AS avg_observed_value,
    ROUND(AVG(threshold_value), 4) AS avg_threshold_value
FROM `${project_id}.${dataset_id}.fact_risk_alerts`
GROUP BY
    alert_date,
    portfolio_id,
    severity,
    alert_type;
