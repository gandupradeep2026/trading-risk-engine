-- =============================================================================
-- Materialized View: mv_realtime_trader_exposure
-- Description: Real-time materialized view aggregating intraday trade executions, notional volume, and slippage.
-- Base Table: fact_trade_orders
-- Partitioning: Grouped by trade_date (matches fact_trade_orders partitioning)
-- Clustering: portfolio_id, ticker, trader_id
-- Refresh Cadence: 15 minutes automated refresh
-- =============================================================================

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
