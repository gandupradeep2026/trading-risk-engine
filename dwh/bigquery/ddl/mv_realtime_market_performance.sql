-- =============================================================================
-- Materialized View: mv_realtime_market_performance
-- Description: Real-time materialized view aggregating intraday market ticks and microstructure metrics.
-- Base Table: fact_market_ticks
-- Partitioning: Grouped by tick_date (matches fact_market_ticks partitioning)
-- Clustering: asset_class, ticker
-- Refresh Cadence: 15 minutes automated refresh
-- =============================================================================

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
