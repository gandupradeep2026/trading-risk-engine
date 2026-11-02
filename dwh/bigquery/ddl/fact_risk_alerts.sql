-- =============================================================================
-- Table: fact_risk_alerts
-- Description: Operational Streaming Anomaly & Risk Alerts Fact Table
-- Grain: One row per streaming anomaly detection event
-- Partitioning: PARTITION BY alert_date (Day-level)
-- Clustering: portfolio_id, ticker, severity, alert_type
-- =============================================================================

CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.fact_risk_alerts`
(
    alert_id STRING NOT NULL OPTIONS(description="Unique UUID primary key for risk alert event"),
    alert_date DATE NOT NULL OPTIONS(description="Calendar date of detection - Partition key"),
    detected_at TIMESTAMP NOT NULL OPTIONS(description="UTC timestamp when streaming anomaly was detected"),
    event_timestamp TIMESTAMP NOT NULL OPTIONS(description="UTC timestamp of the underlying trigger event"),
    date_key INT64 NOT NULL OPTIONS(description="Foreign key to dim_date (YYYYMMDD)"),
    portfolio_id STRING NOT NULL OPTIONS(description="Trading portfolio book - FK to dim_portfolios, Clustering key"),
    ticker STRING OPTIONS(description="Financial instrument symbol - FK to dim_tickers, Clustering key"),
    asset_class STRING OPTIONS(description="Asset class classification"),
    alert_type STRING NOT NULL OPTIONS(description="Anomaly category: FLASH_CRASH_DRAWDOWN, SINGLE_ORDER_FAT_FINGER, NOTIONAL_EXPOSURE_BREACH, CONCENTRATED_SELL_PRESSURE - Clustering key"),
    severity STRING NOT NULL OPTIONS(description="Severity level: INFO, WARNING, CRITICAL - Clustering key"),
    metric_name STRING NOT NULL OPTIONS(description="Breached quantitative risk metric name"),
    threshold_value NUMERIC NOT NULL OPTIONS(description="Configured risk threshold limit"),
    observed_value NUMERIC NOT NULL OPTIONS(description="Empirical value observed during evaluation"),
    payload_json STRING OPTIONS(description="Diagnostic event JSON payload"),
    _ingested_at TIMESTAMP NOT NULL OPTIONS(description="Audit timestamp when alert was loaded into BigQuery")
)
PARTITION BY alert_date
CLUSTER BY portfolio_id, ticker, severity, alert_type
OPTIONS (
    description = "Fact table storing all real-time market anomalies, flash crash drawdowns, and limit breaches."
);
