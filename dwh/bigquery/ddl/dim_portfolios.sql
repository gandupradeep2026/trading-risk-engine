-- =============================================================================
-- Table: dim_portfolios
-- Description: Trading Portfolio Book Dimension
-- Grain: One row per institutional trading portfolio/book
-- Clustering: desk, portfolio_id
-- =============================================================================

CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.dim_portfolios`
(
    portfolio_key STRING NOT NULL OPTIONS(description="Surrogate primary key for trading portfolio book"),
    portfolio_id STRING NOT NULL OPTIONS(description="Unique institutional portfolio identifier"),
    portfolio_name STRING NOT NULL OPTIONS(description="Descriptive portfolio book name"),
    desk STRING NOT NULL OPTIONS(description="Trading desk ownership: EQUITY_DERIVATIVES, FX_SPOT, COMMODITIES_MACRO, CRYPTO_ARB"),
    portfolio_manager STRING NOT NULL OPTIONS(description="Lead portfolio manager name or identity"),
    risk_limit_usd NUMERIC NOT NULL OPTIONS(description="Maximum gross exposure risk limit in USD"),
    daily_loss_limit_usd NUMERIC NOT NULL OPTIONS(description="Stop-loss threshold limit for single trading day in USD"),
    max_var_limit_usd NUMERIC NOT NULL OPTIONS(description="Maximum allowable 99% 1-day Value-at-Risk in USD"),
    base_currency STRING NOT NULL OPTIONS(description="Base accounting currency"),
    strategy_type STRING NOT NULL OPTIONS(description="Quantitative investment strategy"),
    is_active BOOL NOT NULL OPTIONS(description="Status flag indicating whether book is actively trading"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Audit creation timestamp in DWH"),
    updated_at TIMESTAMP NOT NULL OPTIONS(description="Audit update timestamp in DWH")
)
CLUSTER BY desk, portfolio_id
OPTIONS (
    description = "Dimension table maintaining portfolio books, trading desks, mandate limits, and risk thresholds."
);
