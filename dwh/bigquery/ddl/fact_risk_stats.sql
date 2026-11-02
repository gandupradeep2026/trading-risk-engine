-- =============================================================================
-- Table: fact_risk_stats
-- Description: Portfolio Value-at-Risk (VaR) & Quantitative Risk Analytics Fact Table
-- Grain: One row per portfolio valuation snapshot (Daily/Intraday)
-- Partitioning: PARTITION BY valuation_date (Day-level)
-- Clustering: portfolio_id, risk_regime, confidence_level
-- Cost Optimization: require_partition_filter = TRUE
-- =============================================================================

CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.fact_risk_stats`
(
    risk_stat_id STRING NOT NULL OPTIONS(description="Unique primary key for risk analytics snapshot"),
    valuation_date DATE NOT NULL OPTIONS(description="Risk valuation calendar date - Partition key"),
    valuation_timestamp TIMESTAMP NOT NULL OPTIONS(description="UTC timestamp of risk calculation engine run"),
    date_key INT64 NOT NULL OPTIONS(description="Foreign key to dim_date (YYYYMMDD)"),
    portfolio_id STRING NOT NULL OPTIONS(description="Trading portfolio book - FK to dim_portfolios, Clustering key"),
    risk_regime STRING NOT NULL OPTIONS(description="Assigned regime: LOW_RISK, MODERATE_RISK, ELEVATED_RISK, HIGH_RISK, CRITICAL_BREACH - Clustering key"),
    confidence_level NUMERIC NOT NULL OPTIONS(description="Statistical confidence level (0.95 or 0.99) - Clustering key"),
    horizon_days INT64 NOT NULL OPTIONS(description="Risk horizon holding period in days"),
    parametric_var_usd NUMERIC NOT NULL OPTIONS(description="Variance-covariance parametric Value-at-Risk in USD"),
    historical_var_usd NUMERIC NOT NULL OPTIONS(description="Historical simulation Value-at-Risk in USD"),
    monte_carlo_var_usd NUMERIC NOT NULL OPTIONS(description="Correlated Monte Carlo simulation Value-at-Risk in USD"),
    cvar_usd NUMERIC NOT NULL OPTIONS(description="Conditional Value-at-Risk (Expected Shortfall) in USD"),
    portfolio_limit_usd NUMERIC NOT NULL OPTIONS(description="Allocated portfolio risk limit in USD"),
    var_limit_usd NUMERIC NOT NULL OPTIONS(description="Maximum allowable VaR threshold in USD"),
    limit_utilization_pct NUMERIC NOT NULL OPTIONS(description="Percentage utilization of risk limit"),
    is_limit_breached BOOL NOT NULL OPTIONS(description="Indicator flag if any risk limit threshold was exceeded"),
    gross_exposure_usd NUMERIC NOT NULL OPTIONS(description="Sum of absolute market notional position values in USD"),
    net_exposure_usd NUMERIC NOT NULL OPTIONS(description="Directional net market delta exposure in USD"),
    unrealized_pnl_usd NUMERIC NOT NULL OPTIONS(description="Mark-to-market unrealized profit and loss in USD"),
    diversification_benefit_pct NUMERIC NOT NULL OPTIONS(description="Percentage reduction in VaR due to cross-asset correlation offsets"),
    hhi_concentration NUMERIC NOT NULL OPTIONS(description="Herfindahl-Hirschman Index measuring portfolio position concentration"),
    num_positions INT64 NOT NULL OPTIONS(description="Number of open active trading positions in portfolio"),
    _source_batch_id STRING OPTIONS(description="Databricks Gold lakehouse execution batch identifier"),
    _calculated_at TIMESTAMP NOT NULL OPTIONS(description="Audit timestamp when record was loaded into BigQuery")
)
PARTITION BY valuation_date
CLUSTER BY portfolio_id, risk_regime, confidence_level
OPTIONS (
    require_partition_filter = TRUE,
    description = "Fact table storing institutional portfolio Value-at-Risk (VaR), CVaR, limits, exposures, and concentration."
);
