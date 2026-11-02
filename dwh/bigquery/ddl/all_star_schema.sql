-- =============================================================================
-- TRADING RISK PLATFORM: BIGQUERY ENTERPRISE STAR SCHEMA MASTER DDL
-- Order of Execution: Dimensions first (dim_date, dim_tickers, dim_portfolios, dim_traders)
-- Followed by Fact Tables (fact_trade_orders, fact_market_ticks, fact_risk_stats, fact_risk_alerts)
-- =============================================================================

-- 1. Financial Calendar Dimension
CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.dim_date`
(
    date_key INT64 NOT NULL OPTIONS(description="Surrogate primary key formatted as YYYYMMDD integer"),
    calendar_date DATE NOT NULL OPTIONS(description="Calendar date (YYYY-MM-DD)"),
    year INT64 NOT NULL OPTIONS(description="Calendar year"),
    quarter INT64 NOT NULL OPTIONS(description="Calendar quarter (1 to 4)"),
    month INT64 NOT NULL OPTIONS(description="Calendar month number (1 to 12)"),
    month_name STRING NOT NULL OPTIONS(description="Full name of the calendar month"),
    day_of_month INT64 NOT NULL OPTIONS(description="Day of month (1 to 31)"),
    day_of_week INT64 NOT NULL OPTIONS(description="Day of week (1=Monday .. 7=Sunday)"),
    day_name STRING NOT NULL OPTIONS(description="Day name"),
    week_of_year INT64 NOT NULL OPTIONS(description="ISO week number (1 to 53)"),
    is_weekend BOOL NOT NULL OPTIONS(description="Flag indicating Saturday or Sunday"),
    is_trading_day BOOL NOT NULL OPTIONS(description="Flag indicating active market trading session"),
    is_us_holiday BOOL NOT NULL OPTIONS(description="Flag indicating US Federal or exchange holiday"),
    fiscal_year INT64 NOT NULL OPTIONS(description="Corporate fiscal accounting year"),
    fiscal_quarter INT64 NOT NULL OPTIONS(description="Corporate fiscal accounting quarter")
)
CLUSTER BY year, quarter
OPTIONS (
    description = "Dimension table maintaining financial calendar attributes, trading holidays, and fiscal periods."
);

-- 2. Tickers & Instruments Dimension
CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.dim_tickers`
(
    ticker_key STRING NOT NULL OPTIONS(description="Surrogate primary key for financial instrument"),
    ticker STRING NOT NULL OPTIONS(description="Financial instrument symbol or ticker"),
    ticker_name STRING NOT NULL OPTIONS(description="Descriptive legal name of financial instrument"),
    asset_class STRING NOT NULL OPTIONS(description="Asset class classification: EQUITY, FX, COMMODITY, CRYPTO, ETF"),
    exchange STRING NOT NULL OPTIONS(description="Primary trading exchange or venue"),
    currency STRING NOT NULL OPTIONS(description="Trading quoting currency"),
    sector STRING OPTIONS(description="Macro industry sector"),
    industry STRING OPTIONS(description="Sub-industry classification"),
    lot_size INT64 NOT NULL OPTIONS(description="Minimum trade lot contract size"),
    tick_size NUMERIC NOT NULL OPTIONS(description="Minimum price fluctuation increment"),
    is_active BOOL NOT NULL OPTIONS(description="Flag indicating if instrument is currently active for trading"),
    valid_from TIMESTAMP NOT NULL OPTIONS(description="SCD Type 2 validity start timestamp"),
    valid_to TIMESTAMP OPTIONS(description="SCD Type 2 validity end timestamp"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Audit timestamp when record was created in DWH"),
    updated_at TIMESTAMP NOT NULL OPTIONS(description="Audit timestamp when record was last updated in DWH")
)
CLUSTER BY asset_class, ticker
OPTIONS (
    description = "Dimension table storing master metadata for multi-asset trading instruments."
);

-- 3. Portfolios Dimension
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

-- 4. Traders Dimension
CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.dim_traders`
(
    trader_key STRING NOT NULL OPTIONS(description="Surrogate primary key for trading personnel"),
    trader_id STRING NOT NULL OPTIONS(description="Institutional trader identifier"),
    trader_name STRING NOT NULL OPTIONS(description="Full legal name of trader"),
    desk STRING NOT NULL OPTIONS(description="Trading desk assignment"),
    role STRING NOT NULL OPTIONS(description="Operational role (SENIOR_TRADER, PORTFOLIO_MANAGER, QUANT_RESEARCHER, EXECUTION_ALGO)"),
    max_single_order_usd NUMERIC NOT NULL OPTIONS(description="Trader individual single order notional mandate limit in USD"),
    email STRING OPTIONS(description="Institutional corporate email address"),
    is_active BOOL NOT NULL OPTIONS(description="Operational trading authorization status"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Audit creation timestamp in DWH")
)
CLUSTER BY desk, trader_id
OPTIONS (
    description = "Dimension table maintaining authorized trading personnel, desks, execution algos, and mandates."
);

-- 5. Fact Trade Orders
CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.fact_trade_orders`
(
    trade_order_id STRING NOT NULL OPTIONS(description="Unique primary key for trade order fact record"),
    order_id STRING NOT NULL OPTIONS(description="Original business order ID from execution engine"),
    trade_date DATE NOT NULL OPTIONS(description="Trade execution calendar date - Partition key"),
    execution_timestamp TIMESTAMP NOT NULL OPTIONS(description="UTC timestamp of execution print"),
    date_key INT64 NOT NULL OPTIONS(description="Foreign key to dim_date (YYYYMMDD)"),
    ticker STRING NOT NULL OPTIONS(description="Instrument symbol - FK to dim_tickers, Clustering key"),
    portfolio_id STRING NOT NULL OPTIONS(description="Trading portfolio book - FK to dim_portfolios, Clustering key"),
    trader_id STRING NOT NULL OPTIONS(description="Executing trader - FK to dim_traders, Clustering key"),
    asset_class STRING NOT NULL OPTIONS(description="Asset class: EQUITY, FX, COMMODITY, CRYPTO, ETF"),
    side STRING NOT NULL OPTIONS(description="Order transaction side: BUY or SELL"),
    order_type STRING NOT NULL OPTIONS(description="Order execution type: MARKET, LIMIT, STOP_LOSS, STOP_LIMIT"),
    execution_status STRING NOT NULL OPTIONS(description="Execution status: FILLED, PARTIALLY_FILLED, CANCELLED, REJECTED - Clustering key"),
    quantity NUMERIC NOT NULL OPTIONS(description="Executed transaction volume/shares"),
    price NUMERIC NOT NULL OPTIONS(description="Execution unit price"),
    notional_usd NUMERIC NOT NULL OPTIONS(description="Total gross notional exposure in USD"),
    fee_usd NUMERIC NOT NULL OPTIONS(description="Exchange and broker execution fee in USD"),
    slippage_bps NUMERIC OPTIONS(description="Execution slippage against arrival price in basis points"),
    is_aggressive BOOL NOT NULL OPTIONS(description="Taker flag indicating liquidity removing execution"),
    sequence_number INT64 OPTIONS(description="Monotonic order sequence number"),
    _cleansed_record_hash STRING OPTIONS(description="SHA-256 lineage hash from Lakehouse Silver tier"),
    _ingested_at TIMESTAMP NOT NULL OPTIONS(description="Audit timestamp when record was loaded into BigQuery")
)
PARTITION BY trade_date
CLUSTER BY portfolio_id, ticker, trader_id, execution_status
OPTIONS (
    require_partition_filter = TRUE,
    description = "Fact table tracking all executed trade orders, fills, fees, slippage, and notional exposure."
);

-- 6. Fact Market Ticks
CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.fact_market_ticks`
(
    tick_id STRING NOT NULL OPTIONS(description="Unique primary key for market tick fact record"),
    tick_date DATE NOT NULL OPTIONS(description="Tick event calendar date - Partition key"),
    event_timestamp TIMESTAMP NOT NULL OPTIONS(description="High-precision microsecond UTC timestamp of market print"),
    date_key INT64 NOT NULL OPTIONS(description="Foreign key to dim_date (YYYYMMDD)"),
    ticker STRING NOT NULL OPTIONS(description="Instrument symbol - FK to dim_tickers, Clustering key"),
    asset_class STRING NOT NULL OPTIONS(description="Asset class: EQUITY, FX, COMMODITY, CRYPTO, ETF - Clustering key"),
    bid_price NUMERIC NOT NULL OPTIONS(description="Best prevailing bid price"),
    ask_price NUMERIC NOT NULL OPTIONS(description="Best prevailing ask/offer price"),
    mid_price NUMERIC NOT NULL OPTIONS(description="Mid-point price between bid and ask"),
    spread NUMERIC NOT NULL OPTIONS(description="Absolute bid-ask spread (ask - bid)"),
    relative_spread_bps NUMERIC NOT NULL OPTIONS(description="Relative spread expressed in basis points (spread / mid * 10000)"),
    last_price NUMERIC NOT NULL OPTIONS(description="Last traded execution print price"),
    volume NUMERIC NOT NULL OPTIONS(description="Last traded contract or share volume"),
    notional_usd NUMERIC NOT NULL OPTIONS(description="Print notional value in USD"),
    sequence_number INT64 NOT NULL OPTIONS(description="Monotonically increasing feed sequence number"),
    is_crossed_quote BOOL NOT NULL OPTIONS(description="Validation flag indicating whether bid exceeds ask"),
    _raw_record_hash STRING OPTIONS(description="SHA-256 lineage fingerprint from streaming ingestion tier"),
    _ingested_at TIMESTAMP NOT NULL OPTIONS(description="Audit timestamp when record was loaded into BigQuery")
)
PARTITION BY tick_date
CLUSTER BY asset_class, ticker
OPTIONS (
    require_partition_filter = TRUE,
    description = "Fact table storing granular high-frequency market tick prints, spreads, and order book top."
);

-- 7. Fact Risk Stats (VaR & Feature Store Metrics)
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

-- 8. Fact Risk Alerts (Operational Anomalies)
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
