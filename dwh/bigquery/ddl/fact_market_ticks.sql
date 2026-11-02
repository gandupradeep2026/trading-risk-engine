-- =============================================================================
-- Table: fact_market_ticks
-- Description: High-Frequency Market Tick Price & Volume Fact Table
-- Grain: One row per tick print
-- Partitioning: PARTITION BY tick_date (Day-level)
-- Clustering: asset_class, ticker
-- Cost Optimization: require_partition_filter = TRUE
-- =============================================================================

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
