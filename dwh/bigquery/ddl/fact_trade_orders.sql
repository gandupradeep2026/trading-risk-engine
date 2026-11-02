-- =============================================================================
-- Table: fact_trade_orders
-- Description: Financial Trade Order Execution Fact Table
-- Grain: One row per executed order / fill event
-- Partitioning: PARTITION BY trade_date (Day-level)
-- Clustering: portfolio_id, ticker, trader_id, execution_status
-- Cost Optimization: require_partition_filter = TRUE
-- =============================================================================

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
