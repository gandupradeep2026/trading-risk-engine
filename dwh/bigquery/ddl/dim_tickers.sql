-- =============================================================================
-- Table: dim_tickers
-- Description: Financial Instrument Dimension (Equities, FX, Commodities, Crypto, ETFs)
-- Grain: One row per financial ticker/instrument
-- Clustering: asset_class, ticker
-- =============================================================================

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
