-- =============================================================================
-- Table: dim_traders
-- Description: Trading Personnel & Execution Algorithmic Agents Dimension
-- Grain: One row per authorized trader
-- Clustering: desk, trader_id
-- =============================================================================

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
