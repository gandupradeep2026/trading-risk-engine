-- =============================================================================
-- View: v_federated_portfolio_limits
-- Description: Real-time federated view querying Cloud SQL PostgreSQL portfolio_limits
-- Target: BigQuery External Connection (Day 25)
-- =============================================================================

CREATE OR REPLACE VIEW `${project_id}.${dataset_id}.v_federated_portfolio_limits`
OPTIONS (
    description = "Real-time federated view querying operational portfolio_limits from Cloud SQL PostgreSQL via BigQuery external query connection."
)
AS
SELECT
    limit_id,
    portfolio_id,
    asset_class,
    max_notional_usd,
    max_drawdown_pct,
    max_var_99_usd,
    is_active,
    updated_at
FROM EXTERNAL_QUERY(
    "${project_id}.${region}.${connection_id}",
    """SELECT limit_id::text AS limit_id, portfolio_id, asset_class, max_notional_usd::numeric AS max_notional_usd, max_drawdown_pct::numeric AS max_drawdown_pct, max_var_99_usd::numeric AS max_var_99_usd, is_active, updated_at FROM portfolio_limits"""
);
