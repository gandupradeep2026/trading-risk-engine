-- =============================================================================
-- Master Script: all_federated_views.sql
-- Description: Creates all BigQuery federated views over Cloud SQL PostgreSQL (Day 25)
-- Target: BigQuery External Connection (projects/${project_id}/locations/${region}/connections/${connection_id})
-- =============================================================================

-- 1. v_federated_risk_alerts
CREATE OR REPLACE VIEW `${project_id}.${dataset_id}.v_federated_risk_alerts`
OPTIONS (
    description = "Real-time federated view querying operational risk_alerts from Cloud SQL PostgreSQL via BigQuery external query connection."
)
AS
SELECT
    alert_id,
    portfolio_id,
    ticker,
    asset_class,
    alert_type,
    severity,
    metric_name,
    threshold_value,
    observed_value,
    event_timestamp,
    created_at,
    status,
    payload_json
FROM EXTERNAL_QUERY(
    "${project_id}.${region}.${connection_id}",
    """SELECT alert_id::text AS alert_id, portfolio_id, ticker, asset_class, alert_type, severity, metric_name, threshold_value::numeric AS threshold_value, observed_value::numeric AS observed_value, event_timestamp, created_at, status, payload_json::text AS payload_json FROM risk_alerts"""
);

-- 2. v_federated_portfolio_limits
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
