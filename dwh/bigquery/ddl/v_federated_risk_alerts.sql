-- =============================================================================
-- View: v_federated_risk_alerts
-- Description: Real-time federated view querying Cloud SQL PostgreSQL risk_alerts
-- Target: BigQuery External Connection (Day 25)
-- =============================================================================

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
