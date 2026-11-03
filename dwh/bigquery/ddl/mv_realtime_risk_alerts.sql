-- =============================================================================
-- Materialized View: mv_realtime_risk_alerts
-- Description: Real-time materialized view aggregating streaming risk alerts and anomaly breach metrics.
-- Base Table: fact_risk_alerts
-- Partitioning: Grouped by alert_date (matches fact_risk_alerts partitioning)
-- Clustering: portfolio_id, severity, alert_type
-- Refresh Cadence: 15 minutes automated refresh
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS `${project_id}.${dataset_id}.mv_realtime_risk_alerts`
CLUSTER BY portfolio_id, severity, alert_type
OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 15,
    description = "Real-time materialized view aggregating streaming risk alerts and anomaly breach metrics."
) AS
SELECT
    alert_date,
    portfolio_id,
    severity,
    alert_type,
    COUNT(*) AS alert_count,
    ROUND(MAX(observed_value), 4) AS max_observed_value,
    ROUND(AVG(observed_value), 4) AS avg_observed_value,
    ROUND(AVG(threshold_value), 4) AS avg_threshold_value
FROM `${project_id}.${dataset_id}.fact_risk_alerts`
GROUP BY
    alert_date,
    portfolio_id,
    severity,
    alert_type;
