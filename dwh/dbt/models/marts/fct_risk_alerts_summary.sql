{{ config(materialized='table', tags=['marts', 'risk', 'alerts']) }}

WITH alerts AS (
    SELECT * FROM {{ ref('stg_risk_alerts') }}
),

portfolios AS (
    SELECT * FROM {{ ref('stg_portfolios') }}
),

aggregated_alerts AS (
    SELECT
        alert_date,
        portfolio_id,
        alert_type,
        severity,
        COUNT(*) AS total_alerts,
        COUNT(DISTINCT ticker) AS affected_tickers_count,
        ROUND(AVG(threshold_value), 4) AS avg_threshold_value,
        ROUND(AVG(observed_value), 4) AS avg_observed_value,
        ROUND(MAX(observed_value), 4) AS max_observed_value,
        ROUND(MAX(observed_value - threshold_value), 4) AS max_breach_spread
    FROM alerts
    GROUP BY
        alert_date,
        portfolio_id,
        alert_type,
        severity
)

SELECT
    MD5(CONCAT(a.portfolio_id, '|', CAST(a.alert_date AS STRING), '|', a.alert_type, '|', a.severity)) AS alert_summary_key,
    a.alert_date,
    a.portfolio_id,
    COALESCE(p.portfolio_name, a.portfolio_id) AS portfolio_name,
    COALESCE(p.desk, 'UNKNOWN') AS desk,
    a.alert_type,
    a.severity,
    a.total_alerts,
    a.affected_tickers_count,
    a.avg_threshold_value,
    a.avg_observed_value,
    a.max_observed_value,
    a.max_breach_spread,
    CASE
        WHEN a.severity = 'CRITICAL' THEN TRUE
        ELSE FALSE
    END AS is_critical
FROM aggregated_alerts a
LEFT JOIN portfolios p ON a.portfolio_id = p.portfolio_id
