{{ config(materialized='view', tags=['staging', 'risk']) }}

WITH source_data AS (
    SELECT
        alert_id,
        alert_date,
        portfolio_id,
        ticker,
        UPPER(TRIM(alert_type)) AS alert_type,
        UPPER(TRIM(severity)) AS severity,
        metric_name,
        CAST(threshold_value AS DOUBLE) AS threshold_value,
        CAST(observed_value AS DOUBLE) AS observed_value,
        FALSE AS is_acknowledged,
        detected_at AS alert_timestamp,
        _ingested_at AS created_at
    FROM {{ source('trading_risk', 'fact_risk_alerts') }}
    WHERE alert_id IS NOT NULL
)

SELECT
    MD5(CONCAT(alert_id, '|', CAST(alert_date AS STRING))) AS risk_alert_key,
    alert_id,
    alert_date,
    portfolio_id,
    ticker,
    alert_type,
    severity,
    metric_name,
    threshold_value,
    observed_value,
    is_acknowledged,
    alert_timestamp,
    created_at,
    CASE WHEN severity = 'CRITICAL' THEN TRUE ELSE FALSE END AS is_critical
FROM source_data
