{{ config(materialized='table', tags=['marts', 'risk', 'capital']) }}

WITH breaches AS (
    SELECT * FROM {{ ref('int_risk_limit_breaches') }}
),

portfolios AS (
    SELECT * FROM {{ ref('stg_portfolios') }}
)

SELECT
    MD5(CONCAT(b.portfolio_id, '|', CAST(b.valuation_date AS STRING), '|', CAST(b.confidence_level AS STRING))) AS portfolio_risk_key,
    b.valuation_date,
    b.portfolio_id,
    COALESCE(b.portfolio_name, p.portfolio_name, b.portfolio_id) AS portfolio_name,
    COALESCE(b.desk, p.desk, 'UNKNOWN') AS desk,
    b.confidence_level,
    b.parametric_var,
    b.historical_var,
    b.monte_carlo_var,
    b.cvar_expected_shortfall,
    b.gross_exposure,
    b.net_exposure,
    b.portfolio_limit_usd,
    b.risk_headroom_usd,
    b.limit_utilization_pct,
    b.tail_risk_ratio,
    b.risk_regime,
    b.breach_severity,
    CASE
        WHEN b.risk_headroom_usd >= 0 THEN TRUE
        ELSE FALSE
    END AS is_capital_adequate,
    b.active_alerts_count,
    b.critical_alerts_count,
    b.valuation_timestamp
FROM breaches b
LEFT JOIN portfolios p ON b.portfolio_id = p.portfolio_id
