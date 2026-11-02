{{ config(materialized='view', tags=['intermediate', 'risk', 'compliance']) }}

WITH stats AS (
    SELECT * FROM {{ ref('stg_risk_stats') }}
),

portfolios AS (
    SELECT * FROM {{ ref('stg_portfolios') }}
),

alerts AS (
    SELECT
        portfolio_id,
        alert_date,
        COUNT(alert_id) AS active_alerts_count,
        COUNT(CASE WHEN is_critical THEN 1 END) AS critical_alerts_count
    FROM {{ ref('stg_risk_alerts') }}
    GROUP BY
        portfolio_id,
        alert_date
)

SELECT
    s.risk_stat_key AS risk_breach_key,
    s.risk_stat_id,
    s.valuation_date,
    s.portfolio_id,
    p.portfolio_name,
    p.desk,
    s.confidence_level,
    s.parametric_var,
    s.historical_var,
    s.monte_carlo_var,
    s.cvar_expected_shortfall,
    s.gross_exposure,
    s.net_exposure,
    COALESCE(s.portfolio_limit_usd, p.max_var_limit_usd, 5000000.0) AS portfolio_limit_usd,
    s.limit_utilization_pct,
    s.risk_headroom_usd,
    s.tail_risk_ratio,
    s.risk_regime,
    CASE
        WHEN s.limit_utilization_pct >= 100.0 THEN 'CRITICAL'
        WHEN s.limit_utilization_pct >= 85.0 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS breach_severity,
    COALESCE(a.active_alerts_count, 0) AS active_alerts_count,
    COALESCE(a.critical_alerts_count, 0) AS critical_alerts_count,
    s.valuation_timestamp
FROM stats s
LEFT JOIN portfolios p ON s.portfolio_id = p.portfolio_id
LEFT JOIN alerts a ON s.portfolio_id = a.portfolio_id AND s.valuation_date = a.alert_date
