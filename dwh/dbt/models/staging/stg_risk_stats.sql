{{ config(materialized='view', tags=['staging', 'risk']) }}

WITH source_data AS (
    SELECT
        risk_stat_id,
        valuation_date,
        portfolio_id,
        CAST(confidence_level AS DOUBLE) AS confidence_level,
        CAST(parametric_var_usd AS NUMERIC) AS parametric_var,
        CAST(historical_var_usd AS NUMERIC) AS historical_var,
        CAST(monte_carlo_var_usd AS NUMERIC) AS monte_carlo_var,
        CAST(cvar_usd AS NUMERIC) AS cvar_expected_shortfall,
        CAST(gross_exposure_usd AS NUMERIC) AS gross_exposure,
        CAST(net_exposure_usd AS NUMERIC) AS net_exposure,
        CAST(portfolio_limit_usd AS NUMERIC) AS portfolio_limit_usd,
        CAST(limit_utilization_pct AS NUMERIC) AS limit_utilization_pct,
        UPPER(TRIM(risk_regime)) AS risk_regime,
        valuation_timestamp,
        _calculated_at AS created_at
    FROM {{ source('trading_risk', 'fact_risk_stats') }}
    WHERE risk_stat_id IS NOT NULL
      AND parametric_var_usd >= 0
)

SELECT
    MD5(CONCAT(portfolio_id, '|', CAST(valuation_date AS STRING), '|', CAST(confidence_level AS STRING))) AS risk_stat_key,
    risk_stat_id,
    valuation_date,
    portfolio_id,
    confidence_level,
    parametric_var,
    historical_var,
    monte_carlo_var,
    cvar_expected_shortfall,
    gross_exposure,
    net_exposure,
    portfolio_limit_usd,
    limit_utilization_pct,
    risk_regime,
    valuation_timestamp,
    created_at,
    ROUND(portfolio_limit_usd - gross_exposure, 2) AS risk_headroom_usd,
    ROUND(cvar_expected_shortfall / NULLIF(parametric_var, 0), 4) AS tail_risk_ratio
FROM source_data
