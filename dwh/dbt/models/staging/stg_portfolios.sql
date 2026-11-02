{{ config(materialized='view', tags=['staging', 'dimensions']) }}

SELECT
    UPPER(TRIM(portfolio_id)) AS portfolio_id,
    TRIM(portfolio_name) AS portfolio_name,
    UPPER(TRIM(desk)) AS desk,
    UPPER(TRIM(base_currency)) AS base_currency,
    CAST(max_var_limit_usd AS NUMERIC) AS max_var_limit_usd,
    COALESCE(is_active, TRUE) AS is_active,
    created_at
FROM {{ source('trading_risk', 'dim_portfolios') }}
WHERE portfolio_id IS NOT NULL
