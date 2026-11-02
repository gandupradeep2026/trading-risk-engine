{{ config(materialized='view', tags=['staging', 'dimensions']) }}

SELECT
    UPPER(TRIM(trader_id)) AS trader_id,
    TRIM(trader_name) AS trader_name,
    UPPER(TRIM(desk)) AS desk,
    UPPER(TRIM(role)) AS trader_role,
    CAST(max_single_order_usd AS NUMERIC) AS single_order_limit_usd,
    COALESCE(is_active, TRUE) AS is_active,
    created_at
FROM {{ source('trading_risk', 'dim_traders') }}
WHERE trader_id IS NOT NULL
