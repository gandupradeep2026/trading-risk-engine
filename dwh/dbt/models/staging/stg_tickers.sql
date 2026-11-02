{{ config(materialized='view', tags=['staging', 'dimensions']) }}

SELECT
    UPPER(TRIM(ticker)) AS ticker,
    UPPER(TRIM(asset_class)) AS asset_class,
    TRIM(ticker_name) AS instrument_name,
    UPPER(TRIM(currency)) AS currency,
    CAST(lot_size AS DOUBLE) AS contract_size,
    CAST(tick_size AS DOUBLE) AS tick_size,
    COALESCE(is_active, TRUE) AS is_active,
    created_at
FROM {{ source('trading_risk', 'dim_tickers') }}
WHERE ticker IS NOT NULL
