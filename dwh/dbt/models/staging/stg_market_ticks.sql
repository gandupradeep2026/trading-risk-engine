{{ config(materialized='view', tags=['staging', 'market_data']) }}

WITH source_data AS (
    SELECT
        tick_id,
        tick_date,
        UPPER(TRIM(ticker)) AS ticker,
        UPPER(TRIM(asset_class)) AS asset_class,
        CAST(bid_price AS NUMERIC) AS bid_price,
        CAST(ask_price AS NUMERIC) AS ask_price,
        CAST(last_price AS NUMERIC) AS last_price,
        CAST(volume AS DOUBLE) AS volume,
        CAST(ask_price AS NUMERIC) - CAST(bid_price AS NUMERIC) AS spread,
        (CAST(bid_price AS NUMERIC) + CAST(ask_price AS NUMERIC)) / 2.0 AS mid_price,
        ROUND(
            ((CAST(ask_price AS NUMERIC) - CAST(bid_price AS NUMERIC)) / 
             NULLIF((CAST(bid_price AS NUMERIC) + CAST(ask_price AS NUMERIC)) / 2.0, 0)) * 10000, 
            2
        ) AS relative_spread_bps,
        event_timestamp,
        _ingested_at AS ingested_at
    FROM {{ source('trading_risk', 'fact_market_ticks') }}
    WHERE ticker IS NOT NULL
      AND last_price > 0
      AND volume >= 0
)

SELECT
    MD5(CONCAT(tick_id, '|', ticker, '|', CAST(event_timestamp AS STRING))) AS market_tick_key,
    tick_id,
    tick_date,
    ticker,
    asset_class,
    bid_price,
    ask_price,
    last_price,
    volume,
    spread,
    mid_price,
    relative_spread_bps,
    event_timestamp,
    ingested_at,
    CASE WHEN bid_price > ask_price THEN TRUE ELSE FALSE END AS is_crossed_quote
FROM source_data
