{{ config(materialized='view', tags=['staging', 'orders']) }}

WITH source_data AS (
    SELECT
        order_id,
        trade_date,
        portfolio_id,
        ticker,
        trader_id,
        UPPER(TRIM(side)) AS side,
        UPPER(TRIM(order_type)) AS order_type,
        CAST(quantity AS DOUBLE) AS quantity,
        CAST(price AS NUMERIC) AS order_price,
        ROUND(CAST(price AS NUMERIC) * CAST(quantity AS NUMERIC), 4) AS notional_usd,
        UPPER(TRIM(execution_status)) AS execution_status,
        execution_timestamp AS order_timestamp,
        execution_timestamp,
        CAST(COALESCE(fee_usd, 0.0) AS NUMERIC) AS fee_usd,
        CAST(COALESCE(slippage_bps, 0.0) AS NUMERIC) AS slippage_bps
    FROM {{ source('trading_risk', 'fact_trade_orders') }}
    WHERE order_id IS NOT NULL
      AND price > 0
      AND quantity > 0
)

SELECT
    MD5(CONCAT(order_id, '|', CAST(trade_date AS STRING))) AS trade_order_key,
    order_id,
    trade_date,
    portfolio_id,
    ticker,
    trader_id,
    side,
    order_type,
    quantity,
    order_price,
    notional_usd,
    execution_status,
    order_timestamp,
    execution_timestamp,
    fee_usd,
    slippage_bps,
    CASE WHEN execution_status = 'FILLED' THEN TRUE ELSE FALSE END AS is_filled,
    CASE WHEN side = 'BUY' THEN TRUE ELSE FALSE END AS is_buy,
    CASE WHEN side = 'SELL' THEN TRUE ELSE FALSE END AS is_sell
FROM source_data
