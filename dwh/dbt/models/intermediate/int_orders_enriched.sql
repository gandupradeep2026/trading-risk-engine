{{ config(materialized='view', tags=['intermediate', 'orders', 'risk']) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_trade_orders') }}
),

tickers AS (
    SELECT * FROM {{ ref('stg_tickers') }}
),

portfolios AS (
    SELECT * FROM {{ ref('stg_portfolios') }}
),

traders AS (
    SELECT * FROM {{ ref('stg_traders') }}
)

SELECT
    o.trade_order_key,
    o.order_id,
    o.trade_date,
    o.portfolio_id,
    p.portfolio_name,
    COALESCE(p.desk, t.desk, 'UNKNOWN') AS desk,
    o.ticker,
    k.instrument_name,
    k.asset_class,
    COALESCE(k.currency, p.base_currency, 'USD') AS currency,
    o.trader_id,
    t.trader_name,
    t.trader_role,
    o.side,
    o.order_type,
    o.quantity,
    o.order_price,
    o.notional_usd,
    CASE
        WHEN o.side = 'BUY' THEN o.notional_usd
        ELSE -o.notional_usd
    END AS signed_notional_usd,
    CASE
        WHEN o.notional_usd >= 500000 THEN 'BLOCK'
        WHEN o.notional_usd >= 100000 THEN 'LARGE'
        WHEN o.notional_usd >= 10000 THEN 'STANDARD'
        ELSE 'MICRO'
    END AS notional_tier,
    CASE
        WHEN o.order_type = 'MARKET' THEN TRUE
        ELSE FALSE
    END AS is_aggressive,
    o.execution_status,
    o.is_filled,
    o.is_buy,
    o.is_sell,
    o.fee_usd,
    o.slippage_bps,
    COALESCE(t.single_order_limit_usd, 1000000.0) AS single_order_limit_usd,
    COALESCE(p.max_var_limit_usd, 5000000.0) AS max_var_limit_usd,
    CASE
        WHEN o.notional_usd > COALESCE(t.single_order_limit_usd, 1000000.0) THEN TRUE
        ELSE FALSE
    END AS is_limit_breach,
    EXTRACT(HOUR FROM o.order_timestamp) AS order_hour,
    o.order_timestamp,
    o.execution_timestamp,
    ROW_NUMBER() OVER (
        PARTITION BY o.trader_id, o.trade_date 
        ORDER BY o.order_timestamp
    ) AS trader_daily_order_sequence
FROM orders o
LEFT JOIN tickers k ON o.ticker = k.ticker
LEFT JOIN portfolios p ON o.portfolio_id = p.portfolio_id
LEFT JOIN traders t ON o.trader_id = t.trader_id
