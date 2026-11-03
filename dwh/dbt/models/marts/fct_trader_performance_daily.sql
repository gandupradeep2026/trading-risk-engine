{{ config(materialized='table', tags=['marts', 'performance', 'trader']) }}

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

traders AS (
    SELECT * FROM {{ ref('stg_traders') }}
),

portfolios AS (
    SELECT * FROM {{ ref('stg_portfolios') }}
),

daily_trader_orders AS (
    SELECT
        trade_date,
        trader_id,
        portfolio_id,
        desk,
        COUNT(*) AS total_orders_executed,
        COUNT(CASE WHEN is_filled THEN 1 END) AS filled_orders_count,
        COUNT(CASE WHEN is_buy THEN 1 END) AS buy_orders_count,
        COUNT(CASE WHEN is_sell THEN 1 END) AS sell_orders_count,
        ROUND(SUM(notional_usd), 2) AS gross_turnover_usd,
        ROUND(SUM(CASE WHEN is_buy THEN notional_usd ELSE 0.0 END), 2) AS buy_notional_usd,
        ROUND(SUM(CASE WHEN is_sell THEN notional_usd ELSE 0.0 END), 2) AS sell_notional_usd,
        ROUND(SUM(signed_notional_usd), 2) AS net_delta_usd,
        ROUND(AVG(notional_usd), 2) AS avg_order_size_usd,
        ROUND(MAX(notional_usd), 2) AS max_single_order_usd,
        ROUND(SUM(fee_usd), 2) AS total_fees_paid_usd,
        ROUND(AVG(slippage_bps), 2) AS avg_slippage_bps,
        COUNT(CASE WHEN is_aggressive THEN 1 END) AS aggressive_orders_count,
        COUNT(CASE WHEN notional_tier = 'BLOCK' THEN 1 END) AS block_trades_count,
        COUNT(CASE WHEN is_limit_breach THEN 1 END) AS limit_breaches_count
    FROM orders
    GROUP BY
        trade_date,
        trader_id,
        portfolio_id,
        desk
)

SELECT
    MD5(CONCAT(d.trader_id, '|', d.portfolio_id, '|', CAST(d.trade_date AS STRING))) AS trader_perf_key,
    d.trade_date,
    d.trader_id,
    COALESCE(t.trader_name, 'UNKNOWN') AS trader_name,
    COALESCE(t.trader_role, 'TRADER') AS trader_role,
    d.portfolio_id,
    COALESCE(p.portfolio_name, d.portfolio_id) AS portfolio_name,
    d.desk,
    d.total_orders_executed,
    d.filled_orders_count,
    d.buy_orders_count,
    d.sell_orders_count,
    d.gross_turnover_usd,
    d.buy_notional_usd,
    d.sell_notional_usd,
    d.net_delta_usd,
    d.avg_order_size_usd,
    d.max_single_order_usd,
    d.total_fees_paid_usd,
    d.avg_slippage_bps,
    d.aggressive_orders_count,
    ROUND((d.aggressive_orders_count * 100.0) / NULLIF(d.total_orders_executed, 0), 2) AS aggressive_order_pct,
    d.block_trades_count,
    d.limit_breaches_count,
    COALESCE(t.single_order_limit_usd, 1000000.0) AS authorized_single_order_limit_usd,
    CASE
        WHEN d.limit_breaches_count > 0 THEN TRUE
        ELSE FALSE
    END AS has_limit_breach
FROM daily_trader_orders d
LEFT JOIN traders t ON d.trader_id = t.trader_id
LEFT JOIN portfolios p ON d.portfolio_id = p.portfolio_id
