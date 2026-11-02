{{ config(materialized='view', tags=['intermediate', 'portfolio', 'risk']) }}

WITH enriched_orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

hourly_aggregated AS (
    SELECT
        portfolio_id,
        desk,
        trade_date,
        order_hour,
        COUNT(order_id) AS total_orders,
        COUNT(CASE WHEN is_filled THEN 1 END) AS filled_orders,
        COUNT(CASE WHEN is_buy THEN 1 END) AS buy_orders,
        COUNT(CASE WHEN is_sell THEN 1 END) AS sell_orders,
        COALESCE(SUM(CASE WHEN is_buy THEN notional_usd ELSE 0 END), 0) AS total_buy_notional,
        COALESCE(SUM(CASE WHEN is_sell THEN notional_usd ELSE 0 END), 0) AS total_sell_notional,
        COALESCE(SUM(notional_usd), 0) AS gross_turnover_usd,
        COALESCE(SUM(signed_notional_usd), 0) AS net_exposure_usd,
        ROUND(AVG(notional_usd), 2) AS avg_trade_size_usd,
        MAX(notional_usd) AS max_trade_size_usd,
        COUNT(CASE WHEN is_aggressive THEN 1 END) AS aggressive_order_count,
        ROUND(
            COUNT(CASE WHEN is_aggressive THEN 1 END) * 1.0 / NULLIF(COUNT(order_id), 0), 
            4
        ) AS aggressive_ratio,
        COUNT(CASE WHEN is_limit_breach THEN 1 END) AS limit_breach_count
    FROM enriched_orders
    GROUP BY
        portfolio_id,
        desk,
        trade_date,
        order_hour
)

SELECT
    MD5(CONCAT(portfolio_id, '|', CAST(trade_date AS STRING), '|', CAST(order_hour AS STRING))) AS exposure_window_key,
    portfolio_id,
    desk,
    trade_date,
    order_hour,
    total_orders,
    filled_orders,
    buy_orders,
    sell_orders,
    total_buy_notional,
    total_sell_notional,
    gross_turnover_usd,
    net_exposure_usd,
    avg_trade_size_usd,
    max_trade_size_usd,
    aggressive_order_count,
    aggressive_ratio,
    limit_breach_count,
    ROUND(
        (total_buy_notional - total_sell_notional) / 
        NULLIF(total_buy_notional + total_sell_notional, 0), 
        4
    ) AS order_flow_imbalance
FROM hourly_aggregated
