{{ config(materialized='table', tags=['marts', 'market_data', 'performance']) }}

WITH hourly AS (
    SELECT * FROM {{ ref('int_market_microstructure_hourly') }}
),

tickers AS (
    SELECT * FROM {{ ref('stg_tickers') }}
),

ranked_hours AS (
    SELECT
        h.*,
        ROW_NUMBER() OVER (
            PARTITION BY h.ticker, h.tick_date 
            ORDER BY h.tick_hour ASC
        ) AS rn_first_hour,
        ROW_NUMBER() OVER (
            PARTITION BY h.ticker, h.tick_date 
            ORDER BY h.tick_hour DESC
        ) AS rn_last_hour
    FROM hourly h
),

daily_aggregates AS (
    SELECT
        ticker,
        asset_class,
        tick_date,
        COUNT(*) AS active_trading_hours,
        SUM(tick_count) AS total_ticks,
        MAX(CASE WHEN rn_first_hour = 1 THEN open_price END) AS session_open,
        MAX(high_price) AS session_high,
        MIN(low_price) AS session_low,
        MAX(CASE WHEN rn_last_hour = 1 THEN close_price END) AS session_close,
        SUM(total_volume) AS total_volume,
        ROUND(SUM(hourly_vwap * total_volume) / NULLIF(SUM(total_volume), 0), 4) AS session_vwap,
        ROUND(AVG(avg_relative_spread_bps), 2) AS avg_relative_spread_bps,
        ROUND(MAX(max_relative_spread_bps), 2) AS max_relative_spread_bps,
        SUM(crossed_quotes_count) AS crossed_quotes_count
    FROM ranked_hours
    GROUP BY
        ticker,
        asset_class,
        tick_date
)

SELECT
    MD5(CONCAT(d.ticker, '|', CAST(d.tick_date AS STRING))) AS performance_date_key,
    d.ticker,
    d.tick_date,
    d.asset_class,
    COALESCE(k.instrument_name, d.ticker) AS instrument_name,
    COALESCE(k.currency, 'USD') AS currency,
    COALESCE(k.contract_size, 1.0) AS contract_size,
    d.active_trading_hours,
    d.total_ticks,
    d.session_open,
    d.session_high,
    d.session_low,
    d.session_close,
    d.total_volume,
    COALESCE(d.session_vwap, d.session_close) AS session_vwap,
    ROUND(((d.session_high - d.session_low) / NULLIF(d.session_low, 0)) * 10000, 2) AS price_range_bps,
    d.avg_relative_spread_bps,
    d.max_relative_spread_bps,
    d.crossed_quotes_count,
    CASE
        WHEN ROUND(((d.session_high - d.session_low) / NULLIF(d.session_low, 0)) * 10000, 2) >= 200.0 THEN TRUE
        ELSE FALSE
    END AS is_volatile_session,
    CASE
        WHEN d.total_volume >= 50000 THEN 'HIGH'
        WHEN d.total_volume >= 10000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS liquidity_tier
FROM daily_aggregates d
LEFT JOIN tickers k ON d.ticker = k.ticker
