{{ config(materialized='view', tags=['intermediate', 'microstructure', 'market_data']) }}

WITH raw_ticks AS (
    SELECT
        t.*,
        EXTRACT(HOUR FROM t.event_timestamp) AS tick_hour
    FROM {{ ref('stg_market_ticks') }} t
),

ranked_ticks AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY t.ticker, t.tick_date, t.tick_hour 
            ORDER BY t.event_timestamp ASC
        ) AS rn_first,
        ROW_NUMBER() OVER (
            PARTITION BY t.ticker, t.tick_date, t.tick_hour 
            ORDER BY t.event_timestamp DESC
        ) AS rn_last
    FROM raw_ticks t
),

window_aggregates AS (
    SELECT
        ticker,
        asset_class,
        tick_date,
        tick_hour,
        COUNT(*) AS tick_count,
        MAX(last_price) AS high_price,
        MIN(last_price) AS low_price,
        MAX(CASE WHEN rn_first = 1 THEN last_price END) AS open_price,
        MAX(CASE WHEN rn_last = 1 THEN last_price END) AS close_price,
        SUM(volume) AS total_volume,
        ROUND(SUM(last_price * volume) / NULLIF(SUM(volume), 0), 4) AS hourly_vwap,
        ROUND(AVG(relative_spread_bps), 2) AS avg_relative_spread_bps,
        ROUND(MAX(relative_spread_bps), 2) AS max_relative_spread_bps,
        COUNT(CASE WHEN is_crossed_quote THEN 1 END) AS crossed_quotes_count
    FROM ranked_ticks
    GROUP BY
        ticker,
        asset_class,
        tick_date,
        tick_hour
)

SELECT
    MD5(CONCAT(ticker, '|', CAST(tick_date AS STRING), '|', CAST(tick_hour AS STRING))) AS microstructure_window_key,
    ticker,
    asset_class,
    tick_date,
    tick_hour,
    tick_count,
    open_price,
    high_price,
    low_price,
    close_price,
    total_volume,
    hourly_vwap,
    avg_relative_spread_bps,
    max_relative_spread_bps,
    crossed_quotes_count,
    ROUND(((high_price - low_price) / NULLIF(low_price, 0)) * 10000, 2) AS price_range_bps
FROM window_aggregates
