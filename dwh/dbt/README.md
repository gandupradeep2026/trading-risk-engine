# trading_risk_dbt: BigQuery & DuckDB dbt Project (Day 23)

## Architecture Overview
`trading_risk_dbt` is an enterprise financial analytics dbt project transforming raw BigQuery star-schema tables into standardized staging models and multi-dimensional intermediate analytical datasets.

```mermaid
flowchart TD
    subgraph Sources [BigQuery Star-Schema / Raw Data]
        S_ORDERS[fact_trade_orders]
        S_TICKS[fact_market_ticks]
        S_STATS[fact_risk_stats]
        S_ALERTS[fact_risk_alerts]
        S_TICKERS[dim_tickers]
        S_PORTS[dim_portfolios]
        S_TRADERS[dim_traders]
    end

    subgraph Staging [Staging Views: stg_]
        STG_ORDERS[stg_trade_orders]
        STG_TICKS[stg_market_ticks]
        STG_STATS[stg_risk_stats]
        STG_ALERTS[stg_risk_alerts]
        STG_TICKERS[stg_tickers]
        STG_PORTS[stg_portfolios]
        STG_TRADERS[stg_traders]
    end

    subgraph Intermediate [Intermediate Models: int_]
        INT_ENRICHED[int_orders_enriched]
        INT_HOURLY[int_portfolio_hourly_exposure]
        INT_MICRO[int_market_microstructure_hourly]
        INT_BREACHES[int_risk_limit_breaches]
    end

    S_ORDERS --> STG_ORDERS
    S_TICKS --> STG_TICKS
    S_STATS --> STG_STATS
    S_ALERTS --> STG_ALERTS
    S_TICKERS --> STG_TICKERS
    S_PORTS --> STG_PORTS
    S_TRADERS --> STG_TRADERS

    STG_ORDERS --> INT_ENRICHED
    STG_TICKERS --> INT_ENRICHED
    STG_PORTS --> INT_ENRICHED
    STG_TRADERS --> INT_ENRICHED

    INT_ENRICHED --> INT_HOURLY

    STG_TICKS --> INT_MICRO
    STG_TICKERS --> INT_MICRO

    STG_STATS --> INT_BREACHES
    STG_PORTS --> INT_BREACHES
    STG_ALERTS --> INT_BREACHES
```

## Models Catalog

### Staging Layer (`models/staging/`)
- `stg_trade_orders`: Extracts trade order fills, generates deterministic MD5 surrogate key `trade_order_key`, computes exact notional USD, and cleans flags (`is_filled`, `is_buy`, `is_sell`).
- `stg_market_ticks`: Extracts tick prints, computes absolute spread, mid price, relative spread in basis points (`relative_spread_bps`), and surrogate key `market_tick_key`.
- `stg_risk_stats`: Standardizes portfolio VaR (parametric, historical, Monte Carlo) and CVaR / Expected Shortfall, calculates risk headroom and tail risk ratio.
- `stg_risk_alerts`: Extracts operational anomalies and limit breaches with critical severity classification.
- `stg_tickers`, `stg_portfolios`, `stg_traders`: Dimensional staging views standardizing business keys, desks, and risk mandates.

### Intermediate Layer (`models/intermediate/`)
- `int_orders_enriched`: Denormalizes orders with ticker metadata, trader limits, signed notional USD, notional tier classification (`MICRO`, `STANDARD`, `LARGE`, `BLOCK`), aggressive taker flag (`is_aggressive`), single-order limit breach detection, and trader intra-day order sequencing.
- `int_portfolio_hourly_exposure`: Rolls up orders hourly per portfolio and desk, deriving gross turnover, net directional exposure, order flow imbalance, and aggressive trade ratios.
- `int_market_microstructure_hourly`: Rolls up high-frequency ticks hourly per instrument, computing OHLC prices, exact hourly VWAP, total contract volume, and volatility price range in basis points.
- `int_risk_limit_breaches`: Evaluates portfolio risk headroom, tail risk ratios, limit breach severity (`NORMAL`, `WARNING`, `CRITICAL`), and correlated operational alerts.
