"""BigQuery Star Schema DDL, Management, and Local Emulation Package (Day 22)."""

from dwh.bigquery.config import (
    BigQueryDatasetConfig,
    STAR_SCHEMA_CATALOG,
    TablePartitionClusteringSpec,
    DIM_TICKERS_SPEC,
    DIM_PORTFOLIOS_SPEC,
    DIM_TRADERS_SPEC,
    DIM_DATE_SPEC,
    FACT_TRADE_ORDERS_SPEC,
    FACT_MARKET_TICKS_SPEC,
    FACT_RISK_STATS_SPEC,
    FACT_RISK_ALERTS_SPEC,
)
from dwh.bigquery.schema_manager import (
    BigQuerySchemaManager,
    SchemaValidationError,
)
from dwh.bigquery.emulator import (
    BigQueryEmulatorError,
    BigQueryPartitionFilterRequiredError,
    BigQueryTelemetry,
    LocalBigQueryEmulator,
)
from dwh.bigquery.seed_data import (
    get_dim_date_records,
    get_dim_portfolios_records,
    get_dim_tickers_records,
    get_dim_traders_records,
    generate_mock_market_ticks,
    generate_mock_risk_alerts,
    generate_mock_risk_stats,
    generate_mock_trade_orders,
)

__all__ = [
    "BigQueryDatasetConfig",
    "STAR_SCHEMA_CATALOG",
    "TablePartitionClusteringSpec",
    "DIM_TICKERS_SPEC",
    "DIM_PORTFOLIOS_SPEC",
    "DIM_TRADERS_SPEC",
    "DIM_DATE_SPEC",
    "FACT_TRADE_ORDERS_SPEC",
    "FACT_MARKET_TICKS_SPEC",
    "FACT_RISK_STATS_SPEC",
    "FACT_RISK_ALERTS_SPEC",
    "BigQuerySchemaManager",
    "SchemaValidationError",
    "BigQueryEmulatorError",
    "BigQueryPartitionFilterRequiredError",
    "BigQueryTelemetry",
    "LocalBigQueryEmulator",
    "get_dim_date_records",
    "get_dim_portfolios_records",
    "get_dim_tickers_records",
    "get_dim_traders_records",
    "generate_mock_market_ticks",
    "generate_mock_risk_alerts",
    "generate_mock_risk_stats",
    "generate_mock_trade_orders",
]
