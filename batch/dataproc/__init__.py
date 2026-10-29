"""Dataproc batch processing package for Trading Risk Engine."""

from batch.dataproc.daily_tick_aggregation import (
    PORTFOLIO_FLOW_ARROW_SCHEMA,
    TICKER_SUMMARY_ARROW_SCHEMA,
    DailyPortfolioFlowRecord,
    DailyTickerSummaryRecord,
    DataprocBatchAggregationTelemetry,
    compute_portfolio_flow_summaries,
    compute_record_hash,
    compute_ticker_daily_summaries,
    run_batch_aggregation,
    write_portfolio_flow_parquet,
    write_ticker_summaries_parquet,
)

__all__ = [
    "TICKER_SUMMARY_ARROW_SCHEMA",
    "PORTFOLIO_FLOW_ARROW_SCHEMA",
    "DailyTickerSummaryRecord",
    "DailyPortfolioFlowRecord",
    "DataprocBatchAggregationTelemetry",
    "compute_record_hash",
    "compute_ticker_daily_summaries",
    "compute_portfolio_flow_summaries",
    "write_ticker_summaries_parquet",
    "write_portfolio_flow_parquet",
    "run_batch_aggregation",
]
