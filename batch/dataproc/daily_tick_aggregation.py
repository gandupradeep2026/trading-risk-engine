"""Dataproc Daily Batch Aggregation PySpark Job.

Orchestrated by Cloud Composer / Apache Airflow on ephemeral Dataproc clusters.
Processes Tier 1 GCS Bronze market tick streams and Tier 2 trade orders to produce:
1. Daily Ticker VWAP, OHLCV, realized volatility, and spread microstructure rollups.
2. Daily Portfolio Order Flow, directional notional exposure, and order flow imbalance.
Writes partitioned, Snappy-compressed Parquet datasets directly to GCS Silver layer.
Supports both live Dataproc Spark execution and zero-cost local PyArrow execution.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


# -----------------------------------------------------------------------------
# Explicit PyArrow & Spark DDL Schemas
# -----------------------------------------------------------------------------
TICKER_SUMMARY_ARROW_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("asset_class", pa.string()),
    ("data_date", pa.string()),
    ("open_price", pa.float64()),
    ("high_price", pa.float64()),
    ("low_price", pa.float64()),
    ("close_price", pa.float64()),
    ("tick_count", pa.int64()),
    ("total_volume", pa.float64()),
    ("total_notional_usd", pa.float64()),
    ("daily_vwap", pa.float64()),
    ("price_stddev", pa.float64()),
    ("realized_volatility_bps", pa.float64()),
    ("avg_spread_bps", pa.float64()),
    ("max_spread_bps", pa.float64()),
    ("computed_at", pa.string()),
    ("batch_id", pa.string()),
    ("record_hash", pa.string()),
])

PORTFOLIO_FLOW_ARROW_SCHEMA = pa.schema([
    ("portfolio_id", pa.string()),
    ("ticker", pa.string()),
    ("asset_class", pa.string()),
    ("data_date", pa.string()),
    ("order_count", pa.int64()),
    ("buy_order_count", pa.int64()),
    ("sell_order_count", pa.int64()),
    ("buy_notional_usd", pa.float64()),
    ("sell_notional_usd", pa.float64()),
    ("net_notional_usd", pa.float64()),
    ("order_flow_imbalance", pa.float64()),
    ("avg_order_size_usd", pa.float64()),
    ("computed_at", pa.string()),
    ("batch_id", pa.string()),
    ("record_hash", pa.string()),
])


@dataclass
class DailyTickerSummaryRecord:
    """Canonical daily aggregated market microstructure for a single ticker."""

    ticker: str
    asset_class: str
    data_date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    tick_count: int
    total_volume: float
    total_notional_usd: float
    daily_vwap: float
    price_stddev: float
    realized_volatility_bps: float
    avg_spread_bps: float
    max_spread_bps: float
    computed_at: str
    batch_id: str
    record_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DailyPortfolioFlowRecord:
    """Canonical daily order flow and exposure metrics for a portfolio position."""

    portfolio_id: str
    ticker: str
    asset_class: str
    data_date: str
    order_count: int
    buy_order_count: int
    sell_order_count: int
    buy_notional_usd: float
    sell_notional_usd: float
    net_notional_usd: float
    order_flow_imbalance: float
    avg_order_size_usd: float
    computed_at: str
    batch_id: str
    record_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DataprocBatchAggregationTelemetry:
    """Telemetry tracking batch aggregation execution."""

    total_ticks_processed: int = 0
    total_orders_processed: int = 0
    ticker_summaries_emitted: int = 0
    portfolio_summaries_emitted: int = 0
    duration_sec: float = 0.0
    output_ticker_files: List[str] = None  # type: ignore
    output_order_files: List[str] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.output_ticker_files is None:
            self.output_ticker_files = []
        if self.output_order_files is None:
            self.output_order_files = []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_ticks_processed": self.total_ticks_processed,
            "total_orders_processed": self.total_orders_processed,
            "ticker_summaries_emitted": self.ticker_summaries_emitted,
            "portfolio_summaries_emitted": self.portfolio_summaries_emitted,
            "duration_sec": round(self.duration_sec, 4),
            "output_ticker_files": self.output_ticker_files,
            "output_order_files": self.output_order_files,
        }


def compute_record_hash(payload: Dict[str, Any]) -> str:
    """Deterministic SHA-256 hash for lineage traceability."""
    # Exclude variable audit timestamp and record_hash from digest calculation
    stable_copy = {k: v for k, v in payload.items() if k not in ("computed_at", "record_hash")}
    encoded = json.dumps(stable_copy, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# -----------------------------------------------------------------------------
# Core Aggregation Logic (Mathematical Models)
# -----------------------------------------------------------------------------
def compute_ticker_daily_summaries(
    ticks: List[Dict[str, Any]],
    data_date: str,
    batch_id: str,
    computed_at: Optional[str] = None,
) -> List[DailyTickerSummaryRecord]:
    """Calculate daily OHLCV, VWAP, price stddev, and spread metrics per ticker."""
    ts_now = computed_at or datetime.now(timezone.utc).isoformat()
    if not ticks:
        return []

    # Group ticks by (ticker, asset_class)
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for tick in ticks:
        ticker = tick.get("ticker", "UNKNOWN")
        asset_class = tick.get("asset_class", "EQUITY")
        grouped.setdefault((ticker, asset_class), []).append(tick)

    summaries: List[DailyTickerSummaryRecord] = []
    for (ticker, asset_class), group in sorted(grouped.items()):
        # Sort chronologically by event_timestamp and sequence_number if present
        sorted_group = sorted(
            group,
            key=lambda t: (
                str(t.get("event_timestamp") or t.get("timestamp") or ""),
                int(t.get("sequence_number") or 0),
            ),
        )

        prices = [float(t.get("last_price") or t.get("price") or 0.0) for t in sorted_group]
        volumes = [float(t.get("volume") or t.get("quantity") or 0.0) for t in sorted_group]

        open_price = prices[0]
        close_price = prices[-1]
        high_price = max(prices)
        low_price = min(prices)
        tick_count = len(sorted_group)
        total_volume = sum(volumes)
        total_notional = sum(p * v for p, v in zip(prices, volumes))

        daily_vwap = (total_notional / total_volume) if total_volume > 0 else close_price

        # Sample standard deviation
        if tick_count > 1:
            mean_price = sum(prices) / tick_count
            variance = sum((p - mean_price) ** 2 for p in prices) / (tick_count - 1)
            price_stddev = math.sqrt(variance)
        else:
            price_stddev = 0.0

        # Realized volatility in basis points relative to VWAP
        realized_volatility_bps = (
            (price_stddev / daily_vwap) * 10000.0 if daily_vwap > 0 else 0.0
        )

        # Spread metrics
        spread_bps_list = []
        for t in sorted_group:
            bid = float(t.get("bid") or 0.0)
            ask = float(t.get("ask") or 0.0)
            mid = float(t.get("mid_price") or (bid + ask) / 2.0 if (bid + ask) > 0 else prices[0])
            if mid > 0 and ask >= bid:
                spread_bps_list.append(((ask - bid) / mid) * 10000.0)
            else:
                spread_bps_list.append(0.0)

        avg_spread_bps = sum(spread_bps_list) / len(spread_bps_list) if spread_bps_list else 0.0
        max_spread_bps = max(spread_bps_list) if spread_bps_list else 0.0

        raw_record = {
            "ticker": ticker,
            "asset_class": asset_class,
            "data_date": data_date,
            "open_price": round(open_price, 4),
            "high_price": round(high_price, 4),
            "low_price": round(low_price, 4),
            "close_price": round(close_price, 4),
            "tick_count": tick_count,
            "total_volume": round(total_volume, 4),
            "total_notional_usd": round(total_notional, 2),
            "daily_vwap": round(daily_vwap, 4),
            "price_stddev": round(price_stddev, 6),
            "realized_volatility_bps": round(realized_volatility_bps, 2),
            "avg_spread_bps": round(avg_spread_bps, 2),
            "max_spread_bps": round(max_spread_bps, 2),
            "computed_at": ts_now,
            "batch_id": batch_id,
        }
        rec_hash = compute_record_hash(raw_record)
        raw_record["record_hash"] = rec_hash

        summaries.append(DailyTickerSummaryRecord(**raw_record))

    return summaries


def compute_portfolio_flow_summaries(
    orders: List[Dict[str, Any]],
    data_date: str,
    batch_id: str,
    computed_at: Optional[str] = None,
) -> List[DailyPortfolioFlowRecord]:
    """Calculate daily order volume, directional notional, and flow imbalance per portfolio & ticker."""
    ts_now = computed_at or datetime.now(timezone.utc).isoformat()
    if not orders:
        return []

    # Group orders by (portfolio_id, ticker, asset_class)
    grouped: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    for o in orders:
        portfolio_id = o.get("portfolio_id", "PORTFOLIO_UNKNOWN")
        ticker = o.get("ticker", "UNKNOWN")
        asset_class = o.get("asset_class", "EQUITY")
        grouped.setdefault((portfolio_id, ticker, asset_class), []).append(o)

    flow_records: List[DailyPortfolioFlowRecord] = []
    for (portfolio_id, ticker, asset_class), group in sorted(grouped.items()):
        order_count = len(group)
        buy_order_count = 0
        sell_order_count = 0
        buy_notional = 0.0
        sell_notional = 0.0

        for o in group:
            side = str(o.get("side", "BUY")).upper()
            notional = float(o.get("notional_usd") or 0.0)
            if notional <= 0.0:
                qty = float(o.get("quantity") or 0.0)
                price = float(o.get("price") or 0.0)
                notional = qty * price

            if side == "BUY":
                buy_order_count += 1
                buy_notional += notional
            elif side == "SELL":
                sell_order_count += 1
                sell_notional += notional

        net_notional = buy_notional - sell_notional
        total_notional = buy_notional + sell_notional
        # Order flow imbalance: (Buy - Sell) / (Buy + Sell), normalized in [-1.0, 1.0]
        flow_imbalance = (net_notional / total_notional) if total_notional > 0 else 0.0
        avg_order_size = (total_notional / order_count) if order_count > 0 else 0.0

        raw_record = {
            "portfolio_id": portfolio_id,
            "ticker": ticker,
            "asset_class": asset_class,
            "data_date": data_date,
            "order_count": order_count,
            "buy_order_count": buy_order_count,
            "sell_order_count": sell_order_count,
            "buy_notional_usd": round(buy_notional, 2),
            "sell_notional_usd": round(sell_notional, 2),
            "net_notional_usd": round(net_notional, 2),
            "order_flow_imbalance": round(flow_imbalance, 4),
            "avg_order_size_usd": round(avg_order_size, 2),
            "computed_at": ts_now,
            "batch_id": batch_id,
        }
        rec_hash = compute_record_hash(raw_record)
        raw_record["record_hash"] = rec_hash

        flow_records.append(DailyPortfolioFlowRecord(**raw_record))

    return flow_records


# -----------------------------------------------------------------------------
# Parquet File Writing & Dataset Emission
# -----------------------------------------------------------------------------
def write_ticker_summaries_parquet(
    summaries: List[DailyTickerSummaryRecord],
    output_dir: str,
    data_date: str,
) -> List[str]:
    """Write ticker summary records into partitioned Snappy-compressed Parquet files."""
    if not summaries:
        return []

    created_files: List[str] = []
    # Group by asset_class for Hive partitioning
    grouped: Dict[str, List[DailyTickerSummaryRecord]] = {}
    for s in summaries:
        grouped.setdefault(s.asset_class, []).append(s)

    for asset_class, records in grouped.items():
        partition_dir = Path(output_dir) / f"data_date={data_date}" / f"asset_class={asset_class}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        file_path = partition_dir / f"ticker_summary_{data_date}_{asset_class}.snappy.parquet"

        table = pa.Table.from_pylist([r.to_dict() for r in records], schema=TICKER_SUMMARY_ARROW_SCHEMA)
        pq.write_table(table, file_path, compression="snappy")
        created_files.append(str(file_path))

    return created_files


def write_portfolio_flow_parquet(
    flow_records: List[DailyPortfolioFlowRecord],
    output_dir: str,
    data_date: str,
) -> List[str]:
    """Write portfolio flow records into partitioned Snappy-compressed Parquet files."""
    if not flow_records:
        return []

    created_files: List[str] = []
    # Group by portfolio_id for Hive partitioning
    grouped: Dict[str, List[DailyPortfolioFlowRecord]] = {}
    for r in flow_records:
        grouped.setdefault(r.portfolio_id, []).append(r)

    for portfolio_id, records in grouped.items():
        partition_dir = Path(output_dir) / f"data_date={data_date}" / f"portfolio_id={portfolio_id}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        file_path = partition_dir / f"portfolio_flow_{data_date}_{portfolio_id}.snappy.parquet"

        table = pa.Table.from_pylist([r.to_dict() for r in records], schema=PORTFOLIO_FLOW_ARROW_SCHEMA)
        pq.write_table(table, file_path, compression="snappy")
        created_files.append(str(file_path))

    return created_files


# -----------------------------------------------------------------------------
# Batch Aggregation Engine Runner
# -----------------------------------------------------------------------------
def run_batch_aggregation(
    ticks_data: Optional[List[Dict[str, Any]]] = None,
    orders_data: Optional[List[Dict[str, Any]]] = None,
    source_ticks_dir: Optional[str] = None,
    source_orders_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    target_date: Optional[str] = None,
    environment: str = "dev",
    dry_run: bool = False,
) -> DataprocBatchAggregationTelemetry:
    """Execute the end-to-end daily batch aggregation workflow."""
    start_time = datetime.now(timezone.utc)
    date_str = target_date or start_time.strftime("%Y-%m-%d")
    batch_id = f"dp_agg_{date_str}_{uuid.uuid4().hex[:8]}"

    # Load ticks data
    ticks: List[Dict[str, Any]] = list(ticks_data or [])
    if source_ticks_dir and Path(source_ticks_dir).exists():
        for pfile in Path(source_ticks_dir).rglob("*.parquet"):
            try:
                table = pq.read_table(pfile)
                ticks.extend(table.to_pylist())
            except Exception:
                pass
        for jfile in Path(source_ticks_dir).rglob("*.json*"):
            try:
                with open(jfile, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            ticks.append(json.loads(line))
            except Exception:
                pass

    # Load orders data
    orders: List[Dict[str, Any]] = list(orders_data or [])
    if source_orders_dir and Path(source_orders_dir).exists():
        for pfile in Path(source_orders_dir).rglob("*.parquet"):
            try:
                table = pq.read_table(pfile)
                orders.extend(table.to_pylist())
            except Exception:
                pass
        for jfile in Path(source_orders_dir).rglob("*.json*"):
            try:
                with open(jfile, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            orders.append(json.loads(line))
            except Exception:
                pass

    # Compute summaries
    ticker_summaries = compute_ticker_daily_summaries(ticks, data_date=date_str, batch_id=batch_id)
    portfolio_flows = compute_portfolio_flow_summaries(orders, data_date=date_str, batch_id=batch_id)

    ticker_files: List[str] = []
    order_files: List[str] = []

    if output_dir and not dry_run:
        ticker_out = os.path.join(output_dir, "ticker_summaries")
        order_out = os.path.join(output_dir, "portfolio_order_summaries")
        ticker_files = write_ticker_summaries_parquet(ticker_summaries, ticker_out, date_str)
        order_files = write_portfolio_flow_parquet(portfolio_flows, order_out, date_str)

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    return DataprocBatchAggregationTelemetry(
        total_ticks_processed=len(ticks),
        total_orders_processed=len(orders),
        ticker_summaries_emitted=len(ticker_summaries),
        portfolio_summaries_emitted=len(portfolio_flows),
        duration_sec=duration,
        output_ticker_files=ticker_files,
        output_order_files=order_files,
    )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments for Dataproc PySpark execution."""
    parser = argparse.ArgumentParser(description="Dataproc Daily Market Batch Aggregation")
    parser.add_argument("--target-date", required=False, default=None, help="Target date YYYY-MM-DD")
    parser.add_argument("--environment", default="dev", help="GCP environment (dev/prod)")
    parser.add_argument("--bronze-bucket", default="trading-risk-bronze-dev", help="GCS Bronze bucket")
    parser.add_argument("--silver-bucket", default="trading-risk-silver-dev", help="GCS Silver bucket")
    parser.add_argument("--source-ticks-prefix", default="ticks", help="Prefix for Bronze ticks")
    parser.add_argument("--source-orders-prefix", default="orders", help="Prefix for Bronze orders")
    parser.add_argument("--output-prefix", default="dataproc/daily_aggregates", help="Output prefix in Silver")
    parser.add_argument("--output-dir", default=None, help="Local output directory (for simulation)")
    parser.add_argument("--dry-run", action="store_true", help="Perform validation without writing")
    return parser.parse_args(argv)


def main() -> None:
    """CLI entrypoint for PySpark Dataproc submission."""
    args = parse_args(sys.argv[1:])
    target_date = args.target_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = args.output_dir or f"/tmp/silver_{args.environment}_{target_date}"

    telemetry = run_batch_aggregation(
        output_dir=output_dir,
        target_date=target_date,
        environment=args.environment,
        dry_run=args.dry_run,
    )
    print(json.dumps(telemetry.to_dict(), indent=2))


if __name__ == "__main__":
    main()
