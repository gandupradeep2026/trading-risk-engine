"""Data Schemas and Quality Contracts for Databricks Delta Bronze Layer.

Defines:
- Explicit Spark SQL DDL schemas for Delta Bronze tables.
- PyArrow schemas for high-performance zero-cost local Parquet/Delta writing.
- Record validators and bad-record quarantine definitions.
- Common audit column specifications (`_ingested_at`, `_source_file`, `_raw_record_hash`, `_ingest_batch_id`, `_ingest_date`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional, Tuple
import pyarrow as pa


# -----------------------------------------------------------------------------
# 1. SPARK SQL DDL SCHEMAS FOR DELTA LAKE BRONZE TABLES
# -----------------------------------------------------------------------------

DELTA_BRONZE_TICK_DDL = """
    ticker STRING NOT NULL,
    bid DOUBLE NOT NULL,
    ask DOUBLE NOT NULL,
    last_price DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    timestamp STRING NOT NULL,
    asset_class STRING NOT NULL,
    sequence_number BIGINT NOT NULL,
    source STRING,
    spread DOUBLE,
    mid_price DOUBLE,
    notional_usd DOUBLE,
    relative_spread_bps DOUBLE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT,
    _ingested_at TIMESTAMP NOT NULL,
    _source_file STRING NOT NULL,
    _raw_record_hash STRING NOT NULL,
    _ingest_batch_id STRING NOT NULL,
    _ingest_date DATE NOT NULL
""".strip()

DELTA_BRONZE_ORDER_DDL = """
    order_id STRING NOT NULL,
    portfolio_id STRING NOT NULL,
    ticker STRING NOT NULL,
    side STRING NOT NULL,
    quantity DOUBLE NOT NULL,
    price DOUBLE NOT NULL,
    order_type STRING NOT NULL,
    timestamp STRING NOT NULL,
    trader_id STRING,
    execution_status STRING,
    sequence_number BIGINT,
    notional_usd DOUBLE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _source_file STRING NOT NULL,
    _raw_record_hash STRING NOT NULL,
    _ingest_batch_id STRING NOT NULL,
    _ingest_date DATE NOT NULL
""".strip()

DELTA_BRONZE_VWAP_DDL = """
    ticker STRING NOT NULL,
    asset_class STRING NOT NULL,
    window_start STRING NOT NULL,
    window_end STRING NOT NULL,
    tick_count BIGINT NOT NULL,
    open_price DOUBLE NOT NULL,
    high_price DOUBLE NOT NULL,
    low_price DOUBLE NOT NULL,
    close_price DOUBLE NOT NULL,
    total_volume DOUBLE NOT NULL,
    total_notional DOUBLE NOT NULL,
    vwap DOUBLE NOT NULL,
    price_stddev DOUBLE NOT NULL,
    price_variance DOUBLE NOT NULL,
    volatility_multiplier DOUBLE NOT NULL,
    upper_band DOUBLE NOT NULL,
    lower_band DOUBLE NOT NULL,
    bandwidth_bps DOUBLE NOT NULL,
    percent_b DOUBLE NOT NULL,
    is_upper_breach BOOLEAN NOT NULL,
    is_lower_breach BOOLEAN NOT NULL,
    volatility_spike BOOLEAN NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT,
    _ingested_at TIMESTAMP NOT NULL,
    _source_file STRING NOT NULL,
    _raw_record_hash STRING NOT NULL,
    _ingest_batch_id STRING NOT NULL,
    _ingest_date DATE NOT NULL
""".strip()


# -----------------------------------------------------------------------------
# 2. PYARROW SCHEMAS FOR ZERO-COST HERMETIC LOCAL PROCESSING
# -----------------------------------------------------------------------------

ARROW_DELTA_BRONZE_TICK_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("bid", pa.float64()),
    ("ask", pa.float64()),
    ("last_price", pa.float64()),
    ("volume", pa.float64()),
    ("timestamp", pa.string()),
    ("asset_class", pa.string()),
    ("sequence_number", pa.int64()),
    ("source", pa.string()),
    ("spread", pa.float64()),
    ("mid_price", pa.float64()),
    ("notional_usd", pa.float64()),
    ("relative_spread_bps", pa.float64()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("hour", pa.int32()),
    ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ("_source_file", pa.string()),
    ("_raw_record_hash", pa.string()),
    ("_ingest_batch_id", pa.string()),
    ("_ingest_date", pa.date32()),
])

ARROW_DELTA_BRONZE_ORDER_SCHEMA = pa.schema([
    ("order_id", pa.string()),
    ("portfolio_id", pa.string()),
    ("ticker", pa.string()),
    ("side", pa.string()),
    ("quantity", pa.float64()),
    ("price", pa.float64()),
    ("order_type", pa.string()),
    ("timestamp", pa.string()),
    ("trader_id", pa.string()),
    ("execution_status", pa.string()),
    ("sequence_number", pa.int64()),
    ("notional_usd", pa.float64()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ("_source_file", pa.string()),
    ("_raw_record_hash", pa.string()),
    ("_ingest_batch_id", pa.string()),
    ("_ingest_date", pa.date32()),
])

ARROW_DELTA_BRONZE_VWAP_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("asset_class", pa.string()),
    ("window_start", pa.string()),
    ("window_end", pa.string()),
    ("tick_count", pa.int64()),
    ("open_price", pa.float64()),
    ("high_price", pa.float64()),
    ("low_price", pa.float64()),
    ("close_price", pa.float64()),
    ("total_volume", pa.float64()),
    ("total_notional", pa.float64()),
    ("vwap", pa.float64()),
    ("price_stddev", pa.float64()),
    ("price_variance", pa.float64()),
    ("volatility_multiplier", pa.float64()),
    ("upper_band", pa.float64()),
    ("lower_band", pa.float64()),
    ("bandwidth_bps", pa.float64()),
    ("percent_b", pa.float64()),
    ("is_upper_breach", pa.bool_()),
    ("is_lower_breach", pa.bool_()),
    ("volatility_spike", pa.bool_()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("hour", pa.int32()),
    ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ("_source_file", pa.string()),
    ("_raw_record_hash", pa.string()),
    ("_ingest_batch_id", pa.string()),
    ("_ingest_date", pa.date32()),
])


# -----------------------------------------------------------------------------
# 3. QUARANTINE MODEL & VALIDATION LOGIC
# -----------------------------------------------------------------------------

@dataclass
class QuarantinedBronzeRecord:
    """Represents a corrupt, malformed, or invariant-violating record quarantined during Bronze ingestion."""

    record_index: int
    raw_payload: str
    error_category: str
    error_message: str
    source_file: str
    quarantined_at: str
    batch_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert quarantined record to dictionary for JSON serialization."""
        return {
            "record_index": self.record_index,
            "raw_payload": self.raw_payload,
            "error_category": self.error_category,
            "error_message": self.error_message,
            "source_file": self.source_file,
            "quarantined_at": self.quarantined_at,
            "batch_id": self.batch_id,
        }


def validate_bronze_tick_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate financial tick invariants. Returns (is_valid, error_category, error_message)."""
    required_fields = ["ticker", "bid", "ask", "last_price", "volume", "timestamp", "asset_class"]
    for field in required_fields:
        if field not in record or record[field] is None:
            return False, "MISSING_REQUIRED_FIELD", f"Missing required field: '{field}'"

    try:
        bid = float(record["bid"])
        ask = float(record["ask"])
        last_price = float(record["last_price"])
        volume = float(record["volume"])
    except (ValueError, TypeError) as exc:
        return False, "INVALID_NUMERIC_TYPE", f"Failed to parse numeric price or volume: {exc}"

    if bid <= 0.0 or ask <= 0.0 or last_price <= 0.0:
        return False, "NON_POSITIVE_PRICE", f"Prices must be strictly positive (bid={bid}, ask={ask}, last={last_price})"

    if bid > ask:
        return False, "CROSSED_QUOTES", f"Crossed market quotes detected: bid ({bid}) > ask ({ask})"

    if volume < 0.0:
        return False, "NEGATIVE_VOLUME", f"Volume cannot be negative: {volume}"

    return True, None, None


def validate_bronze_order_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate trade order invariants. Returns (is_valid, error_category, error_message)."""
    required_fields = ["order_id", "portfolio_id", "ticker", "side", "quantity", "price", "order_type", "timestamp"]
    for field in required_fields:
        if field not in record or record[field] is None:
            return False, "MISSING_REQUIRED_FIELD", f"Missing required field: '{field}'"

    side = str(record["side"]).upper()
    if side not in {"BUY", "SELL"}:
        return False, "INVALID_SIDE", f"Side must be BUY or SELL, got '{side}'"

    order_type = str(record["order_type"]).upper()
    valid_types = {"MARKET", "LIMIT", "STOP_LOSS", "STOP_LIMIT"}
    if order_type not in valid_types:
        return False, "INVALID_ORDER_TYPE", f"Order type must be one of {valid_types}, got '{order_type}'"

    try:
        qty = float(record["quantity"])
        price = float(record["price"])
    except (ValueError, TypeError) as exc:
        return False, "INVALID_NUMERIC_TYPE", f"Failed to parse numeric quantity or price: {exc}"

    if qty <= 0.0:
        return False, "NON_POSITIVE_QUANTITY", f"Quantity must be positive, got {qty}"

    if price <= 0.0:
        return False, "NON_POSITIVE_PRICE", f"Price must be positive, got {price}"

    return True, None, None


def validate_bronze_vwap_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate rolling VWAP aggregations. Returns (is_valid, error_category, error_message)."""
    required_fields = ["ticker", "asset_class", "window_start", "window_end", "vwap", "total_volume"]
    for field in required_fields:
        if field not in record or record[field] is None:
            return False, "MISSING_REQUIRED_FIELD", f"Missing required field: '{field}'"

    try:
        vwap = float(record["vwap"])
        volume = float(record["total_volume"])
    except (ValueError, TypeError) as exc:
        return False, "INVALID_NUMERIC_TYPE", f"Failed to parse numeric VWAP or volume: {exc}"

    if vwap <= 0.0:
        return False, "NON_POSITIVE_VWAP", f"VWAP must be positive, got {vwap}"

    if volume < 0.0:
        return False, "NEGATIVE_VOLUME", f"Volume cannot be negative: {volume}"

    return True, None, None
