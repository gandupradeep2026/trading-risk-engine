"""Data Schemas and Quality Contracts for Databricks Delta Silver Layer.

Defines:
- Explicit Spark SQL DDL schemas for Delta Silver tables.
- PyArrow schemas for high-performance zero-cost local Parquet/Delta processing.
- Record cleansing, schema enforcement, domain validation, and bad-record quarantine definitions.
- Derived quantitative microstructure enrichments (relative spreads, quote conditions,
  notional tiers, aggressive order indicators, volatility regimes, trend directions).
- Lineage audit specifications (_cleansed_at, _cleansed_record_hash, _cleansing_batch_id).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
import pyarrow as pa


# -----------------------------------------------------------------------------
# 1. SPARK SQL DDL SCHEMAS FOR DELTA LAKE SILVER TABLES
# -----------------------------------------------------------------------------

DELTA_SILVER_TICK_DDL = """
    ticker STRING NOT NULL,
    asset_class STRING NOT NULL,
    bid DOUBLE NOT NULL,
    ask DOUBLE NOT NULL,
    last_price DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    timestamp STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    sequence_number BIGINT NOT NULL,
    source STRING NOT NULL,
    spread DOUBLE NOT NULL,
    mid_price DOUBLE NOT NULL,
    notional_usd DOUBLE NOT NULL,
    relative_spread_bps DOUBLE NOT NULL,
    quote_condition STRING NOT NULL,
    is_crossed_quote BOOLEAN NOT NULL,
    price_return DOUBLE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _cleansed_at TIMESTAMP NOT NULL,
    _source_file STRING NOT NULL,
    _raw_record_hash STRING NOT NULL,
    _cleansed_record_hash STRING NOT NULL,
    _cleansing_batch_id STRING NOT NULL,
    _ingest_date DATE NOT NULL
""".strip()

DELTA_SILVER_ORDER_DDL = """
    order_id STRING NOT NULL,
    portfolio_id STRING NOT NULL,
    ticker STRING NOT NULL,
    side STRING NOT NULL,
    order_type STRING NOT NULL,
    quantity DOUBLE NOT NULL,
    price DOUBLE NOT NULL,
    notional_usd DOUBLE NOT NULL,
    timestamp STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    trader_id STRING NOT NULL,
    execution_status STRING NOT NULL,
    sequence_number BIGINT NOT NULL,
    order_category STRING NOT NULL,
    notional_tier STRING NOT NULL,
    is_aggressive BOOLEAN NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _cleansed_at TIMESTAMP NOT NULL,
    _source_file STRING NOT NULL,
    _raw_record_hash STRING NOT NULL,
    _cleansed_record_hash STRING NOT NULL,
    _cleansing_batch_id STRING NOT NULL,
    _ingest_date DATE NOT NULL
""".strip()

DELTA_SILVER_VWAP_DDL = """
    ticker STRING NOT NULL,
    asset_class STRING NOT NULL,
    window_start STRING NOT NULL,
    window_end STRING NOT NULL,
    window_start_timestamp TIMESTAMP NOT NULL,
    window_end_timestamp TIMESTAMP NOT NULL,
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
    volatility_regime STRING NOT NULL,
    trend STRING NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _cleansed_at TIMESTAMP NOT NULL,
    _source_file STRING NOT NULL,
    _raw_record_hash STRING NOT NULL,
    _cleansed_record_hash STRING NOT NULL,
    _cleansing_batch_id STRING NOT NULL,
    _ingest_date DATE NOT NULL
""".strip()


# -----------------------------------------------------------------------------
# 2. PYARROW SCHEMAS FOR ZERO-COST HERMETIC LOCAL PROCESSING
# -----------------------------------------------------------------------------

ARROW_DELTA_SILVER_TICK_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("asset_class", pa.string()),
    ("bid", pa.float64()),
    ("ask", pa.float64()),
    ("last_price", pa.float64()),
    ("volume", pa.float64()),
    ("timestamp", pa.string()),
    ("event_timestamp", pa.timestamp("us", tz="UTC")),
    ("sequence_number", pa.int64()),
    ("source", pa.string()),
    ("spread", pa.float64()),
    ("mid_price", pa.float64()),
    ("notional_usd", pa.float64()),
    ("relative_spread_bps", pa.float64()),
    ("quote_condition", pa.string()),
    ("is_crossed_quote", pa.bool_()),
    ("price_return", pa.float64()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("hour", pa.int32()),
    ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ("_cleansed_at", pa.timestamp("us", tz="UTC")),
    ("_source_file", pa.string()),
    ("_raw_record_hash", pa.string()),
    ("_cleansed_record_hash", pa.string()),
    ("_cleansing_batch_id", pa.string()),
    ("_ingest_date", pa.date32()),
])

ARROW_DELTA_SILVER_ORDER_SCHEMA = pa.schema([
    ("order_id", pa.string()),
    ("portfolio_id", pa.string()),
    ("ticker", pa.string()),
    ("side", pa.string()),
    ("order_type", pa.string()),
    ("quantity", pa.float64()),
    ("price", pa.float64()),
    ("notional_usd", pa.float64()),
    ("timestamp", pa.string()),
    ("event_timestamp", pa.timestamp("us", tz="UTC")),
    ("trader_id", pa.string()),
    ("execution_status", pa.string()),
    ("sequence_number", pa.int64()),
    ("order_category", pa.string()),
    ("notional_tier", pa.string()),
    ("is_aggressive", pa.bool_()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ("_cleansed_at", pa.timestamp("us", tz="UTC")),
    ("_source_file", pa.string()),
    ("_raw_record_hash", pa.string()),
    ("_cleansed_record_hash", pa.string()),
    ("_cleansing_batch_id", pa.string()),
    ("_ingest_date", pa.date32()),
])

ARROW_DELTA_SILVER_VWAP_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("asset_class", pa.string()),
    ("window_start", pa.string()),
    ("window_end", pa.string()),
    ("window_start_timestamp", pa.timestamp("us", tz="UTC")),
    ("window_end_timestamp", pa.timestamp("us", tz="UTC")),
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
    ("volatility_regime", pa.string()),
    ("trend", pa.string()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("hour", pa.int32()),
    ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ("_cleansed_at", pa.timestamp("us", tz="UTC")),
    ("_source_file", pa.string()),
    ("_raw_record_hash", pa.string()),
    ("_cleansed_record_hash", pa.string()),
    ("_cleansing_batch_id", pa.string()),
    ("_ingest_date", pa.date32()),
])


# -----------------------------------------------------------------------------
# 3. QUARANTINE MODEL
# -----------------------------------------------------------------------------

@dataclass
class QuarantinedSilverRecord:
    """Represents a corrupt or invariant-violating record isolated during Silver cleansing."""

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


# -----------------------------------------------------------------------------
# 4. PARSING & TIMESTAMP UTILITIES
# -----------------------------------------------------------------------------

def parse_iso_or_epoch_timestamp(ts: Any) -> datetime:
    """Parse string, numeric, or datetime into UTC datetime."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    if isinstance(ts, (int, float)):
        # Check if milliseconds or microseconds
        if ts > 1e14:  # microseconds
            return datetime.fromtimestamp(ts / 1e6, tz=timezone.utc)
        elif ts > 1e11:  # milliseconds
            return datetime.fromtimestamp(ts / 1e3, tz=timezone.utc)
        else:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)

    s = str(ts).strip()
    # Normalize Z to +00:00
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        # Fallback regex parsing
        clean_s = re.sub(r"[^\d\-T:\.+ ]", "", s)
        dt = datetime.fromisoformat(clean_s.replace(" ", "T"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)


def compute_cleansed_hash(record: Dict[str, Any]) -> str:
    """Compute deterministic SHA-256 hash across cleansed business attributes."""
    excluded = {
        "_ingested_at",
        "_cleansed_at",
        "_source_file",
        "_raw_record_hash",
        "_cleansed_record_hash",
        "_cleansing_batch_id",
        "_ingest_date",
        "year",
        "month",
        "day",
        "hour",
    }
    payload = {
        k: v
        for k, v in sorted(record.items())
        if k not in excluded and not k.startswith("_")
    }
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


# -----------------------------------------------------------------------------
# 5. DOMAIN VALIDATION AND RECORD ENRICHMENT
# -----------------------------------------------------------------------------

def validate_silver_tick_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate financial tick invariants for Silver tier.

    Enforces:
    - Required non-empty string fields
    - Positive bid, ask, last_price
    - Non-negative volume
    - Non-crossed quotes (bid <= ask)
    """
    required = ["ticker", "bid", "ask", "last_price", "volume", "timestamp", "asset_class"]
    for field_name in required:
        val = record.get(field_name)
        if val is None or (isinstance(val, str) and not val.strip()):
            return False, "MISSING_REQUIRED_FIELD", f"Missing or blank required field: '{field_name}'"

    try:
        bid = float(record["bid"])
        ask = float(record["ask"])
        last_price = float(record["last_price"])
        volume = float(record["volume"])
    except (ValueError, TypeError) as exc:
        return False, "INVALID_NUMERIC_TYPE", f"Failed to parse numeric value: {exc}"

    if bid <= 0.0 or ask <= 0.0 or last_price <= 0.0:
        return False, "NON_POSITIVE_PRICE", f"Prices must be strictly positive (bid={bid}, ask={ask}, last={last_price})"

    if bid > ask:
        return False, "CROSSED_QUOTES", f"Crossed quotes detected: bid ({bid}) > ask ({ask})"

    if volume < 0.0:
        return False, "NEGATIVE_VOLUME", f"Volume cannot be negative: {volume}"

    try:
        _ = parse_iso_or_epoch_timestamp(record["timestamp"])
    except Exception as exc:
        return False, "INVALID_TIMESTAMP", f"Unable to parse timestamp '{record['timestamp']}': {exc}"

    return True, None, None


def cleanse_and_enrich_tick(
    raw_record: Dict[str, Any],
    source_file: str,
    batch_id: str,
    cleansed_at: datetime,
    ingest_date: Optional[date] = None,
    previous_price: Optional[float] = None,
) -> Dict[str, Any]:
    """Cleanse, type-cast, schema-enforce, and enrich a market tick record."""
    ticker = str(raw_record["ticker"]).strip().upper()
    asset_class = str(raw_record["asset_class"]).strip().upper()
    bid = float(raw_record["bid"])
    ask = float(raw_record["ask"])
    last_price = float(raw_record["last_price"])
    volume = float(raw_record["volume"])
    raw_ts = str(raw_record["timestamp"]).strip()
    event_timestamp = parse_iso_or_epoch_timestamp(raw_ts)

    seq_num = int(raw_record.get("sequence_number", 0))
    source = str(raw_record.get("source") or "STREAM").strip().upper()

    # Financial Microstructure Enrichments
    spread = round(ask - bid, 6)
    mid_price = round((bid + ask) / 2.0, 6)
    relative_spread_bps = round((spread / mid_price) * 10000.0, 4) if mid_price > 0 else 0.0
    notional_usd = round(last_price * volume, 4)

    if relative_spread_bps <= 5.0:
        quote_condition = "TIGHT_SPREAD"
    elif relative_spread_bps > 50.0:
        quote_condition = "WIDE_SPREAD"
    else:
        quote_condition = "REGULAR"

    price_return = None
    if previous_price is not None and previous_price > 0:
        price_return = round((last_price - previous_price) / previous_price, 6)

    # Date partitions
    year = event_timestamp.year
    month = event_timestamp.month
    day = event_timestamp.day
    hour = event_timestamp.hour
    active_date = ingest_date or event_timestamp.date()

    ingested_at = raw_record.get("_ingested_at")
    if ingested_at:
        ingested_at_dt = parse_iso_or_epoch_timestamp(ingested_at)
    else:
        ingested_at_dt = event_timestamp

    raw_hash = str(raw_record.get("_raw_record_hash") or "")

    cleansed: Dict[str, Any] = {
        "ticker": ticker,
        "asset_class": asset_class,
        "bid": bid,
        "ask": ask,
        "last_price": last_price,
        "volume": volume,
        "timestamp": raw_ts,
        "event_timestamp": event_timestamp,
        "sequence_number": seq_num,
        "source": source,
        "spread": spread,
        "mid_price": mid_price,
        "notional_usd": notional_usd,
        "relative_spread_bps": relative_spread_bps,
        "quote_condition": quote_condition,
        "is_crossed_quote": False,
        "price_return": price_return,
        "year": year,
        "month": month,
        "day": day,
        "hour": hour,
        "_ingested_at": ingested_at_dt,
        "_cleansed_at": cleansed_at,
        "_source_file": source_file,
        "_raw_record_hash": raw_hash,
        "_cleansing_batch_id": batch_id,
        "_ingest_date": active_date,
    }
    cleansed["_cleansed_record_hash"] = compute_cleansed_hash(cleansed)
    return cleansed


def validate_silver_order_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate trade order invariants for Silver tier."""
    required = ["order_id", "portfolio_id", "ticker", "side", "quantity", "price", "order_type", "timestamp"]
    for field_name in required:
        val = record.get(field_name)
        if val is None or (isinstance(val, str) and not val.strip()):
            return False, "MISSING_REQUIRED_FIELD", f"Missing or blank required field: '{field_name}'"

    side = str(record["side"]).strip().upper()
    if side not in {"BUY", "SELL"}:
        return False, "INVALID_SIDE", f"Side must be BUY or SELL, got '{side}'"

    order_type = str(record["order_type"]).strip().upper()
    valid_types = {"MARKET", "LIMIT", "STOP_LOSS", "STOP_LIMIT"}
    if order_type not in valid_types:
        return False, "INVALID_ORDER_TYPE", f"Order type must be one of {valid_types}, got '{order_type}'"

    try:
        qty = float(record["quantity"])
        price = float(record["price"])
    except (ValueError, TypeError) as exc:
        return False, "INVALID_NUMERIC_TYPE", f"Failed to parse numeric quantity or price: {exc}"

    if qty <= 0.0:
        return False, "NON_POSITIVE_QUANTITY", f"Quantity must be strictly positive, got {qty}"

    if price <= 0.0:
        return False, "NON_POSITIVE_PRICE", f"Price must be strictly positive, got {price}"

    try:
        _ = parse_iso_or_epoch_timestamp(record["timestamp"])
    except Exception as exc:
        return False, "INVALID_TIMESTAMP", f"Unable to parse timestamp '{record['timestamp']}': {exc}"

    return True, None, None


def cleanse_and_enrich_order(
    raw_record: Dict[str, Any],
    source_file: str,
    batch_id: str,
    cleansed_at: datetime,
    ingest_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Cleanse, type-cast, schema-enforce, and enrich a trade order record."""
    order_id = str(raw_record["order_id"]).strip()
    portfolio_id = str(raw_record["portfolio_id"]).strip().upper()
    ticker = str(raw_record["ticker"]).strip().upper()
    side = str(raw_record["side"]).strip().upper()
    order_type = str(raw_record["order_type"]).strip().upper()
    quantity = float(raw_record["quantity"])
    price = float(raw_record["price"])
    notional_usd = round(quantity * price, 4)
    raw_ts = str(raw_record["timestamp"]).strip()
    event_timestamp = parse_iso_or_epoch_timestamp(raw_ts)

    trader_id = str(raw_record.get("trader_id") or "UNKNOWN").strip().upper()
    execution_status = str(raw_record.get("execution_status") or "FILLED").strip().upper()
    seq_num = int(raw_record.get("sequence_number", 0))

    # Categorization enrichments
    if ticker in {"BTCUSD", "ETHUSD", "SOLUSD"}:
        order_category = "CRYPTO_ORDER"
    elif ticker in {"EURUSD", "GBPUSD", "USDJPY"}:
        order_category = "FX_ORDER"
    elif ticker in {"SPY", "QQQ", "IWM"}:
        order_category = "ETF_ORDER"
    elif ticker in {"XAUUSD", "BRENT", "WTI"}:
        order_category = "COMMODITY_ORDER"
    else:
        order_category = "EQUITY_ORDER"

    # Notional Tier
    if notional_usd < 10000.0:
        notional_tier = "MICRO"
    elif notional_usd < 100000.0:
        notional_tier = "STANDARD"
    elif notional_usd < 500000.0:
        notional_tier = "LARGE"
    else:
        notional_tier = "BLOCK"

    is_aggressive = (order_type == "MARKET")

    year = event_timestamp.year
    month = event_timestamp.month
    day = event_timestamp.day
    active_date = ingest_date or event_timestamp.date()

    ingested_at = raw_record.get("_ingested_at")
    if ingested_at:
        ingested_at_dt = parse_iso_or_epoch_timestamp(ingested_at)
    else:
        ingested_at_dt = event_timestamp

    raw_hash = str(raw_record.get("_raw_record_hash") or "")

    cleansed: Dict[str, Any] = {
        "order_id": order_id,
        "portfolio_id": portfolio_id,
        "ticker": ticker,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
        "price": price,
        "notional_usd": notional_usd,
        "timestamp": raw_ts,
        "event_timestamp": event_timestamp,
        "trader_id": trader_id,
        "execution_status": execution_status,
        "sequence_number": seq_num,
        "order_category": order_category,
        "notional_tier": notional_tier,
        "is_aggressive": is_aggressive,
        "year": year,
        "month": month,
        "day": day,
        "_ingested_at": ingested_at_dt,
        "_cleansed_at": cleansed_at,
        "_source_file": source_file,
        "_raw_record_hash": raw_hash,
        "_cleansing_batch_id": batch_id,
        "_ingest_date": active_date,
    }
    cleansed["_cleansed_record_hash"] = compute_cleansed_hash(cleansed)
    return cleansed


def validate_silver_vwap_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate rolling VWAP aggregations for Silver tier."""
    required = ["ticker", "asset_class", "window_start", "window_end", "vwap", "total_volume"]
    for field_name in required:
        val = record.get(field_name)
        if val is None or (isinstance(val, str) and not val.strip()):
            return False, "MISSING_REQUIRED_FIELD", f"Missing or blank required field: '{field_name}'"

    try:
        vwap = float(record["vwap"])
        volume = float(record["total_volume"])
    except (ValueError, TypeError) as exc:
        return False, "INVALID_NUMERIC_TYPE", f"Failed to parse numeric VWAP or volume: {exc}"

    if vwap <= 0.0:
        return False, "NON_POSITIVE_VWAP", f"VWAP must be strictly positive, got {vwap}"

    if volume < 0.0:
        return False, "NEGATIVE_VOLUME", f"Volume cannot be negative: {volume}"

    try:
        _ = parse_iso_or_epoch_timestamp(record["window_start"])
        _ = parse_iso_or_epoch_timestamp(record["window_end"])
    except Exception as exc:
        return False, "INVALID_TIMESTAMP", f"Unable to parse window timestamps: {exc}"

    return True, None, None


def cleanse_and_enrich_vwap(
    raw_record: Dict[str, Any],
    source_file: str,
    batch_id: str,
    cleansed_at: datetime,
    ingest_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Cleanse, type-cast, schema-enforce, and enrich a rolling VWAP record."""
    ticker = str(raw_record["ticker"]).strip().upper()
    asset_class = str(raw_record["asset_class"]).strip().upper()
    raw_start = str(raw_record["window_start"]).strip()
    raw_end = str(raw_record["window_end"]).strip()
    start_ts = parse_iso_or_epoch_timestamp(raw_start)
    end_ts = parse_iso_or_epoch_timestamp(raw_end)

    tick_count = int(raw_record.get("tick_count", 1))
    open_p = float(raw_record.get("open_price", raw_record["vwap"]))
    high_p = float(raw_record.get("high_price", raw_record["vwap"]))
    low_p = float(raw_record.get("low_price", raw_record["vwap"]))
    close_p = float(raw_record.get("close_price", raw_record["vwap"]))
    total_vol = float(raw_record.get("total_volume", 0.0))
    total_notional = float(raw_record.get("total_notional", total_vol * float(raw_record["vwap"])))
    vwap = float(raw_record["vwap"])
    price_stddev = float(raw_record.get("price_stddev", 0.0))
    price_variance = float(raw_record.get("price_variance", price_stddev ** 2))
    vol_multiplier = float(raw_record.get("volatility_multiplier", 2.0))
    upper_band = float(raw_record.get("upper_band", vwap + vol_multiplier * price_stddev))
    lower_band = float(raw_record.get("lower_band", vwap - vol_multiplier * price_stddev))
    bandwidth_bps = float(raw_record.get("bandwidth_bps", ((upper_band - lower_band) / vwap) * 10000.0 if vwap > 0 else 0.0))
    band_width = upper_band - lower_band
    percent_b = float(raw_record.get("percent_b", (close_p - lower_band) / band_width if band_width > 0 else 0.5))

    is_upper_breach = bool(raw_record.get("is_upper_breach", close_p > upper_band))
    is_lower_breach = bool(raw_record.get("is_lower_breach", close_p < lower_band))
    volatility_spike = bool(raw_record.get("volatility_spike", (price_stddev / vwap) >= 0.03 if vwap > 0 else False))

    # Regimes and trends
    if bandwidth_bps < 10.0:
        volatility_regime = "LOW"
    elif bandwidth_bps < 50.0:
        volatility_regime = "NORMAL"
    elif bandwidth_bps < 150.0:
        volatility_regime = "HIGH"
    else:
        volatility_regime = "EXTREME"

    if close_p > open_p:
        trend = "BULLISH"
    elif close_p < open_p:
        trend = "BEARISH"
    else:
        trend = "NEUTRAL"

    year = end_ts.year
    month = end_ts.month
    day = end_ts.day
    hour = end_ts.hour
    active_date = ingest_date or end_ts.date()

    ingested_at = raw_record.get("_ingested_at")
    if ingested_at:
        ingested_at_dt = parse_iso_or_epoch_timestamp(ingested_at)
    else:
        ingested_at_dt = end_ts

    raw_hash = str(raw_record.get("_raw_record_hash") or "")

    cleansed: Dict[str, Any] = {
        "ticker": ticker,
        "asset_class": asset_class,
        "window_start": raw_start,
        "window_end": raw_end,
        "window_start_timestamp": start_ts,
        "window_end_timestamp": end_ts,
        "tick_count": tick_count,
        "open_price": open_p,
        "high_price": high_p,
        "low_price": low_p,
        "close_price": close_p,
        "total_volume": total_vol,
        "total_notional": total_notional,
        "vwap": vwap,
        "price_stddev": price_stddev,
        "price_variance": price_variance,
        "volatility_multiplier": vol_multiplier,
        "upper_band": upper_band,
        "lower_band": lower_band,
        "bandwidth_bps": bandwidth_bps,
        "percent_b": percent_b,
        "is_upper_breach": is_upper_breach,
        "is_lower_breach": is_lower_breach,
        "volatility_spike": volatility_spike,
        "volatility_regime": volatility_regime,
        "trend": trend,
        "year": year,
        "month": month,
        "day": day,
        "hour": hour,
        "_ingested_at": ingested_at_dt,
        "_cleansed_at": cleansed_at,
        "_source_file": source_file,
        "_raw_record_hash": raw_hash,
        "_cleansing_batch_id": batch_id,
        "_ingest_date": active_date,
    }
    cleansed["_cleansed_record_hash"] = compute_cleansed_hash(cleansed)
    return cleansed
