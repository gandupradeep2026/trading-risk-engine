"""Spark SQL Schema Definitions & Microstructure Record Validators for Market Ticks.

Defines explicit Spark Structured Streaming DDL schemas (preventing runtime schema
inference overhead), enriched market tick models, corrupt record quarantine structures,
and deterministic validation rules for continuous Kafka consumption.
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional, Tuple, Union

from ingestion.market_tick import ALLOWED_ASSET_CLASSES

# Explicit Spark SQL DDL Schema for `from_json` in Structured Streaming
MARKET_TICK_SCHEMA_DDL: str = (
    "trade_id STRING, "
    "ticker STRING, "
    "asset_class STRING, "
    "bid DOUBLE, "
    "ask DOUBLE, "
    "last_price DOUBLE, "
    "volume DOUBLE, "
    "spread DOUBLE, "
    "mid_price DOUBLE, "
    "timestamp STRING, "
    "sequence_number LONG, "
    "_corrupt_record STRING"
)

REQUIRED_TICK_FIELDS: Tuple[str, ...] = (
    "trade_id",
    "ticker",
    "asset_class",
    "bid",
    "ask",
    "last_price",
    "volume",
    "timestamp",
)

KAFKA_ENVELOPE_COLUMNS: Tuple[str, ...] = (
    "key",
    "value",
    "topic",
    "partition",
    "offset",
    "timestamp",
    "timestampType",
)


@dataclass(frozen=True)
class SparkFieldSpec:
    """Descriptor for a single Spark StructField."""

    name: str
    data_type: str
    nullable: bool = True


@dataclass(frozen=True)
class SparkStructTypeSpec:
    """Descriptor for a Spark StructType schema usable with or without a live JVM."""

    fields: Tuple[SparkFieldSpec, ...]

    @property
    def field_names(self) -> List[str]:
        """Return ordered list of field names in the schema."""
        return [f.name for f in self.fields]

    def to_ddl(self) -> str:
        """Export schema to Spark SQL DDL string."""
        return ", ".join(f"{f.name} {f.data_type}" for f in self.fields)


MARKET_TICK_STRUCT_SPEC = SparkStructTypeSpec(
    fields=(
        SparkFieldSpec("trade_id", "STRING", nullable=False),
        SparkFieldSpec("ticker", "STRING", nullable=False),
        SparkFieldSpec("asset_class", "STRING", nullable=False),
        SparkFieldSpec("bid", "DOUBLE", nullable=False),
        SparkFieldSpec("ask", "DOUBLE", nullable=False),
        SparkFieldSpec("last_price", "DOUBLE", nullable=False),
        SparkFieldSpec("volume", "DOUBLE", nullable=False),
        SparkFieldSpec("spread", "DOUBLE", nullable=True),
        SparkFieldSpec("mid_price", "DOUBLE", nullable=True),
        SparkFieldSpec("timestamp", "STRING", nullable=False),
        SparkFieldSpec("sequence_number", "LONG", nullable=True),
        SparkFieldSpec("_corrupt_record", "STRING", nullable=True),
    )
)


def get_pyspark_tick_schema() -> Any:
    """Return native pyspark.sql.types.StructType if PySpark is installed, else SparkStructTypeSpec."""
    try:
        from pyspark.sql.types import (
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        type_map = {
            "STRING": StringType(),
            "DOUBLE": DoubleType(),
            "LONG": LongType(),
        }
        return StructType(
            [
                StructField(f.name, type_map[f.data_type], nullable=f.nullable)
                for f in MARKET_TICK_STRUCT_SPEC.fields
            ]
        )
    except ImportError:
        return MARKET_TICK_STRUCT_SPEC


@dataclass
class ParsedTickRecord:
    """Validated and enriched market tick record emitted by the Spark streaming consumer."""

    trade_id: str
    ticker: str
    asset_class: str
    bid: float
    ask: float
    last_price: float
    volume: float
    spread: float
    mid_price: float
    notional_usd: float
    relative_spread_bps: float
    event_timestamp: str
    event_epoch_sec: float
    sequence_number: int
    year: int
    month: int
    day: int
    hour: int
    kafka_topic: str = "market.ticks.v1"
    kafka_partition: int = 0
    kafka_offset: int = 0
    kafka_key: Optional[str] = None
    ingested_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Serialize enriched tick record to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize enriched tick record to JSON string."""
        return json.dumps(self.to_dict())


@dataclass
class CorruptTickRecord:
    """Quarantined record representing an unparseable or invariant-violating Kafka message."""

    raw_payload: str
    error_type: str
    error_message: str
    kafka_topic: str = "market.ticks.v1"
    kafka_partition: int = 0
    kafka_offset: int = 0
    kafka_key: Optional[str] = None
    quarantined_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Serialize corrupt record to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize corrupt record to JSON string."""
        return json.dumps(self.to_dict())


def _parse_iso_timestamp(raw_ts: Any) -> Tuple[datetime, float, str]:
    """Parse an ISO-8601 string or epoch number into a UTC datetime, epoch seconds, and ISO string."""
    if isinstance(raw_ts, (int, float)):
        epoch_sec = float(raw_ts) if raw_ts < 1e11 else float(raw_ts) / 1000.0
        dt = datetime.fromtimestamp(epoch_sec, tz=timezone.utc)
        return dt, epoch_sec, dt.isoformat()

    if isinstance(raw_ts, str) and raw_ts.strip():
        cleaned = raw_ts.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt, dt.timestamp(), dt.isoformat()

    raise ValueError(f"Invalid or empty timestamp value: {raw_ts!r}")


def parse_and_validate_kafka_record(
    raw_value: Union[bytes, str, Dict[str, Any], None],
    kafka_key: Optional[Union[bytes, str]] = None,
    kafka_topic: str = "market.ticks.v1",
    kafka_partition: int = 0,
    kafka_offset: int = 0,
) -> Union[ParsedTickRecord, CorruptTickRecord]:
    """Decode, validate, and enrich a single raw Kafka tick message.

    Enforces:
    1. Valid UTF-8 encoding and JSON object structure.
    2. Presence of all required fields (`trade_id`, `ticker`, `asset_class`, `bid`, `ask`, `last_price`, `volume`, `timestamp`).
    3. Supported `asset_class` (`EQUITY`, `FX`, `COMMODITY`, `CRYPTO`, `ETF`).
    4. Strictly positive `bid`, `ask`, `last_price`, and `volume`.
    5. Non-crossed quotes (`bid <= ask`).
    6. Valid ISO-8601 or epoch `timestamp`.

    Returns:
        `ParsedTickRecord` on valid tick, or `CorruptTickRecord` if any check fails.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    key_str: Optional[str] = None
    if isinstance(kafka_key, bytes):
        key_str = kafka_key.decode("utf-8", errors="replace")
    elif isinstance(kafka_key, str):
        key_str = kafka_key

    # 1. Decode UTF-8 & JSON
    payload_str: str = ""
    payload_dict: Dict[str, Any]

    if raw_value is None:
        return CorruptTickRecord(
            raw_payload="",
            error_type="EmptyPayloadError",
            error_message="Kafka message value is None",
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=key_str,
            quarantined_at=now_iso,
        )

    if isinstance(raw_value, bytes):
        try:
            payload_str = raw_value.decode("utf-8")
        except UnicodeDecodeError as ude:
            return CorruptTickRecord(
                raw_payload=raw_value.hex(),
                error_type="UnicodeDecodeError",
                error_message=f"Invalid UTF-8 byte sequence: {ude}",
                kafka_topic=kafka_topic,
                kafka_partition=kafka_partition,
                kafka_offset=kafka_offset,
                kafka_key=key_str,
                quarantined_at=now_iso,
            )
    elif isinstance(raw_value, str):
        payload_str = raw_value
    elif isinstance(raw_value, dict):
        payload_dict = raw_value
        payload_str = json.dumps(raw_value)
    else:
        return CorruptTickRecord(
            raw_payload=str(raw_value),
            error_type="UnsupportedPayloadTypeError",
            error_message=f"Unsupported Kafka value type: {type(raw_value).__name__}",
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=key_str,
            quarantined_at=now_iso,
        )

    if not isinstance(raw_value, dict):
        try:
            parsed_json = json.loads(payload_str)
        except Exception as jex:
            return CorruptTickRecord(
                raw_payload=payload_str,
                error_type="JSONDecodeError",
                error_message=f"Malformed JSON payload: {jex}",
                kafka_topic=kafka_topic,
                kafka_partition=kafka_partition,
                kafka_offset=kafka_offset,
                kafka_key=key_str,
                quarantined_at=now_iso,
            )

        if not isinstance(parsed_json, dict):
            return CorruptTickRecord(
                raw_payload=payload_str,
                error_type="JSONDecodeError",
                error_message=f"Expected JSON object (dict), got {type(parsed_json).__name__}",
                kafka_topic=kafka_topic,
                kafka_partition=kafka_partition,
                kafka_offset=kafka_offset,
                kafka_key=key_str,
                quarantined_at=now_iso,
            )
        payload_dict = parsed_json

    # 2. Check required fields
    missing = [
        f
        for f in REQUIRED_TICK_FIELDS
        if f not in payload_dict or payload_dict[f] is None or str(payload_dict[f]).strip() == ""
    ]
    if missing:
        return CorruptTickRecord(
            raw_payload=payload_str,
            error_type="MissingFieldError",
            error_message=f"Missing required tick fields: {', '.join(missing)}",
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=key_str,
            quarantined_at=now_iso,
        )

    # 3. Validate domain invariants
    trade_id = str(payload_dict["trade_id"]).strip()
    ticker = str(payload_dict["ticker"]).strip().upper()
    asset_class = str(payload_dict["asset_class"]).strip().upper()

    if asset_class not in ALLOWED_ASSET_CLASSES:
        return CorruptTickRecord(
            raw_payload=payload_str,
            error_type="ValidationError",
            error_message=f"Unsupported asset_class '{asset_class}'. Must be one of {sorted(ALLOWED_ASSET_CLASSES)}",
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=key_str,
            quarantined_at=now_iso,
        )

    try:
        bid = float(payload_dict["bid"])
        ask = float(payload_dict["ask"])
        last_price = float(payload_dict["last_price"])
        volume = float(payload_dict["volume"])
        sequence_number = int(payload_dict.get("sequence_number") or 0)
    except (ValueError, TypeError) as nex:
        return CorruptTickRecord(
            raw_payload=payload_str,
            error_type="ValidationError",
            error_message=f"Numeric conversion failed on tick fields: {nex}",
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=key_str,
            quarantined_at=now_iso,
        )

    if bid <= 0 or ask <= 0 or last_price <= 0 or volume <= 0:
        return CorruptTickRecord(
            raw_payload=payload_str,
            error_type="ValidationError",
            error_message=(
                f"Non-positive price or volume: bid={bid}, ask={ask}, "
                f"last_price={last_price}, volume={volume}"
            ),
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=key_str,
            quarantined_at=now_iso,
        )

    if bid > ask:
        return CorruptTickRecord(
            raw_payload=payload_str,
            error_type="CrossedQuoteError",
            error_message=f"Crossed market quote detected: bid ({bid}) > ask ({ask})",
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=key_str,
            quarantined_at=now_iso,
        )

    # 4. Parse event timestamp
    try:
        dt_utc, epoch_sec, iso_ts = _parse_iso_timestamp(payload_dict["timestamp"])
    except Exception as tex:
        return CorruptTickRecord(
            raw_payload=payload_str,
            error_type="ValidationError",
            error_message=f"Invalid timestamp '{payload_dict.get('timestamp')}': {tex}",
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_key=key_str,
            quarantined_at=now_iso,
        )

    # 5. Compute microstructure enrichments
    spread = round(ask - bid, 4)
    mid_price = round((bid + ask) / 2.0, 4)
    notional_usd = round(last_price * volume, 4)
    relative_spread_bps = round((spread / mid_price) * 10000.0, 4) if mid_price > 0 else 0.0

    return ParsedTickRecord(
        trade_id=trade_id,
        ticker=ticker,
        asset_class=asset_class,
        bid=round(bid, 4),
        ask=round(ask, 4),
        last_price=round(last_price, 4),
        volume=round(volume, 4),
        spread=spread,
        mid_price=mid_price,
        notional_usd=notional_usd,
        relative_spread_bps=relative_spread_bps,
        event_timestamp=iso_ts,
        event_epoch_sec=epoch_sec,
        sequence_number=sequence_number,
        year=dt_utc.year,
        month=dt_utc.month,
        day=dt_utc.day,
        hour=dt_utc.hour,
        kafka_topic=kafka_topic,
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
        kafka_key=key_str,
        ingested_at=now_iso,
    )
