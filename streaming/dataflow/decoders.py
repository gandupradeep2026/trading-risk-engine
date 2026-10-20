"""Financial Trade Order JSON Decoders and Dead-Letter Routing DoFns.

Provides:
- Tagged outputs for valid orders vs. deadletter / schema-violating records.
- DeadLetterRecord envelope capturing failure reason, payload, and diagnostics.
- DecodeAndValidateTradeOrderFn Beam DoFn with comprehensive error handling,
  event-time timestamp extraction, and Prometheus / Cloud Monitoring metrics.
- FormatDeadLetterForPubSubFn for routing failed records to Cloud Pub/Sub DLQs.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, Generator, List, Optional, Union

import apache_beam as beam
from apache_beam import pvalue, window
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics

from ingestion.pubsub_client import TradeOrder

logger = logging.getLogger(__name__)

# Canonical Tagged Output Identifiers
TAG_VALID_ORDERS = "valid_orders"
TAG_DEADLETTER = "deadletter_records"
TAG_RAW_AUDIT = "raw_audit"


@dataclass
class DeadLetterRecord:
    """Envelope for messages that failed JSON decoding or schema validation."""

    payload: str
    error_type: str
    error_message: str
    error_details: Dict[str, Any]
    timestamp: str  # ISO 8601 UTC
    source_attributes: Dict[str, str] = field(default_factory=dict)
    retryable: bool = False
    ordering_key: Optional[str] = None
    original_message_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert dead-letter record to a plain dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize dead-letter record to a formatted JSON string."""
        return json.dumps(self.to_dict())

    def to_bytes(self) -> bytes:
        """Serialize dead-letter record to UTF-8 bytes for Pub/Sub publishing."""
        return self.to_json().encode("utf-8")

    def to_pubsub_message(self) -> PubsubMessage:
        """Create a Cloud Pub/Sub PubsubMessage object with diagnostic attributes."""
        attrs = {
            "error_type": self.error_type,
            "timestamp": self.timestamp,
            "source": "dataflow-trade-decoder",
            "retryable": str(self.retryable).lower(),
        }
        if self.ordering_key:
            attrs["ordering_key"] = self.ordering_key
        if self.original_message_id:
            attrs["original_message_id"] = self.original_message_id

        # Merge original source attributes if available (prefixed to avoid collisions)
        for k, v in self.source_attributes.items():
            if k not in attrs:
                attrs[f"orig_{k}"] = str(v)

        return PubsubMessage(
            data=self.to_bytes(),
            attributes=attrs,
            ordering_key=self.ordering_key or "",
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeadLetterRecord":
        """Reconstruct a DeadLetterRecord from a dictionary."""
        return cls(
            payload=data.get("payload", ""),
            error_type=data.get("error_type", "UnknownError"),
            error_message=data.get("error_message", ""),
            error_details=data.get("error_details", {}),
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            source_attributes=data.get("source_attributes", {}),
            retryable=data.get("retryable", False),
            ordering_key=data.get("ordering_key"),
            original_message_id=data.get("original_message_id"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "DeadLetterRecord":
        """Reconstruct a DeadLetterRecord from a JSON string."""
        return cls.from_dict(json.loads(json_str))


class DecodeAndValidateTradeOrderFn(beam.DoFn):
    """Beam DoFn for deserializing and validating Pub/Sub trade execution events.

    Decodes UTF-8 payloads, verifies JSON structure and financial domain invariants,
    extracts event-time timestamps for sliding window aggregations, and routes failed
    messages to the Dead-Letter Queue (DLQ) tagged output.
    """

    def __init__(self) -> None:
        super().__init__()
        # Metrics tracking
        self.messages_received = Metrics.counter("risk_dataflow", "messages_received")
        self.valid_orders_decoded = Metrics.counter("risk_dataflow", "valid_orders_decoded")
        self.json_decode_errors = Metrics.counter("risk_dataflow", "json_decode_errors")
        self.schema_validation_errors = Metrics.counter("risk_dataflow", "schema_validation_errors")
        self.unicode_decode_errors = Metrics.counter("risk_dataflow", "unicode_decode_errors")
        self.missing_field_errors = Metrics.counter("risk_dataflow", "missing_field_errors")
        self.deadletter_emitted = Metrics.counter("risk_dataflow", "deadletter_emitted")
        self.order_notional_dist = Metrics.distribution("risk_dataflow", "order_notional_usd")
        self.payload_size_dist = Metrics.distribution("risk_dataflow", "payload_size_bytes")

    def process(
        self,
        element: Union[PubsubMessage, bytes, str, Dict[str, Any]],
        timestamp: Union[beam.utils.timestamp.Timestamp, float] = beam.DoFn.TimestampParam,
        *args,
        **kwargs,
    ) -> Generator[Union[TradeOrder, pvalue.TaggedOutput], None, None]:
        """Process an incoming Pub/Sub message element.

        Yields:
            beam.window.TimestampedValue[TradeOrder] to the main output (TAG_VALID_ORDERS)
            TaggedOutput[TAG_DEADLETTER, DeadLetterRecord] for unparseable / invalid events
        """
        self.messages_received.inc()

        payload_bytes: bytes = b""
        payload_str: str = ""
        attributes: Dict[str, str] = {}
        ordering_key: Optional[str] = None
        message_id: Optional[str] = None

        # 1. Unpack message based on input type
        if isinstance(element, PubsubMessage):
            payload_bytes = element.data or b""
            attributes = dict(element.attributes or {})
            ordering_key = getattr(element, "ordering_key", None)
            message_id = getattr(element, "message_id", None)
        elif isinstance(element, bytes):
            payload_bytes = element
        elif isinstance(element, str):
            payload_bytes = element.encode("utf-8")
            payload_str = element
        elif isinstance(element, dict):
            try:
                payload_str = json.dumps(element)
                payload_bytes = payload_str.encode("utf-8")
            except Exception as e:
                yield from self._emit_deadletter(
                    payload="<unserializable dict>",
                    error_type="SerializationError",
                    error_message=f"Failed to serialize dict element: {str(e)}",
                    error_details={"exception": str(e)},
                    attributes={},
                )
                return
        else:
            yield from self._emit_deadletter(
                payload=str(element),
                error_type="UnsupportedInputTypeError",
                error_message=f"Expected PubsubMessage, bytes, str, or dict, got {type(element).__name__}",
                error_details={"type": type(element).__name__},
                attributes={},
            )
            return

        self.payload_size_dist.update(len(payload_bytes))

        # 2. Decode UTF-8 if not already decoded
        if not payload_str:
            try:
                payload_str = payload_bytes.decode("utf-8")
            except UnicodeDecodeError as ude:
                self.unicode_decode_errors.inc()
                yield from self._emit_deadletter(
                    payload=payload_bytes.hex(),
                    error_type="UnicodeDecodeError",
                    error_message=f"Invalid UTF-8 payload: {str(ude)}",
                    error_details={"hex_length": len(payload_bytes.hex()), "encoding": "hex"},
                    attributes=attributes,
                    ordering_key=ordering_key,
                    message_id=message_id,
                )
                return

        # 3. Parse JSON structure
        try:
            payload_data = json.loads(payload_str)
        except json.JSONDecodeError as jde:
            self.json_decode_errors.inc()
            yield from self._emit_deadletter(
                payload=payload_str,
                error_type="JSONDecodeError",
                error_message=f"Malformed JSON: {jde.msg} at line {jde.lineno} col {jde.colno}",
                error_details={"lineno": jde.lineno, "colno": jde.colno, "pos": jde.pos},
                attributes=attributes,
                ordering_key=ordering_key,
                message_id=message_id,
            )
            return

        if not isinstance(payload_data, dict):
            self.json_decode_errors.inc()
            yield from self._emit_deadletter(
                payload=payload_str,
                error_type="JSONDecodeError",
                error_message=f"Expected JSON object (dict), got {type(payload_data).__name__}",
                error_details={"parsed_type": type(payload_data).__name__},
                attributes=attributes,
                ordering_key=ordering_key,
                message_id=message_id,
            )
            return

        # 4. Validate Required Fields
        required_fields = [
            "order_id",
            "portfolio_id",
            "ticker",
            "asset_class",
            "side",
            "order_type",
            "quantity",
            "price",
        ]
        missing = [f for f in required_fields if f not in payload_data or payload_data[f] is None]
        if missing:
            self.missing_field_errors.inc()
            yield from self._emit_deadletter(
                payload=payload_str,
                error_type="MissingFieldError",
                error_message=f"Missing mandatory order fields: {', '.join(missing)}",
                error_details={"missing_fields": missing},
                attributes=attributes,
                ordering_key=ordering_key,
                message_id=message_id,
            )
            return

        # 5. Validate Domain Invariants
        validation_errors: Dict[str, str] = {}

        # Numeric validations
        quantity = payload_data.get("quantity")
        if not isinstance(quantity, (int, float)) or quantity <= 0:
            validation_errors["quantity"] = f"Quantity must be a positive number, got {quantity}"

        price = payload_data.get("price")
        if not isinstance(price, (int, float)) or price <= 0:
            validation_errors["price"] = f"Price must be a positive number, got {price}"

        # String non-empty validations
        order_id = str(payload_data.get("order_id", "")).strip()
        if not order_id:
            validation_errors["order_id"] = "order_id must be non-empty string"

        portfolio_id = str(payload_data.get("portfolio_id", "")).strip()
        if not portfolio_id:
            validation_errors["portfolio_id"] = "portfolio_id must be non-empty string"

        ticker = str(payload_data.get("ticker", "")).strip()
        if not ticker:
            validation_errors["ticker"] = "ticker must be non-empty string"

        # Categorical validations
        side = str(payload_data.get("side", "")).strip().upper()
        if side not in ("BUY", "SELL"):
            validation_errors["side"] = f"Invalid order side: '{side}'. Must be BUY or SELL."

        order_type = str(payload_data.get("order_type", "")).strip().upper()
        if order_type not in ("MARKET", "LIMIT", "STOP_LOSS", "STOP_LIMIT"):
            validation_errors["order_type"] = (
                f"Invalid order type: '{order_type}'. Must be MARKET, LIMIT, STOP_LOSS, or STOP_LIMIT."
            )

        # Timestamp parsing & Event-Time Extraction
        raw_ts = payload_data.get("timestamp")
        event_timestamp_sec: float = 0.0
        iso_timestamp_str: str = ""

        if not raw_ts:
            if isinstance(timestamp, (int, float, beam.utils.timestamp.Timestamp)) and float(timestamp) > 0:
                event_timestamp_sec = float(timestamp)
                iso_timestamp_str = datetime.fromtimestamp(event_timestamp_sec, tz=timezone.utc).isoformat()
            else:
                now_dt = datetime.now(timezone.utc)
                event_timestamp_sec = now_dt.timestamp()
                iso_timestamp_str = now_dt.isoformat()
        elif isinstance(raw_ts, (int, float)):
            # Epoch timestamp (support seconds or milliseconds)
            event_timestamp_sec = float(raw_ts) if raw_ts < 1e11 else float(raw_ts) / 1000.0
            iso_timestamp_str = datetime.fromtimestamp(event_timestamp_sec, tz=timezone.utc).isoformat()
        elif isinstance(raw_ts, str):
            try:
                # Handle ISO 8601 strings, replacing 'Z' with UTC offset
                cleaned_ts = raw_ts.replace("Z", "+00:00")
                parsed_dt = datetime.fromisoformat(cleaned_ts)
                if parsed_dt.tzinfo is None:
                    parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                event_timestamp_sec = parsed_dt.timestamp()
                iso_timestamp_str = parsed_dt.isoformat()
            except ValueError as ve:
                validation_errors["timestamp"] = f"Invalid ISO 8601 timestamp '{raw_ts}': {str(ve)}"
        else:
            validation_errors["timestamp"] = f"Timestamp must be ISO string or epoch number, got {type(raw_ts).__name__}"

        # If any validation errors accumulated, route to dead-letter queue
        if validation_errors:
            self.schema_validation_errors.inc()
            yield from self._emit_deadletter(
                payload=payload_str,
                error_type="ValidationError",
                error_message=f"Schema validation failed: {'; '.join(validation_errors.values())}",
                error_details=validation_errors,
                attributes=attributes,
                ordering_key=ordering_key,
                message_id=message_id,
            )
            return

        # 6. Construct Validated TradeOrder
        calculated_notional = round(float(quantity) * float(price), 2)
        provided_notional = payload_data.get("notional_usd")
        if provided_notional is not None:
            try:
                if abs(float(provided_notional) - calculated_notional) > 0.10:
                    logger.warning(
                        "Notional discrepancy in order %s: provided=%.2f, calculated=%.2f",
                        order_id,
                        float(provided_notional),
                        calculated_notional,
                    )
            except (ValueError, TypeError):
                pass

        order = TradeOrder(
            order_id=order_id,
            portfolio_id=portfolio_id,
            ticker=ticker.upper(),
            asset_class=str(payload_data.get("asset_class", "EQUITY")).upper(),
            side=side,
            order_type=order_type,
            quantity=float(quantity),
            price=float(price),
            notional_usd=calculated_notional,
            timestamp=iso_timestamp_str,
            trader_id=str(payload_data.get("trader_id", "ALGO-EXEC-01")),
            execution_status=str(payload_data.get("execution_status", "FILLED")).upper(),
            sequence_number=int(payload_data.get("sequence_number", 1)),
        )

        # 7. Update Metrics and Emit with Event-Time Timestamp
        self.valid_orders_decoded.inc()
        self.order_notional_dist.update(int(calculated_notional))

        yield window.TimestampedValue(order, event_timestamp_sec)

    def _emit_deadletter(
        self,
        payload: str,
        error_type: str,
        error_message: str,
        error_details: Dict[str, Any],
        attributes: Dict[str, str],
        ordering_key: Optional[str] = None,
        message_id: Optional[str] = None,
    ) -> Generator[pvalue.TaggedOutput, None, None]:
        """Emit a structured DeadLetterRecord to the TAG_DEADLETTER tagged output."""
        self.deadletter_emitted.inc()
        record = DeadLetterRecord(
            payload=payload,
            error_type=error_type,
            error_message=error_message,
            error_details=error_details,
            timestamp=datetime.now(timezone.utc).isoformat(),
            source_attributes=attributes,
            retryable=False,
            ordering_key=ordering_key,
            original_message_id=message_id,
        )
        yield pvalue.TaggedOutput(TAG_DEADLETTER, record)


class FormatDeadLetterForPubSubFn(beam.DoFn):
    """Formats a DeadLetterRecord into a PubsubMessage for dead-letter topic publication."""

    def process(self, element: DeadLetterRecord) -> Generator[PubsubMessage, None, None]:
        """Convert DeadLetterRecord to a PubsubMessage with diagnostic headers."""
        if isinstance(element, DeadLetterRecord):
            yield element.to_pubsub_message()
        elif isinstance(element, dict):
            record = DeadLetterRecord.from_dict(element)
            yield record.to_pubsub_message()
        else:
            logger.error("Unexpected element type for deadletter formatting: %s", type(element))


class DecodeAndValidateOrders(beam.PTransform):
    """Composite PTransform encapsulating decoding, validation, and deadletter branching."""

    def expand(self, pcoll: beam.PCollection) -> beam.pvalue.DoOutputsTuple:
        """Apply DecodeAndValidateTradeOrderFn with deadletter tagged output."""
        return pcoll | "DecodeAndValidate" >> beam.ParDo(
            DecodeAndValidateTradeOrderFn()
        ).with_outputs(TAG_DEADLETTER, main=TAG_VALID_ORDERS)
