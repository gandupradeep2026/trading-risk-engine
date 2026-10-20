"""Fluent Stream Scenario Builder and TestStream Fixtures for Apache Beam.

Provides an enterprise-grade fluent builder on top of Apache Beam's TestStream,
enforcing watermark monotonicity, handling event-time timestamps, out-of-order event
injection, processing time advancement, and pre-packaged test scenarios for financial risk testing.
"""

from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional, Union

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from streaming.dataflow.testing.mock_events import (
    CorruptionType,
    create_corrupt_message,
    create_fat_finger_event,
    create_flash_crash_event_sequence,
    create_mock_order_bytes,
    create_mock_order_dict,
    create_mock_pubsub_message,
    create_multi_asset_portfolio_stream,
    create_notional_breach_event_sequence,
    create_sell_pressure_event_sequence,
)


def _extract_epoch_sec(timestamp_val: Optional[Union[str, float, int, datetime]]) -> float:
    """Helper to convert any valid timestamp format into epoch seconds."""
    if timestamp_val is None:
        return 0.0
    if isinstance(timestamp_val, (int, float)):
        return float(timestamp_val) if timestamp_val < 1e11 else float(timestamp_val) / 1000.0
    if isinstance(timestamp_val, datetime):
        if timestamp_val.tzinfo is None:
            timestamp_val = timestamp_val.replace(tzinfo=timezone.utc)
        return timestamp_val.timestamp()
    if isinstance(timestamp_val, str):
        cleaned = timestamp_val.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    return float(timestamp_val)


class StreamScenarioBuilder:
    """Fluent API for constructing Apache Beam TestStream instances for financial testing."""

    def __init__(self, start_watermark_sec: Optional[float] = None) -> None:
        self._test_stream = TestStream()
        self._current_watermark: float = float("-inf")
        self._watermark_infinity_called: bool = False
        self._pending_elements: List[TimestampedValue] = []

        if start_watermark_sec is not None:
            self.advance_watermark_to(start_watermark_sec)

    def _flush_pending_elements(self) -> None:
        """Flush any pending buffered timestamped elements to the TestStream."""
        if self._pending_elements:
            self._test_stream.add_elements(self._pending_elements)
            self._pending_elements = []

    @property
    def current_watermark(self) -> float:
        """Get the current watermark timestamp in epoch seconds."""
        return self._current_watermark

    def advance_watermark_to(self, new_watermark_sec: float) -> "StreamScenarioBuilder":
        """Advance watermark to an absolute epoch timestamp (must be strictly greater than current)."""
        self._flush_pending_elements()
        new_wm = float(new_watermark_sec)
        if new_wm <= self._current_watermark:
            raise ValueError(
                f"Watermark must strictly advance monotonically. "
                f"Attempted to set {new_wm} <= current {self._current_watermark}."
            )
        self._current_watermark = new_wm
        self._test_stream.advance_watermark_to(new_wm)
        return self

    def advance_watermark_by(self, delta_seconds: float) -> "StreamScenarioBuilder":
        """Advance watermark forward by a relative delta in seconds."""
        if delta_seconds <= 0:
            raise ValueError(f"Watermark delta must be strictly positive, got {delta_seconds}.")
        base = 0.0 if self._current_watermark == float("-inf") else self._current_watermark
        return self.advance_watermark_to(base + delta_seconds)

    def advance_processing_time(self, duration_seconds: float) -> "StreamScenarioBuilder":
        """Advance simulated processing time by positive seconds."""
        self._flush_pending_elements()
        if duration_seconds <= 0:
            raise ValueError(f"Processing time advance must be positive, got {duration_seconds}.")
        self._test_stream.advance_processing_time(duration_seconds)
        return self

    def add_order(
        self,
        order_dict_or_kwargs: Optional[Union[Dict[str, Any], Any]] = None,
        event_timestamp: Optional[Union[str, float, int, datetime]] = None,
        as_pubsub_message: bool = False,
        as_bytes: bool = True,
        attributes: Optional[Dict[str, str]] = None,
        ordering_key: Optional[str] = None,
        **kwargs: Any,
    ) -> "StreamScenarioBuilder":
        """Add a single trade order element at the given event timestamp."""
        if isinstance(order_dict_or_kwargs, PubsubMessage):
            element = order_dict_or_kwargs
            ts_sec = _extract_epoch_sec(event_timestamp)
        elif isinstance(order_dict_or_kwargs, bytes):
            element = order_dict_or_kwargs
            ts_sec = _extract_epoch_sec(event_timestamp)
        elif isinstance(order_dict_or_kwargs, dict):
            order_dict = order_dict_or_kwargs
            ts_sec = _extract_epoch_sec(event_timestamp or order_dict.get("timestamp"))
            if as_pubsub_message:
                element = create_mock_pubsub_message(
                    order_dict,
                    attributes=attributes,
                    ordering_key=ordering_key,
                )
            elif as_bytes:
                element = json.dumps(order_dict).encode("utf-8")
            else:
                element = order_dict
        else:
            if "timestamp" not in kwargs and event_timestamp is not None:
                kwargs["timestamp"] = event_timestamp
            order_dict = create_mock_order_dict(**kwargs)
            ts_sec = _extract_epoch_sec(event_timestamp or order_dict.get("timestamp"))
            if as_pubsub_message:
                element = create_mock_pubsub_message(
                    order_dict,
                    attributes=attributes,
                    ordering_key=ordering_key,
                )
            elif as_bytes:
                element = json.dumps(order_dict).encode("utf-8")
            else:
                element = order_dict

        if ts_sec == 0.0 and self._current_watermark != float("-inf"):
            ts_sec = self._current_watermark

        self._pending_elements.append(TimestampedValue(element, ts_sec))
        return self

    def add_orders(
        self,
        orders: List[Dict[str, Any]],
        as_pubsub_message: bool = False,
        as_bytes: bool = True,
    ) -> "StreamScenarioBuilder":
        """Add multiple trade orders in a single bundle, using each order's embedded timestamp."""
        for o in orders:
            ts_sec = _extract_epoch_sec(o.get("timestamp"))
            if ts_sec == 0.0 and self._current_watermark != float("-inf"):
                ts_sec = self._current_watermark

            if as_pubsub_message:
                elem = create_mock_pubsub_message(o)
            elif as_bytes:
                elem = json.dumps(o).encode("utf-8")
            else:
                elem = o

            self._pending_elements.append(TimestampedValue(elem, ts_sec))

        return self

    def add_corrupt_payload(
        self,
        corruption_type: CorruptionType,
        event_timestamp: Optional[Union[str, float, int, datetime]] = None,
        missing_field: str = "price",
        as_pubsub_message: bool = False,
        order_id: str = "ORD-CORRUPT-TEST",
        attributes: Optional[Dict[str, str]] = None,
    ) -> "StreamScenarioBuilder":
        """Add a synthetically corrupted payload to the stream for Dead-Letter testing."""
        corrupt_elem = create_corrupt_message(
            corruption_type=corruption_type,
            missing_field=missing_field,
            as_pubsub_message=as_pubsub_message,
            attributes=attributes,
            order_id=order_id,
        )
        ts_sec = _extract_epoch_sec(event_timestamp)
        if ts_sec == 0.0 and self._current_watermark != float("-inf"):
            ts_sec = self._current_watermark

        self._pending_elements.append(TimestampedValue(corrupt_elem, ts_sec))
        return self

    def add_flash_crash_sequence(
        self,
        portfolio_id: str = "PORT-ALPHA",
        ticker: str = "NVDA",
        base_price: float = 140.0,
        drop_pct: float = 0.08,
        start_timestamp_sec: Optional[float] = None,
        duration_seconds: int = 120,
        num_orders: int = 6,
        as_bytes: bool = True,
        as_pubsub_message: bool = False,
    ) -> "StreamScenarioBuilder":
        """Generate and append an entire institutional flash crash sequence."""
        start_ts = start_timestamp_sec or (
            0.0 if self._current_watermark == float("-inf") else self._current_watermark
        )
        orders = create_flash_crash_event_sequence(
            portfolio_id=portfolio_id,
            ticker=ticker,
            base_price=base_price,
            drop_pct=drop_pct,
            start_timestamp_sec=start_ts,
            duration_seconds=duration_seconds,
            num_orders=num_orders,
        )
        return self.add_orders(orders, as_pubsub_message=as_pubsub_message, as_bytes=as_bytes)

    def add_out_of_order_scenario(
        self,
        portfolio_id: str = "PORT-ALPHA",
        ticker: str = "NVDA",
        base_ts: float = 1792324800.0,
        as_bytes: bool = True,
    ) -> "StreamScenarioBuilder":
        """Add out-of-order events where an earlier event arrives after later events before watermark moves."""
        # Order 1: t=10s, Order 3: t=40s, Order 2: t=25s arrives LAST
        o1 = create_mock_order_dict(
            order_id="ORD-OO-1",
            portfolio_id=portfolio_id,
            ticker=ticker,
            price=140.0,
            quantity=50.0,
            timestamp=base_ts + 10.0,
            sequence_number=1,
        )
        o3 = create_mock_order_dict(
            order_id="ORD-OO-3",
            portfolio_id=portfolio_id,
            ticker=ticker,
            price=126.0,
            quantity=80.0,
            timestamp=base_ts + 40.0,
            sequence_number=3,
        )
        o2 = create_mock_order_dict(
            order_id="ORD-OO-2",
            portfolio_id=portfolio_id,
            ticker=ticker,
            price=133.0,
            quantity=60.0,
            timestamp=base_ts + 25.0,
            sequence_number=2,
        )

        # Emit o1, then o3, then o2
        for order in [o1, o3, o2]:
            ts = _extract_epoch_sec(order["timestamp"])
            elem = json.dumps(order).encode("utf-8") if as_bytes else order
            self._pending_elements.append(TimestampedValue(elem, ts))

        return self

    def advance_watermark_to_infinity(self) -> "StreamScenarioBuilder":
        """Signal that no more events will be produced, closing all active windows."""
        self._flush_pending_elements()
        self._test_stream.advance_watermark_to_infinity()
        self._watermark_infinity_called = True
        return self

    def build(self) -> TestStream:
        """Finalize and return the underlying Apache Beam TestStream.

        Automatically calls advance_watermark_to_infinity() if not already called.
        """
        self._flush_pending_elements()
        if not self._watermark_infinity_called:
            self.advance_watermark_to_infinity()
        return self._test_stream


# -----------------------------------------------------------------------------
# PREPACKAGED SCENARIO GENERATORS
# -----------------------------------------------------------------------------

def build_happy_path_scenario(
    base_ts: float = 1792324800.0,
    num_orders: int = 10,
    as_bytes: bool = True,
) -> TestStream:
    """Build a standard streaming scenario with regular on-time orders across 5 minutes."""
    builder = StreamScenarioBuilder(start_watermark_sec=base_ts)
    for i in range(num_orders):
        event_time = base_ts + (i * 25.0)
        builder.add_order(
            order_id=f"ORD-HAPPY-{i + 1}",
            portfolio_id="PORT-ALPHA",
            ticker="AAPL",
            side="BUY" if i % 2 == 0 else "SELL",
            quantity=50.0,
            price=220.0 + (i * 0.25),
            event_timestamp=event_time,
            sequence_number=i + 1,
            as_bytes=as_bytes,
        )
        if i % 3 == 0:
            builder.advance_watermark_to(event_time + 1.0)

    builder.advance_watermark_to(base_ts + 300.0)
    return builder.build()


def build_flash_crash_scenario(
    portfolio_id: str = "PORT-ALPHA",
    ticker: str = "NVDA",
    base_price: float = 140.0,
    drop_pct: float = 0.08,
    base_ts: float = 1792324800.0,
    as_bytes: bool = True,
) -> TestStream:
    """Build a flash crash stream advancing watermarks past window boundaries."""
    builder = StreamScenarioBuilder(start_watermark_sec=base_ts)
    builder.add_flash_crash_sequence(
        portfolio_id=portfolio_id,
        ticker=ticker,
        base_price=base_price,
        drop_pct=drop_pct,
        start_timestamp_sec=base_ts,
        duration_seconds=90,
        num_orders=5,
        as_bytes=as_bytes,
    )
    # Advance watermark through multiple 1-minute slide periods
    builder.advance_watermark_to(base_ts + 60.0)
    builder.advance_watermark_to(base_ts + 120.0)
    builder.advance_watermark_to(base_ts + 360.0)
    return builder.build()


def build_dlq_corruption_scenario(
    base_ts: float = 1792324800.0,
    as_pubsub_message: bool = False,
) -> TestStream:
    """Build a mixed stream with valid orders and all corruption modes for DLQ validation."""
    builder = StreamScenarioBuilder(start_watermark_sec=base_ts)

    # 1. Valid order
    builder.add_order(
        order_id="ORD-VALID-1",
        event_timestamp=base_ts + 10.0,
        as_pubsub_message=as_pubsub_message,
    )

    # 2. Syntax error
    builder.add_corrupt_payload(
        CorruptionType.JSON_SYNTAX_ERROR,
        event_timestamp=base_ts + 15.0,
        as_pubsub_message=as_pubsub_message,
        order_id="ORD-ERR-SYNTAX",
    )

    # 3. Unicode error
    builder.add_corrupt_payload(
        CorruptionType.UNICODE_DECODE_ERROR,
        event_timestamp=base_ts + 20.0,
        as_pubsub_message=as_pubsub_message,
    )

    # 4. Valid order
    builder.add_order(
        order_id="ORD-VALID-2",
        event_timestamp=base_ts + 25.0,
        as_pubsub_message=as_pubsub_message,
    )

    # 5. Missing field (price)
    builder.add_corrupt_payload(
        CorruptionType.MISSING_FIELD,
        event_timestamp=base_ts + 30.0,
        missing_field="price",
        as_pubsub_message=as_pubsub_message,
        order_id="ORD-ERR-NOPRICE",
    )

    # 6. Negative price
    builder.add_corrupt_payload(
        CorruptionType.NON_POSITIVE_PRICE,
        event_timestamp=base_ts + 35.0,
        as_pubsub_message=as_pubsub_message,
        order_id="ORD-ERR-NEGPRICE",
    )

    # 7. Non-positive quantity
    builder.add_corrupt_payload(
        CorruptionType.NON_POSITIVE_QUANTITY,
        event_timestamp=base_ts + 40.0,
        as_pubsub_message=as_pubsub_message,
        order_id="ORD-ERR-ZEROQTY",
    )

    # 8. Invalid side
    builder.add_corrupt_payload(
        CorruptionType.INVALID_SIDE,
        event_timestamp=base_ts + 45.0,
        as_pubsub_message=as_pubsub_message,
        order_id="ORD-ERR-BADSIDE",
    )

    # 9. Invalid order type
    builder.add_corrupt_payload(
        CorruptionType.INVALID_ORDER_TYPE,
        event_timestamp=base_ts + 50.0,
        as_pubsub_message=as_pubsub_message,
        order_id="ORD-ERR-BADTYPE",
    )

    # 10. Invalid timestamp
    builder.add_corrupt_payload(
        CorruptionType.INVALID_TIMESTAMP,
        event_timestamp=base_ts + 55.0,
        as_pubsub_message=as_pubsub_message,
        order_id="ORD-ERR-BADTS",
    )

    # 11. Array instead of dict
    builder.add_corrupt_payload(
        CorruptionType.NOT_A_DICT,
        event_timestamp=base_ts + 60.0,
        as_pubsub_message=as_pubsub_message,
    )

    # 12. Final valid order
    builder.add_order(
        order_id="ORD-VALID-3",
        event_timestamp=base_ts + 65.0,
        as_pubsub_message=as_pubsub_message,
    )

    builder.advance_watermark_to(base_ts + 120.0)
    return builder.build()


def build_multi_portfolio_scenario(
    base_ts: float = 1792324800.0,
    as_bytes: bool = True,
) -> TestStream:
    """Build multi-portfolio stream with NVDA crash, AAPL normal, MSFT breach, BTC sell."""
    stream_orders = create_multi_asset_portfolio_stream(start_timestamp_sec=base_ts)
    builder = StreamScenarioBuilder(start_watermark_sec=base_ts)
    builder.add_orders(stream_orders, as_bytes=as_bytes)
    builder.advance_watermark_to(base_ts + 120.0)
    builder.advance_watermark_to(base_ts + 360.0)
    return builder.build()
