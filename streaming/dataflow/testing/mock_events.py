"""Mock Event Generators and Financial Stream Fixtures for Apache Beam Testing.

Provides deterministic, schema-compliant financial mock events, corrupted payload fixtures
for Dead-Letter Queue (DLQ) testing, and specialized quantitative sequences (flash crashes,
sell pressure concentration, fat-finger spikes, and cumulative notional breaches).
"""

from datetime import datetime, timezone
from enum import Enum
import json
from typing import Any, Dict, List, Optional, Union
import uuid

from apache_beam.io.gcp.pubsub import PubsubMessage


class CorruptionType(str, Enum):
    """Catalog of synthetic corruption modes for DLQ and resilience testing."""

    JSON_SYNTAX_ERROR = "JSON_SYNTAX_ERROR"
    UNICODE_DECODE_ERROR = "UNICODE_DECODE_ERROR"
    MISSING_FIELD = "MISSING_FIELD"
    NON_POSITIVE_PRICE = "NON_POSITIVE_PRICE"
    NON_POSITIVE_QUANTITY = "NON_POSITIVE_QUANTITY"
    INVALID_SIDE = "INVALID_SIDE"
    INVALID_ORDER_TYPE = "INVALID_ORDER_TYPE"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    NOT_A_DICT = "NOT_A_DICT"


def create_mock_order_dict(
    order_id: Optional[str] = None,
    portfolio_id: str = "PORT-ALPHA",
    ticker: str = "NVDA",
    asset_class: str = "EQUITY",
    side: str = "BUY",
    order_type: str = "LIMIT",
    quantity: float = 100.0,
    price: float = 140.0,
    timestamp: Optional[Union[str, float, datetime]] = None,
    sequence_number: int = 1,
    trader_id: str = "ALGO-EXEC-01",
    execution_status: str = "FILLED",
    notional_usd: Optional[float] = None,
) -> Dict[str, Any]:
    """Create a dictionary representation of a valid trade execution order.

    Args:
        order_id: Unique order identifier; auto-generated if omitted.
        portfolio_id: Portfolio partition key (e.g. 'PORT-ALPHA').
        ticker: Asset ticker symbol (e.g. 'NVDA', 'AAPL').
        asset_class: Financial asset class (e.g. 'EQUITY', 'CRYPTO', 'FX').
        side: Order side ('BUY' or 'SELL').
        order_type: Order type ('LIMIT', 'MARKET', 'STOP_LOSS', 'STOP_LIMIT').
        quantity: Traded volume (must be positive).
        price: Execution price per unit (must be positive).
        timestamp: Event-time timestamp (ISO 8601 string, epoch float/int, or datetime).
        sequence_number: Strictly monotonic sequence number per portfolio/ticker.
        trader_id: Identifier of the executing trader or algorithm.
        execution_status: Execution status (e.g. 'FILLED', 'PARTIAL').
        notional_usd: Gross notional value; calculated as quantity * price if omitted.

    Returns:
        Dictionary adhering to the TradeOrder ingestion schema.
    """
    if order_id is None:
        order_id = f"ORD-{ticker.upper()}-{uuid.uuid4().hex[:8].upper()}"

    if timestamp is None:
        iso_timestamp = datetime.now(timezone.utc).isoformat()
    elif isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        iso_timestamp = timestamp.isoformat()
    elif isinstance(timestamp, (int, float)):
        ts_sec = float(timestamp) if timestamp < 1e11 else float(timestamp) / 1000.0
        iso_timestamp = datetime.fromtimestamp(ts_sec, tz=timezone.utc).isoformat()
    else:
        iso_timestamp = str(timestamp)

    calculated_notional = round(float(quantity) * float(price), 2)
    effective_notional = round(float(notional_usd), 2) if notional_usd is not None else calculated_notional

    return {
        "order_id": order_id,
        "portfolio_id": portfolio_id.upper(),
        "ticker": ticker.upper(),
        "asset_class": asset_class.upper(),
        "side": side.upper(),
        "order_type": order_type.upper(),
        "quantity": float(quantity),
        "price": float(price),
        "notional_usd": effective_notional,
        "timestamp": iso_timestamp,
        "trader_id": trader_id,
        "execution_status": execution_status.upper(),
        "sequence_number": int(sequence_number),
    }


def create_mock_order_bytes(
    **kwargs: Any,
) -> bytes:
    """Create a UTF-8 JSON encoded bytes representation of a valid trade order."""
    order_dict = create_mock_order_dict(**kwargs)
    return json.dumps(order_dict).encode("utf-8")


def create_mock_pubsub_message(
    order_dict_or_kwargs: Optional[Union[Dict[str, Any], Any]] = None,
    attributes: Optional[Dict[str, str]] = None,
    ordering_key: Optional[str] = None,
    message_id: Optional[str] = None,
    **kwargs: Any,
) -> PubsubMessage:
    """Create an Apache Beam PubsubMessage containing a valid trade order.

    Args:
        order_dict_or_kwargs: Existing order dictionary, or None to build from kwargs.
        attributes: Pub/Sub message attributes dictionary.
        ordering_key: Pub/Sub message ordering key.
        message_id: Pub/Sub message ID.
        **kwargs: Arguments forwarded to create_mock_order_dict if dict not provided.

    Returns:
        PubsubMessage instance ready for pipeline ingestion.
    """
    if isinstance(order_dict_or_kwargs, dict):
        order_dict = order_dict_or_kwargs
    else:
        order_dict = create_mock_order_dict(**kwargs)

    data = json.dumps(order_dict).encode("utf-8")
    msg_attrs = attributes.copy() if attributes else {}

    if "source" not in msg_attrs:
        msg_attrs["source"] = "test-stream"
    if "ticker" not in msg_attrs:
        msg_attrs["ticker"] = order_dict["ticker"]
    if "portfolio_id" not in msg_attrs:
        msg_attrs["portfolio_id"] = order_dict["portfolio_id"]

    effective_ordering_key = ordering_key or f"{order_dict['portfolio_id']}#{order_dict['ticker']}"

    return PubsubMessage(
        data=data,
        attributes=msg_attrs,
        message_id=message_id or f"msg-{uuid.uuid4().hex[:12]}",
        ordering_key=effective_ordering_key,
    )


def create_corrupt_message(
    corruption_type: CorruptionType,
    missing_field: str = "price",
    as_pubsub_message: bool = False,
    attributes: Optional[Dict[str, str]] = None,
    order_id: str = "ORD-CORRUPT-001",
    timestamp: Optional[Union[str, float, datetime]] = "2026-10-18T12:00:00+00:00",
) -> Union[bytes, PubsubMessage]:
    """Generate synthetically corrupted mock message for Dead-Letter Queue (DLQ) testing.

    Args:
        corruption_type: Target corruption mode.
        missing_field: Field to omit when corruption_type is MISSING_FIELD.
        as_pubsub_message: If True, wraps result in a Beam PubsubMessage.
        attributes: Pub/Sub attributes to attach.
        order_id: Order identifier to embed when applicable.
        timestamp: Deterministic event timestamp to embed when applicable.

    Returns:
        Raw corrupted bytes or a PubsubMessage.
    """
    raw_bytes: bytes

    if corruption_type == CorruptionType.JSON_SYNTAX_ERROR:
        raw_bytes = f'{{"order_id": "{order_id}", "ticker": "NVDA", "quantity": 100.0, "price": 140.0, "unterminated_syntax'.encode("utf-8")

    elif corruption_type == CorruptionType.UNICODE_DECODE_ERROR:
        raw_bytes = b"\xff\xfe\x00\x00corrupt_non_utf8_binary_payload"

    elif corruption_type == CorruptionType.MISSING_FIELD:
        payload = create_mock_order_dict(order_id=order_id, timestamp=timestamp)
        if missing_field in payload:
            del payload[missing_field]
        raw_bytes = json.dumps(payload).encode("utf-8")

    elif corruption_type == CorruptionType.NON_POSITIVE_PRICE:
        payload = create_mock_order_dict(order_id=order_id, price=-12.50, timestamp=timestamp)
        raw_bytes = json.dumps(payload).encode("utf-8")

    elif corruption_type == CorruptionType.NON_POSITIVE_QUANTITY:
        payload = create_mock_order_dict(order_id=order_id, quantity=0.0, timestamp=timestamp)
        raw_bytes = json.dumps(payload).encode("utf-8")

    elif corruption_type == CorruptionType.INVALID_SIDE:
        payload = create_mock_order_dict(order_id=order_id, side="HOLD", timestamp=timestamp)
        raw_bytes = json.dumps(payload).encode("utf-8")

    elif corruption_type == CorruptionType.INVALID_ORDER_TYPE:
        payload = create_mock_order_dict(order_id=order_id, order_type="CUSTOM_PEGGED", timestamp=timestamp)
        raw_bytes = json.dumps(payload).encode("utf-8")

    elif corruption_type == CorruptionType.INVALID_TIMESTAMP:
        payload = create_mock_order_dict(order_id=order_id)
        payload["timestamp"] = "unparseable-date-2026/99/99"
        raw_bytes = json.dumps(payload).encode("utf-8")

    elif corruption_type == CorruptionType.NOT_A_DICT:
        raw_bytes = json.dumps(["array_element_1", "array_element_2"]).encode("utf-8")

    else:
        raise ValueError(f"Unsupported corruption type: {corruption_type}")

    if as_pubsub_message:
        msg_attrs = attributes.copy() if attributes else {"corrupt": "true"}
        return PubsubMessage(data=raw_bytes, attributes=msg_attrs)

    return raw_bytes


def create_flash_crash_event_sequence(
    portfolio_id: str = "PORT-ALPHA",
    ticker: str = "NVDA",
    base_price: float = 140.0,
    drop_pct: float = 0.08,
    start_timestamp_sec: float = 1792324800.0,  # 2026-10-18T12:00:00Z
    duration_seconds: int = 120,
    num_orders: int = 6,
) -> List[Dict[str, Any]]:
    """Generate a chronological sequence of trade orders simulating an institutional flash crash.

    Args:
        portfolio_id: Target portfolio ID.
        ticker: Asset ticker symbol.
        base_price: Starting pre-crash price.
        drop_pct: Total drawdown fraction (e.g. 0.08 for an 8% drop).
        start_timestamp_sec: Epoch seconds for first order.
        duration_seconds: Total duration of crash sequence in seconds.
        num_orders: Total orders in sequence (minimum 3).

    Returns:
        Chronologically ordered list of trade order dictionaries.
    """
    if num_orders < 3:
        raise ValueError("Flash crash sequence requires at least 3 orders.")

    orders: List[Dict[str, Any]] = []
    trough_price = round(base_price * (1.0 - drop_pct), 4)
    time_step = duration_seconds / (num_orders - 1)

    # 1. First order at base price
    orders.append(
        create_mock_order_dict(
            order_id=f"ORD-{ticker}-CRASH-1",
            portfolio_id=portfolio_id,
            ticker=ticker,
            side="BUY",
            order_type="LIMIT",
            quantity=50.0,
            price=base_price,
            timestamp=start_timestamp_sec,
            sequence_number=1,
        )
    )

    # 2. Intermediate sell orders with descending prices
    for i in range(1, num_orders - 1):
        fraction = i / (num_orders - 1)
        # Accelerate price drop exponentially towards trough
        current_px = round(base_price - (base_price - trough_price) * (fraction**1.2), 4)
        ts = start_timestamp_sec + (i * time_step)
        orders.append(
            create_mock_order_dict(
                order_id=f"ORD-{ticker}-CRASH-{i + 1}",
                portfolio_id=portfolio_id,
                ticker=ticker,
                side="SELL",
                order_type="MARKET",
                quantity=100.0 * (1 + i * 0.5),
                price=current_px,
                timestamp=ts,
                sequence_number=i + 1,
            )
        )

    # 3. Final order at trough price
    orders.append(
        create_mock_order_dict(
            order_id=f"ORD-{ticker}-CRASH-{num_orders}",
            portfolio_id=portfolio_id,
            ticker=ticker,
            side="SELL",
            order_type="MARKET",
            quantity=250.0,
            price=trough_price,
            timestamp=start_timestamp_sec + duration_seconds,
            sequence_number=num_orders,
        )
    )

    return orders


def create_fat_finger_event(
    portfolio_id: str = "PORT-BETA",
    ticker: str = "TSLA",
    quantity: float = 25000.0,
    price: float = 250.0,
    timestamp_sec: float = 1792324830.0,
) -> Dict[str, Any]:
    """Generate a single order with abnormally massive notional value (e.g. $6.25M)."""
    return create_mock_order_dict(
        order_id=f"ORD-{ticker}-FATFINGER",
        portfolio_id=portfolio_id,
        ticker=ticker,
        side="BUY",
        order_type="MARKET",
        quantity=quantity,
        price=price,
        timestamp=timestamp_sec,
        sequence_number=999,
        trader_id="ROGUE-TRADER-99",
    )


def create_sell_pressure_event_sequence(
    portfolio_id: str = "PORT-GAMMA",
    ticker: str = "AAPL",
    base_price: float = 220.0,
    start_timestamp_sec: float = 1792324800.0,
    num_orders: int = 5,
    sell_quantity: float = 1000.0,
    buy_quantity: float = 100.0,
) -> List[Dict[str, Any]]:
    """Generate orders with heavy institutional sell-side concentration (>= 85% sell notional)."""
    orders: List[Dict[str, Any]] = []

    # One modest buy order
    orders.append(
        create_mock_order_dict(
            order_id=f"ORD-{ticker}-SELLCONC-1",
            portfolio_id=portfolio_id,
            ticker=ticker,
            side="BUY",
            quantity=buy_quantity,
            price=base_price,
            timestamp=start_timestamp_sec,
            sequence_number=1,
        )
    )

    # Massive institutional sell orders
    for i in range(1, num_orders):
        orders.append(
            create_mock_order_dict(
                order_id=f"ORD-{ticker}-SELLCONC-{i + 1}",
                portfolio_id=portfolio_id,
                ticker=ticker,
                side="SELL",
                quantity=sell_quantity,
                price=round(base_price - (i * 0.5), 2),
                timestamp=start_timestamp_sec + (i * 15.0),
                sequence_number=i + 1,
            )
        )

    return orders


def create_notional_breach_event_sequence(
    portfolio_id: str = "PORT-EXP",
    ticker: str = "MSFT",
    base_price: float = 400.0,
    target_cumulative_notional: float = 3_000_000.0,
    start_timestamp_sec: float = 1792324800.0,
    num_orders: int = 4,
) -> List[Dict[str, Any]]:
    """Generate orders whose cumulative notional exceeds the window threshold ($2.5M)."""
    orders: List[Dict[str, Any]] = []
    notional_per_order = target_cumulative_notional / num_orders
    qty_per_order = round(notional_per_order / base_price, 2)

    for i in range(num_orders):
        orders.append(
            create_mock_order_dict(
                order_id=f"ORD-{ticker}-NOTIONAL-{i + 1}",
                portfolio_id=portfolio_id,
                ticker=ticker,
                side="BUY" if i % 2 == 0 else "SELL",
                quantity=qty_per_order,
                price=base_price,
                timestamp=start_timestamp_sec + (i * 20.0),
                sequence_number=i + 1,
            )
        )

    return orders


def create_multi_asset_portfolio_stream(
    start_timestamp_sec: float = 1792324800.0,
) -> List[Dict[str, Any]]:
    """Generate an interleaved multi-asset portfolio stream with distinct behaviors.

    Includes:
    - PORT-ALPHA / NVDA: Flash crash sequence (8% drop)
    - PORT-ALPHA / AAPL: Normal steady market flow (no alerts)
    - PORT-BETA / MSFT: Notional exposure breach ($3M cumulative)
    - PORT-GAMMA / BTCUSD: Concentrated institutional sell pressure
    """
    stream: List[Dict[str, Any]] = []

    # 1. NVDA flash crash (PORT-ALPHA)
    nvda_crash = create_flash_crash_event_sequence(
        portfolio_id="PORT-ALPHA",
        ticker="NVDA",
        base_price=140.0,
        drop_pct=0.09,
        start_timestamp_sec=start_timestamp_sec,
        duration_seconds=90,
        num_orders=4,
    )
    stream.extend(nvda_crash)

    # 2. AAPL normal flow (PORT-ALPHA) - price stays stable
    for i in range(4):
        stream.append(
            create_mock_order_dict(
                order_id=f"ORD-AAPL-STABLE-{i + 1}",
                portfolio_id="PORT-ALPHA",
                ticker="AAPL",
                side="BUY" if i % 2 == 0 else "SELL",
                quantity=50.0,
                price=225.0 + (i * 0.10),
                timestamp=start_timestamp_sec + 5.0 + (i * 25.0),
                sequence_number=i + 1,
            )
        )

    # 3. MSFT notional breach (PORT-BETA)
    msft_breach = create_notional_breach_event_sequence(
        portfolio_id="PORT-BETA",
        ticker="MSFT",
        base_price=420.0,
        target_cumulative_notional=3_200_000.0,
        start_timestamp_sec=start_timestamp_sec + 10.0,
        num_orders=4,
    )
    stream.extend(msft_breach)

    # 4. BTCUSD sell concentration (PORT-GAMMA)
    btc_sell = create_sell_pressure_event_sequence(
        portfolio_id="PORT-GAMMA",
        ticker="BTCUSD",
        base_price=65000.0,
        start_timestamp_sec=start_timestamp_sec + 15.0,
        num_orders=4,
        sell_quantity=5.0,
        buy_quantity=0.5,
    )
    stream.extend(btc_sell)

    # Sort all events strictly by event timestamp for realistic simulation
    stream.sort(key=lambda x: (x["timestamp"], x["sequence_number"]))
    return stream
