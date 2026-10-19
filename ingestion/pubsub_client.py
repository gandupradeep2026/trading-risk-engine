"""Local Pub/Sub Client and Zero-Cost Testing Utilities.

Provides:
- Resilient client initialization supporting both GCP Cloud and Local Emulator.
- Message validation against trading order schema.
- Dead-letter queue simulation logic.
"""

from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, List, Optional
import uuid


@dataclass
class TradeOrder:
    """Represents a validated financial trade order execution."""

    order_id: str
    portfolio_id: str
    ticker: str
    asset_class: str
    side: str  # BUY or SELL
    order_type: str  # MARKET, LIMIT, STOP_LOSS
    quantity: float
    price: float
    notional_usd: float
    timestamp: str
    trader_id: str = "ALGO-EXEC-01"
    execution_status: str = "FILLED"
    sequence_number: int = 1

    @classmethod
    def create(
        cls,
        portfolio_id: str,
        ticker: str,
        asset_class: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float,
        order_id: Optional[str] = None,
        timestamp: Optional[str] = None,
        trader_id: Optional[str] = None,
        execution_status: Optional[str] = None,
        sequence_number: Optional[int] = None,
    ) -> "TradeOrder":
        """Factory method with automatic validation and UUID generation."""
        if quantity <= 0:
            raise ValueError(f"Quantity must be strictly positive, got {quantity}")
        if price <= 0:
            raise ValueError(f"Price must be strictly positive, got {price}")
        if side.upper() not in ("BUY", "SELL"):
            raise ValueError(f"Invalid order side: {side}. Must be BUY or SELL.")
        if order_type.upper() not in ("MARKET", "LIMIT", "STOP_LOSS", "STOP_LIMIT"):
            raise ValueError(f"Invalid order type: {order_type}. Must be MARKET, LIMIT, STOP_LOSS, or STOP_LIMIT.")

        notional = round(quantity * price, 2)
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        oid = order_id or str(uuid.uuid4())

        return cls(
            order_id=oid,
            portfolio_id=portfolio_id,
            ticker=ticker.upper(),
            asset_class=asset_class.upper(),
            side=side.upper(),
            order_type=order_type.upper(),
            quantity=quantity,
            price=price,
            notional_usd=notional,
            timestamp=ts,
            trader_id=trader_id or "ALGO-EXEC-01",
            execution_status=(execution_status or "FILLED").upper(),
            sequence_number=sequence_number or 1,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert trade order to dictionary representation."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize trade order to JSON string."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "TradeOrder":
        """Deserialize from JSON string, handling backwards compatibility."""
        payload = json.loads(data)
        valid_fields = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in payload.items() if k in valid_fields}
        return cls(**filtered)

    def get_ordering_key(self, strategy: str = "compound") -> str:
        """Generate Pub/Sub message ordering key based on partition strategy."""
        strat = strategy.lower()
        if strat == "compound":
            return f"{self.portfolio_id}#{self.ticker}"
        elif strat == "portfolio":
            return self.portfolio_id
        elif strat == "ticker":
            return self.ticker
        return ""

    def get_attributes(self, environment: str = "dev") -> Dict[str, str]:
        """Generate Cloud Pub/Sub message attributes dictionary."""
        return {
            "order_id": self.order_id,
            "portfolio_id": self.portfolio_id,
            "ticker": self.ticker,
            "asset_class": self.asset_class,
            "side": self.side,
            "order_type": self.order_type,
            "notional_usd": f"{self.notional_usd:.2f}",
            "trader_id": self.trader_id,
            "execution_status": self.execution_status,
            "sequence_number": str(self.sequence_number),
            "environment": environment,
        }

    def validate(self) -> None:
        """Validate order invariants, raising ValueError if any constraint fails."""
        if not self.order_id or not self.order_id.strip():
            raise ValueError("order_id must be non-empty")
        if not self.portfolio_id or not self.portfolio_id.strip():
            raise ValueError("portfolio_id must be non-empty")
        if not self.ticker or not self.ticker.strip():
            raise ValueError("ticker must be non-empty")
        if self.quantity <= 0:
            raise ValueError(f"Quantity must be strictly positive, got {self.quantity}")
        if self.price <= 0:
            raise ValueError(f"Price must be strictly positive, got {self.price}")
        if self.side.upper() not in ("BUY", "SELL"):
            raise ValueError(f"Invalid order side: {self.side}. Must be BUY or SELL.")
        if self.order_type.upper() not in ("MARKET", "LIMIT", "STOP_LOSS", "STOP_LIMIT"):
            raise ValueError(f"Invalid order type: {self.order_type}")

    def is_valid(self) -> bool:
        """Check whether order satisfies all business rules without raising."""
        try:
            self.validate()
            return True
        except Exception:
            return False


class LocalPubSubMock:
    """In-memory zero-cost mock of Cloud Pub/Sub with Dead-Letter Queue (DLQ) support.

    Used when running unit/integration tests without live GCP or Docker containers.
    """

    def __init__(self, max_delivery_attempts: int = 5) -> None:
        self.max_delivery_attempts = max_delivery_attempts
        self.topics: Dict[str, List[Dict[str, Any]]] = {}
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self.dlq_messages: List[Dict[str, Any]] = []

    def create_topic(self, topic_name: str) -> None:
        """Create a new topic queue."""
        if topic_name not in self.topics:
            self.topics[topic_name] = []

    def create_subscription(
        self,
        sub_name: str,
        topic_name: str,
        dlq_topic: Optional[str] = None,
    ) -> None:
        """Create a subscription bound to a topic."""
        self.create_topic(topic_name)
        if dlq_topic:
            self.create_topic(dlq_topic)

        self.subscriptions[sub_name] = {
            "topic": topic_name,
            "dlq_topic": dlq_topic,
            "messages": [],
        }

    def publish(self, topic_name: str, data: bytes, attributes: Optional[Dict[str, str]] = None) -> str:
        """Publish a message to a topic."""
        if topic_name not in self.topics:
            raise KeyError(f"Topic {topic_name} does not exist.")

        msg_id = str(uuid.uuid4())
        record = {
            "id": msg_id,
            "data": data,
            "attributes": attributes or {},
            "attempts": 0,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        self.topics[topic_name].append(record)

        # Distribute to subscriptions listening to this topic
        for sub in self.subscriptions.values():
            if sub["topic"] == topic_name:
                sub["messages"].append(dict(record))

        return msg_id

    def process_message(self, sub_name: str, handler_func) -> bool:
        """Simulate consuming and processing a message with DLQ routing on failure."""
        if sub_name not in self.subscriptions:
            raise KeyError(f"Subscription {sub_name} does not exist.")

        sub = self.subscriptions[sub_name]
        if not sub["messages"]:
            return False

        msg = sub["messages"][0]
        msg["attempts"] += 1

        try:
            handler_func(msg["data"], msg["attributes"])
            # Success: acknowledge by removing from queue
            sub["messages"].pop(0)
            return True
        except Exception:
            # Failure: check if exceeded max delivery attempts
            if msg["attempts"] >= self.max_delivery_attempts:
                dlq_topic = sub.get("dlq_topic")
                if dlq_topic and dlq_topic in self.topics:
                    self.topics[dlq_topic].append(msg)
                self.dlq_messages.append(msg)
                sub["messages"].pop(0)
            return False
