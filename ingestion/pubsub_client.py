"""Local Pub/Sub Client and Zero-Cost Testing Utilities.

Provides:
- Resilient client initialization supporting both GCP Cloud and Local Emulator.
- Message validation against trading order schema.
- Dead-letter queue simulation logic.
"""

from dataclasses import asdict, dataclass
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
    order_type: str  # MARKET or LIMIT
    quantity: float
    price: float
    notional_usd: float
    timestamp: str

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
    ) -> "TradeOrder":
        """Factory method with automatic validation and UUID generation."""
        if quantity <= 0:
            raise ValueError(f"Quantity must be strictly positive, got {quantity}")
        if price <= 0:
            raise ValueError(f"Price must be strictly positive, got {price}")
        if side.upper() not in ("BUY", "SELL"):
            raise ValueError(f"Invalid order side: {side}. Must be BUY or SELL.")

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
        )

    def to_json(self) -> str:
        """Serialize trade order to JSON string."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "TradeOrder":
        """Deserialize from JSON string."""
        payload = json.loads(data)
        return cls(**payload)


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
