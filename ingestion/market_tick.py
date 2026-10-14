"""Trading Risk Engine - High-Throughput Market Tick Data Model & Partitioning.

Provides:
- Strict dataclass representation for real-time financial market ticks.
- Invariant validation (non-inverted spread, positive prices and volumes).
- Deterministic multi-partition routing key generation ensuring strict per-ticker
  chronological ordering in Apache Kafka.
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Dict, Optional, Set
import uuid

ALLOWED_ASSET_CLASSES: Set[str] = {"EQUITY", "FX", "COMMODITY", "CRYPTO", "ETF"}


@dataclass
class MarketTick:
    """Represents a single validated multi-asset market quote or trade execution tick."""

    trade_id: str
    ticker: str
    asset_class: str
    bid: float
    ask: float
    last_price: float
    volume: float
    spread: float
    mid_price: float
    timestamp: str
    sequence_number: int = 0

    @classmethod
    def create(
        cls,
        ticker: str,
        asset_class: str,
        bid: float,
        ask: float,
        last_price: float,
        volume: float,
        trade_id: Optional[str] = None,
        timestamp: Optional[str] = None,
        sequence_number: int = 0,
    ) -> "MarketTick":
        """Factory method with strict quantitative invariants validation.

        Raises:
            ValueError: If prices/volume are non-positive, if quotes are crossed (bid > ask),
                        or if asset_class is unsupported.
        """
        asset_cls = asset_class.strip().upper()
        if asset_cls not in ALLOWED_ASSET_CLASSES:
            raise ValueError(
                f"Unsupported asset class '{asset_class}'. Must be one of {sorted(ALLOWED_ASSET_CLASSES)}"
            )

        if bid <= 0:
            raise ValueError(f"Bid price must be strictly positive, got {bid}")
        if ask <= 0:
            raise ValueError(f"Ask price must be strictly positive, got {ask}")
        if last_price <= 0:
            raise ValueError(f"Last traded price must be strictly positive, got {last_price}")
        if volume <= 0:
            raise ValueError(f"Trade volume must be strictly positive, got {volume}")
        if bid > ask:
            raise ValueError(f"Crossed market quote: bid ({bid}) cannot exceed ask ({ask})")

        spread = round(ask - bid, 4)
        mid_price = round((bid + ask) / 2.0, 4)
        tid = trade_id or str(uuid.uuid4())
        ts = timestamp or datetime.now(timezone.utc).isoformat()

        return cls(
            trade_id=tid,
            ticker=ticker.strip().upper(),
            asset_class=asset_cls,
            bid=round(bid, 4),
            ask=round(ask, 4),
            last_price=round(last_price, 4),
            volume=round(volume, 4),
            spread=spread,
            mid_price=mid_price,
            timestamp=ts,
            sequence_number=sequence_number,
        )

    @property
    def partition_key(self) -> str:
        """Deterministic partition key (ASSET_CLASS#TICKER) ensuring strict ordering."""
        return f"{self.asset_class}#{self.ticker}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert tick to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize tick to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, data: str) -> "MarketTick":
        """Deserialize from JSON string."""
        payload = json.loads(data)
        return cls(**payload)


class DeterministicPartitioner:
    """Deterministic hash partitioner simulating Kafka default partitioner.

    Ensures ticks sharing the same partition key always route to the exact same partition index,
    preventing out-of-order execution in downstream Spark Structured Streaming windows.
    """

    @staticmethod
    def get_partition(key: str, num_partitions: int = 6) -> int:
        """Compute target partition index for a given partition key."""
        if num_partitions <= 0:
            raise ValueError(f"num_partitions must be > 0, got {num_partitions}")

        # Deterministic MD5 hash to 32-bit unsigned integer
        digest = hashlib.md5(key.encode("utf-8")).digest()
        hash_val = int.from_bytes(digest[:4], byteorder="big", signed=False)
        return hash_val % num_partitions
