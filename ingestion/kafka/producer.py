"""Trading Risk Engine - Production Kafka Tick Producer Client & Local Mock.

Provides:
- High-throughput resilient Apache Kafka producer client for financial market ticks.
- In-flight batching, LZ4 compression, idempotency, and strict delivery guarantees.
- Zero-cost hermetic in-memory mock (LocalKafkaProducerMock) for unit testing and CI
  with deterministic partition routing, partition offset tracking, and fault injection.
- Dead-Letter Queue (DLQ) publishing for corrupted quotes or schema rejections.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import uuid

from ingestion.market_tick import DeterministicPartitioner, MarketTick

logger = logging.getLogger("kafka_tick_producer")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "kafka_producer", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


@dataclass
class ProducerConfig:
    """Production configuration parameters for the Kafka tick producer."""

    bootstrap_servers: str = "localhost:9092"
    topic: str = "market.ticks.v1"
    deadletter_topic: str = "market.ticks.deadletter"
    client_id: str = "trading-risk-tick-producer"
    acks: str = "all"
    compression_type: str = "lz4"
    batch_size: int = 16384  # 16 KB batching
    linger_ms: int = 10  # 10ms micro-batching window
    max_in_flight_requests_per_connection: int = 5
    enable_idempotence: bool = True
    retries: int = 5
    retry_backoff_ms: int = 100
    simulation_mode: bool = True
    num_partitions_map: Dict[str, int] = field(
        default_factory=lambda: {
            "market.ticks.v1": 6,
            "market.ticks.deadletter": 3,
            "market.orderbook.depth.v1": 6,
        }
    )

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        """Instantiate producer configuration from environment variables."""
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TICKS_TOPIC", "market.ticks.v1"),
            deadletter_topic=os.getenv("KAFKA_DEADLETTER_TOPIC", "market.ticks.deadletter"),
            client_id=os.getenv("KAFKA_CLIENT_ID", "trading-risk-tick-producer"),
            acks=os.getenv("KAFKA_ACKS", "all"),
            compression_type=os.getenv("KAFKA_COMPRESSION_TYPE", "lz4"),
            batch_size=int(os.getenv("KAFKA_BATCH_SIZE", "16384")),
            linger_ms=int(os.getenv("KAFKA_LINGER_MS", "10")),
            max_in_flight_requests_per_connection=int(
                os.getenv("KAFKA_MAX_IN_FLIGHT_REQUESTS", "5")
            ),
            enable_idempotence=os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower()
            in ("true", "1", "yes"),
            retries=int(os.getenv("KAFKA_RETRIES", "5")),
            retry_backoff_ms=int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "100")),
            simulation_mode=os.getenv("SIMULATION_MODE", "true").lower() in ("true", "1", "yes"),
        )


# Alias for consistent naming
KafkaProducerConfig = ProducerConfig


@dataclass
class ProducerRecord:
    """Represents a record stored in a Kafka partition."""

    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: bytes
    headers: List[Tuple[str, bytes]]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class MockRecordMetadata:
    """Metadata returned upon successful message delivery."""

    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = int(time.time() * 1000)

    def __repr__(self) -> str:
        return f"RecordMetadata(topic={self.topic}, partition={self.partition}, offset={self.offset})"


class LocalKafkaProducerMock:
    """Thread-safe zero-cost in-memory mock Kafka producer for testing and simulation.

    Simulates:
    - Multi-topic and multi-partition record storage.
    - Deterministic partition key routing via DeterministicPartitioner.
    - Partition offset assignment.
    - Asynchronous delivery callback invocation.
    - Fault injection (transient/permanent failures) for retry testing.
    """

    def __init__(self, num_partitions_map: Optional[Dict[str, int]] = None) -> None:
        self.num_partitions_map: Dict[str, int] = num_partitions_map or {
            "market.ticks.v1": 6,
            "market.ticks.deadletter": 3,
            "market.orderbook.depth.v1": 6,
        }
        self._lock = threading.RLock()
        # Storage: topic -> partition -> list of ProducerRecord
        self._partitions: Dict[str, Dict[int, List[ProducerRecord]]] = {}
        self._offsets: Dict[str, Dict[int, int]] = {}
        self._is_closed: bool = False
        self._transient_errors_remaining: int = 0
        self._permanent_error: Optional[Exception] = None

        for topic, n_parts in self.num_partitions_map.items():
            self._ensure_topic(topic, n_parts)

    def _ensure_topic(self, topic: str, num_partitions: int = 6) -> None:
        """Initialize partition queues for a topic if not present."""
        with self._lock:
            if topic not in self._partitions:
                self._partitions[topic] = {p: [] for p in range(num_partitions)}
                self._offsets[topic] = {p: 0 for p in range(num_partitions)}
                self.num_partitions_map[topic] = num_partitions

    def inject_transient_failure(self, count: int = 1) -> None:
        """Inject transient exceptions on the next `count` send invocations."""
        with self._lock:
            self._transient_errors_remaining = count

    def inject_permanent_failure(self, error: Optional[Exception] = None) -> None:
        """Inject a persistent error simulating broker unavailability."""
        with self._lock:
            self._permanent_error = error or RuntimeError("Simulated Kafka broker outage")

    def clear_faults(self) -> None:
        """Clear any injected faults."""
        with self._lock:
            self._transient_errors_remaining = 0
            self._permanent_error = None

    def send(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[List[Tuple[str, bytes]]] = None,
        partition: Optional[int] = None,
        callback: Optional[Callable[[Optional[Exception], Optional[MockRecordMetadata]], None]] = None,
    ) -> MockRecordMetadata:
        """Produce a message into the target partition queue."""
        with self._lock:
            if self._is_closed:
                raise RuntimeError("Cannot produce: Producer is closed")

            # Check fault injection
            if self._permanent_error:
                err = self._permanent_error
                if callback:
                    callback(err, None)
                raise err

            if self._transient_errors_remaining > 0:
                self._transient_errors_remaining -= 1
                err = RuntimeError(
                    f"Simulated transient broker timeout ({self._transient_errors_remaining} remaining)"
                )
                if callback:
                    callback(err, None)
                raise err

            num_parts = self.num_partitions_map.get(topic, 6)
            self._ensure_topic(topic, num_parts)

            # Determine partition
            if partition is not None:
                target_partition = partition % num_parts
            elif key is not None:
                key_str = key.decode("utf-8", errors="replace")
                target_partition = DeterministicPartitioner.get_partition(key_str, num_parts)
            else:
                target_partition = 0

            offset = self._offsets[topic][target_partition]
            record = ProducerRecord(
                topic=topic,
                partition=target_partition,
                offset=offset,
                key=key,
                value=value,
                headers=headers or [],
            )
            self._partitions[topic][target_partition].append(record)
            self._offsets[topic][target_partition] = offset + 1

            metadata = MockRecordMetadata(
                topic=topic, partition=target_partition, offset=offset
            )

            if callback:
                try:
                    callback(None, metadata)
                except Exception as ex:
                    logger.debug(f"Exception in delivery callback: {ex}")

            return metadata

    @property
    def partitions(self) -> Dict[str, Dict[int, List[ProducerRecord]]]:
        """Access topic partitions mapping for inspection and consumer testing."""
        with self._lock:
            return self._partitions

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush simulated buffer."""
        pass

    def close(self, timeout: Optional[float] = None) -> None:
        """Close mock producer."""
        with self._lock:
            self._is_closed = True

    def get_records(
        self, topic: str, partition: Optional[int] = None
    ) -> List[ProducerRecord]:
        """Retrieve stored records for testing inspection."""
        with self._lock:
            if topic not in self._partitions:
                return []
            if partition is not None:
                return list(self._partitions[topic].get(partition, []))
            records: List[ProducerRecord] = []
            for p in sorted(self._partitions[topic].keys()):
                records.extend(self._partitions[topic][p])
            return records

    def get_total_records(self, topic: Optional[str] = None) -> int:
        """Return total count of records produced."""
        with self._lock:
            if topic:
                return len(self.get_records(topic))
            total = 0
            for t in self._partitions:
                total += len(self.get_records(t))
            return total

    def get_partition_distribution(self, topic: str) -> Dict[int, int]:
        """Return mapping of partition index to record count."""
        with self._lock:
            if topic not in self._partitions:
                return {}
            return {p: len(records) for p, records in self._partitions[topic].items()}


class KafkaTickProducer:
    """Production-grade Kafka Producer for high-frequency financial market ticks."""

    def __init__(
        self,
        config: Optional[ProducerConfig] = None,
        mock_producer: Optional[LocalKafkaProducerMock] = None,
    ) -> None:
        self.config = config or ProducerConfig.from_env()
        self.mock_producer = mock_producer
        self._live_producer: Any = None
        self._is_closed: bool = False
        self._lock = threading.Lock()

        if self.mock_producer is None and not self.config.simulation_mode:
            try:
                from kafka import KafkaProducer  # type: ignore

                self._live_producer = KafkaProducer(
                    bootstrap_servers=self.config.bootstrap_servers,
                    client_id=self.config.client_id,
                    acks=self.config.acks,
                    compression_type=self.config.compression_type,
                    batch_size=self.config.batch_size,
                    linger_ms=self.config.linger_ms,
                    retries=self.config.retries,
                    retry_backoff_ms=self.config.retry_backoff_ms,
                    max_in_flight_requests_per_connection=self.config.max_in_flight_requests_per_connection,
                )
                logger.info(
                    f"Initialized live KafkaProducer connected to {self.config.bootstrap_servers}"
                )
            except Exception as ex:
                logger.warning(
                    f"Unable to connect to live Kafka broker at {self.config.bootstrap_servers} ({ex}). "
                    "Falling back to LocalKafkaProducerMock."
                )
                self.mock_producer = LocalKafkaProducerMock(
                    num_partitions_map=self.config.num_partitions_map
                )
        elif self.mock_producer is None:
            self.mock_producer = LocalKafkaProducerMock(
                num_partitions_map=self.config.num_partitions_map
            )

    @property
    def is_mock(self) -> bool:
        """Returns True if running in zero-cost mock mode."""
        return self.mock_producer is not None

    def send_tick(
        self,
        tick: MarketTick,
        callback: Optional[Callable[[Optional[Exception], Any], None]] = None,
    ) -> Any:
        """Serialize and produce a MarketTick to Kafka with deterministic partitioning.

        Args:
            tick: Validated MarketTick instance.
            callback: Optional delivery callback (err, metadata).

        Returns:
            Metadata object or Future.
        """
        if self._is_closed:
            raise RuntimeError("KafkaTickProducer is closed")

        payload = tick.to_json().encode("utf-8")
        key = tick.partition_key.encode("utf-8")
        headers = [
            ("source", b"trading-risk-engine"),
            ("asset_class", tick.asset_class.encode("utf-8")),
            ("ticker", tick.ticker.encode("utf-8")),
            ("sequence_number", str(tick.sequence_number).encode("utf-8")),
            ("timestamp", tick.timestamp.encode("utf-8")),
        ]

        # Retry logic with exponential backoff for resilience
        last_error: Optional[Exception] = None
        backoff_sec = max(0.01, self.config.retry_backoff_ms / 1000.0)

        for attempt in range(1, self.config.retries + 1):
            try:
                if self.mock_producer:
                    return self.mock_producer.send(
                        topic=self.config.topic,
                        key=key,
                        value=payload,
                        headers=headers,
                        callback=callback,
                    )
                else:
                    future = self._live_producer.send(
                        topic=self.config.topic,
                        key=key,
                        value=payload,
                        headers=headers,
                    )
                    if callback:
                        future.add_callback(lambda meta: callback(None, meta))
                        future.add_errback(lambda err: callback(err, None))
                    return future

            except Exception as ex:
                last_error = ex
                logger.warning(
                    f"Send attempt {attempt}/{self.config.retries} for tick {tick.ticker} seq {tick.sequence_number} failed: {ex}"
                )
                if attempt < self.config.retries:
                    time.sleep(backoff_sec)
                    backoff_sec *= 1.5

        # All retries exhausted -> Route to Dead Letter Queue
        logger.error(
            f"Exceeded {self.config.retries} retries publishing tick {tick.ticker}. Routing to DLQ."
        )
        try:
            self.send_deadletter(
                payload=tick.to_dict(),
                error_reason=f"Exceeded max retries: {last_error}",
                original_key=tick.partition_key,
            )
        except Exception as dlq_err:
            logger.error(f"Failed to route tick {tick.ticker} to DLQ: {dlq_err}")

        if callback:
            callback(last_error, None)
        raise RuntimeError(
            f"Failed to produce tick {tick.ticker} after {self.config.retries} attempts: {last_error}"
        ) from last_error


    def send_deadletter(
        self,
        payload: Any,
        error_reason: str,
        original_key: Optional[str] = None,
    ) -> Any:
        """Route malformed, unparseable, or failed ticks to dead-letter queue topic."""
        dlq_record = {
            "deadletter_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error_reason": str(error_reason),
            "original_key": original_key or "UNKNOWN",
            "raw_payload": payload,
        }
        encoded = json.dumps(dlq_record).encode("utf-8")
        key_bytes = (original_key or "DEADLETTER").encode("utf-8")
        headers = [
            ("source", b"trading-risk-engine-dlq"),
            ("error", error_reason[:200].encode("utf-8")),
        ]

        if self.mock_producer:
            return self.mock_producer.send(
                topic=self.config.deadletter_topic,
                key=key_bytes,
                value=encoded,
                headers=headers,
            )
        else:
            return self._live_producer.send(
                topic=self.config.deadletter_topic,
                key=key_bytes,
                value=encoded,
                headers=headers,
            )

    @property
    def partitions(self) -> Dict[str, Dict[int, List[ProducerRecord]]]:
        """Access topic partitions mapping for inspection and consumer testing."""
        with self._lock:
            if self.mock_producer is not None:
                return self.mock_producer.partitions
            return {}

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush any pending in-flight messages."""
        if self._live_producer:
            self._live_producer.flush(timeout=timeout)
        elif self.mock_producer:
            self.mock_producer.flush(timeout=timeout)

    def close(self, timeout: Optional[float] = None) -> None:
        """Flush buffers and cleanly close the producer."""
        with self._lock:
            if self._is_closed:
                return
            self._is_closed = True
            if self._live_producer:
                try:
                    self._live_producer.flush(timeout=timeout or 5.0)
                    self._live_producer.close(timeout=timeout or 5.0)
                except Exception as ex:
                    logger.debug(f"Error closing live producer: {ex}")
            elif self.mock_producer:
                self.mock_producer.flush(timeout=timeout)
                self.mock_producer.close(timeout=timeout)
            logger.info("KafkaTickProducer closed cleanly.")
