"""Production Asynchronous Trade Order Producer for Google Cloud Pub/Sub.

Features:
- High-throughput asynchronous publishing supporting both Google Cloud Pub/Sub and zero-cost mock.
- Batching controls: max_messages, max_bytes, max_latency.
- Strict message ordering per ordering key (portfolio_id#ticker) to maintain FIFO trade order stream.
- In-flight message flow control and backpressure handling via asyncio.Semaphore.
- Exponential backoff retry policies for transient network and broker failures.
- Dead-Letter Queue (DLQ) automated routing for malformed orders or exhausted retries.
- Thread-safe and coroutine-safe real-time telemetry metrics and Prometheus exposition.
- Clean graceful flush and lifecycle management.
"""

import asyncio
from collections import deque
import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from ingestion.pubsub_client import TradeOrder
from ingestion.pubsub.mock import AsyncLocalPubSubMock, PubSubPublishFuture

logger = logging.getLogger("async_trade_order_producer")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class PubSubProducerError(Exception):
    """Base exception for AsyncTradeOrderProducer."""
    pass


class OrderValidationError(PubSubProducerError):
    """Raised when an order fails business validation prior to publication."""
    pass


class PubSubPublishError(PubSubProducerError):
    """Raised when an order fails to publish after exhausting all retry attempts."""
    pass


@dataclass
class AsyncProducerConfig:
    """Configuration parameters for asynchronous trade order publisher."""

    project_id: str = "trading-risk-dev-001"
    topic_id: str = "projects/trading-risk-dev-001/topics/top-trade-orders"
    dlq_topic_id: str = "projects/trading-risk-dev-001/topics/top-trade-orders-dlq"
    environment: str = "dev"
    batch_max_messages: int = 100
    batch_max_bytes: int = 1_048_576  # 1 MB
    batch_max_latency_seconds: float = 0.05  # 50 ms
    enable_message_ordering: bool = True
    ordering_key_strategy: str = "compound"  # compound, portfolio, ticker, none
    max_inflight_messages: int = 500
    max_retries: int = 3
    initial_backoff_sec: float = 0.05
    backoff_multiplier: float = 2.0
    max_backoff_sec: float = 1.0
    simulation_mode: bool = False
    emulator_host: Optional[str] = None

    @classmethod
    def from_env(cls) -> "AsyncProducerConfig":
        """Instantiate configuration from environment variables."""
        return cls(
            project_id=os.getenv("PROJECT_ID", "trading-risk-dev-001"),
            topic_id=os.getenv(
                "PUBSUB_TOPIC_ID",
                "projects/trading-risk-dev-001/topics/top-trade-orders",
            ),
            dlq_topic_id=os.getenv(
                "PUBSUB_DLQ_TOPIC_ID",
                "projects/trading-risk-dev-001/topics/top-trade-orders-dlq",
            ),
            environment=os.getenv("ENVIRONMENT", "dev"),
            batch_max_messages=int(os.getenv("BATCH_MAX_MESSAGES", "100")),
            batch_max_bytes=int(os.getenv("BATCH_MAX_BYTES", "1048576")),
            batch_max_latency_seconds=float(os.getenv("BATCH_MAX_LATENCY_SEC", "0.05")),
            enable_message_ordering=os.getenv("ENABLE_MESSAGE_ORDERING", "true").lower() in ("true", "1", "yes"),
            ordering_key_strategy=os.getenv("ORDERING_KEY_STRATEGY", "compound"),
            max_inflight_messages=int(os.getenv("MAX_INFLIGHT_MESSAGES", "500")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            initial_backoff_sec=float(os.getenv("INITIAL_BACKOFF_SEC", "0.05")),
            backoff_multiplier=float(os.getenv("BACKOFF_MULTIPLIER", "2.0")),
            max_backoff_sec=float(os.getenv("MAX_BACKOFF_SEC", "1.0")),
            simulation_mode=os.getenv("SIMULATION_MODE", "false").lower() in ("true", "1", "yes"),
            emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
        )


class AsyncProducerTelemetry:
    """Thread-safe and coroutine-safe telemetry store for async publisher."""

    def __init__(self, latency_window_size: int = 1000) -> None:
        self._lock = threading.Lock()
        self.start_time: float = time.time()
        self.orders_published: int = 0
        self.orders_failed: int = 0
        self.dlq_messages_routed: int = 0
        self.notional_usd_total: float = 0.0
        self.retries_attempted: int = 0
        self.inflight_orders: int = 0
        self.last_order_id: Optional[str] = None
        self.last_published_timestamp: Optional[str] = None
        self._latencies_ms: deque = deque(maxlen=latency_window_size)

    def record_success(self, order_id: str, notional: float, latency_ms: float) -> None:
        with self._lock:
            self.orders_published += 1
            self.notional_usd_total = round(self.notional_usd_total + notional, 2)
            self.last_order_id = order_id
            self.last_published_timestamp = datetime.now(timezone.utc).isoformat()
            self._latencies_ms.append(latency_ms)

    def record_error(self) -> None:
        with self._lock:
            self.orders_failed += 1

    def record_retry(self) -> None:
        with self._lock:
            self.retries_attempted += 1

    def record_dlq(self) -> None:
        with self._lock:
            self.dlq_messages_routed += 1

    def inc_inflight(self) -> None:
        with self._lock:
            self.inflight_orders += 1

    def dec_inflight(self) -> None:
        with self._lock:
            self.inflight_orders = max(0, self.inflight_orders - 1)

    def get_snapshot(self) -> Dict[str, Any]:
        """Obtain consistent snapshot of publisher telemetry."""
        with self._lock:
            uptime = max(0.001, time.time() - self.start_time)
            latencies = list(self._latencies_ms)
            p50 = p95 = p99 = mean_lat = 0.0
            if latencies:
                sorted_lats = sorted(latencies)
                n = len(sorted_lats)
                p50 = sorted_lats[int(n * 0.5)]
                p95 = sorted_lats[min(n - 1, int(n * 0.95))]
                p99 = sorted_lats[min(n - 1, int(n * 0.99))]
                mean_lat = round(sum(latencies) / n, 2)

            rate = round(self.orders_published / uptime, 2)

            return {
                "uptime_seconds": round(uptime, 2),
                "orders_published": self.orders_published,
                "orders_failed": self.orders_failed,
                "dlq_messages_routed": self.dlq_messages_routed,
                "notional_usd_total": round(self.notional_usd_total, 2),
                "retries_attempted": self.retries_attempted,
                "inflight_orders": self.inflight_orders,
                "orders_per_second": rate,
                "latency_ms_mean": mean_lat,
                "latency_ms_p50": round(p50, 2),
                "latency_ms_p95": round(p95, 2),
                "latency_ms_p99": round(p99, 2),
                "last_order_id": self.last_order_id,
                "last_published_timestamp": self.last_published_timestamp,
            }

    def format_prometheus(self) -> str:
        """Format metrics in Prometheus text exposition format."""
        s = self.get_snapshot()
        lines = [
            "# HELP trading_orders_published_total Total trade orders published successfully",
            "# TYPE trading_orders_published_total counter",
            f"trading_orders_published_total {s['orders_published']}",
            "# HELP trading_orders_failed_total Total trade orders failed to publish",
            "# TYPE trading_orders_failed_total counter",
            f"trading_orders_failed_total {s['orders_failed']}",
            "# HELP trading_orders_dlq_routed_total Total orders routed to dead-letter queue",
            "# TYPE trading_orders_dlq_routed_total counter",
            f"trading_orders_dlq_routed_total {s['dlq_messages_routed']}",
            "# HELP trading_orders_notional_usd_total Cumulative USD notional published",
            "# TYPE trading_orders_notional_usd_total counter",
            f"trading_orders_notional_usd_total {s['notional_usd_total']:.2f}",
            "# HELP trading_orders_retries_total Cumulative publish retries attempted",
            "# TYPE trading_orders_retries_total counter",
            f"trading_orders_retries_total {s['retries_attempted']}",
            "# HELP trading_orders_inflight_current Current inflight unconfirmed orders",
            "# TYPE trading_orders_inflight_current gauge",
            f"trading_orders_inflight_current {s['inflight_orders']}",
            "# HELP trading_orders_publish_rate Throughput in orders per second",
            "# TYPE trading_orders_publish_rate gauge",
            f"trading_orders_publish_rate {s['orders_per_second']:.2f}",
            "# HELP trading_orders_latency_ms_p95 95th percentile publish latency in ms",
            "# TYPE trading_orders_latency_ms_p95 gauge",
            f"trading_orders_latency_ms_p95 {s['latency_ms_p95']:.2f}",
        ]
        return "\n".join(lines) + "\n"


class AsyncTradeOrderProducer:
    """High-performance Asynchronous Trade Order Producer for Cloud Pub/Sub."""

    def __init__(
        self,
        config: Optional[AsyncProducerConfig] = None,
        publisher: Any = None,
    ) -> None:
        self.config = config or AsyncProducerConfig.from_env()
        self.telemetry = AsyncProducerTelemetry()
        self._inflight_semaphore: Optional[asyncio.Semaphore] = None
        self._closed: bool = False
        self._pending_tasks: Set[asyncio.Task] = set()

        if publisher is not None:
            self._publisher = publisher
        elif self.config.simulation_mode or self.config.emulator_host:
            mock = AsyncLocalPubSubMock(enable_ordering=self.config.enable_message_ordering)
            mock.create_topic(self.config.topic_id)
            mock.create_topic(self.config.dlq_topic_id)
            self._publisher = mock
        else:
            try:
                from google.cloud import pubsub_v1

                batch_settings = pubsub_v1.types.BatchSettings(
                    max_messages=self.config.batch_max_messages,
                    max_bytes=self.config.batch_max_bytes,
                    max_latency=self.config.batch_max_latency_seconds,
                )
                publisher_options = pubsub_v1.types.PublisherOptions(
                    enable_message_ordering=self.config.enable_message_ordering
                )
                self._publisher = pubsub_v1.PublisherClient(
                    batch_settings=batch_settings,
                    publisher_options=publisher_options,
                )
            except (ImportError, Exception) as exc:
                logger.info(
                    f"Google Cloud Pub/Sub client not available ({exc}), initializing AsyncLocalPubSubMock."
                )
                mock = AsyncLocalPubSubMock(enable_ordering=self.config.enable_message_ordering)
                mock.create_topic(self.config.topic_id)
                mock.create_topic(self.config.dlq_topic_id)
                self._publisher = mock

    @property
    def semaphore(self) -> asyncio.Semaphore:
        """Lazy initialization of semaphore bound to current event loop."""
        if self._inflight_semaphore is None:
            self._inflight_semaphore = asyncio.Semaphore(self.config.max_inflight_messages)
        return self._inflight_semaphore

    def compute_ordering_key(self, order: TradeOrder) -> str:
        """Derive Pub/Sub ordering key from order attributes."""
        if not self.config.enable_message_ordering:
            return ""
        return order.get_ordering_key(strategy=self.config.ordering_key_strategy)

    async def _await_future(self, future: Any) -> str:
        """Asynchronously await a concurrent.futures.Future or custom future."""
        if isinstance(future, PubSubPublishFuture):
            return await future
        elif isinstance(future, concurrent.futures.Future):
            loop = asyncio.get_running_loop()
            return await asyncio.wrap_future(future, loop=loop)
        elif asyncio.iscoroutine(future):
            return await future
        elif hasattr(future, "result"):
            # Synchronous or already completed
            return str(future.result())
        return str(future)

    async def route_to_dlq(
        self,
        order: TradeOrder,
        error_reason: str,
        attempts: int = 0,
    ) -> str:
        """Route failed or malformed order execution directly to dead-letter topic."""
        payload = order.to_json().encode("utf-8")
        dlq_attributes = order.get_attributes(environment=self.config.environment)
        dlq_attributes["error_reason"] = error_reason[:255]
        dlq_attributes["failed_topic"] = self.config.topic_id
        dlq_attributes["retry_attempts"] = str(attempts)
        dlq_attributes["routed_to_dlq_at"] = datetime.now(timezone.utc).isoformat()

        logger.warning(
            f"Routing order {order.order_id} ({order.ticker}) to DLQ {self.config.dlq_topic_id}: {error_reason}"
        )

        future = self._publisher.publish(
            self.config.dlq_topic_id,
            data=payload,
            ordering_key="",
            **dlq_attributes,
        )
        msg_id = await self._await_future(future)
        self.telemetry.record_dlq()
        return msg_id

    async def publish(self, order: TradeOrder) -> str:
        """Publish a single trade order asynchronously with flow control, ordering, and retry policies.

        Raises:
            OrderValidationError: If order fails structural validation.
            PubSubPublishError: If publish attempts are exhausted without success.
        """
        if self._closed:
            raise RuntimeError("AsyncTradeOrderProducer is closed.")

        # 1. Structural validation
        try:
            order.validate()
        except ValueError as val_err:
            reason = f"Pre-publish validation failed: {val_err}"
            self.telemetry.record_error()
            await self.route_to_dlq(order, error_reason=reason, attempts=0)
            raise OrderValidationError(reason) from val_err

        # 2. Acquire flow control semaphore to prevent client memory bloat
        await self.semaphore.acquire()
        self.telemetry.inc_inflight()

        ordering_key = self.compute_ordering_key(order)
        payload = order.to_json().encode("utf-8")
        attributes = order.get_attributes(environment=self.config.environment)

        backoff = self.config.initial_backoff_sec
        last_error: Optional[Exception] = None

        try:
            for attempt in range(1, self.config.max_retries + 1):
                start_time = time.perf_counter()
                try:
                    # Publish to Cloud Pub/Sub or mock
                    future = self._publisher.publish(
                        self.config.topic_id,
                        data=payload,
                        ordering_key=ordering_key,
                        **attributes,
                    )

                    msg_id = await self._await_future(future)
                    latency_ms = (time.perf_counter() - start_time) * 1000.0

                    self.telemetry.record_success(order.order_id, order.notional_usd, latency_ms)
                    return msg_id

                except Exception as ex:
                    last_error = ex
                    self.telemetry.record_retry()
                    logger.warning(
                        f"Publish attempt {attempt}/{self.config.max_retries} failed for order {order.order_id} "
                        f"({order.ticker}, key={ordering_key}): {ex}. Backing off {backoff:.3f}s..."
                    )

                    # Resume publishing on ordering key if publisher supports it
                    if ordering_key and hasattr(self._publisher, "resume_publish"):
                        try:
                            self._publisher.resume_publish(self.config.topic_id, ordering_key)
                        except Exception as resume_err:
                            logger.debug(f"Error resuming ordering key {ordering_key}: {resume_err}")

                    if attempt < self.config.max_retries:
                        await asyncio.sleep(backoff)
                        backoff = min(self.config.max_backoff_sec, backoff * self.config.backoff_multiplier)

            # 3. Exhausted all retries -> Route to DLQ
            self.telemetry.record_error()
            error_reason = f"Exhausted {self.config.max_retries} retries: {last_error}"
            await self.route_to_dlq(order, error_reason=error_reason, attempts=self.config.max_retries)

            raise PubSubPublishError(
                f"Failed to publish order {order.order_id} after {self.config.max_retries} attempts: {last_error}"
            ) from last_error

        finally:
            self.telemetry.dec_inflight()
            self.semaphore.release()

    async def publish_batch(self, orders: List[TradeOrder]) -> List[str]:
        """Concurrently publish a batch of trade orders respecting flow control limits."""
        if not orders:
            return []
        tasks = [asyncio.create_task(self.publish(order)) for order in orders]
        return await asyncio.gather(*tasks)

    def publish_defer(
        self,
        order: TradeOrder,
        on_success: Optional[Callable[[str], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> Any:
        """Non-blocking fire-and-forget publish dispatching a background coroutine."""
        async def _coro():
            try:
                msg_id = await self.publish(order)
                if on_success:
                    on_success(msg_id)
            except Exception as ex:
                if on_error:
                    on_error(ex)

        loop = asyncio.get_event_loop()
        task = loop.create_task(_coro())
        return task

    async def flush(self, timeout: float = 10.0) -> None:
        """Wait until all inflight publishing tasks have completed."""
        start = time.time()
        while self.telemetry.inflight_orders > 0 and (time.time() - start) < timeout:
            await asyncio.sleep(0.01)

    async def aclose(self) -> None:
        """Flush inflight messages and mark producer closed."""
        self._closed = True
        await self.flush(timeout=5.0)

    async def __aenter__(self) -> "AsyncTradeOrderProducer":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()
