"""Zero-Cost Hermetic Asynchronous Google Cloud Pub/Sub Mock.

Provides:
- Thread-safe and coroutine-safe in-memory mock of google.cloud.pubsub_v1.PublisherClient.
- concurrent.futures.Future and asyncio-compatible future with callback support.
- Strict ordering key routing and simulated pause on failure (matching GCP Pub/Sub behavior).
- Transient and permanent fault injection for zero-cost failure and DLQ recovery testing.
- Low-overhead zero external cloud dependency execution.
"""

import asyncio
import concurrent.futures
from datetime import datetime, timezone
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set
import uuid


class PubSubPublishFuture(concurrent.futures.Future):
    """Async-compatible Pub/Sub future mimicking google.cloud.pubsub_v1.publisher.futures.Future."""

    def __init__(
        self,
        message_id: Optional[str] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        super().__init__()
        if exception is not None:
            self.set_exception(exception)
        elif message_id is not None:
            self.set_result(message_id)

    def __await__(self):
        """Allow awaiting this concurrent future directly inside asyncio coroutines."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return asyncio.wrap_future(self, loop=loop).__await__()


class AsyncLocalPubSubMock:
    """In-memory asynchronous mock of Google Cloud Pub/Sub PublisherClient.

    Mimics PublisherClient API:
    - publish(topic, data, ordering_key='', **attributes) -> Future
    - resume_publish(topic, ordering_key) -> None
    """

    def __init__(
        self,
        simulate_latency_seconds: float = 0.0,
        enable_ordering: bool = True,
    ) -> None:
        self.simulate_latency_seconds = simulate_latency_seconds
        self.enable_ordering = enable_ordering
        self._lock = threading.Lock()
        self.topics: Dict[str, List[Dict[str, Any]]] = {}
        self.ordered_messages: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self.transient_failures: Dict[str, int] = {}
        self.permanent_failures: Set[str] = set()
        self.paused_ordering_keys: Set[str] = set()
        self.failure_exceptions: Dict[str, Exception] = {}
        self.publish_count: int = 0

    def create_topic(self, topic: str) -> None:
        """Initialize topic storage."""
        with self._lock:
            if topic not in self.topics:
                self.topics[topic] = []
            if topic not in self.ordered_messages:
                self.ordered_messages[topic] = {}

    def publish(
        self,
        topic: str,
        data: bytes,
        ordering_key: str = "",
        **attributes: str,
    ) -> PubSubPublishFuture:
        """Publish a message asynchronously, returning a PubSubPublishFuture."""
        with self._lock:
            self.publish_count += 1
            if topic not in self.topics:
                self.topics[topic] = []
            if topic not in self.ordered_messages:
                self.ordered_messages[topic] = {}

            future = PubSubPublishFuture()

            # 1. Check permanent failure injection
            if topic in self.permanent_failures:
                exc = self.failure_exceptions.get(
                    topic,
                    RuntimeError(f"Pub/Sub service unavailable for topic {topic}"),
                )
                self._resolve_future(future, exception=exc)
                return future

            # 2. Check transient failure injection
            if self.transient_failures.get(topic, 0) > 0:
                self.transient_failures[topic] -= 1
                if ordering_key and self.enable_ordering:
                    self.paused_ordering_keys.add(f"{topic}#{ordering_key}")
                exc = self.failure_exceptions.get(
                    topic,
                    RuntimeError(f"Pub/Sub transient network timeout on {topic}"),
                )
                self._resolve_future(future, exception=exc)
                return future

            # 3. Check paused ordering key (GCP Pub/Sub pauses an ordering key on error until resumed)
            compound_key = f"{topic}#{ordering_key}" if ordering_key else ""
            if compound_key and compound_key in self.paused_ordering_keys:
                exc = RuntimeError(
                    f"Ordering key '{ordering_key}' is paused due to earlier error. Call resume_publish first."
                )
                self._resolve_future(future, exception=exc)
                return future

            # 4. Generate message ID and store record
            msg_id = f"mock-msg-{uuid.uuid4().hex[:12]}"
            record = {
                "message_id": msg_id,
                "topic": topic,
                "data": data,
                "ordering_key": ordering_key,
                "attributes": dict(attributes),
                "published_at": datetime.now(timezone.utc).isoformat(),
            }

            self.topics[topic].append(record)
            if ordering_key:
                if ordering_key not in self.ordered_messages[topic]:
                    self.ordered_messages[topic][ordering_key] = []
                self.ordered_messages[topic][ordering_key].append(record)

            self._resolve_future(future, result=msg_id)
            return future

    def _resolve_future(
        self,
        future: PubSubPublishFuture,
        result: Optional[str] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Resolve future immediately or after simulated latency."""
        if self.simulate_latency_seconds <= 0:
            if exception is not None:
                future.set_exception(exception)
            else:
                future.set_result(result or "")
        else:
            def delayed_resolve():
                time.sleep(self.simulate_latency_seconds)
                if exception is not None:
                    future.set_exception(exception)
                else:
                    future.set_result(result or "")

            thread = threading.Thread(target=delayed_resolve, daemon=True)
            thread.start()

    def resume_publish(self, topic: str, ordering_key: str) -> None:
        """Resume publishing on a paused ordering key after failure recovery."""
        with self._lock:
            compound_key = f"{topic}#{ordering_key}"
            self.paused_ordering_keys.discard(compound_key)

    def inject_transient_failure(
        self,
        topic: str,
        count: int = 1,
        exception: Optional[Exception] = None,
    ) -> None:
        """Inject N consecutive transient failures on a specific topic."""
        with self._lock:
            self.transient_failures[topic] = count
            if exception:
                self.failure_exceptions[topic] = exception

    def inject_permanent_failure(
        self,
        topic: str,
        exception: Optional[Exception] = None,
    ) -> None:
        """Inject permanent failure on a specific topic."""
        with self._lock:
            self.permanent_failures.add(topic)
            if exception:
                self.failure_exceptions[topic] = exception

    def reset_failures(self, topic: Optional[str] = None) -> None:
        """Clear injected failures."""
        with self._lock:
            if topic:
                self.transient_failures.pop(topic, None)
                self.permanent_failures.discard(topic)
                self.failure_exceptions.pop(topic, None)
                # clear paused keys for this topic
                self.paused_ordering_keys = {
                    k for k in self.paused_ordering_keys if not k.startswith(f"{topic}#")
                }
            else:
                self.transient_failures.clear()
                self.permanent_failures.clear()
                self.failure_exceptions.clear()
                self.paused_ordering_keys.clear()

    def get_published_records(self, topic: str) -> List[Dict[str, Any]]:
        """Retrieve copy of all messages published to a topic."""
        with self._lock:
            return list(self.topics.get(topic, []))

    def get_ordered_records(self, topic: str, ordering_key: str) -> List[Dict[str, Any]]:
        """Retrieve ordered messages published for a specific ordering key."""
        with self._lock:
            return list(self.ordered_messages.get(topic, {}).get(ordering_key, []))

    def clear(self) -> None:
        """Clear all stored state."""
        with self._lock:
            self.topics.clear()
            self.ordered_messages.clear()
            self.transient_failures.clear()
            self.permanent_failures.clear()
            self.paused_ordering_keys.clear()
            self.failure_exceptions.clear()
            self.publish_count = 0
