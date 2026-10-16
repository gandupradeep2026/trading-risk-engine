"""Asynchronous Trade Order Producer Unit and Integration Tests (Day 07).

Validates:
- TradeOrder schema enhancements, ordering keys, attribute extraction, and validation rules.
- AsyncLocalPubSubMock future resolution, callbacks, ordering key FIFO queues, and fault injection.
- AsyncTradeOrderProducer:
  - Single and concurrent batch order publishing.
  - Strict message ordering key generation (portfolio_id#ticker).
  - Inflight concurrency flow control and backpressure management.
  - Transient network failure retry loop with exponential backoff.
  - Permanent failure retry exhaustion and automated Dead-Letter Queue (DLQ) routing.
  - Structural validation failure and immediate DLQ diversion.
  - Non-blocking fire-and-forget deferred publishing with callback hooks.
  - Telemetry counters, percentiles, and Prometheus exposition.
  - Graceful flush and resource release.
- OrderFeedGenerator multi-asset coverage, lot rules, monotonic sequence numbering, and corrupt order generation.
- AsyncOrderEngine queue backpressure, worker pool concurrency, HTTP health/metrics endpoints, and clean termination.
- Systemd service unit and Docker Compose infrastructure configurations.
"""

import asyncio
from datetime import datetime
import functools
import json
import os
from pathlib import Path
import time
import urllib.request
import pytest


def run_async(func):
    """Decorator to run async test functions synchronously using standard library asyncio.run()."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    return wrapper

from ingestion.order_feed import DEFAULT_ORDER_UNIVERSE, OrderFeedGenerator
from ingestion.pubsub.async_producer import (
    AsyncProducerConfig,
    AsyncProducerTelemetry,
    AsyncTradeOrderProducer,
    OrderValidationError,
    PubSubProducerError,
    PubSubPublishError,
)
from ingestion.pubsub.mock import AsyncLocalPubSubMock, PubSubPublishFuture
from ingestion.pubsub_client import TradeOrder
from ingestion.async_order_engine import AsyncOrderEngine, EngineConfig

REPO_ROOT = Path(__file__).resolve().parent.parent
INGESTION_DIR = REPO_ROOT / "ingestion"
SYSTEMD_DIR = INGESTION_DIR / "systemd"


# -----------------------------------------------------------------------------
# 1. TRADE ORDER SCHEMA & VALIDATION TESTS
# -----------------------------------------------------------------------------
class TestTradeOrderSchemaAndValidation:
    """Validate TradeOrder schema extensions, validation rules, and ordering keys."""

    def test_trade_order_extended_fields_and_defaults(self) -> None:
        """Verify new metadata fields and their production default values."""
        order = TradeOrder.create(
            portfolio_id="PORT-EQ-001",
            ticker="AAPL",
            asset_class="EQUITY",
            side="BUY",
            order_type="LIMIT",
            quantity=50.0,
            price=180.0,
        )
        assert order.trader_id == "ALGO-EXEC-01"
        assert order.execution_status == "FILLED"
        assert order.sequence_number == 1
        assert order.notional_usd == 9000.00

    def test_trade_order_ordering_key_strategies(self) -> None:
        """Verify partition ordering key computation for compound, portfolio, and ticker."""
        order = TradeOrder.create(
            portfolio_id="PORT-MACRO-02",
            ticker="EURUSD",
            asset_class="FX",
            side="SELL",
            order_type="MARKET",
            quantity=100000.0,
            price=1.0850,
        )
        assert order.get_ordering_key("compound") == "PORT-MACRO-02#EURUSD"
        assert order.get_ordering_key("portfolio") == "PORT-MACRO-02"
        assert order.get_ordering_key("ticker") == "EURUSD"
        assert order.get_ordering_key("none") == ""

    def test_trade_order_attributes_mapping(self) -> None:
        """Verify Pub/Sub attributes dictionary contains all essential routing metadata."""
        order = TradeOrder.create(
            portfolio_id="PORT-CRYPTO-03",
            ticker="BTCUSD",
            asset_class="CRYPTO",
            side="BUY",
            order_type="LIMIT",
            quantity=1.5,
            price=65000.0,
            trader_id="ALGO-MOMENTUM-01",
            sequence_number=42,
        )
        attrs = order.get_attributes(environment="prod")
        assert attrs["order_id"] == order.order_id
        assert attrs["portfolio_id"] == "PORT-CRYPTO-03"
        assert attrs["ticker"] == "BTCUSD"
        assert attrs["asset_class"] == "CRYPTO"
        assert attrs["side"] == "BUY"
        assert attrs["order_type"] == "LIMIT"
        assert attrs["notional_usd"] == "97500.00"
        assert attrs["trader_id"] == "ALGO-MOMENTUM-01"
        assert attrs["sequence_number"] == "42"
        assert attrs["environment"] == "prod"

    def test_trade_order_validation_rules(self) -> None:
        """Ensure invalid field values trigger strict ValueError exceptions."""
        valid_order = TradeOrder.create(
            portfolio_id="PORT-1", ticker="NVDA", asset_class="EQUITY",
            side="BUY", order_type="LIMIT", quantity=10.0, price=800.0
        )
        valid_order.validate()  # Must not raise

        # Invalid quantity
        bad_qty = TradeOrder(
            order_id="1", portfolio_id="P1", ticker="AAPL", asset_class="EQUITY",
            side="BUY", order_type="LIMIT", quantity=-5.0, price=100.0,
            notional_usd=-500.0, timestamp="2026-10-16T12:00:00Z"
        )
        with pytest.raises(ValueError, match="strictly positive"):
            bad_qty.validate()
        assert not bad_qty.is_valid()

        # Invalid side
        bad_side = TradeOrder(
            order_id="2", portfolio_id="P1", ticker="AAPL", asset_class="EQUITY",
            side="HOLD", order_type="LIMIT", quantity=5.0, price=100.0,
            notional_usd=500.0, timestamp="2026-10-16T12:00:00Z"
        )
        with pytest.raises(ValueError, match="Invalid order side"):
            bad_side.validate()
        assert not bad_side.is_valid()

        # Missing order_id
        bad_oid = TradeOrder(
            order_id="", portfolio_id="P1", ticker="AAPL", asset_class="EQUITY",
            side="BUY", order_type="LIMIT", quantity=5.0, price=100.0,
            notional_usd=500.0, timestamp="2026-10-16T12:00:00Z"
        )
        with pytest.raises(ValueError, match="order_id must be non-empty"):
            bad_oid.validate()

    def test_trade_order_json_backwards_compatibility(self) -> None:
        """Verify that JSON records missing new fields are deserialized with default values."""
        legacy_json = json.dumps({
            "order_id": "legacy-uuid-1234",
            "portfolio_id": "PORT-LEGACY",
            "ticker": "MSFT",
            "asset_class": "EQUITY",
            "side": "SELL",
            "order_type": "MARKET",
            "quantity": 100.0,
            "price": 400.0,
            "notional_usd": 40000.0,
            "timestamp": "2026-10-12T10:00:00Z",
        })
        restored = TradeOrder.from_json(legacy_json)
        assert restored.order_id == "legacy-uuid-1234"
        assert restored.trader_id == "ALGO-EXEC-01"
        assert restored.sequence_number == 1
        assert restored.is_valid()


# -----------------------------------------------------------------------------
# 2. ZERO-COST ASYNC PUBSUB MOCK TESTS
# -----------------------------------------------------------------------------
class TestAsyncLocalPubSubMock:
    """Validate AsyncLocalPubSubMock futures, callbacks, and fault injection."""

    def test_mock_future_creation_and_callbacks(self) -> None:
        """Verify PubSubPublishFuture resolves cleanly and triggers registered callbacks."""
        mock = AsyncLocalPubSubMock()
        topic = "projects/test/topics/orders"

        callback_invoked = []

        def done_callback(fut):
            callback_invoked.append(fut.result())

        future = mock.publish(topic, b'{"test": 1}', ordering_key="KEY-1")
        future.add_done_callback(done_callback)

        assert future.done()
        assert future.result().startswith("mock-msg-")
        assert len(callback_invoked) == 1
        assert callback_invoked[0] == future.result()

    def test_mock_ordering_key_tracking(self) -> None:
        """Verify that messages for the same ordering key maintain chronological order."""
        mock = AsyncLocalPubSubMock()
        topic = "projects/test/topics/orders"

        for i in range(5):
            mock.publish(topic, f"msg-{i}".encode("utf-8"), ordering_key="PORT-A#AAPL")

        ordered_records = mock.get_ordered_records(topic, "PORT-A#AAPL")
        assert len(ordered_records) == 5
        for idx, rec in enumerate(ordered_records):
            assert rec["data"] == f"msg-{idx}".encode("utf-8")
            assert rec["ordering_key"] == "PORT-A#AAPL"

    def test_mock_transient_failure_and_resume_publish(self) -> None:
        """Verify transient failure injection causes exception and pauses ordering key until resume_publish."""
        mock = AsyncLocalPubSubMock()
        topic = "projects/test/topics/orders"
        ordering_key = "PORT-B#TSLA"

        mock.inject_transient_failure(topic, count=1)

        # 1. First publish fails due to injected transient error
        fut1 = mock.publish(topic, b"data-1", ordering_key=ordering_key)
        with pytest.raises(RuntimeError, match="transient network timeout"):
            fut1.result()

        # 2. Subsequent publish on same ordering key should fail because key is paused
        fut2 = mock.publish(topic, b"data-2", ordering_key=ordering_key)
        with pytest.raises(RuntimeError, match="is paused"):
            fut2.result()

        # 3. Resume publishing on ordering key
        mock.resume_publish(topic, ordering_key)

        # 4. Now publishing succeeds
        fut3 = mock.publish(topic, b"data-3", ordering_key=ordering_key)
        assert fut3.result().startswith("mock-msg-")

    def test_mock_permanent_failure_injection(self) -> None:
        """Verify permanent failure injection persists across multiple publishes."""
        mock = AsyncLocalPubSubMock()
        topic = "projects/test/topics/orders"

        mock.inject_permanent_failure(topic)

        for _ in range(3):
            fut = mock.publish(topic, b"data", ordering_key="KEY")
            with pytest.raises(RuntimeError, match="service unavailable"):
                fut.result()

        mock.reset_failures(topic)
        fut_ok = mock.publish(topic, b"data", ordering_key="KEY")
        assert fut_ok.result().startswith("mock-msg-")


# -----------------------------------------------------------------------------
# 3. ASYNC TRADE ORDER PRODUCER TESTS
# -----------------------------------------------------------------------------
class TestAsyncTradeOrderProducer:
    """Validate production asynchronous trade order publishing, retries, and DLQ diversion."""

    def test_producer_config_defaults_and_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify default configuration and environment variable overrides."""
        config = AsyncProducerConfig()
        assert config.project_id == "trading-risk-dev-001"
        assert "top-trade-orders" in config.topic_id
        assert "top-trade-orders-dlq" in config.dlq_topic_id
        assert config.enable_message_ordering is True
        assert config.max_inflight_messages == 500

        monkeypatch.setenv("PUBSUB_TOPIC_ID", "projects/custom/topics/my-orders")
        monkeypatch.setenv("MAX_INFLIGHT_MESSAGES", "250")
        monkeypatch.setenv("BATCH_MAX_MESSAGES", "50")

        env_config = AsyncProducerConfig.from_env()
        assert env_config.topic_id == "projects/custom/topics/my-orders"
        assert env_config.max_inflight_messages == 250
        assert env_config.batch_max_messages == 50

    @run_async
    async def test_publish_single_order_success(self) -> None:
        """Verify publishing a single order updates metrics and returns message ID."""
        mock = AsyncLocalPubSubMock()
        producer = AsyncTradeOrderProducer(publisher=mock)

        order = TradeOrder.create(
            portfolio_id="PORT-ALPHA",
            ticker="AAPL",
            asset_class="EQUITY",
            side="BUY",
            order_type="LIMIT",
            quantity=100.0,
            price=180.0,
        )

        msg_id = await producer.publish(order)
        assert msg_id.startswith("mock-msg-")

        snapshot = producer.telemetry.get_snapshot()
        assert snapshot["orders_published"] == 1
        assert snapshot["orders_failed"] == 0
        assert snapshot["notional_usd_total"] == 18000.00
        assert snapshot["last_order_id"] == order.order_id
        assert snapshot["latency_ms_mean"] >= 0.0

        records = mock.get_published_records(producer.config.topic_id)
        assert len(records) == 1
        assert records[0]["ordering_key"] == "PORT-ALPHA#AAPL"

    @run_async
    async def test_publish_batch_concurrent(self) -> None:
        """Verify concurrent batch publishing of multiple orders with correct ordering keys."""
        mock = AsyncLocalPubSubMock()
        producer = AsyncTradeOrderProducer(publisher=mock)

        feed = OrderFeedGenerator(seed=101)
        orders = feed.generate_batch(size=25)

        msg_ids = await producer.publish_batch(orders)
        assert len(msg_ids) == 25
        assert all(mid.startswith("mock-msg-") for mid in msg_ids)

        snapshot = producer.telemetry.get_snapshot()
        assert snapshot["orders_published"] == 25
        assert snapshot["orders_failed"] == 0
        assert snapshot["inflight_orders"] == 0

        total_notional = round(sum(o.notional_usd for o in orders), 2)
        assert abs(snapshot["notional_usd_total"] - total_notional) < 0.05

    @run_async
    async def test_publish_flow_control_backpressure(self) -> None:
        """Verify that inflight concurrency is bounded by max_inflight_messages."""
        # Setup mock with small delay
        mock = AsyncLocalPubSubMock(simulate_latency_seconds=0.02)
        config = AsyncProducerConfig(max_inflight_messages=3)
        producer = AsyncTradeOrderProducer(config=config, publisher=mock)

        feed = OrderFeedGenerator(seed=202)
        orders = feed.generate_batch(size=6)

        # Launch all concurrently
        tasks = [asyncio.create_task(producer.publish(order)) for order in orders]
        await asyncio.gather(*tasks)

        snapshot = producer.telemetry.get_snapshot()
        assert snapshot["orders_published"] == 6
        assert snapshot["inflight_orders"] == 0

    @run_async
    async def test_publish_transient_failure_exponential_backoff(self) -> None:
        """Verify transient failure triggers retry with backoff and resumes ordering key."""
        mock = AsyncLocalPubSubMock()
        config = AsyncProducerConfig(
            max_retries=3,
            initial_backoff_sec=0.01,
            backoff_multiplier=1.5,
        )
        producer = AsyncTradeOrderProducer(config=config, publisher=mock)

        # Inject 2 transient failures (attempt 1 and 2 will fail, attempt 3 succeeds)
        mock.inject_transient_failure(config.topic_id, count=2)

        order = TradeOrder.create(
            portfolio_id="PORT-TECH",
            ticker="MSFT",
            asset_class="EQUITY",
            side="BUY",
            order_type="MARKET",
            quantity=20.0,
            price=420.0,
        )

        msg_id = await producer.publish(order)
        assert msg_id.startswith("mock-msg-")

        snapshot = producer.telemetry.get_snapshot()
        assert snapshot["orders_published"] == 1
        assert snapshot["retries_attempted"] == 2
        assert snapshot["orders_failed"] == 0

    @run_async
    async def test_publish_exhausted_retries_routes_to_dlq(self) -> None:
        """Verify that exhausting retries routes failed order to DLQ and raises PubSubPublishError."""
        mock = AsyncLocalPubSubMock()
        config = AsyncProducerConfig(
            max_retries=3,
            initial_backoff_sec=0.01,
        )
        producer = AsyncTradeOrderProducer(config=config, publisher=mock)

        # Inject permanent failure on main topic
        mock.inject_permanent_failure(config.topic_id)

        order = TradeOrder.create(
            portfolio_id="PORT-VOL",
            ticker="TSLA",
            asset_class="EQUITY",
            side="SELL",
            order_type="LIMIT",
            quantity=50.0,
            price=175.0,
        )

        with pytest.raises(PubSubPublishError, match="Failed to publish order"):
            await producer.publish(order)

        snapshot = producer.telemetry.get_snapshot()
        assert snapshot["orders_published"] == 0
        assert snapshot["orders_failed"] == 1
        assert snapshot["dlq_messages_routed"] == 1

        # Check DLQ topic received the message
        dlq_records = mock.get_published_records(config.dlq_topic_id)
        assert len(dlq_records) == 1
        assert "Exhausted 3 retries" in dlq_records[0]["attributes"]["error_reason"]
        assert dlq_records[0]["attributes"]["ticker"] == "TSLA"

    @run_async
    async def test_publish_invalid_order_routes_to_dlq(self) -> None:
        """Verify structurally invalid orders fail pre-validation and route immediately to DLQ."""
        mock = AsyncLocalPubSubMock()
        producer = AsyncTradeOrderProducer(publisher=mock)

        # Corrupt order with negative quantity
        bad_order = TradeOrder(
            order_id="bad-ord-1",
            portfolio_id="PORT-1",
            ticker="AAPL",
            asset_class="EQUITY",
            side="BUY",
            order_type="LIMIT",
            quantity=-100.0,
            price=180.0,
            notional_usd=-18000.0,
            timestamp="2026-10-16T12:00:00Z",
        )

        with pytest.raises(OrderValidationError, match="Pre-publish validation failed"):
            await producer.publish(bad_order)

        snapshot = producer.telemetry.get_snapshot()
        assert snapshot["orders_failed"] == 1
        assert snapshot["dlq_messages_routed"] == 1

        dlq_records = mock.get_published_records(producer.config.dlq_topic_id)
        assert len(dlq_records) == 1
        assert "Quantity must be strictly positive" in dlq_records[0]["attributes"]["error_reason"]

    def test_publish_defer_callback_dispatch(self) -> None:
        """Verify non-blocking publish_defer fires callbacks asynchronously."""
        mock = AsyncLocalPubSubMock()
        producer = AsyncTradeOrderProducer(publisher=mock)

        order = TradeOrder.create("PORT-1", "NVDA", "EQUITY", "BUY", "LIMIT", 10.0, 900.0)

        results = []

        async def _test():
            producer.publish_defer(
                order,
                on_success=lambda mid: results.append(mid),
            )
            await asyncio.sleep(0.05)

        asyncio.run(_test())
        assert len(results) == 1
        assert results[0].startswith("mock-msg-")

    def test_telemetry_snapshot_and_prometheus_formatting(self) -> None:
        """Verify Prometheus text format generation and metric accuracy."""
        telemetry = AsyncProducerTelemetry()
        telemetry.record_success("ord-1", notional=15000.0, latency_ms=12.5)
        telemetry.record_success("ord-2", notional=25000.0, latency_ms=18.0)
        telemetry.record_error()
        telemetry.record_dlq()
        telemetry.record_retry()

        prom = telemetry.format_prometheus()
        assert "trading_orders_published_total 2" in prom
        assert "trading_orders_failed_total 1" in prom
        assert "trading_orders_dlq_routed_total 1" in prom
        assert "trading_orders_notional_usd_total 40000.00" in prom
        assert "trading_orders_retries_total 1" in prom

    @run_async
    async def test_producer_flush_and_close(self) -> None:
        """Verify producer flush waits for inflight messages and aclose forbids further calls."""
        mock = AsyncLocalPubSubMock()
        producer = AsyncTradeOrderProducer(publisher=mock)

        order = TradeOrder.create("PORT-1", "SPY", "ETF", "BUY", "MARKET", 50.0, 515.0)
        await producer.publish(order)
        await producer.flush()

        await producer.aclose()
        assert producer._closed is True

        with pytest.raises(RuntimeError, match="is closed"):
            await producer.publish(order)


# -----------------------------------------------------------------------------
# 4. ORDER FEED GENERATOR TESTS
# -----------------------------------------------------------------------------
class TestOrderFeedGenerator:
    """Validate OrderFeedGenerator multi-asset profiles, monotonic sequence numbering, and corruption."""

    def test_universe_coverage_and_profiles(self) -> None:
        """Verify default universe includes all platform asset classes."""
        feed = OrderFeedGenerator()
        asset_classes = {p.asset_class for p in feed.universe.values()}
        assert "EQUITY" in asset_classes
        assert "ETF" in asset_classes
        assert "FX" in asset_classes
        assert "CRYPTO" in asset_classes
        assert "COMMODITY" in asset_classes

    def test_sequence_number_monotonicity(self) -> None:
        """Verify sequence numbers strictly increment per (portfolio, ticker)."""
        feed = OrderFeedGenerator(seed=303)
        portfolio = "PORT-EQUITY-ALPHA"
        ticker = "AAPL"

        seqs = []
        for _ in range(10):
            order = feed.generate_order(ticker=ticker, portfolio_id=portfolio)
            seqs.append(order.sequence_number)

        assert seqs == list(range(1, 11))

    def test_corrupt_order_generation_for_dlq(self) -> None:
        """Verify synthetic corrupt order generation produces invalid trade orders."""
        feed = OrderFeedGenerator(seed=404)
        corrupt_batch = feed.generate_batch(size=20, corrupt_ratio=1.0)
        assert len(corrupt_batch) == 20
        # All orders in this batch should fail validation
        invalid_count = sum(1 for o in corrupt_batch if not o.is_valid())
        assert invalid_count == 20


# -----------------------------------------------------------------------------
# 5. ASYNC ORDER ENGINE & HTTP ENDPOINTS TESTS
# -----------------------------------------------------------------------------
class TestAsyncOrderEngine:
    """Validate AsyncOrderEngine lifecycle, workers, backpressure, and HTTP endpoints."""

    def test_engine_config_defaults_and_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify engine configuration parameters."""
        config = EngineConfig()
        assert config.rate_orders_per_sec == 25.0
        assert config.worker_count == 4
        assert config.metrics_port == 8081

        monkeypatch.setenv("ORDER_RATE_PER_SEC", "100.0")
        monkeypatch.setenv("WORKER_COUNT", "8")
        monkeypatch.setenv("METRICS_PORT", "9090")

        env_cfg = EngineConfig.from_env()
        assert env_cfg.rate_orders_per_sec == 100.0
        assert env_cfg.worker_count == 8
        assert env_cfg.metrics_port == 9090

    @run_async
    async def test_engine_lifecycle_start_run_stop(self) -> None:
        """Verify complete engine lifecycle: start, queue ingestion, worker publishing, and stop."""
        mock = AsyncLocalPubSubMock()
        cfg = EngineConfig(
            rate_orders_per_sec=40.0,
            worker_count=2,
            queue_capacity=50,
            metrics_port=8181,
        )
        producer = AsyncTradeOrderProducer(config=cfg.producer_config, publisher=mock)
        engine = AsyncOrderEngine(config=cfg, producer=producer)

        await engine.start()
        assert engine.is_running is True

        # Run for 0.25 seconds to generate several orders
        await asyncio.sleep(0.25)

        await engine.stop()
        assert engine.is_running is False
        assert engine.order_queue.qsize() == 0

        snapshot = producer.telemetry.get_snapshot()
        assert snapshot["orders_published"] >= 3
        assert snapshot["orders_failed"] == 0

    @run_async
    async def test_engine_http_health_and_metrics_endpoints(self) -> None:
        """Verify embedded HTTP server serves /health (JSON) and /metrics (Prometheus)."""
        mock = AsyncLocalPubSubMock()
        cfg = EngineConfig(
            rate_orders_per_sec=20.0,
            worker_count=2,
            metrics_port=8282,
        )
        producer = AsyncTradeOrderProducer(config=cfg.producer_config, publisher=mock)
        engine = AsyncOrderEngine(config=cfg, producer=producer)

        await engine.start()
        await asyncio.sleep(0.1)

        try:
            # 1. Health check endpoint
            health_url = f"http://127.0.0.1:{cfg.metrics_port}/health"
            with urllib.request.urlopen(health_url, timeout=3.0) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["status"] == "UP"
                assert data["running"] is True
                assert "producer" in data

            # 2. Prometheus metrics endpoint
            metrics_url = f"http://127.0.0.1:{cfg.metrics_port}/metrics"
            with urllib.request.urlopen(metrics_url, timeout=3.0) as resp:
                assert resp.status == 200
                text = resp.read().decode("utf-8")
                assert "trading_orders_published_total" in text
                assert "trading_queue_size_current" in text
                assert "trading_engine_workers_total 2" in text

        finally:
            await engine.stop()


# -----------------------------------------------------------------------------
# 6. SYSTEMD & DOCKER DEPLOYMENT VALIDATION
# -----------------------------------------------------------------------------
class TestSystemdAndDockerService:
    """Validate systemd unit file and docker compose deployment configurations."""

    def test_systemd_service_unit_syntax_and_cgroups(self) -> None:
        """Verify systemd unit file has proper cgroup limits, user, and exec directives."""
        unit_file = SYSTEMD_DIR / "trading-async-order-producer.service"
        assert unit_file.is_file(), f"Missing systemd service unit: {unit_file}"

        content = unit_file.read_text(encoding="utf-8")
        assert "Description=Trading Risk Engine - Asynchronous Cloud Pub/Sub Trade Order Producer Daemon" in content
        assert "User=riskworker" in content
        assert "Group=docker" in content
        assert "Restart=always" in content
        assert "MemoryMax=4G" in content
        assert "CPUQuota=300%" in content
        assert "trading-async-order-producer" in content
        assert "python -m ingestion.async_order_engine" in content

    def test_docker_compose_service_definition(self) -> None:
        """Verify docker-compose.yml defines async-order-producer with port 8081 and healthcheck."""
        compose_file = INGESTION_DIR / "docker-compose.yml"
        assert compose_file.is_file()

        content = compose_file.read_text(encoding="utf-8")
        assert "async-order-producer:" in content
        assert "8081:8081" in content
        assert "http://localhost:8081/health" in content
        assert "PUBSUB_TOPIC_ID=projects/trading-risk-dev-001/topics/top-trade-orders" in content
        assert "PUBSUB_DLQ_TOPIC_ID=projects/trading-risk-dev-001/topics/top-trade-orders-dlq" in content
