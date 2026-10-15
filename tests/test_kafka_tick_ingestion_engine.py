"""Kafka Tick Streaming Ingestion Engine Unit & Integration Tests (Day 06).

Validates:
- Multi-asset ticker universe profiles (Equities, ETFs, FX, Crypto, Commodities).
- Quantitative market feed simulation (GBM with Ornstein-Uhlenbeck drift, spread invariants).
- Thread-safe monotonic sequence numbering and dead-letter corrupt quote generation.
- Zero-cost LocalKafkaProducerMock (deterministic partition routing, callbacks, fault injection).
- Production KafkaTickProducer retry policies, exponential backoff, and DLQ routing.
- MultiThreadedTickEngine concurrency (feed thread fleet, producer worker fleet, bounded queue).
- Backpressure handling, watermark monitoring, and dead-letter isolation.
- Embedded HTTP health (/health) and Prometheus metrics (/metrics) endpoints.
- Graceful shutdown lifecycle, poison-pill thread draining, and systemd service unit integrity.
"""

from datetime import datetime
import json
import os
from pathlib import Path
import time
import urllib.request
import pytest

from ingestion.kafka.producer import (
    KafkaProducerConfig,
    KafkaTickProducer,
    LocalKafkaProducerMock,
    MockRecordMetadata,
)
from ingestion.market_feed import (
    DEFAULT_MARKET_UNIVERSE,
    MarketFeedGenerator,
    TickerProfile,
    TickerSimulator,
)
from ingestion.market_tick import DeterministicPartitioner, MarketTick
from ingestion.streaming_engine import (
    EngineTelemetry,
    MultiThreadedTickEngine,
    StreamingEngineConfig,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
INGESTION_DIR = REPO_ROOT / "ingestion"


class TestMarketFeedSimulation:
    """Validate financial market data feed simulation and quantitative invariants."""

    def test_default_universe_profiles(self) -> None:
        """Verify all major asset classes are represented in the default market universe."""
        universe = DEFAULT_MARKET_UNIVERSE
        assert "AAPL" in universe
        assert "MSFT" in universe
        assert "SPY" in universe
        assert "EURUSD" in universe
        assert "BTCUSD" in universe
        assert "XAUUSD" in universe

        # Check asset class coverage
        asset_classes = {p.asset_class for p in universe.values()}
        assert "EQUITY" in asset_classes
        assert "ETF" in asset_classes
        assert "FX" in asset_classes
        assert "CRYPTO" in asset_classes
        assert "COMMODITY" in asset_classes

    def test_profile_invariants(self) -> None:
        """Ensure invalid profile specifications raise ValueError."""
        # Non-positive base price
        with pytest.raises(ValueError, match="base_price must be > 0"):
            TickerProfile("BAD", "EQUITY", base_price=-10.0, volatility=0.2, mean_reversion=0.1, spread_bps=1.0)

        # Invalid asset class
        with pytest.raises(ValueError, match="Invalid asset_class"):
            TickerProfile("BAD", "NOT_A_CLASS", base_price=100.0, volatility=0.2, mean_reversion=0.1, spread_bps=1.0)

        # Non-positive spread bps
        with pytest.raises(ValueError, match="spread_bps must be > 0"):
            TickerProfile("BAD", "EQUITY", base_price=100.0, volatility=0.2, mean_reversion=0.1, spread_bps=0.0)

    def test_ticker_simulator_gbm_step_and_spread(self) -> None:
        """Verify stochastic simulation generates strictly valid market ticks."""
        profile = DEFAULT_MARKET_UNIVERSE["AAPL"]
        sim = TickerSimulator(profile, seed=42)

        for _ in range(25):
            tick = sim.generate_tick(dt_seconds=0.1)
            assert isinstance(tick, MarketTick)
            assert tick.ticker == "AAPL"
            assert tick.asset_class == "EQUITY"
            assert tick.bid > 0
            assert tick.ask > tick.bid
            assert tick.spread >= profile.min_tick_size
            assert tick.bid <= tick.last_price <= tick.ask
            assert tick.volume > 0
            assert tick.timestamp is not None

    def test_sequence_number_monotonicity(self) -> None:
        """Ensure per-ticker sequence numbers are strictly monotonically increasing."""
        profile = DEFAULT_MARKET_UNIVERSE["NVDA"]
        sim = TickerSimulator(profile, seed=123)

        seqs = [sim.generate_tick().sequence_number for _ in range(30)]
        assert seqs == list(range(1, 31))

    def test_market_feed_generator_tick_generation(self) -> None:
        """Verify MarketFeedGenerator can generate uniform or specific ticker quotes."""
        feed = MarketFeedGenerator(tickers=["AAPL", "EURUSD", "BTCUSD"], seed=99)
        assert sorted(feed.tickers) == ["AAPL", "BTCUSD", "EURUSD"]

        # Specific ticker
        tick_fx = feed.simulate_tick(ticker="EURUSD")
        assert tick_fx.ticker == "EURUSD"
        assert tick_fx.asset_class == "FX"

        # Random ticker
        random_tick = feed.simulate_tick()
        assert random_tick.ticker in ["AAPL", "BTCUSD", "EURUSD"]

    def test_corrupt_tick_generation_for_dlq(self) -> None:
        """Verify corrupt quote generation produces crossed quotes and negative bounds."""
        profile = DEFAULT_MARKET_UNIVERSE["MSFT"]
        sim = TickerSimulator(profile)

        # Crossed quote
        crossed = sim.generate_corrupt_tick(corruption_type="crossed")
        assert crossed["bid"] > crossed["ask"]

        # Negative price
        neg_price = sim.generate_corrupt_tick(corruption_type="negative_price")
        assert neg_price["bid"] < 0 or neg_price["last_price"] < 0

        # Negative volume
        neg_vol = sim.generate_corrupt_tick(corruption_type="negative_volume")
        assert neg_vol["volume"] < 0


class TestLocalKafkaProducerMock:
    """Validate zero-cost in-memory mock producer behavior, partitioning, and fault injection."""

    def test_mock_producer_initialization_and_partitions(self) -> None:
        """Verify partition queues exist for default topics."""
        mock = LocalKafkaProducerMock()
        assert mock.num_partitions_map["market.ticks.v1"] == 6
        assert mock.num_partitions_map["market.ticks.deadletter"] == 3

        dist = mock.get_partition_distribution("market.ticks.v1")
        assert len(dist) == 6
        assert all(count == 0 for count in dist.values())

    def test_mock_producer_deterministic_routing(self) -> None:
        """Verify records with same key route to the exact same partition index."""
        mock = LocalKafkaProducerMock()
        key = b"EQUITY#AAPL"
        expected_partition = DeterministicPartitioner.get_partition("EQUITY#AAPL", 6)

        for i in range(20):
            meta = mock.send("market.ticks.v1", key=key, value=f'{{"tick": {i}}}'.encode("utf-8"))
            assert meta.partition == expected_partition
            assert meta.offset == i

        records = mock.get_records("market.ticks.v1", partition=expected_partition)
        assert len(records) == 20
        assert mock.get_total_records("market.ticks.v1") == 20

    def test_mock_producer_callbacks_and_metadata(self) -> None:
        """Verify delivery callback receives valid metadata with topic and partition."""
        mock = LocalKafkaProducerMock()
        callback_invoked = []

        def on_delivery(err, meta):
            assert err is None
            assert isinstance(meta, MockRecordMetadata)
            callback_invoked.append(meta)

        mock.send(
            "market.ticks.v1",
            key=b"FX#EURUSD",
            value=b"test_payload",
            callback=on_delivery,
        )
        assert len(callback_invoked) == 1
        assert callback_invoked[0].topic == "market.ticks.v1"

    def test_mock_producer_transient_failure_injection(self) -> None:
        """Verify transient failure injection causes controlled temporary exceptions."""
        mock = LocalKafkaProducerMock()
        mock.inject_transient_failure(count=2)

        # First 2 attempts should fail
        with pytest.raises(RuntimeError, match="Simulated transient broker timeout"):
            mock.send("market.ticks.v1", value=b"val", key=b"EQUITY#AAPL")

        with pytest.raises(RuntimeError, match="Simulated transient broker timeout"):
            mock.send("market.ticks.v1", value=b"val", key=b"EQUITY#AAPL")

        # Third attempt should succeed
        meta = mock.send("market.ticks.v1", value=b"val", key=b"EQUITY#AAPL")
        assert meta.offset == 0

    def test_mock_producer_permanent_failure_injection(self) -> None:
        """Verify permanent failure blocks sends until cleared."""
        mock = LocalKafkaProducerMock()
        mock.inject_permanent_failure(RuntimeError("Outage"))

        with pytest.raises(RuntimeError, match="Outage"):
            mock.send("market.ticks.v1", value=b"val")

        mock.clear_faults()
        meta = mock.send("market.ticks.v1", value=b"val")
        assert meta.offset == 0

    def test_mock_producer_closed_guard(self) -> None:
        """Verify sending to a closed producer raises RuntimeError."""
        mock = LocalKafkaProducerMock()
        mock.close()

        with pytest.raises(RuntimeError, match="Producer is closed"):
            mock.send("market.ticks.v1", value=b"val")


class TestKafkaTickProducer:
    """Validate high-level KafkaTickProducer API, retries, and dead-letter routing."""

    def test_producer_config_defaults_and_env(self, monkeypatch) -> None:
        """Verify config defaults and environment variable parsing."""
        cfg = KafkaProducerConfig()
        assert cfg.topic == "market.ticks.v1"
        assert cfg.deadletter_topic == "market.ticks.deadletter"
        assert cfg.batch_size == 16384
        assert cfg.simulation_mode is True

        monkeypatch.setenv("KAFKA_TICKS_TOPIC", "custom.ticks")
        monkeypatch.setenv("KAFKA_RETRIES", "7")
        monkeypatch.setenv("KAFKA_LINGER_MS", "25")

        env_cfg = KafkaProducerConfig.from_env()
        assert env_cfg.topic == "custom.ticks"
        assert env_cfg.retries == 7
        assert env_cfg.linger_ms == 25

    def test_send_valid_tick_with_headers(self) -> None:
        """Verify tick produces with partition key, JSON body, and tracing headers."""
        mock = LocalKafkaProducerMock()
        producer = KafkaTickProducer(mock_producer=mock)

        tick = MarketTick.create("AAPL", "EQUITY", 185.10, 185.30, 185.20, 100.0, sequence_number=1)
        res = producer.send_tick(tick)
        assert res is not None

        expected_partition = DeterministicPartitioner.get_partition("EQUITY#AAPL", 6)
        records = mock.get_records("market.ticks.v1", partition=expected_partition)
        assert len(records) == 1

        rec = records[0]
        assert rec.key == b"EQUITY#AAPL"
        assert b'"ticker": "AAPL"' in rec.value

        headers_dict = {k: v for k, v in rec.headers}
        assert headers_dict["source"] == b"trading-risk-engine"
        assert headers_dict["asset_class"] == b"EQUITY"
        assert headers_dict["ticker"] == b"AAPL"
        assert headers_dict["sequence_number"] == b"1"

    def test_send_tick_retry_and_backoff_on_transient_failure(self) -> None:
        """Verify producer retries on transient errors and eventually succeeds."""
        mock = LocalKafkaProducerMock()
        mock.inject_transient_failure(count=2)

        cfg = KafkaProducerConfig(retries=4, retry_backoff_ms=10)
        producer = KafkaTickProducer(config=cfg, mock_producer=mock)

        tick = MarketTick.create("MSFT", "EQUITY", 420.0, 420.2, 420.1, 50.0)
        res = producer.send_tick(tick)
        assert res is not None
        assert mock.get_total_records("market.ticks.v1") == 1

    def test_send_tick_exhausted_retries_routes_to_dlq(self) -> None:
        """Verify tick is forwarded to DLQ topic when retry budget is exhausted."""
        mock = LocalKafkaProducerMock()
        # Inject 2 transient failures, matching retries=2
        mock.inject_transient_failure(count=2)

        cfg = KafkaProducerConfig(retries=2, retry_backoff_ms=5)
        producer = KafkaTickProducer(config=cfg, mock_producer=mock)

        tick = MarketTick.create("TSLA", "EQUITY", 240.0, 240.5, 240.2, 10.0)

        with pytest.raises(RuntimeError, match="Failed to produce tick TSLA after 2 attempts"):
            producer.send_tick(tick)

        # Primary topic should have 0 records, DLQ should have 1 record
        assert mock.get_total_records("market.ticks.v1") == 0
        dlq_records = mock.get_records("market.ticks.deadletter")
        assert len(dlq_records) == 1
        assert b"EQUITY#TSLA" in dlq_records[0].key


    def test_send_deadletter_record_structure(self) -> None:
        """Verify deadletter records contain diagnostic metadata and original payload."""
        mock = LocalKafkaProducerMock()
        producer = KafkaTickProducer(mock_producer=mock)

        corrupted_payload = {"ticker": "AAPL", "bid": 150.0, "ask": 140.0, "error": "crossed quote"}
        producer.send_deadletter(
            payload=corrupted_payload,
            error_reason="Crossed market quote: bid (150.0) cannot exceed ask (140.0)",
            original_key="EQUITY#AAPL",
        )

        dlq_records = mock.get_records("market.ticks.deadletter")
        assert len(dlq_records) == 1

        rec = dlq_records[0]
        assert rec.key == b"EQUITY#AAPL"
        parsed = json.loads(rec.value.decode("utf-8"))
        assert parsed["original_key"] == "EQUITY#AAPL"
        assert "Crossed market quote" in parsed["error_reason"]
        assert parsed["raw_payload"] == corrupted_payload
        assert "deadletter_id" in parsed
        assert "timestamp" in parsed


class TestMultiThreadedTickEngine:
    """Validate multi-threaded streaming architecture, worker fleet, telemetry, and lifecycle."""

    def test_engine_config_defaults_and_env(self, monkeypatch) -> None:
        """Verify engine configuration parameters and environment overrides."""
        cfg = StreamingEngineConfig()
        assert cfg.num_feed_threads == 4
        assert cfg.num_producer_threads == 4
        assert cfg.buffer_capacity == 10000
        assert cfg.metrics_port == 8081

        monkeypatch.setenv("NUM_FEED_THREADS", "8")
        monkeypatch.setenv("NUM_PRODUCER_THREADS", "6")
        monkeypatch.setenv("BUFFER_CAPACITY", "5000")
        monkeypatch.setenv("METRICS_PORT", "8095")

        env_cfg = StreamingEngineConfig.from_env()
        assert env_cfg.num_feed_threads == 8
        assert env_cfg.num_producer_threads == 6
        assert env_cfg.buffer_capacity == 5000
        assert env_cfg.metrics_port == 8095

    def test_telemetry_metrics_tracking(self) -> None:
        """Verify telemetry counters, gauges, and Prometheus exposition formatting."""
        telemetry = EngineTelemetry()
        telemetry.record_generated()
        telemetry.record_generated()
        telemetry.record_published(tick_bytes=256, tick_id="trade-1")
        telemetry.record_failed()
        telemetry.record_deadlettered()
        telemetry.record_backpressure(queue_size=100)
        telemetry.set_status("RUNNING")

        snap = telemetry.get_snapshot()
        assert snap["ticks_generated_total"] == 2
        assert snap["ticks_published_total"] == 1
        assert snap["ticks_failed_total"] == 1
        assert snap["ticks_deadlettered_total"] == 1
        assert snap["bytes_published_total"] == 256
        assert snap["buffer_high_watermark"] == 100
        assert snap["healthy"] is True
        assert snap["status"] == "RUNNING"

        prom = telemetry.get_prometheus_metrics()
        assert "kafka_streaming_ticks_published_total 1" in prom
        assert "kafka_streaming_ticks_failed_total 1" in prom
        assert "kafka_streaming_ticks_deadlettered_total 1" in prom
        assert "kafka_streaming_healthy 1" in prom

    def test_multi_threaded_streaming_execution(self) -> None:
        """Verify worker fleet concurrently generates and publishes ticks across all partitions."""
        mock = LocalKafkaProducerMock()
        producer = KafkaTickProducer(mock_producer=mock)

        # Unique port to prevent collisions
        config = StreamingEngineConfig(
            tickers=["AAPL", "MSFT", "GOOGL", "EURUSD", "BTCUSD", "XAUUSD"],
            num_feed_threads=3,
            num_producer_threads=3,
            buffer_capacity=500,
            tick_interval_ms=5.0,
            metrics_port=8181,
            simulation_mode=True,
        )
        engine = MultiThreadedTickEngine(config=config, producer=producer)

        engine.start(run_in_background=True)
        # Allow multi-threaded streaming to produce multiple ticks
        time.sleep(0.35)
        engine.stop(drain_timeout=2.0)

        snap = engine.telemetry.get_snapshot()
        assert snap["ticks_generated_total"] > 0
        assert snap["ticks_published_total"] > 0
        assert snap["status"] == "STOPPED"

        # Check Kafka partition distribution
        total_records = mock.get_total_records("market.ticks.v1")
        assert total_records == snap["ticks_published_total"]

        dist = mock.get_partition_distribution("market.ticks.v1")
        # Across 6 multi-asset tickers, multiple partitions should have received ticks
        active_partitions = [p for p, count in dist.items() if count > 0]
        assert len(active_partitions) >= 3, f"Ticks concentrated on too few partitions: {dist}"

    def test_dead_letter_queue_injection_during_streaming(self) -> None:
        """Verify corrupt quotes injected into the pipeline are safely routed to DLQ."""
        mock = LocalKafkaProducerMock()
        producer = KafkaTickProducer(mock_producer=mock)

        config = StreamingEngineConfig(
            tickers=["AAPL", "MSFT"],
            num_feed_threads=1,
            num_producer_threads=2,
            buffer_capacity=100,
            tick_interval_ms=10.0,
            metrics_port=8182,
            simulation_mode=True,
        )
        engine = MultiThreadedTickEngine(config=config, producer=producer)
        engine.start(run_in_background=True)

        # Inject crossed quote and negative price quote
        engine.inject_corrupt_tick(corruption_type="crossed", ticker="AAPL")
        engine.inject_corrupt_tick(corruption_type="negative_price", ticker="MSFT")

        time.sleep(0.30)
        engine.stop(drain_timeout=2.0)

        snap = engine.telemetry.get_snapshot()
        assert snap["ticks_deadlettered_total"] >= 2

        dlq_records = mock.get_records("market.ticks.deadletter")
        assert len(dlq_records) >= 2

    def test_backpressure_and_buffer_watermark(self) -> None:
        """Verify small buffer capacity triggers backpressure event tracking without crashing."""
        mock = LocalKafkaProducerMock()
        producer = KafkaTickProducer(mock_producer=mock)

        config = StreamingEngineConfig(
            tickers=["AAPL", "GOOGL", "MSFT", "NVDA"],
            num_feed_threads=4,
            num_producer_threads=1,
            buffer_capacity=10,  # Intentionally tiny buffer to force queue fills
            tick_interval_ms=1.0,
            metrics_port=8183,
            simulation_mode=True,
        )
        engine = MultiThreadedTickEngine(config=config, producer=producer)
        engine.start(run_in_background=True)

        time.sleep(0.20)
        engine.stop(drain_timeout=2.0)

        snap = engine.telemetry.get_snapshot()
        assert snap["buffer_high_watermark"] > 0
        assert snap["ticks_published_total"] > 0

    def test_embedded_http_health_and_metrics_endpoints(self) -> None:
        """Verify /health and /metrics HTTP endpoints return HTTP 200 with valid content."""
        mock = LocalKafkaProducerMock()
        producer = KafkaTickProducer(mock_producer=mock)

        port = 8184
        config = StreamingEngineConfig(
            tickers=["AAPL"],
            num_feed_threads=1,
            num_producer_threads=1,
            buffer_capacity=50,
            tick_interval_ms=20.0,
            metrics_port=port,
            metrics_host="127.0.0.1",
            simulation_mode=True,
        )
        engine = MultiThreadedTickEngine(config=config, producer=producer)
        engine.start(run_in_background=True)

        try:
            # Query /health
            health_url = f"http://127.0.0.1:{port}/health"
            with urllib.request.urlopen(health_url, timeout=3.0) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["healthy"] is True
                assert data["status"] == "RUNNING"
                assert "uptime_seconds" in data

            # Query /metrics
            metrics_url = f"http://127.0.0.1:{port}/metrics"
            with urllib.request.urlopen(metrics_url, timeout=3.0) as resp:
                assert resp.status == 200
                text = resp.read().decode("utf-8")
                assert "kafka_streaming_ticks_published_total" in text
                assert "kafka_streaming_healthy 1" in text

        finally:
            engine.stop()

    def test_systemd_streaming_unit_structure(self) -> None:
        """Verify systemd service unit defines resource constraints and streaming entrypoint."""
        unit_file = INGESTION_DIR / "systemd" / "trading-kafka-streaming.service"
        assert unit_file.is_file(), f"Missing {unit_file}"
        content = unit_file.read_text(encoding="utf-8")

        assert "Description=Trading Risk Engine - Multi-Threaded Kafka Market Tick Streaming Daemon" in content
        assert "After=docker.service" in content
        assert "MemoryMax=4G" in content
        assert "CPUQuota=400%" in content
        assert "User=riskworker" in content
        assert "python -m ingestion.streaming_engine" in content
        assert "Restart=always" in content
