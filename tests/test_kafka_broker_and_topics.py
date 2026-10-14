"""Kafka Broker Configuration and Multi-Partition Tick-Data Topic Validation Tests (Day 05).

Validates:
- KRaft-mode Kafka broker server.properties configuration (low latency, lz4, zero data loss).
- Docker Compose Kafka service configuration and healthcheck.
- MarketTick financial invariants: bid-ask spread, positive values, JSON roundtrip.
- Deterministic multi-partition hash routing per ticker.
- KafkaTopicManager and zero-cost LocalKafkaAdminMock provisioning and health checks.
- Shell script setup_topics.sh syntax and topic specs.
"""

from pathlib import Path
import re
import pytest

from ingestion.market_tick import ALLOWED_ASSET_CLASSES, DeterministicPartitioner, MarketTick
from ingestion.kafka.topic_manager import (
    CORE_TOPIC_CATALOG,
    KafkaTopicManager,
    LocalKafkaAdminMock,
    TopicDefinition,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
INGESTION_KAFKA_DIR = REPO_ROOT / "ingestion" / "kafka"


class TestKafkaBrokerConfiguration:
    """Validate Kafka KRaft broker properties and Docker Compose setup."""

    def test_server_properties_exists_and_configured(self) -> None:
        """Verify server.properties exists and enforces production financial streaming flags."""
        prop_file = INGESTION_KAFKA_DIR / "server.properties"
        assert prop_file.is_file(), f"Missing {prop_file}"
        content = prop_file.read_text(encoding="utf-8")

        # KRaft mode checks
        assert "process.roles=broker,controller" in content
        assert "node.id=1" in content
        assert "controller.quorum.voters=1@localhost:9093" in content

        # Network listeners
        assert "listeners=" in content
        assert ":9092" in content
        assert ":9093" in content
        assert ":9094" in content

        # Financial risk & data safety invariants
        assert "unclean.leader.election.enable=false" in content
        assert "auto.create.topics.enable=false" in content

        # High-throughput tuning
        assert "compression.type=lz4" in content
        assert "num.partitions=6" in content
        assert "log.retention.hours=24" in content
        assert "log.retention.bytes=10737418240" in content

    def test_docker_compose_kafka_configuration(self) -> None:
        """Verify docker-compose.kafka.yml declares the KRaft broker with healthchecks."""
        compose_file = INGESTION_KAFKA_DIR / "docker-compose.kafka.yml"
        assert compose_file.is_file()
        content = compose_file.read_text(encoding="utf-8")

        assert "services:" in content
        assert "kafka:" in content
        assert "9092:9092" in content
        assert "9094:9094" in content
        assert "healthcheck:" in content

        # KRaft environment variables
        assert "KAFKA_PROCESS_ROLES: 'broker,controller'" in content or 'KAFKA_PROCESS_ROLES: "broker,controller"' in content
        assert "KAFKA_NUM_PARTITIONS: 6" in content
        assert "KAFKA_COMPRESSION_TYPE: 'lz4'" in content
        assert "KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'" in content



class TestMarketTickSchemaAndValidation:
    """Validate MarketTick dataclass invariants and quantitative bounds."""

    def test_market_tick_creation_and_computed_fields(self) -> None:
        """Verify valid creation, spread calculation, and partition key generation."""
        tick = MarketTick.create(
            ticker="AAPL",
            asset_class="EQUITY",
            bid=150.10,
            ask=150.30,
            last_price=150.25,
            volume=500.0,
            sequence_number=1,
        )
        assert tick.ticker == "AAPL"
        assert tick.asset_class == "EQUITY"
        assert tick.spread == 0.20
        assert tick.mid_price == 150.20
        assert tick.partition_key == "EQUITY#AAPL"
        assert tick.trade_id is not None
        assert tick.timestamp is not None
        assert tick.sequence_number == 1

    def test_invalid_market_ticks_rejected(self) -> None:
        """Ensure crossed market quotes, negative values, and unknown asset classes raise ValueError."""
        # Crossed market quote (bid > ask)
        with pytest.raises(ValueError, match="Crossed market quote"):
            MarketTick.create("AAPL", "EQUITY", bid=150.50, ask=150.20, last_price=150.30, volume=100.0)

        # Non-positive prices or volume
        with pytest.raises(ValueError, match="strictly positive"):
            MarketTick.create("AAPL", "EQUITY", bid=-10.0, ask=150.0, last_price=150.0, volume=100.0)

        with pytest.raises(ValueError, match="strictly positive"):
            MarketTick.create("AAPL", "EQUITY", bid=150.0, ask=150.0, last_price=150.0, volume=-5.0)

        # Unsupported asset class
        with pytest.raises(ValueError, match="Unsupported asset class"):
            MarketTick.create("AAPL", "DERIVATIVES_UNKNOWN", bid=150.0, ask=150.5, last_price=150.2, volume=10.0)

    def test_json_serialization_and_roundtrip(self) -> None:
        """Verify lossless JSON roundtrip serialization."""
        tick = MarketTick.create(
            ticker="NVDA",
            asset_class="EQUITY",
            bid=890.25,
            ask=890.50,
            last_price=890.40,
            volume=250.0,
            sequence_number=42,
        )
        json_str = tick.to_json()
        restored = MarketTick.from_json(json_str)

        assert restored.trade_id == tick.trade_id
        assert restored.ticker == "NVDA"
        assert restored.spread == tick.spread
        assert restored.mid_price == tick.mid_price
        assert restored.sequence_number == 42


class TestDeterministicPartitioner:
    """Validate hash partitioner consistency, uniformity, and bounds."""

    def test_partition_consistency_for_same_ticker(self) -> None:
        """Ensure identical partition keys always route to the exact same partition index."""
        key = "EQUITY#MSFT"
        first_partition = DeterministicPartitioner.get_partition(key, num_partitions=6)

        for _ in range(50):
            p = DeterministicPartitioner.get_partition(key, num_partitions=6)
            assert p == first_partition, "Partitioner must be deterministic for identical keys"

    def test_partition_distribution_across_tickers(self) -> None:
        """Ensure diverse tickers distribute across multiple partitions."""
        tickers = [
            "EQUITY#AAPL", "EQUITY#GOOGL", "EQUITY#MSFT", "EQUITY#AMZN",
            "EQUITY#NVDA", "EQUITY#TSLA", "EQUITY#JPM", "EQUITY#GS",
            "FX#EURUSD", "FX#USDJPY", "CRYPTO#BTCUSD", "CRYPTO#ETHUSD",
        ]
        partitions = {DeterministicPartitioner.get_partition(t, num_partitions=6) for t in tickers}
        # Across 12 multi-asset tickers, at least 4 distinct partitions should be populated
        assert len(partitions) >= 4, f"Partitions too concentrated: {partitions}"

    def test_partition_bounds(self) -> None:
        """Verify partition indices are always within [0, num_partitions - 1]."""
        for i in range(100):
            p = DeterministicPartitioner.get_partition(f"EQUITY#TICK_{i}", num_partitions=6)
            assert 0 <= p < 6

    def test_invalid_num_partitions(self) -> None:
        """Ensure non-positive partition count raises ValueError."""
        with pytest.raises(ValueError, match="num_partitions must be > 0"):
            DeterministicPartitioner.get_partition("EQUITY#AAPL", num_partitions=0)


class TestKafkaTopicManagerAndMock:
    """Validate topic catalog, local in-memory admin mock, and health checks."""

    def test_topic_catalog_definitions(self) -> None:
        """Verify core catalog topics meet regulatory & throughput specifications."""
        catalog_names = {t.name: t for t in CORE_TOPIC_CATALOG}
        assert "market.ticks.v1" in catalog_names
        assert "market.ticks.deadletter" in catalog_names
        assert "market.orderbook.depth.v1" in catalog_names

        ticks_topic = catalog_names["market.ticks.v1"]
        assert ticks_topic.num_partitions == 6
        assert ticks_topic.configs["compression.type"] == "lz4"
        assert ticks_topic.configs["retention.ms"] == "86400000"

        dlq_topic = catalog_names["market.ticks.deadletter"]
        assert dlq_topic.num_partitions == 3
        assert dlq_topic.configs["retention.ms"] == "604800000"

    def test_local_kafka_admin_mock_provisioning(self) -> None:
        """Verify topic provisioning and metadata describing via LocalKafkaAdminMock."""
        mock = LocalKafkaAdminMock()
        t = TopicDefinition("test.ticks", num_partitions=4, configs={"retention.ms": "1000"})

        assert mock.create_topic(t) is True
        # Idempotent re-creation
        assert mock.create_topic(t) is False
        assert "test.ticks" in mock.list_topics()

        desc = mock.describe_topic("test.ticks")
        assert desc is not None
        assert desc["num_partitions"] == 4
        assert len(desc["partitions"]) == 4

    def test_mock_produce_and_partition_routing(self) -> None:
        """Verify message routing into correct partition queue."""
        mock = LocalKafkaAdminMock()
        t = TopicDefinition("market.ticks.v1", num_partitions=6)
        mock.create_topic(t)

        tick = MarketTick.create("AAPL", "EQUITY", 150.0, 150.2, 150.1, 100.0)
        expected_partition = DeterministicPartitioner.get_partition(tick.partition_key, 6)

        p = mock.produce("market.ticks.v1", key=tick.partition_key, value=tick.to_json())
        assert p == expected_partition

        records = mock.get_partition_records("market.ticks.v1", p)
        assert len(records) == 1
        assert records[0]["key"] == "EQUITY#AAPL"

    def test_topic_manager_health_verification(self) -> None:
        """Verify KafkaTopicManager provisions all topics and reports health."""
        manager = KafkaTopicManager(use_mock=True)
        results = manager.provision_all_topics()

        for topic in ["market.ticks.v1", "market.ticks.deadletter", "market.orderbook.depth.v1"]:
            assert results[topic] is True
            health = manager.verify_topic_health(topic)
            assert health["healthy"] is True
            assert health["partitions"] >= 3

        # Non-existent topic
        bad_health = manager.verify_topic_health("does.not.exist")
        assert bad_health["healthy"] is False


class TestTopicSetupScript:
    """Validate bash automation script syntax and specifications."""

    def test_setup_topics_script_syntax_and_shebang(self) -> None:
        """Verify setup_topics.sh contains standard error trapping and shebang."""
        script_file = INGESTION_KAFKA_DIR / "setup_topics.sh"
        assert script_file.is_file()
        content = script_file.read_text(encoding="utf-8")

        assert content.startswith("#!/usr/bin/env bash")
        assert "set -euo pipefail" in content
        assert "market.ticks.v1" in content
        assert "--partitions 6" in content
        assert "--config compression.type=lz4" in content
        assert "market.ticks.deadletter" in content
        assert "--partitions 3" in content
