"""Comprehensive Zero-Cost Unit and Integration Test Suite for Dataproc PySpark Streaming.

Verifies:
1. `DataprocStreamingConfig` configuration validation, environment overrides, CLI parsing, and SparkConf dictionaries.
2. `schema.py` explicit Spark SQL DDL, PySpark StructType spec, microstructure record enrichments, and quarantine error vectors.
3. `tick_consumer.py` Structured Streaming execution plan descriptor and mock SparkSession builders.
4. `tick_consumer.py` local hermetic micro-batch engine: per-partition offset tracking, rate-limiting, event-time watermarking,
   late-event dropping, corrupt-record quarantine routing, and local disk checkpoint persistence/recovery.
5. `StreamingQueryTelemetry` and Prometheus text exposition formatting.
6. End-to-end integration: MarketFeedGenerator -> KafkaTickProducer -> LocalKafkaProducerMock -> PySparkKafkaTickConsumer.
7. CLI execution entrypoint (`run_tick_consumer.py`) and Dataproc submission script (`submit_job.sh`).
"""

import json
from pathlib import Path
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from ingestion.kafka.producer import KafkaTickProducer, LocalKafkaProducerMock, ProducerConfig
from ingestion.market_feed import DEFAULT_MARKET_UNIVERSE, MarketFeedGenerator
from ingestion.market_tick import MarketTick
from streaming.dataproc.config import DataprocStreamingConfig, parse_duration_to_seconds
from streaming.dataproc.run_tick_consumer import main as run_consumer_main, run_local_simulation
from streaming.dataproc.schema import (
    KAFKA_ENVELOPE_COLUMNS,
    MARKET_TICK_SCHEMA_DDL,
    MARKET_TICK_STRUCT_SPEC,
    REQUIRED_TICK_FIELDS,
    CorruptTickRecord,
    ParsedTickRecord,
    SparkFieldSpec,
    SparkStructTypeSpec,
    get_pyspark_tick_schema,
    parse_and_validate_kafka_record,
)
from streaming.dataproc.tick_consumer import (
    MicroBatchResult,
    PySparkKafkaTickConsumer,
    SparkStreamPlanDescriptor,
    StreamingQueryTelemetry,
)


# =============================================================================
# 1. Dataproc Streaming Configuration Tests
# =============================================================================


class TestDataprocStreamingConfig:
    """Unit tests for DataprocStreamingConfig validation and GCP resource resolution."""

    def test_default_config_canonical_naming(self) -> None:
        """Verify default configuration resolves canonical GCP and Kafka naming conventions."""
        config = DataprocStreamingConfig(environment="dev")
        config.validate()

        assert config.environment == "dev"
        assert config.cluster_name == "dp-tick-streaming-dev"
        assert config.app_name == "trading-risk-tick-streaming-dev"
        assert config.kafka_consumer_group_id == "cg-dataproc-tick-streaming-dev"
        assert config.checkpoint_location == "gs://trading-risk-dataflow-staging-dev/checkpoints/tick-streaming/"
        assert config.bronze_output_path == "gs://trading-risk-bronze-dev/ticks/"
        assert config.quarantine_output_path == "gs://trading-risk-bronze-dev/quarantine/ticks/"
        assert config.watermark_delay_seconds == 30.0
        assert config.trigger_interval_seconds == 5.0

    def test_environment_resolution_staging_and_prod(self) -> None:
        """Verify environment-specific resolution for staging and prod."""
        prod_cfg = DataprocStreamingConfig(environment="prod", project_id="trading-risk-engine-prod")
        prod_cfg.validate()

        assert prod_cfg.cluster_name == "dp-tick-streaming-prod"
        assert prod_cfg.app_name == "trading-risk-tick-streaming-prod"
        assert "trading-risk-bronze-prod" in prod_cfg.bronze_output_path

    def test_duration_parsing_units_and_errors(self) -> None:
        """Verify Spark duration expression parser across units and error cases."""
        assert parse_duration_to_seconds("500 milliseconds") == 0.5
        assert parse_duration_to_seconds("250 ms") == 0.25
        assert parse_duration_to_seconds("5 seconds") == 5.0
        assert parse_duration_to_seconds("30 s") == 30.0
        assert parse_duration_to_seconds("2 minutes") == 120.0
        assert parse_duration_to_seconds("1 min") == 60.0
        assert parse_duration_to_seconds("1 hour") == 3600.0

        with pytest.raises(ValueError, match="strictly positive"):
            parse_duration_to_seconds("0 seconds")

        with pytest.raises(ValueError, match="Invalid Spark duration expression"):
            parse_duration_to_seconds("unparseable string")

        with pytest.raises(ValueError, match="non-empty string"):
            parse_duration_to_seconds("")

    def test_validation_constraints(self) -> None:
        """Verify strict domain validation for invalid settings."""
        with pytest.raises(ValueError, match="Invalid environment"):
            DataprocStreamingConfig(environment="invalid_env").validate()

        with pytest.raises(ValueError, match="project_id must be a non-empty string"):
            DataprocStreamingConfig(project_id="").validate()

        with pytest.raises(ValueError, match="kafka_bootstrap_servers must be a non-empty"):
            DataprocStreamingConfig(kafka_bootstrap_servers="").validate()

        with pytest.raises(ValueError, match="kafka_subscribe_topics must be a non-empty"):
            DataprocStreamingConfig(kafka_subscribe_topics="").validate()

        with pytest.raises(ValueError, match="Invalid kafka_security_protocol"):
            DataprocStreamingConfig(kafka_security_protocol="INVALID").validate()

        with pytest.raises(ValueError, match="max_offsets_per_trigger must be > 0"):
            DataprocStreamingConfig(max_offsets_per_trigger=0).validate()

        with pytest.raises(ValueError, match="shuffle_partitions must be > 0"):
            DataprocStreamingConfig(shuffle_partitions=-5).validate()

        with pytest.raises(ValueError, match="Unsupported Spark output_mode"):
            DataprocStreamingConfig(output_mode="invalid_mode").validate()

    def test_starting_offsets_validation(self) -> None:
        """Verify starting_offsets accepts earliest, latest, and JSON partition map."""
        cfg_earliest = DataprocStreamingConfig(starting_offsets="earliest")
        cfg_earliest.validate()

        cfg_latest = DataprocStreamingConfig(starting_offsets="latest")
        cfg_latest.validate()

        valid_json = json.dumps({"market.ticks.v1": {"0": 100, "1": 200}})
        cfg_json = DataprocStreamingConfig(starting_offsets=valid_json)
        cfg_json.validate()

        with pytest.raises(ValueError, match="starting_offsets must be 'latest', 'earliest'"):
            DataprocStreamingConfig(starting_offsets="random_invalid_string").validate()

    def test_to_spark_conf_dict(self) -> None:
        """Verify SparkConf dictionary contains Dataproc and GCS connector properties."""
        config = DataprocStreamingConfig()
        conf = config.to_spark_conf_dict()

        assert conf["spark.app.name"] == "trading-risk-tick-streaming-dev"
        assert conf["spark.sql.shuffle.partitions"] == "12"
        assert conf["spark.sql.adaptive.enabled"] == "true"
        assert conf["spark.streaming.backpressure.enabled"] == "true"
        assert conf["spark.sql.parquet.compression.codec"] == "snappy"
        assert "org.apache.spark:spark-sql-kafka" in conf["spark.jars.packages"]
        assert "GoogleHadoopFileSystem" in conf["spark.hadoop.fs.gs.impl"]

    def test_to_kafka_read_options(self) -> None:
        """Verify Kafka read options dictionary matches Spark Structured Streaming Kafka connector spec."""
        config = DataprocStreamingConfig(
            kafka_bootstrap_servers="10.10.10.50:9092",
            kafka_subscribe_topics="market.ticks.v1",
            max_offsets_per_trigger=25000,
        )
        opts = config.to_kafka_read_options()

        assert opts["kafka.bootstrap.servers"] == "10.10.10.50:9092"
        assert opts["subscribe"] == "market.ticks.v1"
        assert opts["maxOffsetsPerTrigger"] == "25000"
        assert opts["failOnDataLoss"] == "false"
        assert opts["kafka.security.protocol"] == "PLAINTEXT"

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify configuration loading from environment variables."""
        monkeypatch.setenv("TRADING_RISK_ENV", "prod")
        monkeypatch.setenv("GCP_PROJECT_ID", "custom-risk-prod")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "10.20.30.40:9092")
        monkeypatch.setenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "75000")

        cfg = DataprocStreamingConfig.from_env()
        assert cfg.environment == "prod"
        assert cfg.project_id == "custom-risk-prod"
        assert cfg.kafka_bootstrap_servers == "10.20.30.40:9092"
        assert cfg.max_offsets_per_trigger == 75000

    def test_from_cli_args(self) -> None:
        """Verify command-line argument parsing."""
        argv = [
            "--environment",
            "staging",
            "--project_id",
            "staging-risk-99",
            "--max_offsets_per_trigger",
            "15000",
            "--trigger_processing_time",
            "10 seconds",
        ]
        cfg = DataprocStreamingConfig.from_cli_args(argv)
        assert cfg.environment == "staging"
        assert cfg.project_id == "staging-risk-99"
        assert cfg.max_offsets_per_trigger == 15000
        assert cfg.trigger_processing_time == "10 seconds"


# =============================================================================
# 2. Spark SQL Schema & Microstructure Validation Tests
# =============================================================================


class TestSparkSchemaAndMicrostructure:
    """Unit tests for Spark SQL DDL schemas, record parsers, and quarantine classifications."""

    def test_schema_ddl_and_struct_spec(self) -> None:
        """Verify explicit Spark SQL DDL syntax and StructType field definitions."""
        assert "trade_id STRING" in MARKET_TICK_SCHEMA_DDL
        assert "ticker STRING" in MARKET_TICK_SCHEMA_DDL
        assert "bid DOUBLE" in MARKET_TICK_SCHEMA_DDL
        assert "ask DOUBLE" in MARKET_TICK_SCHEMA_DDL
        assert "spread DOUBLE" in MARKET_TICK_SCHEMA_DDL
        assert "mid_price DOUBLE" in MARKET_TICK_SCHEMA_DDL
        assert "_corrupt_record STRING" in MARKET_TICK_SCHEMA_DDL

        field_names = MARKET_TICK_STRUCT_SPEC.field_names
        for req in REQUIRED_TICK_FIELDS:
            assert req in field_names

        ddl = MARKET_TICK_STRUCT_SPEC.to_ddl()
        assert "trade_id STRING" in ddl
        assert "_corrupt_record STRING" in ddl

    def test_get_pyspark_tick_schema(self) -> None:
        """Verify get_pyspark_tick_schema returns a valid schema specification."""
        schema = get_pyspark_tick_schema()
        assert schema is not None

    def test_valid_tick_parsing_and_enrichment(self) -> None:
        """Verify parse_and_validate_kafka_record on valid tick payloads computes microstructure fields."""
        raw_tick = {
            "trade_id": "TRD-AAPL-1001",
            "ticker": "AAPL",
            "asset_class": "EQUITY",
            "bid": 185.20,
            "ask": 185.30,
            "last_price": 185.25,
            "volume": 200.0,
            "timestamp": "2026-10-21T10:15:30.500000+00:00",
            "sequence_number": 42,
        }
        record = parse_and_validate_kafka_record(
            raw_value=json.dumps(raw_tick).encode("utf-8"),
            kafka_key=b"EQUITY#AAPL",
            kafka_topic="market.ticks.v1",
            kafka_partition=2,
            kafka_offset=150,
        )

        assert isinstance(record, ParsedTickRecord)
        assert record.trade_id == "TRD-AAPL-1001"
        assert record.ticker == "AAPL"
        assert record.asset_class == "EQUITY"
        assert record.bid == 185.20
        assert record.ask == 185.30
        assert record.last_price == 185.25
        assert record.volume == 200.0
        assert record.spread == 0.10
        assert record.mid_price == 185.25
        assert record.notional_usd == 37050.0  # 185.25 * 200
        assert record.relative_spread_bps == pytest.approx(5.3981, abs=0.01)
        assert record.sequence_number == 42
        assert record.year == 2026
        assert record.month == 10
        assert record.day == 21
        assert record.hour == 10
        assert record.kafka_partition == 2
        assert record.kafka_offset == 150
        assert record.kafka_key == "EQUITY#AAPL"

        # Verify serialization
        d = record.to_dict()
        assert d["trade_id"] == "TRD-AAPL-1001"
        j = record.to_json()
        assert "TRD-AAPL-1001" in j

    def test_valid_tick_from_market_tick_object(self) -> None:
        """Verify parse_and_validate_kafka_record accepts MarketTick dataclass with to_dict()."""
        market_tick = MarketTick.create(
            trade_id="TRD-BTC-001",
            ticker="BTCUSD",
            asset_class="CRYPTO",
            bid=67450.0,
            ask=67460.0,
            last_price=67455.0,
            volume=1.5,
            timestamp="2026-10-21T12:00:00+00:00",
            sequence_number=1,
        )
        record = parse_and_validate_kafka_record(raw_value=market_tick)
        assert isinstance(record, ParsedTickRecord)
        assert record.ticker == "BTCUSD"
        assert record.asset_class == "CRYPTO"
        assert record.notional_usd == round(67455.0 * 1.5, 4)

    def test_quarantine_empty_or_none_payload(self) -> None:
        """Verify None payload is quarantined as EmptyPayloadError."""
        record = parse_and_validate_kafka_record(raw_value=None)
        assert isinstance(record, CorruptTickRecord)
        assert record.error_type == "EmptyPayloadError"

    def test_quarantine_unicode_decode_error(self) -> None:
        """Verify invalid UTF-8 bytes are quarantined as UnicodeDecodeError."""
        invalid_bytes = b"\xff\xfe\x00\x81\xab"
        record = parse_and_validate_kafka_record(raw_value=invalid_bytes)
        assert isinstance(record, CorruptTickRecord)
        assert record.error_type == "UnicodeDecodeError"

    def test_quarantine_malformed_json(self) -> None:
        """Verify malformed JSON is quarantined as JSONDecodeError."""
        record = parse_and_validate_kafka_record(raw_value=b'{"trade_id": "T1", "price": 10.0,')
        assert isinstance(record, CorruptTickRecord)
        assert record.error_type == "JSONDecodeError"

    def test_quarantine_non_dict_json(self) -> None:
        """Verify JSON payload that is not an object is quarantined as JSONDecodeError."""
        record = parse_and_validate_kafka_record(raw_value=b"[1, 2, 3]")
        assert isinstance(record, CorruptTickRecord)
        assert record.error_type == "JSONDecodeError"

    def test_quarantine_missing_required_fields(self) -> None:
        """Verify payloads missing required fields are quarantined as MissingFieldError."""
        payload = {"trade_id": "T1", "ticker": "AAPL"}  # missing bid, ask, last_price, volume, etc.
        record = parse_and_validate_kafka_record(raw_value=payload)
        assert isinstance(record, CorruptTickRecord)
        assert record.error_type == "MissingFieldError"
        assert "bid" in record.error_message

    def test_quarantine_unsupported_asset_class(self) -> None:
        """Verify unsupported asset class is quarantined as ValidationError."""
        payload = {
            "trade_id": "T1",
            "ticker": "CUSTOM",
            "asset_class": "UNKNOWN_DERIVATIVE",
            "bid": 10.0,
            "ask": 11.0,
            "last_price": 10.5,
            "volume": 100.0,
            "timestamp": "2026-10-21T10:00:00+00:00",
        }
        record = parse_and_validate_kafka_record(raw_value=payload)
        assert isinstance(record, CorruptTickRecord)
        assert record.error_type == "ValidationError"
        assert "Unsupported asset_class" in record.error_message

    def test_quarantine_non_positive_price_or_volume(self) -> None:
        """Verify negative or zero prices/volumes are quarantined."""
        payload_neg_bid = {
            "trade_id": "T1",
            "ticker": "AAPL",
            "asset_class": "EQUITY",
            "bid": -10.0,
            "ask": 185.0,
            "last_price": 184.0,
            "volume": 100.0,
            "timestamp": "2026-10-21T10:00:00+00:00",
        }
        rec = parse_and_validate_kafka_record(raw_value=payload_neg_bid)
        assert isinstance(rec, CorruptTickRecord)
        assert rec.error_type == "ValidationError"
        assert "Non-positive price or volume" in rec.error_message

    def test_quarantine_crossed_quote(self) -> None:
        """Verify crossed quotes (bid > ask) are quarantined as CrossedQuoteError."""
        payload_crossed = {
            "trade_id": "T-CROSSED-1",
            "ticker": "NVDA",
            "asset_class": "EQUITY",
            "bid": 130.00,
            "ask": 125.00,  # bid > ask
            "last_price": 127.50,
            "volume": 500.0,
            "timestamp": "2026-10-21T10:00:00+00:00",
        }
        rec = parse_and_validate_kafka_record(raw_value=payload_crossed)
        assert isinstance(rec, CorruptTickRecord)
        assert rec.error_type == "CrossedQuoteError"
        assert "Crossed market quote detected" in rec.error_message


# =============================================================================
# 3. Execution Plan Descriptor & Mock Builders Tests
# =============================================================================


class TestSparkPlanDescriptorAndBuilders:
    """Unit tests for Spark Structured Streaming plan descriptors and builder methods."""

    def test_build_stream_plan_descriptor(self) -> None:
        """Verify build_stream_plan_descriptor exports all query configuration parameters."""
        config = DataprocStreamingConfig(
            environment="staging",
            kafka_subscribe_topics="market.ticks.v1,market.ticks.v2",
            trigger_processing_time="10 seconds",
            watermark_delay_threshold="1 minute",
        )
        consumer = PySparkKafkaTickConsumer(config=config)
        plan = consumer.build_stream_plan_descriptor()

        assert isinstance(plan, SparkStreamPlanDescriptor)
        assert plan.app_name == "trading-risk-tick-streaming-staging"
        assert plan.source_format == "kafka"
        assert "market.ticks.v1,market.ticks.v2" in plan.kafka_options["subscribe"]
        assert plan.watermark_column == "event_timestamp"
        assert plan.watermark_delay == "1 minute"
        assert "relative_spread_bps" in plan.enrichment_columns
        assert plan.partition_by_columns == ["year", "month", "day", "asset_class"]
        assert plan.trigger_processing_time == "10 seconds"

    def test_build_spark_session_with_mock_builder(self) -> None:
        """Verify build_spark_session configures appName and configs on mock SparkSession.builder."""
        config = DataprocStreamingConfig(environment="dev")
        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = "mock_spark_session"

        spark = PySparkKafkaTickConsumer.build_spark_session(config, spark_builder=mock_builder)
        assert spark == "mock_spark_session"
        mock_builder.appName.assert_called_with("trading-risk-tick-streaming-dev")
        assert mock_builder.config.call_count >= 10

    def test_build_kafka_read_stream(self) -> None:
        """Verify build_kafka_read_stream passes Kafka connector options to spark.readStream."""
        config = DataprocStreamingConfig()
        mock_spark = MagicMock()
        mock_format = MagicMock()
        mock_spark.readStream.format.return_value = mock_format
        mock_format.options.return_value = mock_format
        mock_format.load.return_value = "mock_streaming_df"

        stream_df = PySparkKafkaTickConsumer.build_kafka_read_stream(mock_spark, config)
        assert stream_df == "mock_streaming_df"
        mock_spark.readStream.format.assert_called_with("kafka")
        mock_format.options.assert_called_once()
        mock_format.load.assert_called_once()

    def test_start_bronze_and_quarantine_writers(self) -> None:
        """Verify start_bronze_and_quarantine_writers configures Parquet and JSON streaming sinks."""
        config = DataprocStreamingConfig(environment="dev")
        mock_valid_df = MagicMock()
        mock_corrupt_df = MagicMock()

        mock_v_writer = MagicMock()
        mock_valid_df.writeStream.format.return_value = mock_v_writer
        mock_v_writer.outputMode.return_value = mock_v_writer
        mock_v_writer.option.return_value = mock_v_writer
        mock_v_writer.partitionBy.return_value = mock_v_writer
        mock_v_writer.trigger.return_value = mock_v_writer
        mock_v_writer.queryName.return_value = mock_v_writer
        mock_v_writer.start.return_value = "mock_bronze_query"

        mock_c_writer = MagicMock()
        mock_corrupt_df.writeStream.format.return_value = mock_c_writer
        mock_c_writer.outputMode.return_value = mock_c_writer
        mock_c_writer.option.return_value = mock_c_writer
        mock_c_writer.trigger.return_value = mock_c_writer
        mock_c_writer.queryName.return_value = mock_c_writer
        mock_c_writer.start.return_value = "mock_quarantine_query"

        b_query, q_query = PySparkKafkaTickConsumer.start_bronze_and_quarantine_writers(
            mock_valid_df, mock_corrupt_df, config
        )
        assert b_query == "mock_bronze_query"
        assert q_query == "mock_quarantine_query"
        mock_valid_df.writeStream.format.assert_called_with("parquet")
        mock_corrupt_df.writeStream.format.assert_called_with("json")


# =============================================================================
# 4. Local Hermetic Micro-Batch Execution Engine Tests
# =============================================================================


class TestLocalMicroBatchExecutionEngine:
    """Unit tests for stateful, hermetic micro-batch execution without a live cluster."""

    def test_process_raw_messages_happy_path(self) -> None:
        """Verify process_raw_messages successfully parses and accumulates valid records."""
        consumer = PySparkKafkaTickConsumer()
        messages = [
            {
                "topic": "market.ticks.v1",
                "partition": 0,
                "offset": 0,
                "key": b"EQUITY#AAPL",
                "value": json.dumps(
                    {
                        "trade_id": "T1",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 185.0,
                        "ask": 185.1,
                        "last_price": 185.05,
                        "volume": 100.0,
                        "timestamp": "2026-10-21T10:00:00+00:00",
                    }
                ).encode("utf-8"),
            },
            {
                "topic": "market.ticks.v1",
                "partition": 1,
                "offset": 0,
                "key": b"ETF#SPY",
                "value": json.dumps(
                    {
                        "trade_id": "T2",
                        "ticker": "SPY",
                        "asset_class": "ETF",
                        "bid": 550.0,
                        "ask": 550.2,
                        "last_price": 550.1,
                        "volume": 500.0,
                        "timestamp": "2026-10-21T10:00:01+00:00",
                    }
                ).encode("utf-8"),
            },
        ]
        result = consumer.process_raw_messages(messages)

        assert result.batch_id == 0
        assert result.records_polled == 2
        assert len(result.valid_records) == 2
        assert len(result.corrupt_records) == 0
        assert len(consumer.bronze_sink_records) == 2
        assert consumer.telemetry.valid_records_emitted == 2
        assert consumer.telemetry.batches_processed == 1

    def test_interleaved_valid_and_corrupt_records(self) -> None:
        """Verify mixed valid and corrupt payloads are separated cleanly into bronze and quarantine."""
        consumer = PySparkKafkaTickConsumer()
        messages = [
            {
                "topic": "market.ticks.v1",
                "partition": 0,
                "offset": 0,
                "value": json.dumps(
                    {
                        "trade_id": "T-VALID",
                        "ticker": "MSFT",
                        "asset_class": "EQUITY",
                        "bid": 420.0,
                        "ask": 420.2,
                        "last_price": 420.1,
                        "volume": 50.0,
                        "timestamp": "2026-10-21T10:00:00+00:00",
                    }
                ).encode("utf-8"),
            },
            {
                "topic": "market.ticks.v1",
                "partition": 0,
                "offset": 1,
                "value": b'{"trade_id": "T-CORRUPT", malformed_json}}',
            },
            {
                "topic": "market.ticks.v1",
                "partition": 0,
                "offset": 2,
                "value": json.dumps(
                    {
                        "trade_id": "T-CROSSED",
                        "ticker": "MSFT",
                        "asset_class": "EQUITY",
                        "bid": 430.0,  # crossed
                        "ask": 420.0,
                        "last_price": 425.0,
                        "volume": 50.0,
                        "timestamp": "2026-10-21T10:00:01+00:00",
                    }
                ).encode("utf-8"),
            },
        ]
        res = consumer.process_raw_messages(messages)

        assert res.records_polled == 3
        assert len(res.valid_records) == 1
        assert len(res.corrupt_records) == 2
        assert len(consumer.bronze_sink_records) == 1
        assert len(consumer.quarantine_sink_records) == 2
        assert consumer.telemetry.corrupt_records_quarantined == 2

    def test_event_time_watermark_advancement(self) -> None:
        """Verify event-time watermark advances by max_event_time - watermark_delay_seconds."""
        config = DataprocStreamingConfig(watermark_delay_threshold="30 seconds")
        consumer = PySparkKafkaTickConsumer(config=config)

        # Batch 1: event time 10:01:00 UTC (epoch = 1792576860.0)
        t1_iso = "2026-10-21T10:01:00+00:00"
        msg1 = [
            {
                "topic": "market.ticks.v1",
                "partition": 0,
                "offset": 0,
                "value": json.dumps(
                    {
                        "trade_id": "T1",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 185.0,
                        "ask": 185.1,
                        "last_price": 185.05,
                        "volume": 10.0,
                        "timestamp": t1_iso,
                    }
                ).encode("utf-8"),
            }
        ]
        res1 = consumer.process_raw_messages(msg1)
        # Watermark should advance to t1 - 30s
        expected_wm1 = res1.valid_records[0].event_epoch_sec - 30.0
        assert res1.watermark_epoch_sec == expected_wm1
        assert res1.watermark_iso is not None

        # Batch 2: event time 10:02:00 UTC (60s later)
        t2_iso = "2026-10-21T10:02:00+00:00"
        msg2 = [
            {
                "topic": "market.ticks.v1",
                "partition": 0,
                "offset": 1,
                "value": json.dumps(
                    {
                        "trade_id": "T2",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 185.2,
                        "ask": 185.3,
                        "last_price": 185.25,
                        "volume": 10.0,
                        "timestamp": t2_iso,
                    }
                ).encode("utf-8"),
            }
        ]
        res2 = consumer.process_raw_messages(msg2)
        expected_wm2 = res2.valid_records[0].event_epoch_sec - 30.0
        assert res2.watermark_epoch_sec == expected_wm2
        assert res2.watermark_epoch_sec > res1.watermark_epoch_sec

    def test_late_arriving_event_dropped_behind_watermark(self) -> None:
        """Verify late records with event timestamp behind the current watermark are dropped."""
        config = DataprocStreamingConfig(watermark_delay_threshold="10 seconds")
        consumer = PySparkKafkaTickConsumer(config=config)

        # Batch 1: establish watermark at 10:00:50 (event time 10:01:00 - 10s)
        consumer.process_raw_messages(
            [
                {
                    "topic": "market.ticks.v1",
                    "partition": 0,
                    "offset": 0,
                    "value": json.dumps(
                        {
                            "trade_id": "T-INIT",
                            "ticker": "AAPL",
                            "asset_class": "EQUITY",
                            "bid": 185.0,
                            "ask": 185.1,
                            "last_price": 185.05,
                            "volume": 10.0,
                            "timestamp": "2026-10-21T10:01:00+00:00",
                        }
                    ).encode("utf-8"),
                }
            ]
        )
        assert consumer.telemetry.current_watermark_epoch_sec > 0

        # Batch 2: inject a late tick with event time 10:00:30 (older than watermark of 10:00:50)
        late_res = consumer.process_raw_messages(
            [
                {
                    "topic": "market.ticks.v1",
                    "partition": 0,
                    "offset": 1,
                    "value": json.dumps(
                        {
                            "trade_id": "T-LATE",
                            "ticker": "AAPL",
                            "asset_class": "EQUITY",
                            "bid": 184.0,
                            "ask": 184.1,
                            "last_price": 184.05,
                            "volume": 10.0,
                            "timestamp": "2026-10-21T10:00:30+00:00",  # LATE
                        }
                    ).encode("utf-8"),
                }
            ]
        )

        assert len(late_res.dropped_late_records) == 1
        assert late_res.dropped_late_records[0].trade_id == "T-LATE"
        assert len(late_res.valid_records) == 0
        assert consumer.telemetry.late_records_dropped == 1

    def test_max_offsets_per_trigger_rate_limiting(self) -> None:
        """Verify max_offsets_per_trigger accurately throttles consumption per micro-batch."""
        mock_broker = LocalKafkaProducerMock(num_partitions_map={"market.ticks.v1": 2})
        topic = "market.ticks.v1"
        producer = KafkaTickProducer(
            config=ProducerConfig(topic=topic, simulation_mode=True),
            mock_producer=mock_broker,
        )

        # Produce 25 records into partition 0 and 25 records into partition 1 (total 50)
        for i in range(50):
            tick = MarketTick.create(
                trade_id=f"T-{i}",
                ticker="AAPL" if i % 2 == 0 else "MSFT",
                asset_class="EQUITY",
                bid=100.0,
                ask=100.1,
                last_price=100.05,
                volume=10.0,
                timestamp="2026-10-21T10:00:00+00:00",
                sequence_number=i,
            )
            producer.send_tick(tick)

        assert mock_broker.get_total_records(topic) == 50

        # Set max_offsets_per_trigger = 15
        config = DataprocStreamingConfig(
            kafka_subscribe_topics=topic,
            starting_offsets="earliest",
            max_offsets_per_trigger=15,
        )
        consumer = PySparkKafkaTickConsumer(config=config)

        batch1 = consumer.poll_from_producer(mock_broker)
        assert batch1.records_polled == 15

        batch2 = consumer.poll_from_producer(mock_broker)
        assert batch2.records_polled == 15

        batch3 = consumer.poll_from_producer(mock_broker)
        assert batch3.records_polled == 15

        batch4 = consumer.poll_from_producer(mock_broker)
        assert batch4.records_polled == 5

        batch5 = consumer.poll_from_producer(mock_broker)
        assert batch5.records_polled == 0

        assert consumer.telemetry.records_received == 50

    def test_starting_offsets_latest_skips_preexisting(self) -> None:
        """Verify starting_offsets='latest' with initialize_offsets_from_producer skips historical data."""
        mock_broker = LocalKafkaProducerMock(num_partitions_map={"market.ticks.v1": 2})
        topic = "market.ticks.v1"
        producer = KafkaTickProducer(
            config=ProducerConfig(topic=topic, simulation_mode=True),
            mock_producer=mock_broker,
        )

        # Produce 10 pre-existing records
        for i in range(10):
            tick = MarketTick.create(
                trade_id=f"HIST-{i}",
                ticker="AAPL",
                asset_class="EQUITY",
                bid=100.0,
                ask=100.1,
                last_price=100.05,
                volume=10.0,
                timestamp="2026-10-21T10:00:00+00:00",
            )
            producer.send_tick(tick)

        config = DataprocStreamingConfig(
            kafka_subscribe_topics=topic,
            starting_offsets="latest",
        )
        consumer = PySparkKafkaTickConsumer(config=config)
        consumer.initialize_offsets_from_producer(mock_broker)

        # Poll immediately: should receive 0 historical records
        b1 = consumer.poll_from_producer(mock_broker)
        assert b1.records_polled == 0

        # Produce 5 new records
        for i in range(5):
            tick = MarketTick.create(
                trade_id=f"NEW-{i}",
                ticker="AAPL",
                asset_class="EQUITY",
                bid=101.0,
                ask=101.1,
                last_price=101.05,
                volume=10.0,
                timestamp="2026-10-21T10:05:00+00:00",
            )
            producer.send_tick(tick)

        # Poll again: should receive only the 5 new records
        b2 = consumer.poll_from_producer(mock_broker)
        assert b2.records_polled == 5
        assert b2.valid_records[0].trade_id.startswith("NEW-")

    def test_local_disk_checkpoint_persistence_and_recovery(self) -> None:
        """Verify atomic checkpoint persistence to disk and stateful recovery across consumer restarts."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            ckpt_path = Path(tmp_dir) / "checkpoints" / "tick-streaming"
            config = DataprocStreamingConfig(
                checkpoint_location=str(ckpt_path),
                starting_offsets="earliest",
            )
            consumer = PySparkKafkaTickConsumer(config=config)

            msg = [
                {
                    "topic": "market.ticks.v1",
                    "partition": 0,
                    "offset": 0,
                    "value": json.dumps(
                        {
                            "trade_id": "T1",
                            "ticker": "AAPL",
                            "asset_class": "EQUITY",
                            "bid": 185.0,
                            "ask": 185.1,
                            "last_price": 185.05,
                            "volume": 100.0,
                            "timestamp": "2026-10-21T10:00:00+00:00",
                        }
                    ).encode("utf-8"),
                }
            ]
            consumer.process_raw_messages(msg)

            ckpt_file = ckpt_path / "offsets.json"
            assert ckpt_file.exists()
            data = json.loads(ckpt_file.read_text(encoding="utf-8"))
            assert data["batches_processed"] == 1
            assert data["valid_records_emitted"] == 1
            assert data["offsets"]["market.ticks.v1"]["0"] == 1

            # Instantiate second consumer pointing to same checkpoint directory
            resumed_consumer = PySparkKafkaTickConsumer(config=config)
            assert resumed_consumer.telemetry.batches_processed == 1
            assert resumed_consumer.telemetry.valid_records_emitted == 1
            assert resumed_consumer.telemetry.partition_offsets["market.ticks.v1"][0] == 1

    def test_foreach_batch_callback_invocation(self) -> None:
        """Verify foreach_batch_fn callback is invoked on each micro-batch."""
        captured_batches: List[MicroBatchResult] = []

        def on_batch(result: MicroBatchResult) -> None:
            captured_batches.append(result)

        consumer = PySparkKafkaTickConsumer()
        msg = [
            {
                "topic": "market.ticks.v1",
                "partition": 0,
                "offset": 0,
                "value": json.dumps(
                    {
                        "trade_id": "T1",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 185.0,
                        "ask": 185.1,
                        "last_price": 185.05,
                        "volume": 100.0,
                        "timestamp": "2026-10-21T10:00:00+00:00",
                    }
                ).encode("utf-8"),
            }
        ]
        consumer.process_raw_messages(msg, foreach_batch_fn=on_batch)
        assert len(captured_batches) == 1
        assert captured_batches[0].batch_id == 0


# =============================================================================
# 5. Telemetry & Prometheus Formatting Tests
# =============================================================================


class TestStreamingTelemetryAndPrometheus:
    """Unit tests for telemetry dictionaries and Prometheus exposition formatting."""

    def test_telemetry_snapshot_dict(self) -> None:
        """Verify telemetry snapshot dictionary exports all counters, timestamps, and offset maps."""
        t = StreamingQueryTelemetry(
            batches_processed=10,
            records_received=1000,
            valid_records_emitted=950,
            corrupt_records_quarantined=40,
            late_records_dropped=10,
            current_watermark_epoch_sec=1792576800.0,
            partition_offsets={"market.ticks.v1": {0: 500, 1: 500}},
        )
        d = t.to_dict()
        assert d["batches_processed"] == 10
        assert d["records_received"] == 1000
        assert d["valid_records_emitted"] == 950
        assert d["corrupt_records_quarantined"] == 40
        assert d["late_records_dropped"] == 10
        assert d["current_watermark_iso"] is not None
        assert d["partition_offsets"]["market.ticks.v1"]["0"] == 500

    def test_telemetry_prometheus_formatting(self) -> None:
        """Verify format_prometheus renders valid Prometheus text exposition lines."""
        t = StreamingQueryTelemetry(
            batches_processed=5,
            records_received=500,
            valid_records_emitted=480,
            corrupt_records_quarantined=15,
            late_records_dropped=5,
            current_watermark_epoch_sec=1792576800.0,
            partition_offsets={"market.ticks.v1": {0: 250, 1: 250}},
        )
        prom_text = t.format_prometheus(app_name="trading-risk-tick-streaming")

        assert "dataproc_streaming_batches_processed_total{app=\"trading-risk-tick-streaming\"} 5" in prom_text
        assert "dataproc_streaming_records_received_total{app=\"trading-risk-tick-streaming\"} 500" in prom_text
        assert "dataproc_streaming_valid_records_emitted_total{app=\"trading-risk-tick-streaming\"} 480" in prom_text
        assert "dataproc_streaming_corrupt_records_quarantined_total{app=\"trading-risk-tick-streaming\"} 15" in prom_text
        assert "dataproc_streaming_late_records_dropped_total{app=\"trading-risk-tick-streaming\"} 5" in prom_text
        assert "dataproc_streaming_watermark_epoch_seconds{app=\"trading-risk-tick-streaming\"} 1792576800.000" in prom_text
        assert 'dataproc_streaming_committed_offset{app="trading-risk-tick-streaming",topic="market.ticks.v1",partition="0"} 250' in prom_text


# =============================================================================
# 6. End-to-End Market Feed Ingestion Integration Tests
# =============================================================================


class TestDataprocE2EIntegration:
    """Integration tests connecting MarketFeedGenerator -> KafkaTickProducer -> PySparkKafkaTickConsumer."""

    def test_e2e_generator_to_producer_to_consumer(self) -> None:
        """Verify full end-to-end ingestion and structured streaming consumption across universe."""
        topic = "market.ticks.v1"
        mock_broker = LocalKafkaProducerMock(num_partitions_map={topic: 6})
        producer = KafkaTickProducer(
            config=ProducerConfig(topic=topic, simulation_mode=True),
            mock_producer=mock_broker,
        )

        feed_gen = MarketFeedGenerator(seed=123)
        # Generate 60 ticks across multiple tickers and asset classes
        for _ in range(60):
            tick = feed_gen.simulate_tick()
            producer.send_tick(tick)

        assert mock_broker.get_total_records(topic) == 60

        # Configure consumer with batch size of 20
        config = DataprocStreamingConfig(
            kafka_subscribe_topics=topic,
            starting_offsets="earliest",
            max_offsets_per_trigger=20,
        )
        consumer = PySparkKafkaTickConsumer(config=config)
        batches = consumer.drain_producer(mock_broker)

        assert len(batches) == 3
        assert sum(b.records_polled for b in batches) == 60
        assert consumer.telemetry.valid_records_emitted == 60
        assert len(consumer.bronze_sink_records) == 60

        # Verify all records have valid microstructure enrichments
        for rec in consumer.bronze_sink_records:
            assert rec.spread > 0
            assert rec.mid_price > 0
            assert rec.notional_usd > 0
            assert rec.relative_spread_bps > 0
            assert rec.year == 2026

    def test_e2e_multi_partition_deterministic_routing(self) -> None:
        """Verify that ticks for the same ticker route to the same partition and offsets increment sequentially."""
        topic = "market.ticks.v1"
        mock_broker = LocalKafkaProducerMock(num_partitions_map={topic: 6})
        producer = KafkaTickProducer(
            config=ProducerConfig(topic=topic, simulation_mode=True),
            mock_producer=mock_broker,
        )

        feed_gen = MarketFeedGenerator(tickers=["AAPL"], seed=99)
        for _ in range(10):
            producer.send_tick(feed_gen.simulate_tick("AAPL"))

        # In AAPL profile: key is "EQUITY#AAPL", routes to a single deterministic partition
        dist = mock_broker.get_partition_distribution(topic)
        active_partitions = [p for p, count in dist.items() if count > 0]
        assert len(active_partitions) == 1
        target_part = active_partitions[0]
        assert dist[target_part] == 10

        # Consume and verify offset monotonicity
        consumer = PySparkKafkaTickConsumer(
            config=DataprocStreamingConfig(kafka_subscribe_topics=topic, starting_offsets="earliest")
        )
        batch = consumer.poll_from_producer(mock_broker)
        assert batch.records_polled == 10
        assert consumer.telemetry.partition_offsets[topic][target_part] == 10


# =============================================================================
# 7. CLI Runner & Submit Shell Script Tests
# =============================================================================


class TestDataprocRunnerCliAndScripts:
    """Unit tests for run_tick_consumer.py CLI and submit_job.sh script."""

    def test_run_local_simulation_helper(self) -> None:
        """Verify run_local_simulation runs cleanly and returns execution summary."""
        config = DataprocStreamingConfig()
        summary = run_local_simulation(config=config, num_ticks=30, seed=42)

        assert summary["mode"] == "local_simulation"
        assert summary["batches_executed"] >= 1
        assert summary["telemetry"]["valid_records_emitted"] == 30

    def test_run_tick_consumer_cli_dry_run(self, capsys: pytest.CaptureFixture) -> None:
        """Verify run_tick_consumer.py with --dry_run outputs valid JSON plan and exits 0."""
        exit_code = run_consumer_main(["--dry_run", "--environment", "dev"])
        assert exit_code == 0

        captured = capsys.readouterr()
        out_json = json.loads(captured.out)
        assert out_json["mode"] == "dry_run"
        assert out_json["config"]["environment"] == "dev"
        assert "spark_conf" in out_json
        assert "plan" in out_json

    def test_run_tick_consumer_cli_local_simulation(self, capsys: pytest.CaptureFixture) -> None:
        """Verify run_tick_consumer.py with --local_simulation outputs execution summary and exits 0."""
        exit_code = run_consumer_main(
            ["--local_simulation", "--simulation_ticks", "15", "--environment", "dev"]
        )
        assert exit_code == 0

        captured = capsys.readouterr()
        out_json = json.loads(captured.out)
        assert out_json["mode"] == "local_simulation"
        assert out_json["telemetry"]["valid_records_emitted"] == 15

    def test_run_tick_consumer_cli_invalid_args(self) -> None:
        """Verify invalid arguments return exit code 2."""
        with pytest.raises(SystemExit) as exc_info:
            run_consumer_main(["--environment", "invalid_environment"])
        assert exc_info.value.code == 2

    def test_submit_job_shell_script_contents_and_syntax(self) -> None:
        """Verify submit_job.sh script exists, uses pipefail, and defines Dataproc job parameters."""
        script_path = Path("streaming/dataproc/submit_job.sh")
        assert script_path.exists()

        content = script_path.read_text(encoding="utf-8")
        assert "#!/usr/bin/env bash" in content
        assert "set -euo pipefail" in content
        assert "gcloud dataproc jobs submit pyspark" in content
        assert "streaming/dataproc/run_tick_consumer.py" in content
        assert "spark.jars.packages" in content
        assert "org.apache.spark:spark-sql-kafka" in content
        assert "--kafka_subscribe_topics" in content
        assert "--watermark_delay_threshold" in content
