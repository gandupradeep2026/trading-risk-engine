"""Zero-Cost Hermetic Test Suite for Day 12: Dataproc PySpark Kafka Tick Streaming Application.

Validates:
- `DataprocStreamingConfig` validation, environment resource naming, SparkConf/Kafka option builders,
  and duration expression parsing (`parse_duration_to_seconds`).
- `MARKET_TICK_SCHEMA_DDL`, `MARKET_TICK_STRUCT_SPEC`, and `parse_and_validate_kafka_record`
  across all valid asset classes, microstructure enrichments (`notional_usd`, `relative_spread_bps`,
  `year`/`month`/`day`/`hour`), and corruption scenarios (invalid UTF-8, malformed JSON, missing
  fields, unsupported asset class, non-positive prices, crossed quotes).
- `PySparkKafkaTickConsumer` SparkSession, Kafka readStream, DataFrame transformation, and
  dual-stream Bronze Parquet + Quarantine sink writer builders.
- Stateful micro-batch execution against `LocalKafkaProducerMock` + `MarketFeedGenerator` with:
  - Per-partition offset tracking (`earliest`, `latest`, and JSON partition offset maps)
  - `max_offsets_per_trigger` rate limiting across micro-batches
  - Fault-tolerant local disk checkpoint persistence & consumer restart recovery
  - Event-time watermark progression (`max_event_time - watermark_delay`) & late-data dropping
  - Corrupt record quarantine routing & Prometheus telemetry formatting
- CLI entrypoint (`run_tick_consumer.py`) `--dry_run` and `--local_simulation` modes + `submit_job.sh`.
"""

import json
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from ingestion.kafka.producer import KafkaTickProducer, LocalKafkaProducerMock, ProducerConfig
from ingestion.market_feed import MarketFeedGenerator
from streaming.dataproc import (
    KAFKA_ENVELOPE_COLUMNS,
    MARKET_TICK_SCHEMA_DDL,
    MARKET_TICK_STRUCT_SPEC,
    CorruptTickRecord,
    DataprocStreamingConfig,
    ParsedTickRecord,
    PySparkKafkaTickConsumer,
    get_pyspark_tick_schema,
    parse_and_validate_kafka_record,
    parse_duration_to_seconds,
)
from streaming.dataproc.run_tick_consumer import main as cli_main, run_local_simulation


# ==============================================================================
# 1. Configuration & Duration Parsing Tests
# ==============================================================================


class TestDataprocStreamingConfig:
    """Tests for DataprocStreamingConfig defaults, validation, and Spark/Kafka option exports."""

    @pytest.mark.parametrize(
        "expr,expected_sec",
        [
            ("500 milliseconds", 0.5),
            ("250 ms", 0.25),
            ("5 seconds", 5.0),
            ("30 sec", 30.0),
            ("2 minutes", 120.0),
            ("1.5 min", 90.0),
            ("1 hour", 3600.0),
        ],
    )
    def test_parse_duration_to_seconds_valid(self, expr: str, expected_sec: float) -> None:
        assert parse_duration_to_seconds(expr) == pytest.approx(expected_sec)

    @pytest.mark.parametrize(
        "invalid_expr",
        ["", "   ", "0 seconds", "-5 seconds", "10 lightyears", "seconds 5"],
    )
    def test_parse_duration_to_seconds_invalid_raises(self, invalid_expr: str) -> None:
        with pytest.raises(ValueError):
            parse_duration_to_seconds(invalid_expr)

    def test_default_resource_naming_by_environment(self) -> None:
        cfg = DataprocStreamingConfig(project_id="trading-risk-engine-prod", environment="prod")
        cfg.validate()
        assert cfg.cluster_name == "dp-tick-streaming-prod"
        assert cfg.app_name == "trading-risk-tick-streaming-prod"
        assert cfg.kafka_consumer_group_id == "cg-dataproc-tick-streaming-prod"
        assert (
            cfg.checkpoint_location
            == "gs://trading-risk-dataflow-staging-prod/checkpoints/tick-streaming/"
        )
        assert cfg.bronze_output_path == "gs://trading-risk-bronze-prod/ticks/"
        assert cfg.quarantine_output_path == "gs://trading-risk-bronze-prod/quarantine/ticks/"
        assert cfg.watermark_delay_seconds == 30.0
        assert cfg.trigger_interval_seconds == 5.0

    def test_spark_conf_and_kafka_options_export(self) -> None:
        cfg = DataprocStreamingConfig(
            environment="staging",
            kafka_bootstrap_servers="10.20.30.40:9092",
            kafka_subscribe_topics="market.ticks.v1",
            max_offsets_per_trigger=25000,
            shuffle_partitions=24,
        )
        spark_conf = cfg.to_spark_conf_dict()
        assert spark_conf["spark.app.name"] == "trading-risk-tick-streaming-staging"
        assert spark_conf["spark.sql.shuffle.partitions"] == "24"
        assert spark_conf["spark.sql.adaptive.enabled"] == "true"
        assert spark_conf["spark.streaming.backpressure.enabled"] == "true"
        assert spark_conf["spark.sql.session.timeZone"] == "UTC"
        assert "spark-sql-kafka-0-10" in spark_conf["spark.jars.packages"]

        kafka_opts = cfg.to_kafka_read_options()
        assert kafka_opts["kafka.bootstrap.servers"] == "10.20.30.40:9092"
        assert kafka_opts["subscribe"] == "market.ticks.v1"
        assert kafka_opts["startingOffsets"] == "latest"
        assert kafka_opts["maxOffsetsPerTrigger"] == "25000"
        assert kafka_opts["failOnDataLoss"] == "false"
        assert kafka_opts["kafka.group.id"] == "cg-dataproc-tick-streaming-staging"

    def test_starting_offsets_json_partition_map_accepted(self) -> None:
        offset_json = json.dumps({"market.ticks.v1": {"0": 15, "1": 42}})
        cfg = DataprocStreamingConfig(starting_offsets=offset_json)
        cfg.validate()

    @pytest.mark.parametrize(
        "kwargs,match_str",
        [
            ({"environment": "sandbox"}, "Invalid environment"),
            ({"project_id": "  "}, "project_id"),
            ({"kafka_bootstrap_servers": ""}, "kafka_bootstrap_servers"),
            ({"kafka_subscribe_topics": ""}, "kafka_subscribe_topics"),
            ({"kafka_security_protocol": "KERBEROS"}, "kafka_security_protocol"),
            ({"starting_offsets": "invalid-offset-spec"}, "starting_offsets"),
            ({"max_offsets_per_trigger": 0}, "max_offsets_per_trigger"),
            ({"shuffle_partitions": -4}, "shuffle_partitions"),
            ({"output_mode": "overwrite"}, "output_mode"),
            ({"watermark_delay_threshold": "invalid"}, "Invalid Spark duration"),
        ],
    )
    def test_config_validation_rejects_invalid_parameters(
        self, kwargs: Dict[str, Any], match_str: str
    ) -> None:
        cfg = DataprocStreamingConfig(**kwargs)
        with pytest.raises(ValueError, match=match_str):
            cfg.validate()

    def test_from_env_and_from_cli_args(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TRADING_RISK_ENV", "staging")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "10.0.0.99:9092")
        monkeypatch.setenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "12345")
        env_cfg = DataprocStreamingConfig.from_env()
        env_cfg.validate()
        assert env_cfg.environment == "staging"
        assert env_cfg.kafka_bootstrap_servers == "10.0.0.99:9092"
        assert env_cfg.max_offsets_per_trigger == 12345

        cli_cfg = DataprocStreamingConfig.from_cli_args(
            [
                "--environment",
                "prod",
                "--kafka_bootstrap_servers",
                "10.1.1.10:9092",
                "--starting_offsets",
                "earliest",
                "--max_offsets_per_trigger",
                "10000",
            ]
        )
        assert cli_cfg.environment == "prod"
        assert cli_cfg.starting_offsets == "earliest"
        assert cli_cfg.max_offsets_per_trigger == 10000


# ==============================================================================
# 2. Schema & Microstructure Validation Tests
# ==============================================================================


class TestSparkTickSchemaAndValidator:
    """Tests for Spark SQL DDL schema definitions and record-level validation/enrichment."""

    def test_schema_ddl_and_struct_spec_consistency(self) -> None:
        assert MARKET_TICK_STRUCT_SPEC.to_ddl() == MARKET_TICK_SCHEMA_DDL
        expected_fields = [
            "trade_id",
            "ticker",
            "asset_class",
            "bid",
            "ask",
            "last_price",
            "volume",
            "spread",
            "mid_price",
            "timestamp",
            "sequence_number",
            "_corrupt_record",
        ]
        assert MARKET_TICK_STRUCT_SPEC.field_names == expected_fields
        schema_obj = get_pyspark_tick_schema()
        assert schema_obj is not None
        assert "key" in KAFKA_ENVELOPE_COLUMNS and "offset" in KAFKA_ENVELOPE_COLUMNS

    def test_parse_valid_tick_computes_microstructure_enrichments(self) -> None:
        raw_payload = json.dumps(
            {
                "trade_id": "trd-0001",
                "ticker": "aapl",
                "asset_class": "equity",
                "bid": 199.90,
                "ask": 200.10,
                "last_price": 200.00,
                "volume": 50.0,
                "timestamp": "2026-10-21T14:35:10+00:00",
                "sequence_number": 77,
            }
        ).encode("utf-8")

        result = parse_and_validate_kafka_record(
            raw_value=raw_payload,
            kafka_key=b"AAPL",
            kafka_topic="market.ticks.v1",
            kafka_partition=2,
            kafka_offset=104,
        )
        assert isinstance(result, ParsedTickRecord)
        assert result.trade_id == "trd-0001"
        assert result.ticker == "AAPL"
        assert result.asset_class == "EQUITY"
        assert result.spread == pytest.approx(0.20)
        assert result.mid_price == pytest.approx(200.00)
        assert result.notional_usd == pytest.approx(10000.00)
        # 0.20 / 200.00 * 10000 = 10.0 bps
        assert result.relative_spread_bps == pytest.approx(10.0)
        assert result.year == 2026
        assert result.month == 10
        assert result.day == 21
        assert result.hour == 14
        assert result.sequence_number == 77
        assert result.kafka_partition == 2
        assert result.kafka_offset == 104
        assert result.kafka_key == "AAPL"

    @pytest.mark.parametrize(
        "raw_value,expected_error_type",
        [
            (None, "EmptyPayloadError"),
            (b"\xff\xfe\xfa\xfb", "UnicodeDecodeError"),
            (b"{not-valid-json", "JSONDecodeError"),
            (b"[1, 2, 3]", "JSONDecodeError"),
            (
                json.dumps({"trade_id": "t1", "ticker": "AAPL"}).encode("utf-8"),
                "MissingFieldError",
            ),
            (
                json.dumps(
                    {
                        "trade_id": "t1",
                        "ticker": "AAPL",
                        "asset_class": "REAL_ESTATE",
                        "bid": 100.0,
                        "ask": 101.0,
                        "last_price": 100.5,
                        "volume": 10.0,
                        "timestamp": "2026-10-21T10:00:00Z",
                    }
                ).encode("utf-8"),
                "ValidationError",
            ),
            (
                json.dumps(
                    {
                        "trade_id": "t1",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": -100.0,
                        "ask": 101.0,
                        "last_price": 100.5,
                        "volume": 10.0,
                        "timestamp": "2026-10-21T10:00:00Z",
                    }
                ).encode("utf-8"),
                "ValidationError",
            ),
            (
                json.dumps(
                    {
                        "trade_id": "t1",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 105.0,
                        "ask": 101.0,
                        "last_price": 103.0,
                        "volume": 10.0,
                        "timestamp": "2026-10-21T10:00:00Z",
                    }
                ).encode("utf-8"),
                "CrossedQuoteError",
            ),
            (
                json.dumps(
                    {
                        "trade_id": "t1",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 100.0,
                        "ask": 101.0,
                        "last_price": 100.5,
                        "volume": 10.0,
                        "timestamp": "not-a-timestamp",
                    }
                ).encode("utf-8"),
                "ValidationError",
            ),
        ],
    )
    def test_parse_corrupt_or_invalid_records_quarantined(
        self, raw_value: Any, expected_error_type: str
    ) -> None:
        res = parse_and_validate_kafka_record(
            raw_value=raw_value,
            kafka_key="BAD_KEY",
            kafka_topic="market.ticks.v1",
            kafka_partition=1,
            kafka_offset=99,
        )
        assert isinstance(res, CorruptTickRecord)
        assert res.error_type == expected_error_type
        assert res.kafka_partition == 1
        assert res.kafka_offset == 99
        assert res.kafka_key == "BAD_KEY"


# ==============================================================================
# 3. PySpark Session & Structured Streaming Plan Builder Tests
# ==============================================================================


class TestPySparkStreamBuilders:
    """Tests for SparkSession, readStream, DataFrame transformation, and writeStream builders."""

    def test_build_spark_session_applies_all_configs(self) -> None:
        cfg = DataprocStreamingConfig(environment="prod", shuffle_partitions=32)
        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_session = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session

        session = PySparkKafkaTickConsumer.build_spark_session(cfg, spark_builder=mock_builder)
        assert session is mock_session
        mock_builder.appName.assert_called_once_with("trading-risk-tick-streaming-prod")
        assert mock_builder.config.call_count == len(cfg.to_spark_conf_dict())

    def test_build_kafka_read_stream_and_writers_with_mock_spark(self) -> None:
        cfg = DataprocStreamingConfig(
            environment="dev",
            kafka_bootstrap_servers="10.10.10.50:9092",
            kafka_subscribe_topics="market.ticks.v1",
        )
        mock_spark = MagicMock()
        mock_stream_reader = MagicMock()
        mock_spark.readStream.format.return_value = mock_stream_reader
        mock_stream_reader.options.return_value = mock_stream_reader
        mock_raw_df = MagicMock()
        mock_stream_reader.load.return_value = mock_raw_df

        df = PySparkKafkaTickConsumer.build_kafka_read_stream(mock_spark, cfg)
        assert df is mock_raw_df
        mock_spark.readStream.format.assert_called_once_with("kafka")
        mock_stream_reader.options.assert_called_once_with(**cfg.to_kafka_read_options())

        # Test transformation fallback & writer setup
        valid_df, corrupt_df = PySparkKafkaTickConsumer.build_parsed_tick_streams(mock_raw_df, cfg)
        assert valid_df is not None
        assert corrupt_df is not None

        bronze_q, quarantine_q = PySparkKafkaTickConsumer.start_Bronze_and_quarantine_writers(
            valid_df, corrupt_df, cfg
        )
        assert bronze_q is not None
        assert quarantine_q is not None


# ==============================================================================
# 4. Stateful Micro-Batch Consumer, Rate Limiting, Watermark & Checkpoint Tests
# ==============================================================================


class TestStatefulMicroBatchExecution:
    """Tests for end-to-end micro-batch execution against LocalKafkaProducerMock."""

    def test_end_to_end_consumption_from_market_feed_generator(self, tmp_path: Path) -> None:
        checkpoint_dir = str(tmp_path / "chk_e2e")
        cfg = DataprocStreamingConfig(
            starting_offsets="earliest",
            max_offsets_per_trigger=25,
            checkpoint_location=checkpoint_dir,
        )

        mock_broker = LocalKafkaProducerMock(num_partitions_map={"market.ticks.v1": 4})
        producer_svc = KafkaTickProducer(
            config=ProducerConfig(topic="market.ticks.v1", simulation_mode=True),
            mock_producer=mock_broker,
        )

        gen = MarketFeedGenerator(seed=101)
        for _ in range(60):
            producer_svc.send_tick(gen.simulate_tick())
        assert mock_broker.get_total_records("market.ticks.v1") == 60

        consumer = PySparkKafkaTickConsumer(config=cfg)
        batches = consumer.drain_producer(mock_broker)

        # 60 ticks with max_offsets_per_trigger=25 -> 3 micro-batches (25 + 25 + 10)
        assert len(batches) == 3
        assert [b.records_polled for b in batches] == [25, 25, 10]
        assert consumer.telemetry.batches_processed == 3
        assert consumer.telemetry.records_received == 60
        assert consumer.telemetry.valid_records_emitted == 60
        assert consumer.telemetry.corrupt_records_quarantined == 0
        assert len(consumer.bronze_sink_records) == 60

    def test_starting_offsets_latest_only_consumes_newly_arriving_ticks(self) -> None:
        cfg = DataprocStreamingConfig(
            starting_offsets="latest",
            max_offsets_per_trigger=100,
            checkpoint_location="gs://non-local-bucket/chk",
        )
        mock_broker = LocalKafkaProducerMock(num_partitions_map={"market.ticks.v1": 2})
        producer_svc = KafkaTickProducer(
            config=ProducerConfig(topic="market.ticks.v1", simulation_mode=True),
            mock_producer=mock_broker,
        )
        gen = MarketFeedGenerator(seed=7)

        # Publish 20 historical ticks before consumer initializes
        for _ in range(20):
            producer_svc.send_tick(gen.simulate_tick())

        consumer = PySparkKafkaTickConsumer(config=cfg)
        consumer.initialize_offsets_from_producer(mock_broker)

        # Polling immediately should yield 0 records
        empty_batch = consumer.poll_from_producer(mock_broker)
        assert empty_batch.records_polled == 0

        # Publish 15 new real-time ticks
        for _ in range(15):
            producer_svc.send_tick(gen.simulate_tick())
        live_batch = consumer.poll_from_producer(mock_broker)
        assert live_batch.records_polled == 15
        assert len(live_batch.valid_records) == 15

    def test_checkpoint_persistence_and_fault_tolerant_restart_recovery(
        self, tmp_path: Path
    ) -> None:
        checkpoint_dir = str(tmp_path / "chk_recovery")
        cfg = DataprocStreamingConfig(
            starting_offsets="earliest",
            max_offsets_per_trigger=15,
            checkpoint_location=checkpoint_dir,
        )

        mock_broker = LocalKafkaProducerMock(num_partitions_map={"market.ticks.v1": 2})
        producer_svc = KafkaTickProducer(
            config=ProducerConfig(topic="market.ticks.v1", simulation_mode=True),
            mock_producer=mock_broker,
        )
        gen = MarketFeedGenerator(seed=99)
        for _ in range(30):
            producer_svc.send_tick(gen.simulate_tick())

        # First consumer instance processes only the first micro-batch (15 records) then "crashes"
        consumer_1 = PySparkKafkaTickConsumer(config=cfg)
        b1 = consumer_1.poll_from_producer(mock_broker)
        assert b1.records_polled == 15
        assert b1.batch_id == 0
        assert (Path(checkpoint_dir) / "offsets.json").exists()

        # Second consumer instance recovers from checkpoint_dir and processes the remaining 15 records
        consumer_2 = PySparkKafkaTickConsumer(config=cfg)
        assert consumer_2.telemetry.records_received == 15
        b2 = consumer_2.poll_from_producer(mock_broker)
        assert b2.batch_id == 1
        assert b2.records_polled == 15
        assert consumer_2.telemetry.records_received == 30
        assert consumer_2.telemetry.valid_records_emitted == 30

        # Ensure no duplicate trade_ids across the two runs
        ids_1 = {r.trade_id for r in b1.valid_records}
        ids_2 = {r.trade_id for r in b2.valid_records}
        assert len(ids_1.intersection(ids_2)) == 0
        assert len(ids_1.union(ids_2)) == 30

    def test_watermark_advancement_and_late_event_dropping(self) -> None:
        cfg = DataprocStreamingConfig(
            starting_offsets="earliest",
            watermark_delay_threshold="30 seconds",
            checkpoint_location="gs://bucket/chk",
        )
        consumer = PySparkKafkaTickConsumer(config=cfg)

        # Batch 1: Event at 12:01:00 UTC -> Watermark advances to 12:00:30 UTC
        batch_1_msgs = [
            {
                "key": b"AAPL",
                "value": json.dumps(
                    {
                        "trade_id": "t-on-time-1",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 150.0,
                        "ask": 150.1,
                        "last_price": 150.05,
                        "volume": 100.0,
                        "timestamp": "2026-10-21T12:01:00+00:00",
                    }
                ).encode("utf-8"),
            }
        ]
        res1 = consumer.process_raw_messages(batch_1_msgs)
        assert len(res1.valid_records) == 1
        assert len(res1.dropped_late_records) == 0
        assert res1.watermark_iso == "2026-10-21T12:00:30+00:00"

        # Batch 2:
        # - Tick A at 12:00:45 UTC (within watermark >= 12:00:30) -> Accepted
        # - Tick B at 12:00:10 UTC (older than watermark < 12:00:30) -> Dropped as late!
        # - Corrupt Tick C -> Quarantined to DLQ
        batch_2_msgs = [
            {
                "key": b"AAPL",
                "value": json.dumps(
                    {
                        "trade_id": "t-within-watermark",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 150.2,
                        "ask": 150.3,
                        "last_price": 150.25,
                        "volume": 80.0,
                        "timestamp": "2026-10-21T12:00:45+00:00",
                    }
                ).encode("utf-8"),
            },
            {
                "key": b"AAPL",
                "value": json.dumps(
                    {
                        "trade_id": "t-too-late",
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 149.8,
                        "ask": 149.9,
                        "last_price": 149.85,
                        "volume": 50.0,
                        "timestamp": "2026-10-21T12:00:10+00:00",
                    }
                ).encode("utf-8"),
            },
            {
                "key": b"CORRUPT",
                "value": b"{broken-json-payload",
            },
        ]
        res2 = consumer.process_raw_messages(batch_2_msgs)
        assert len(res2.valid_records) == 1
        assert res2.valid_records[0].trade_id == "t-within-watermark"
        assert len(res2.dropped_late_records) == 1
        assert res2.dropped_late_records[0].trade_id == "t-too-late"
        assert len(res2.corrupt_records) == 1
        assert res2.corrupt_records[0].error_type == "JSONDecodeError"

        # Verify Prometheus exposition output
        prom_text = consumer.telemetry.format_prometheus()
        assert 'dataproc_streaming_batches_processed_total{app="trading-risk-tick-streaming"} 2' in prom_text
        assert 'dataproc_streaming_valid_records_emitted_total{app="trading-risk-tick-streaming"} 2' in prom_text
        assert 'dataproc_streaming_late_records_dropped_total{app="trading-risk-tick-streaming"} 1' in prom_text
        assert 'dataproc_streaming_corrupt_records_quarantined_total{app="trading-risk-tick-streaming"} 1' in prom_text


# ==============================================================================
# 5. CLI Runner & Deployment Script Tests
# ==============================================================================


class TestCLIRunnerAndDeploymentScript:
    """Tests for `run_tick_consumer.py` CLI modes and `submit_job.sh` script."""

    def test_cli_dry_run_and_local_simulation_modes(self) -> None:
        exit_dry = cli_main(["--dry_run", "--environment", "dev"])
        assert exit_dry == 0

        exit_sim = cli_main(
            [
                "--local_simulation",
                "--simulation_ticks",
                "40",
                "--environment",
                "dev",
                "--max_offsets_per_trigger",
                "20",
            ]
        )
        assert exit_sim == 0

        sim_summary = run_local_simulation(
            DataprocStreamingConfig(max_offsets_per_trigger=25),
            num_ticks=50,
            seed=123,
        )
        assert sim_summary["batches_executed"] == 2
        assert sim_summary["telemetry"]["valid_records_emitted"] == 50

    def test_submit_job_script_exists_and_contains_dataproc_command(self) -> None:
        script_path = Path("streaming/dataproc/submit_job.sh")
        assert script_path.exists()
        content = script_path.read_text(encoding="utf-8")
        assert "gcloud dataproc jobs submit pyspark" in content
        assert "streaming/dataproc/run_tick_consumer.py" in content
        assert "org.apache.spark:spark-sql-kafka-0-10" in content
