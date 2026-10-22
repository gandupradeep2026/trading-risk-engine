"""Zero-Cost Hermetic Test Suite for Day 14: PySpark Streaming Partitioned Snappy Parquet GCS Sink.

Verifies:
1. `ParquetSinkConfig` validation, invariants, and factory builders for Bronze ticks and rolling VWAP.
2. SparkConf direct GCS committer and Snappy compression settings in `DataprocStreamingConfig`.
3. PyArrow schema alignment with Spark SQL DDL schemas for both market ticks and rolling VWAP.
4. Parquet inspection utilities (`verify_snappy_parquet_file`, `extract_partition_keys_from_path`,
   `resolve_storage_path`, `read_partitioned_parquet_dataset`).
5. Hermetic local partitioned Snappy Parquet writing with `LocalParquetDatasetWriter`:
   - True Snappy compression verified on every column of every row group in every Parquet file.
   - Hive-style directory hierarchy partitioning (`year=.../month=.../day=.../asset_class=...`).
   - Round-trip reading and numerical precision verification via PyArrow.
6. Rolling VWAP partitioned Snappy Parquet generation with financial indicators and breach flags.
7. `ParquetSinkTelemetry` tracking written records, files, uncompressed vs compressed bytes,
   compression ratio (> 1.0), and Prometheus text exposition formatting.
8. PySpark Structured Streaming graph builders (`start_partitioned_parquet_sink`,
   `start_parquet_tick_sink`, `start_parquet_vwap_sink`, `start_quarantine_json_sink`) using mock streams.
9. End-to-end integration: `MarketFeedGenerator` -> `KafkaTickProducer` -> `LocalKafkaProducerMock` ->
   `PySparkKafkaTickConsumer` -> `LocalParquetDatasetWriter` writing partitioned Snappy Parquet to disk.
10. CLI runner (`run_tick_consumer.py`) `--dry_run` and `--local_simulation` with `--parquet_output_dir`.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.kafka.producer import KafkaTickProducer, LocalKafkaProducerMock, ProducerConfig
from ingestion.market_feed import MarketFeedGenerator
from streaming.dataproc import (
    ALLOWED_PARQUET_CODECS,
    DEFAULT_PARQUET_TICK_PARTITIONS,
    DEFAULT_PARQUET_VWAP_PARTITIONS,
    MARKET_TICK_STRUCT_SPEC,
    ROLLING_VWAP_STRUCT_SPEC,
    TICK_PARQUET_ARROW_SCHEMA,
    VWAP_PARQUET_ARROW_SCHEMA,
    DataprocStreamingConfig,
    LocalParquetDatasetWriter,
    ParsedTickRecord,
    ParquetSinkConfig,
    ParquetSinkTelemetry,
    PySparkKafkaTickConsumer,
    RollingVWAPStats,
    extract_partition_keys_from_path,
    read_partitioned_parquet_dataset,
    resolve_storage_path,
    start_parquet_tick_sink,
    start_parquet_vwap_sink,
    start_partitioned_parquet_sink,
    start_quarantine_json_sink,
    verify_snappy_parquet_file,
)
from streaming.dataproc.run_tick_consumer import main as run_consumer_main, run_local_simulation


# =============================================================================
# 1. Parquet Configuration & Schema Invariants
# =============================================================================


class TestParquetSinkConfiguration:
    """Tests for ParquetSinkConfig invariants, validation, and DataprocStreamingConfig exports."""

    def test_parquet_sink_config_defaults_and_validation(self) -> None:
        """Verify ParquetSinkConfig defaults and validation rules."""
        cfg = ParquetSinkConfig(
            output_path="gs://trading-risk-bronze-dev/ticks/",
            checkpoint_location="gs://trading-risk-dataflow-staging-dev/checkpoints/parquet/",
        )
        cfg.validate()
        assert cfg.compression_codec == "snappy"
        assert cfg.max_records_per_file == 50000
        assert cfg.partition_columns == ["year", "month", "day", "asset_class"]
        assert cfg.output_mode == "append"

    @pytest.mark.parametrize(
        "invalid_kwargs,expected_match",
        [
            ({"output_path": ""}, "output_path"),
            ({"checkpoint_location": "   "}, "checkpoint_location"),
            ({"compression_codec": "rar"}, "Invalid compression_codec"),
            ({"max_records_per_file": 0}, "max_records_per_file"),
            ({"max_records_per_file": -100}, "max_records_per_file"),
            ({"partition_columns": []}, "partition_columns"),
            ({"output_mode": "invalid_mode"}, "Unsupported output_mode"),
        ],
    )
    def test_parquet_sink_config_validation_rejections(
        self, invalid_kwargs: Dict[str, Any], expected_match: str
    ) -> None:
        """Verify invalid ParquetSinkConfig parameters raise ValueError with descriptive message."""
        base_kwargs = {
            "output_path": "gs://bucket/out/",
            "checkpoint_location": "gs://bucket/chk/",
        }
        base_kwargs.update(invalid_kwargs)
        cfg = ParquetSinkConfig(**base_kwargs)
        with pytest.raises(ValueError, match=expected_match):
            cfg.validate()

    def test_from_streaming_config_for_ticks(self) -> None:
        """Verify factory method creates valid tick sink config matching environment."""
        streaming_cfg = DataprocStreamingConfig(environment="prod", project_id="trading-risk-prod")
        streaming_cfg.validate()

        sink_cfg = ParquetSinkConfig.from_streaming_config_for_ticks(streaming_cfg)
        assert sink_cfg.output_path == "gs://trading-risk-bronze-prod/ticks/"
        assert "bronze_parquet" in sink_cfg.checkpoint_location
        assert sink_cfg.compression_codec == "snappy"
        assert sink_cfg.partition_columns == ["year", "month", "day", "asset_class"]
        assert sink_cfg.query_name == "trading-risk-tick-streaming-prod-bronze-parquet-sink"

    def test_from_streaming_config_for_vwap(self) -> None:
        """Verify factory method creates valid rolling VWAP sink config."""
        streaming_cfg = DataprocStreamingConfig(environment="staging", project_id="trading-risk-staging")
        streaming_cfg.validate()

        sink_cfg = ParquetSinkConfig.from_streaming_config_for_vwap(streaming_cfg)
        assert sink_cfg.output_path == "gs://trading-risk-bronze-staging/vwap/"
        assert "vwap" in sink_cfg.checkpoint_location
        assert sink_cfg.compression_codec == "snappy"
        assert sink_cfg.partition_columns == ["year", "month", "day", "asset_class"]
        assert sink_cfg.query_name == "trading-risk-tick-streaming-staging-vwap-parquet-sink"

    def test_spark_conf_gcs_connector_and_snappy_properties(self) -> None:
        """Verify SparkConf dictionary contains Snappy compression, GCS committer, and GCS filesystem."""
        cfg = DataprocStreamingConfig(
            environment="dev",
            parquet_compression_codec="snappy",
            parquet_max_records_per_file=75000,
        )
        spark_conf = cfg.to_spark_conf_dict()

        assert spark_conf["spark.sql.parquet.compression.codec"] == "snappy"
        assert spark_conf["spark.hadoop.fs.gs.impl"] == "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        assert spark_conf["spark.hadoop.fs.AbstractFileSystem.gs.impl"] == "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        assert spark_conf["spark.hadoop.fs.gs.project.id"] == "trading-risk-engine-dev"
        assert spark_conf["spark.sql.streaming.fileSink.log.compactInterval"] == "10"
        assert spark_conf["spark.sql.parquet.writeLegacyFormat"] == "false"
        assert spark_conf["spark.sql.sources.partitionOverwriteMode"] == "dynamic"
        assert "BindingParquetOutputCommitter" in spark_conf["spark.sql.parquet.output.committer.class"]

    def test_arrow_schemas_match_spark_sql_specs(self) -> None:
        """Verify Arrow schema field names cover Spark SQL schema fields for ticks and VWAP."""
        tick_arrow_names = set(TICK_PARQUET_ARROW_SCHEMA.names)
        for field_name in MARKET_TICK_STRUCT_SPEC.field_names:
            if field_name != "_corrupt_record":
                assert field_name in tick_arrow_names

        vwap_arrow_names = set(VWAP_PARQUET_ARROW_SCHEMA.names)
        for field_name in ROLLING_VWAP_STRUCT_SPEC.field_names:
            assert field_name in vwap_arrow_names


# =============================================================================
# 2. Path & Parquet Verification Utilities Tests
# =============================================================================


class TestParquetVerificationUtilities:
    """Tests for path extraction, GCS resolution, and Snappy Parquet metadata verification."""

    def test_extract_partition_keys_from_path(self) -> None:
        """Verify extraction of Hive-style partition key-values across different path formats."""
        path1 = "gs://trading-risk-bronze-dev/ticks/year=2026/month=10/day=22/asset_class=EQUITY/part-0.parquet"
        tokens1 = extract_partition_keys_from_path(path1)
        assert tokens1 == {
            "year": "2026",
            "month": "10",
            "day": "22",
            "asset_class": "EQUITY",
        }

        path2 = "C:\\scratch\\ticks\\year=2026\\month=11\\day=05\\hour=14\\asset_class=CRYPTO\\chunk.parquet"
        tokens2 = extract_partition_keys_from_path(path2)
        assert tokens2 == {
            "year": "2026",
            "month": "11",
            "day": "05",
            "hour": "14",
            "asset_class": "CRYPTO",
        }

    def test_resolve_storage_path_gcs_and_local(self, tmp_path: Path) -> None:
        """Verify resolve_storage_path maps gs:// URIs into local scratch directories."""
        gcs_uri = "gs://trading-risk-bronze-dev/ticks/subfolder/"
        resolved_gcs = resolve_storage_path(gcs_uri, local_scratch_dir=tmp_path)
        assert resolved_gcs.exists()
        assert str(tmp_path) in str(resolved_gcs)
        assert "trading-risk-bronze-dev" in str(resolved_gcs)

        local_path = tmp_path / "local_output"
        resolved_local = resolve_storage_path(str(local_path))
        assert resolved_local.exists()
        assert resolved_local == local_path

        with pytest.raises(ValueError):
            resolve_storage_path("")

    def test_verify_snappy_parquet_file_nonexistent_raises(self, tmp_path: Path) -> None:
        """Verify verify_snappy_parquet_file raises FileNotFoundError for missing files."""
        missing = tmp_path / "does_not_exist.parquet"
        with pytest.raises(FileNotFoundError):
            verify_snappy_parquet_file(missing)

    def test_verify_snappy_parquet_file_rejects_non_snappy(self, tmp_path: Path) -> None:
        """Verify verify_snappy_parquet_file raises ValueError if file is not SNAPPY-compressed."""
        table = pa.Table.from_pydict({"price": [100.0, 101.5], "ticker": ["AAPL", "MSFT"]})
        non_snappy_file = tmp_path / "test_gzip.parquet"
        pq.write_table(table, non_snappy_file, compression="GZIP")

        with pytest.raises(ValueError, match="compressed with GZIP, expected SNAPPY"):
            verify_snappy_parquet_file(non_snappy_file)


# =============================================================================
# 3. Local Partitioned Snappy Parquet Dataset Writer Tests
# =============================================================================


class TestLocalParquetDatasetWriter:
    """Tests for LocalParquetDatasetWriter generating partitioned Snappy Parquet datasets."""

    def _create_sample_parsed_ticks(self) -> List[ParsedTickRecord]:
        """Create a deterministic collection of enriched ParsedTickRecord fixtures."""
        return [
            ParsedTickRecord(
                trade_id="TICK-001",
                ticker="AAPL",
                asset_class="EQUITY",
                bid=150.10,
                ask=150.15,
                last_price=150.12,
                volume=100.0,
                spread=0.05,
                mid_price=150.125,
                notional_usd=15012.0,
                relative_spread_bps=3.33,
                event_timestamp="2026-10-22T10:00:00+00:00",
                event_epoch_sec=1792663200.0,
                sequence_number=1,
                year=2026,
                month=10,
                day=22,
                hour=10,
                kafka_topic="market.ticks.v1",
                kafka_partition=0,
                kafka_offset=100,
                kafka_key="EQUITY#AAPL",
                ingested_at="2026-10-22T10:00:01+00:00",
            ),
            ParsedTickRecord(
                trade_id="TICK-002",
                ticker="MSFT",
                asset_class="EQUITY",
                bid=380.50,
                ask=380.60,
                last_price=380.55,
                volume=50.0,
                spread=0.10,
                mid_price=380.55,
                notional_usd=19027.5,
                relative_spread_bps=2.63,
                event_timestamp="2026-10-22T10:00:05+00:00",
                event_epoch_sec=1792663205.0,
                sequence_number=2,
                year=2026,
                month=10,
                day=22,
                hour=10,
                kafka_topic="market.ticks.v1",
                kafka_partition=1,
                kafka_offset=101,
                kafka_key="EQUITY#MSFT",
                ingested_at="2026-10-22T10:00:06+00:00",
            ),
            ParsedTickRecord(
                trade_id="TICK-003",
                ticker="BTCUSD",
                asset_class="CRYPTO",
                bid=68500.0,
                ask=68510.0,
                last_price=68505.0,
                volume=0.5,
                spread=10.0,
                mid_price=68505.0,
                notional_usd=34252.5,
                relative_spread_bps=1.46,
                event_timestamp="2026-10-22T11:00:00+00:00",
                event_epoch_sec=1792666800.0,
                sequence_number=3,
                year=2026,
                month=10,
                day=22,
                hour=11,
                kafka_topic="market.ticks.v1",
                kafka_partition=2,
                kafka_offset=102,
                kafka_key="CRYPTO#BTCUSD",
                ingested_at="2026-10-22T11:00:01+00:00",
            ),
            ParsedTickRecord(
                trade_id="TICK-004",
                ticker="EURUSD",
                asset_class="FX",
                bid=1.0850,
                ask=1.0852,
                last_price=1.0851,
                volume=100000.0,
                spread=0.0002,
                mid_price=1.0851,
                notional_usd=108510.0,
                relative_spread_bps=1.84,
                event_timestamp="2026-10-22T11:05:00+00:00",
                event_epoch_sec=1792667100.0,
                sequence_number=4,
                year=2026,
                month=10,
                day=22,
                hour=11,
                kafka_topic="market.ticks.v1",
                kafka_partition=3,
                kafka_offset=103,
                kafka_key="FX#EURUSD",
                ingested_at="2026-10-22T11:05:01+00:00",
            ),
        ]

    def test_write_ticks_partitioned_snappy_parquet(self, tmp_path: Path) -> None:
        """Verify writing ticks creates Hive partition directories and Snappy Parquet files."""
        writer = LocalParquetDatasetWriter(scratch_dir=tmp_path)
        ticks = self._create_sample_parsed_ticks()
        out_dir = tmp_path / "bronze_ticks"

        res = writer.write_ticks(records=ticks, output_dir=out_dir)

        assert res["records_written"] == 4
        assert res["files_written"] >= 3  # At least 3 distinct partitions (EQUITY, CRYPTO, FX)
        assert res["compressed_bytes"] > 0
        assert res["raw_bytes"] > 0
        assert res["compression_ratio"] > 0.0
        assert len(res["file_paths"]) >= 3

        # Verify every generated file is valid and strictly SNAPPY-compressed
        for file_path in res["file_paths"]:
            meta = verify_snappy_parquet_file(file_path)
            assert meta["compression"] == "SNAPPY"
            assert meta["num_rows"] > 0
            assert "trade_id" in meta["columns"]

        # Read the dataset back using read_partitioned_parquet_dataset
        table = read_partitioned_parquet_dataset(out_dir)
        assert table.num_rows == 4
        assert "year" in table.column_names
        assert "asset_class" in table.column_names

        # Verify specific tickers and values round-trip accurately
        tickers = table.column("ticker").to_pylist()
        assert set(tickers) == {"AAPL", "MSFT", "BTCUSD", "EURUSD"}
        notionals = sorted(table.column("notional_usd").to_pylist())
        assert pytest.approx(notionals[0]) == 15012.0

    def test_write_ticks_empty_list_returns_zero_stats(self, tmp_path: Path) -> None:
        """Verify writing an empty list of records returns cleanly with 0 counts."""
        writer = LocalParquetDatasetWriter(scratch_dir=tmp_path)
        res = writer.write_ticks(records=[], output_dir=tmp_path / "empty")
        assert res["records_written"] == 0
        assert res["files_written"] == 0
        assert res["compressed_bytes"] == 0
        assert res["compression_ratio"] == 1.0

    def test_write_ticks_custom_partitioning_columns(self, tmp_path: Path) -> None:
        """Verify custom partition columns (e.g. including hour) create 5-level directory trees."""
        writer = LocalParquetDatasetWriter(scratch_dir=tmp_path)
        ticks = self._create_sample_parsed_ticks()
        out_dir = tmp_path / "custom_part"

        custom_cols = ["year", "month", "day", "hour", "asset_class"]
        res = writer.write_ticks(records=ticks, output_dir=out_dir, partition_cols=custom_cols)

        assert res["records_written"] == 4
        assert any("hour=" in p for p in res["partitions"])

        table = read_partitioned_parquet_dataset(out_dir)
        assert table.num_rows == 4
        assert "hour" in table.column_names

    def test_write_vwap_stats_partitioned_snappy_parquet(self, tmp_path: Path) -> None:
        """Verify writing rolling VWAP summaries into partitioned Snappy Parquet."""
        writer = LocalParquetDatasetWriter(scratch_dir=tmp_path)
        stats = [
            RollingVWAPStats(
                ticker="AAPL",
                asset_class="EQUITY",
                window_start="2026-10-22T10:00:00+00:00",
                window_end="2026-10-22T10:05:00+00:00",
                window_start_epoch_sec=1792663200.0,
                window_end_epoch_sec=1792663500.0,
                tick_count=15,
                open_price=150.0,
                high_price=152.0,
                low_price=149.5,
                close_price=151.8,
                total_volume=1500.0,
                total_notional=226500.0,
                vwap=151.0,
                price_stddev=0.75,
                price_variance=0.5625,
                volatility_multiplier=2.0,
                upper_band=152.5,
                lower_band=149.5,
                bandwidth_bps=198.68,
                percent_b=0.7667,
                is_upper_breach=False,
                is_lower_breach=False,
                volatility_spike=False,
                year=2026,
                month=10,
                day=22,
                hour=10,
                computed_at="2026-10-22T10:05:01+00:00",
            ),
            RollingVWAPStats(
                ticker="BTCUSD",
                asset_class="CRYPTO",
                window_start="2026-10-22T10:00:00+00:00",
                window_end="2026-10-22T10:05:00+00:00",
                window_start_epoch_sec=1792663200.0,
                window_end_epoch_sec=1792663500.0,
                tick_count=8,
                open_price=68000.0,
                high_price=69500.0,
                low_price=67800.0,
                close_price=69400.0,
                total_volume=4.0,
                total_notional=275200.0,
                vwap=68800.0,
                price_stddev=600.0,
                price_variance=360000.0,
                volatility_multiplier=2.0,
                upper_band=70000.0,
                lower_band=67600.0,
                bandwidth_bps=348.84,
                percent_b=0.75,
                is_upper_breach=False,
                is_lower_breach=False,
                volatility_spike=False,
                year=2026,
                month=10,
                day=22,
                hour=10,
                computed_at="2026-10-22T10:05:01+00:00",
            ),
        ]

        out_dir = tmp_path / "vwap_parquet"
        res = writer.write_vwap_stats(records=stats, output_dir=out_dir)

        assert res["records_written"] == 2
        assert res["files_written"] == 2  # 1 for EQUITY, 1 for CRYPTO
        assert res["compressed_bytes"] > 0

        # Verify Snappy compression on every file
        for f in res["file_paths"]:
            meta = verify_snappy_parquet_file(f)
            assert meta["compression"] == "SNAPPY"
            assert "vwap" in meta["columns"]
            assert "upper_band" in meta["columns"]

        table = read_partitioned_parquet_dataset(out_dir)
        assert table.num_rows == 2
        tickers = table.column("ticker").to_pylist()
        assert set(tickers) == {"AAPL", "BTCUSD"}
        vwap_vals = table.column("vwap").to_pylist()
        assert 151.0 in vwap_vals
        assert 68800.0 in vwap_vals


# =============================================================================
# 4. Parquet Sink Telemetry & Prometheus Format Tests
# =============================================================================


class TestParquetSinkTelemetry:
    """Tests for ParquetSinkTelemetry metrics aggregation and Prometheus formatting."""

    def test_telemetry_batch_recording_and_derived_metrics(self) -> None:
        """Verify telemetry counters, compression ratio calculation, and partition tracking."""
        telem = ParquetSinkTelemetry()
        assert telem.compression_ratio == 1.0
        assert telem.avg_records_per_file == 0.0

        telem.record_batch(
            num_records=1000,
            num_files=4,
            raw_bytes=200000,
            compressed_bytes=50000,
            partitions=["year=2026/asset_class=EQUITY", "year=2026/asset_class=FX"],
            latency_ms=12.5,
        )

        assert telem.records_written == 1000
        assert telem.files_written == 4
        assert telem.raw_bytes_uncompressed == 200000
        assert telem.compressed_bytes == 50000
        assert telem.compression_ratio == pytest.approx(4.0)
        assert telem.avg_records_per_file == pytest.approx(250.0)
        assert len(telem.partitions_created) == 2
        assert telem.batches_committed == 1
        assert telem.last_commit_latency_ms == 12.5

    def test_telemetry_prometheus_text_format(self) -> None:
        """Verify format_prometheus produces valid Prometheus exposition text."""
        telem = ParquetSinkTelemetry()
        telem.record_batch(
            num_records=500,
            num_files=2,
            raw_bytes=100000,
            compressed_bytes=25000,
            partitions=["year=2026/asset_class=EQUITY"],
            latency_ms=8.0,
        )

        prom_text = telem.format_prometheus(
            app_name="trading-risk-prod", sink_name="bronze_parquet"
        )
        assert 'dataproc_parquet_sink_records_written_total{app="trading-risk-prod",sink="bronze_parquet"} 500' in prom_text
        assert 'dataproc_parquet_sink_files_written_total{app="trading-risk-prod",sink="bronze_parquet"} 2' in prom_text
        assert 'dataproc_parquet_sink_compressed_bytes_total{app="trading-risk-prod",sink="bronze_parquet"} 25000' in prom_text
        assert 'dataproc_parquet_sink_compression_ratio{app="trading-risk-prod",sink="bronze_parquet"} 4.0' in prom_text
        assert 'dataproc_parquet_sink_active_partitions{app="trading-risk-prod",sink="bronze_parquet"} 1' in prom_text
        assert 'dataproc_parquet_sink_batches_committed_total{app="trading-risk-prod",sink="bronze_parquet"} 1' in prom_text


# =============================================================================
# 5. PySpark Structured Streaming Graph Builders Tests
# =============================================================================


class TestPySparkStreamingSinkBuilders:
    """Tests for PySpark streaming DataStreamWriter builders with mock DataFrames."""

    def test_start_partitioned_parquet_sink_invokes_correct_options(self) -> None:
        """Verify start_partitioned_parquet_sink sets format, path, checkpoint, compression, and partitions."""
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.writeStream = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.queryName.return_value = mock_writer
        mock_query = MagicMock()
        mock_writer.start.return_value = mock_query

        sink_cfg = ParquetSinkConfig(
            output_path="gs://trading-risk-bronze-dev/ticks/",
            checkpoint_location="gs://trading-risk-dataflow-staging-dev/checkpoints/parquet/",
            compression_codec="snappy",
            partition_columns=["year", "month", "day", "asset_class"],
            max_records_per_file=50000,
            trigger_processing_time="5 seconds",
            query_name="test-parquet-sink",
        )

        query = start_partitioned_parquet_sink(mock_df, sink_cfg)
        assert query is mock_query

        mock_writer.format.assert_called_once_with("parquet")
        mock_writer.outputMode.assert_called_once_with("append")
        mock_writer.partitionBy.assert_called_once_with("year", "month", "day", "asset_class")
        mock_writer.queryName.assert_called_once_with("test-parquet-sink")
        mock_writer.trigger.assert_called_once_with(processingTime="5 seconds")

        # Verify option calls
        option_calls = {call.args[0]: call.args[1] for call in mock_writer.option.call_args_list}
        assert option_calls["path"] == "gs://trading-risk-bronze-dev/ticks/"
        assert option_calls["checkpointLocation"] == "gs://trading-risk-dataflow-staging-dev/checkpoints/parquet/"
        assert option_calls["compression"] == "snappy"
        assert option_calls["maxRecordsPerFile"] == "50000"

    def test_start_parquet_tick_sink_and_quarantine_sink(self) -> None:
        """Verify start_parquet_tick_sink and start_quarantine_json_sink initialize queries."""
        cfg = DataprocStreamingConfig(environment="dev")
        mock_valid_df = MagicMock()
        mock_valid_writer = MagicMock()
        mock_valid_df.writeStream = mock_valid_writer
        mock_valid_writer.format.return_value = mock_valid_writer
        mock_valid_writer.outputMode.return_value = mock_valid_writer
        mock_valid_writer.option.return_value = mock_valid_writer
        mock_valid_writer.partitionBy.return_value = mock_valid_writer
        mock_valid_writer.trigger.return_value = mock_valid_writer
        mock_valid_writer.queryName.return_value = mock_valid_writer
        mock_valid_writer.start.return_value = "mock_tick_query"

        mock_corrupt_df = MagicMock()
        mock_corrupt_writer = MagicMock()
        mock_corrupt_df.writeStream = mock_corrupt_writer
        mock_corrupt_writer.format.return_value = mock_corrupt_writer
        mock_corrupt_writer.outputMode.return_value = mock_corrupt_writer
        mock_corrupt_writer.option.return_value = mock_corrupt_writer
        mock_corrupt_writer.trigger.return_value = mock_corrupt_writer
        mock_corrupt_writer.queryName.return_value = mock_corrupt_writer
        mock_corrupt_writer.start.return_value = "mock_quarantine_query"

        tick_query = start_parquet_tick_sink(mock_valid_df, cfg)
        assert tick_query == "mock_tick_query"

        quarantine_query = start_quarantine_json_sink(mock_corrupt_df, cfg)
        assert quarantine_query == "mock_quarantine_query"

    def test_start_parquet_vwap_sink(self) -> None:
        """Verify start_parquet_vwap_sink initializes rolling VWAP writer query."""
        cfg = DataprocStreamingConfig(environment="prod")
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.writeStream = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.queryName.return_value = mock_writer
        mock_writer.start.return_value = "mock_vwap_query"

        query = start_parquet_vwap_sink(mock_df, cfg)
        assert query == "mock_vwap_query"
        mock_writer.format.assert_called_once_with("parquet")


# =============================================================================
# 6. End-to-End Local Micro-Batch & Parquet File Generation Integration
# =============================================================================


class TestConsumerParquetIntegration:
    """Integration tests connecting MarketFeedGenerator -> LocalKafkaProducerMock -> PySparkKafkaTickConsumer -> Parquet."""

    def test_end_to_end_micro_batch_to_parquet_files(self, tmp_path: Path) -> None:
        """Verify end-to-end streaming generates partitioned Snappy Parquet files on disk."""
        cfg = DataprocStreamingConfig(
            environment="dev",
            starting_offsets="earliest",
            parquet_output_dir=str(tmp_path / "parquet_sink"),
        )
        topic = "market.ticks.v1"
        mock_broker = LocalKafkaProducerMock(num_partitions_map={topic: 4})
        producer_cfg = ProducerConfig(
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            topic=topic,
            deadletter_topic=cfg.kafka_dlq_topic,
            simulation_mode=True,
        )
        producer = KafkaTickProducer(config=producer_cfg, mock_producer=mock_broker)

        generator = MarketFeedGenerator(seed=101)
        for _ in range(50):
            producer.send_tick(generator.simulate_tick())

        consumer = PySparkKafkaTickConsumer(config=cfg)
        batches = consumer.drain_producer(mock_broker)

        assert len(batches) > 0
        assert consumer.telemetry.valid_records_emitted > 0

        # Files should have been written automatically because parquet_output_dir was set
        assert consumer.parquet_telemetry.records_written > 0
        assert consumer.parquet_telemetry.files_written > 0
        assert consumer.parquet_telemetry.compression_ratio > 0.0

        # Verify all created parquet files are Snappy compressed
        parquet_files = list(Path(cfg.parquet_output_dir).glob("**/*.parquet"))
        assert len(parquet_files) > 0
        for f in parquet_files:
            meta = verify_snappy_parquet_file(f)
            assert meta["compression"] == "SNAPPY"

        # Read back tick dataset with PyArrow and verify record count
        ticks_dir = Path(cfg.parquet_output_dir) / "ticks"
        ticks_dataset = read_partitioned_parquet_dataset(ticks_dir)
        assert ticks_dataset.num_rows == consumer.telemetry.valid_records_emitted

        # Verify VWAP dataset was also partitioned and written
        vwap_dir = Path(cfg.parquet_output_dir) / "vwap"
        if vwap_dir.exists():
            vwap_dataset = read_partitioned_parquet_dataset(vwap_dir)
            assert vwap_dataset.num_rows > 0

    def test_explicit_micro_batch_and_vwap_parquet_writing(self, tmp_path: Path) -> None:
        """Verify explicit calls to write_micro_batch_to_parquet and write_vwap_to_parquet."""
        cfg = DataprocStreamingConfig(environment="dev", starting_offsets="earliest")
        topic = "market.ticks.v1"
        mock_broker = LocalKafkaProducerMock(num_partitions_map={topic: 2})
        producer = KafkaTickProducer(
            config=ProducerConfig(topic=topic, simulation_mode=True),
            mock_producer=mock_broker,
        )
        generator = MarketFeedGenerator(seed=202)
        for _ in range(30):
            producer.send_tick(generator.simulate_tick())

        consumer = PySparkKafkaTickConsumer(config=cfg)
        batch = consumer.poll_from_producer(mock_broker)

        # Write ticks explicitly
        ticks_dir = tmp_path / "explicit_ticks"
        tick_res = consumer.write_micro_batch_to_parquet(batch, output_dir=ticks_dir)
        assert tick_res["records_written"] == len(batch.valid_records)

        # Write VWAP explicitly
        vwap_dir = tmp_path / "explicit_vwap"
        vwap_res = consumer.write_vwap_to_parquet(batch.vwap_stats, output_dir=vwap_dir)
        assert vwap_res["records_written"] == len(batch.vwap_stats)

        # Verify files exist and are Snappy compressed
        tick_files = list(ticks_dir.glob("**/*.parquet"))
        assert len(tick_files) > 0
        for tf in tick_files:
            assert verify_snappy_parquet_file(tf)["compression"] == "SNAPPY"

        vwap_files = list(vwap_dir.glob("**/*.parquet"))
        assert len(vwap_files) > 0
        for vf in vwap_files:
            assert verify_snappy_parquet_file(vf)["compression"] == "SNAPPY"


# =============================================================================
# 7. CLI Runner & Local Simulation Parquet Tests
# =============================================================================


class TestCLIRunnerParquetOutput:
    """Tests for run_tick_consumer.py CLI dry-run and local simulation with Parquet sinks."""

    def test_cli_dry_run_includes_parquet_plan(self, capsys: Any) -> None:
        """Verify --dry_run CLI output contains Parquet compression, comitter, and partition settings."""
        exit_code = run_consumer_main(["--dry_run", "--environment", "prod"])
        assert exit_code == 0

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["mode"] == "dry_run"
        assert data["spark_conf"]["spark.sql.parquet.compression.codec"] == "snappy"
        assert "BindingParquetOutputCommitter" in data["spark_conf"]["spark.sql.parquet.output.committer.class"]
        assert data["plan"]["parquet_compression_codec"] == "snappy"
        assert data["plan"]["parquet_max_records_per_file"] == 50000
        assert data["plan"]["partition_by_columns"] == ["year", "month", "day", "asset_class"]

    def test_run_local_simulation_with_parquet_dir(self, tmp_path: Path) -> None:
        """Verify run_local_simulation with parquet_output_dir outputs Parquet telemetry and files."""
        sim_dir = str(tmp_path / "sim_parquet")
        cfg = DataprocStreamingConfig(
            environment="dev",
            parquet_output_dir=sim_dir,
        )
        report = run_local_simulation(config=cfg, num_ticks=40, seed=77)

        assert report["mode"] == "local_simulation"
        assert report["batches_executed"] > 0
        assert "parquet_summary" in report
        assert "tick_parquet" in report["parquet_summary"]
        assert report["parquet_summary"]["tick_parquet"]["records_written"] > 0

        # Verify physical files on disk
        files = list(Path(sim_dir).glob("**/*.parquet"))
        assert len(files) > 0
        for f in files:
            assert verify_snappy_parquet_file(f)["compression"] == "SNAPPY"

    def test_cli_runner_rejects_invalid_parquet_codec(self) -> None:
        """Verify CLI runner exits with code 2 when given an invalid Parquet codec."""
        exit_code = run_consumer_main(["--parquet_compression_codec", "unsupported_codec"])
        assert exit_code == 2

    def test_submit_job_sh_contains_snappy_and_parquet_flags(self) -> None:
        """Verify submit_job.sh script contains spark parquet snappy and gcs properties."""
        script_path = Path("streaming/dataproc/submit_job.sh")
        assert script_path.exists()
        content = script_path.read_text(encoding="utf-8")
        assert "spark.sql.parquet.compression.codec=snappy" in content
        assert "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" in content
        assert '--parquet_compression_codec="snappy"' in content
        assert "--parquet_max_records_per_file=50000" in content

    def test_large_batch_compression_ratio(self, tmp_path: Path) -> None:
        """Verify that writing a large batch with repeated ticker symbols yields a high compression ratio."""
        writer = LocalParquetDatasetWriter(scratch_dir=tmp_path)
        ticks = [
            ParsedTickRecord(
                trade_id=f"TICK-{i:05d}",
                ticker="AAPL",
                asset_class="EQUITY",
                bid=150.0 + (i * 0.01),
                ask=150.05 + (i * 0.01),
                last_price=150.02 + (i * 0.01),
                volume=100.0,
                spread=0.05,
                mid_price=150.025 + (i * 0.01),
                notional_usd=15002.0,
                relative_spread_bps=3.33,
                event_timestamp=f"2026-10-22T10:00:{i%60:02d}+00:00",
                event_epoch_sec=1792663200.0 + i,
                sequence_number=i,
                year=2026,
                month=10,
                day=22,
                hour=10,
                kafka_topic="market.ticks.v1",
                kafka_partition=0,
                kafka_offset=i,
                kafka_key="EQUITY#AAPL",
                ingested_at="2026-10-22T10:00:01+00:00",
            )
            for i in range(1000)
        ]
        out_dir = tmp_path / "large_parquet"
        res = writer.write_ticks(records=ticks, output_dir=out_dir)

        assert res["records_written"] == 1000
        assert res["files_written"] == 1  # Single partition
        assert res["raw_bytes"] > res["compressed_bytes"]
        assert res["compression_ratio"] > 1.0  # Dictionary encoding + Snappy compresses efficiently

    def test_multiple_batch_telemetry_aggregation(self, tmp_path: Path) -> None:
        """Verify telemetry aggregates multiple micro-batches monotonically."""
        writer = LocalParquetDatasetWriter(scratch_dir=tmp_path)
        ticks = [
            ParsedTickRecord(
                trade_id=f"TICK-{i:03d}",
                ticker="NVDA",
                asset_class="EQUITY",
                bid=500.0,
                ask=500.1,
                last_price=500.05,
                volume=10.0,
                spread=0.1,
                mid_price=500.05,
                notional_usd=5000.5,
                relative_spread_bps=2.0,
                event_timestamp="2026-10-22T10:00:00+00:00",
                event_epoch_sec=1792663200.0,
                sequence_number=i,
                year=2026,
                month=10,
                day=22,
                hour=10,
            )
            for i in range(5)
        ]

        writer.write_ticks(ticks, output_dir=tmp_path / "batch1")
        writer.write_ticks(ticks, output_dir=tmp_path / "batch2")

        assert writer.telemetry.batches_committed == 2
        assert writer.telemetry.records_written == 10
        assert writer.telemetry.files_written >= 2
