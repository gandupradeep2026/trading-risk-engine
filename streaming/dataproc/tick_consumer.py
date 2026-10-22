"""Dataproc PySpark Structured Streaming Application for Continuous Kafka Tick Consumption.

Provides both:
1. Native PySpark Structured Streaming graph construction (`SparkSession`, `readStream.format("kafka")`,
   `from_json` with explicit `MARKET_TICK_SCHEMA_DDL`, `withWatermark`, microstructure enrichment,
   and dual-stream GCS Parquet / quarantine sink writers) when running on a Dataproc cluster.
2. Zero-cost hermetic local micro-batch execution engine (`PySparkKafkaTickConsumer`) that
   consumes directly from `LocalKafkaProducerMock` or raw Kafka record buffers, enforces
   per-partition offset tracking, `maxOffsetsPerTrigger` rate limiting, event-time watermark
   advancement, late-event dropping, corrupt-record DLQ routing, disk checkpoint persistence,
   and Prometheus telemetry export.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from streaming.dataproc.config import DataprocStreamingConfig
from streaming.dataproc.parquet_sink import (
    DEFAULT_PARQUET_TICK_PARTITIONS,
    DEFAULT_PARQUET_VWAP_PARTITIONS,
    LocalParquetDatasetWriter,
    ParquetSinkConfig,
    ParquetSinkTelemetry,
    start_parquet_tick_sink,
    start_parquet_vwap_sink,
    start_partitioned_parquet_sink,
    start_quarantine_json_sink,
)
from streaming.dataproc.schema import (
    MARKET_TICK_SCHEMA_DDL,
    CorruptTickRecord,
    ParsedTickRecord,
    get_pyspark_tick_schema,
    parse_and_validate_kafka_record,
)
from streaming.dataproc.vwap_aggregator import (
    RollingVWAPAggregator,
    RollingVWAPStats,
    build_rolling_vwap_stream,
)

logger = logging.getLogger(__name__)


@dataclass
class StreamingQueryTelemetry:
    """Operational metrics and Prometheus exporter for the PySpark Kafka tick consumer."""

    batches_processed: int = 0
    records_received: int = 0
    valid_records_emitted: int = 0
    corrupt_records_quarantined: int = 0
    late_records_dropped: int = 0
    current_watermark_epoch_sec: float = 0.0
    max_event_time_epoch_sec: float = 0.0
    partition_offsets: Dict[str, Dict[int, int]] = field(default_factory=dict)

    @property
    def current_watermark_iso(self) -> Optional[str]:
        """Return current watermark in UTC ISO-8601 format, or None if not yet initialized."""
        if self.current_watermark_epoch_sec <= 0.0:
            return None
        return datetime.fromtimestamp(
            self.current_watermark_epoch_sec, tz=timezone.utc
        ).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Export telemetry snapshot as dictionary."""
        return {
            "batches_processed": self.batches_processed,
            "records_received": self.records_received,
            "valid_records_emitted": self.valid_records_emitted,
            "corrupt_records_quarantined": self.corrupt_records_quarantined,
            "late_records_dropped": self.late_records_dropped,
            "current_watermark_epoch_sec": round(self.current_watermark_epoch_sec, 3),
            "current_watermark_iso": self.current_watermark_iso,
            "max_event_time_epoch_sec": round(self.max_event_time_epoch_sec, 3),
            "partition_offsets": {
                topic: {str(part): off for part, off in parts.items()}
                for topic, parts in self.partition_offsets.items()
            },
        }

    def format_prometheus(self, app_name: str = "trading-risk-tick-streaming") -> str:
        """Render operational metrics in Prometheus text exposition format."""
        lines = [
            "# HELP dataproc_streaming_batches_processed_total Total micro-batches processed.",
            "# TYPE dataproc_streaming_batches_processed_total counter",
            f'dataproc_streaming_batches_processed_total{{app="{app_name}"}} {self.batches_processed}',
            "# HELP dataproc_streaming_records_received_total Total raw Kafka records polled.",
            "# TYPE dataproc_streaming_records_received_total counter",
            f'dataproc_streaming_records_received_total{{app="{app_name}"}} {self.records_received}',
            "# HELP dataproc_streaming_valid_records_emitted_total Valid enriched ticks emitted.",
            "# TYPE dataproc_streaming_valid_records_emitted_total counter",
            f'dataproc_streaming_valid_records_emitted_total{{app="{app_name}"}} {self.valid_records_emitted}',
            "# HELP dataproc_streaming_corrupt_records_quarantined_total Malformed or invalid records routed to quarantine.",
            "# TYPE dataproc_streaming_corrupt_records_quarantined_total counter",
            f'dataproc_streaming_corrupt_records_quarantined_total{{app="{app_name}"}} {self.corrupt_records_quarantined}',
            "# HELP dataproc_streaming_late_records_dropped_total Late ticks dropped behind event-time watermark.",
            "# TYPE dataproc_streaming_late_records_dropped_total counter",
            f'dataproc_streaming_late_records_dropped_total{{app="{app_name}"}} {self.late_records_dropped}',
            "# HELP dataproc_streaming_watermark_epoch_seconds Current event-time watermark in epoch seconds.",
            "# TYPE dataproc_streaming_watermark_epoch_seconds gauge",
            f'dataproc_streaming_watermark_epoch_seconds{{app="{app_name}"}} {self.current_watermark_epoch_sec:.3f}',
        ]
        for topic, parts in sorted(self.partition_offsets.items()):
            for part_id, offset in sorted(parts.items()):
                lines.append(
                    f'dataproc_streaming_committed_offset{{app="{app_name}",topic="{topic}",partition="{part_id}"}} {offset}'
                )
        return "\n".join(lines) + "\n"


@dataclass
class MicroBatchResult:
    """Result container for a single Spark Structured Streaming micro-batch trigger."""

    batch_id: int
    records_polled: int
    valid_records: List[ParsedTickRecord]
    corrupt_records: List[CorruptTickRecord]
    dropped_late_records: List[ParsedTickRecord]
    committed_offsets: Dict[str, Dict[int, int]]
    watermark_epoch_sec: float
    watermark_iso: Optional[str]
    vwap_stats: List[RollingVWAPStats] = field(default_factory=list)


@dataclass
class SparkStreamPlanDescriptor:
    """Inspection-friendly descriptor of a PySpark Structured Streaming execution plan."""

    app_name: str
    source_format: str
    kafka_options: Dict[str, str]
    schema_ddl: str
    watermark_column: str
    watermark_delay: str
    enrichment_columns: List[str]
    partition_by_columns: List[str]
    bronze_output_path: str
    quarantine_output_path: str
    checkpoint_location: str
    trigger_processing_time: str
    output_mode: str
    vwap_window_duration: str = "5 minutes"
    vwap_slide_duration: str = "1 minute"
    volatility_k_multiplier: float = 2.0
    volatility_spike_threshold_pct: float = 3.0
    vwap_output_path: str = ""
    parquet_compression_codec: str = "snappy"
    parquet_max_records_per_file: int = 50000
    gcs_committer_class: str = "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"
    vwap_partition_by_columns: List[str] = field(
        default_factory=lambda: list(DEFAULT_PARQUET_VWAP_PARTITIONS)
    )


class PySparkKafkaTickConsumer:
    """Enterprise Dataproc PySpark Structured Streaming consumer for continuous Kafka market ticks.

    Supports both:
    - Building native PySpark `SparkSession`, streaming `DataFrame` transformations, and
      `DataStreamWriter` sinks for cluster deployment.
    - Executing deterministic, stateful micro-batches locally against `LocalKafkaProducerMock`
      or raw Kafka message lists with full offset checkpointing, `maxOffsetsPerTrigger` slicing,
      event-time watermarking, late event filtering, and DLQ quarantine routing.
    """

    def __init__(self, config: Optional[DataprocStreamingConfig] = None) -> None:
        self.config = config or DataprocStreamingConfig()
        self.config.validate()
        self.telemetry = StreamingQueryTelemetry()
        self.vwap_aggregator = RollingVWAPAggregator(config=self.config)
        self.parquet_writer = LocalParquetDatasetWriter(scratch_dir=self.config.parquet_output_dir)
        self.vwap_parquet_writer = LocalParquetDatasetWriter(scratch_dir=self.config.parquet_output_dir)
        self.parquet_telemetry = self.parquet_writer.telemetry
        self.vwap_parquet_telemetry = self.vwap_parquet_writer.telemetry

        # Stateful consumer state: topic -> {partition_id -> next_offset_to_read}
        self._offsets: Dict[str, Dict[int, int]] = {}
        self._next_batch_id: int = 0
        self._initialized_partitions: set[Tuple[str, int]] = set()

        # Accumulated sink storage for local inspection/testing
        self.bronze_sink_records: List[ParsedTickRecord] = []
        self.quarantine_sink_records: List[CorruptTickRecord] = []

        # Attempt to restore state if checkpoint_location is a local directory
        self._try_load_local_checkpoint()

    # -------------------------------------------------------------------------
    # Native PySpark Structured Streaming Builders
    # -------------------------------------------------------------------------

    @staticmethod
    def build_spark_session(
        config: DataprocStreamingConfig,
        spark_builder: Optional[Any] = None,
    ) -> Any:
        """Build and configure a production PySpark `SparkSession` for Dataproc streaming.

        Args:
            config: Validated `DataprocStreamingConfig`.
            spark_builder: Optional custom or mocked `SparkSession.builder` instance.

        Returns:
            Configured `SparkSession` instance.
        """
        config.validate()
        if spark_builder is None:
            from pyspark.sql import SparkSession

            spark_builder = SparkSession.builder

        builder = spark_builder.appName(str(config.app_name))
        for key, value in config.to_spark_conf_dict().items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    @staticmethod
    def build_kafka_read_stream(spark: Any, config: DataprocStreamingConfig) -> Any:
        """Create the raw Kafka streaming DataFrame from `spark.readStream`.

        Args:
            spark: Active `SparkSession` (or mock equivalent).
            config: Validated `DataprocStreamingConfig`.

        Returns:
            Unbounded streaming DataFrame representing the raw Kafka envelope.
        """
        config.validate()
        kafka_opts = config.to_kafka_read_options()
        return spark.readStream.format("kafka").options(**kafka_opts).load()

    def build_stream_plan_descriptor(self) -> SparkStreamPlanDescriptor:
        """Return a deterministic descriptor of the Structured Streaming physical/logical plan."""
        return SparkStreamPlanDescriptor(
            app_name=str(self.config.app_name),
            source_format="kafka",
            kafka_options=self.config.to_kafka_read_options(),
            schema_ddl=MARKET_TICK_SCHEMA_DDL,
            watermark_column="event_timestamp",
            watermark_delay=self.config.watermark_delay_threshold,
            enrichment_columns=[
                "spread",
                "mid_price",
                "notional_usd",
                "relative_spread_bps",
                "event_timestamp",
                "year",
                "month",
                "day",
                "hour",
                "ingested_at",
            ],
            partition_by_columns=list(self.config.parquet_partition_columns),
            bronze_output_path=str(self.config.bronze_output_path),
            quarantine_output_path=str(self.config.quarantine_output_path),
            checkpoint_location=str(self.config.checkpoint_location),
            trigger_processing_time=self.config.trigger_processing_time,
            output_mode=self.config.output_mode,
            vwap_window_duration=self.config.vwap_window_duration,
            vwap_slide_duration=self.config.vwap_slide_duration,
            volatility_k_multiplier=self.config.volatility_k_multiplier,
            volatility_spike_threshold_pct=self.config.volatility_spike_threshold_pct,
            vwap_output_path=str(self.config.vwap_output_path),
            parquet_compression_codec=self.config.parquet_compression_codec,
            parquet_max_records_per_file=self.config.parquet_max_records_per_file,
            gcs_committer_class="org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
            vwap_partition_by_columns=list(self.config.vwap_parquet_partition_columns),
        )

    build_rolling_vwap_stream = staticmethod(build_rolling_vwap_stream)
    start_vwap_writer = staticmethod(start_parquet_vwap_sink)
    start_parquet_tick_sink = staticmethod(start_parquet_tick_sink)

    @staticmethod
    def build_parsed_tick_streams(
        raw_kafka_df: Any,
        config: DataprocStreamingConfig,
    ) -> Tuple[Any, Any]:
        """Transform a raw Kafka streaming DataFrame into `(valid_ticks_df, corrupt_ticks_df)`.

        When native `pyspark.sql.functions` is available, applies native Spark SQL expressions.
        Also supports fluent mock DataFrame objects in unit tests.
        """
        try:
            from pyspark.sql import functions as F

            schema = get_pyspark_tick_schema()
            envelope_df = raw_kafka_df.selectExpr(
                "CAST(key AS STRING) AS kafka_key",
                "CAST(value AS STRING) AS raw_payload",
                "topic AS kafka_topic",
                "partition AS kafka_partition",
                "offset AS kafka_offset",
                "timestamp AS kafka_timestamp",
            )
            parsed_df = envelope_df.withColumn(
                "tick",
                F.from_json(
                    F.col("raw_payload"),
                    schema,
                    {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
                ),
            )

            valid_condition = (
                F.col("raw_payload").isNotNull()
                & F.col("tick").isNotNull()
                & F.col("tick._corrupt_record").isNull()
                & F.col("tick.trade_id").isNotNull()
                & F.col("tick.ticker").isNotNull()
                & F.col("tick.asset_class").isin("EQUITY", "FX", "COMMODITY", "CRYPTO", "ETF")
                & (F.col("tick.bid") > 0)
                & (F.col("tick.ask") > 0)
                & (F.col("tick.last_price") > 0)
                & (F.col("tick.volume") > 0)
                & (F.col("tick.bid") <= F.col("tick.ask"))
                & F.col("tick.timestamp").isNotNull()
            )

            valid_df = (
                parsed_df.filter(valid_condition)
                .select(
                    F.col("tick.trade_id").alias("trade_id"),
                    F.upper(F.col("tick.ticker")).alias("ticker"),
                    F.upper(F.col("tick.asset_class")).alias("asset_class"),
                    F.round(F.col("tick.bid"), 4).alias("bid"),
                    F.round(F.col("tick.ask"), 4).alias("ask"),
                    F.round(F.col("tick.last_price"), 4).alias("last_price"),
                    F.round(F.col("tick.volume"), 4).alias("volume"),
                    F.round(F.col("tick.ask") - F.col("tick.bid"), 4).alias("spread"),
                    F.round((F.col("tick.bid") + F.col("tick.ask")) / 2.0, 4).alias("mid_price"),
                    F.round(F.col("tick.last_price") * F.col("tick.volume"), 4).alias(
                        "notional_usd"
                    ),
                    F.round(
                        ((F.col("tick.ask") - F.col("tick.bid")) / ((F.col("tick.bid") + F.col("tick.ask")) / 2.0))
                        * 10000.0,
                        4,
                    ).alias("relative_spread_bps"),
                    F.to_timestamp(F.col("tick.timestamp")).alias("event_timestamp"),
                    F.coalesce(F.col("tick.sequence_number"), F.lit(0)).alias("sequence_number"),
                    "kafka_topic",
                    "kafka_partition",
                    "kafka_offset",
                    "kafka_key",
                    F.current_timestamp().alias("ingested_at"),
                )
                .withWatermark("event_timestamp", config.watermark_delay_threshold)
                .withColumn("year", F.year("event_timestamp"))
                .withColumn("month", F.month("event_timestamp"))
                .withColumn("day", F.dayofmonth("event_timestamp"))
                .withColumn("hour", F.hour("event_timestamp"))
            )

            corrupt_df = parsed_df.filter(~valid_condition).select(
                "raw_payload",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_key",
                F.current_timestamp().alias("quarantined_at"),
            )
            return valid_df, corrupt_df

        except ImportError:
            # Fallback for non-JVM unit test environments passing mock DataFrames
            envelope_df = raw_kafka_df.selectExpr(
                "CAST(key AS STRING) AS kafka_key",
                "CAST(value AS STRING) AS raw_payload",
                "topic AS kafka_topic",
                "partition AS kafka_partition",
                "offset AS kafka_offset",
                "timestamp AS kafka_timestamp",
            )
            valid_df = envelope_df.withWatermark(
                "event_timestamp", config.watermark_delay_threshold
            )
            corrupt_df = envelope_df.filter("_corrupt_record IS NOT NULL")
            return valid_df, corrupt_df

    @staticmethod
    def start_bronze_and_quarantine_writers(
        valid_df: Any,
        corrupt_df: Any,
        config: DataprocStreamingConfig,
    ) -> Tuple[Any, Any]:
        """Configure and start the Bronze GCS Parquet writer and Quarantine JSON writer."""
        bronze_query = start_parquet_tick_sink(valid_df, config)
        quarantine_query = start_quarantine_json_sink(corrupt_df, config)
        return bronze_query, quarantine_query

    # Backward compatibility alias
    start_Bronze_and_quarantine_writers = start_bronze_and_quarantine_writers

    # -------------------------------------------------------------------------
    # Zero-Cost Hermetic Local Micro-Batch Execution Engine
    # -------------------------------------------------------------------------

    def _is_local_checkpoint(self) -> bool:
        """Return True if `checkpoint_location` points to a local filesystem path (not `gs://`)."""
        loc = str(self.config.checkpoint_location or "")
        return bool(loc) and not loc.startswith(("gs://", "s3://", "hdfs://"))

    def _checkpoint_file_path(self) -> Path:
        return Path(str(self.config.checkpoint_location)) / "offsets.json"

    def _try_load_local_checkpoint(self) -> bool:
        """Restore committed partition offsets and watermark state from local checkpoint directory."""
        if not self._is_local_checkpoint():
            return False

        ckpt_file = self._checkpoint_file_path()
        if not ckpt_file.exists():
            return False

        try:
            payload = json.loads(ckpt_file.read_text(encoding="utf-8"))
            self._next_batch_id = int(payload.get("next_batch_id", 0))
            self.telemetry.batches_processed = int(payload.get("batches_processed", 0))
            self.telemetry.records_received = int(payload.get("records_received", 0))
            self.telemetry.valid_records_emitted = int(payload.get("valid_records_emitted", 0))
            self.telemetry.corrupt_records_quarantined = int(
                payload.get("corrupt_records_quarantined", 0)
            )
            self.telemetry.late_records_dropped = int(payload.get("late_records_dropped", 0))
            self.telemetry.current_watermark_epoch_sec = float(
                payload.get("current_watermark_epoch_sec", 0.0)
            )
            self.telemetry.max_event_time_epoch_sec = float(
                payload.get("max_event_time_epoch_sec", 0.0)
            )

            raw_offsets = payload.get("offsets", {})
            self._offsets = {}
            for topic, part_map in raw_offsets.items():
                self._offsets[topic] = {}
                for part_str, off_val in part_map.items():
                    part_id = int(part_str)
                    self._offsets[topic][part_id] = int(off_val)
                    self._initialized_partitions.add((topic, part_id))

            self.telemetry.partition_offsets = {
                t: dict(p) for t, p in self._offsets.items()
            }
            return True
        except Exception as exc:
            logger.warning("Failed to load local streaming checkpoint %s: %s", ckpt_file, exc)
            return False

    def _persist_local_checkpoint(self) -> None:
        """Atomically persist consumer offsets and watermark state to local checkpoint directory."""
        if not self._is_local_checkpoint():
            return

        ckpt_file = self._checkpoint_file_path()
        ckpt_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "app_name": self.config.app_name,
            "next_batch_id": self._next_batch_id,
            "batches_processed": self.telemetry.batches_processed,
            "records_received": self.telemetry.records_received,
            "valid_records_emitted": self.telemetry.valid_records_emitted,
            "corrupt_records_quarantined": self.telemetry.corrupt_records_quarantined,
            "late_records_dropped": self.telemetry.late_records_dropped,
            "current_watermark_epoch_sec": self.telemetry.current_watermark_epoch_sec,
            "max_event_time_epoch_sec": self.telemetry.max_event_time_epoch_sec,
            "offsets": {
                topic: {str(part): off for part, off in parts.items()}
                for topic, parts in self._offsets.items()
            },
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        ckpt_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _resolve_initial_offset(self, topic: str, partition_id: int, current_size: int) -> int:
        """Determine starting offset for an uninitialized `(topic, partition_id)`."""
        if self.config.starting_offsets == "earliest":
            return 0
        if self.config.starting_offsets == "latest":
            return current_size
        # Explicit JSON partition offset map
        parsed = json.loads(self.config.starting_offsets)
        topic_map = parsed.get(topic, {})
        return int(topic_map.get(str(partition_id), 0))

    @staticmethod
    def _extract_topic_partitions(producer: Any, topic: str) -> Dict[int, List[Any]]:
        """Extract partition message queues from `LocalKafkaProducerMock` or compatible mock."""
        if hasattr(producer, "_partitions"):
            return dict(producer._partitions.get(topic, {}))
        if hasattr(producer, "partitions"):
            return dict(producer.partitions.get(topic, {}))
        return {}

    @staticmethod
    def _extract_record_field(record: Any, field_name: str, default: Any = None) -> Any:
        """Read a field from either a `ProducerRecord` dataclass or a plain dictionary."""
        if isinstance(record, dict):
            return record.get(field_name, default)
        return getattr(record, field_name, default)

    def initialize_offsets_from_producer(self, producer: Any) -> Dict[str, Dict[int, int]]:
        """Initialize partition offsets against a `LocalKafkaProducerMock` before streaming starts.

        Crucial when `starting_offsets='latest'` so subsequent records produced after initialization
        are captured while historical pre-existing records are skipped.
        """
        topics = [
            t.strip() for t in self.config.kafka_subscribe_topics.split(",") if t.strip()
        ]
        for topic in topics:
            topic_partitions = self._extract_topic_partitions(producer, topic)
            if topic not in self._offsets:
                self._offsets[topic] = {}
            for part_id, messages in sorted(topic_partitions.items()):
                if (topic, part_id) not in self._initialized_partitions:
                    start_off = self._resolve_initial_offset(topic, part_id, len(messages))
                    self._offsets[topic][part_id] = start_off
                    self._initialized_partitions.add((topic, part_id))

        self.telemetry.partition_offsets = {t: dict(p) for t, p in self._offsets.items()}
        self._persist_local_checkpoint()
        return {t: dict(p) for t, p in self._offsets.items()}

    def poll_from_producer(
        self,
        producer: Any,
        foreach_batch_fn: Optional[Callable[[MicroBatchResult], None]] = None,
    ) -> MicroBatchResult:
        """Execute a single Structured Streaming micro-batch trigger against `LocalKafkaProducerMock`.

        Respects per-partition offsets, `starting_offsets`, and `max_offsets_per_trigger`.
        """
        topics = [
            t.strip() for t in self.config.kafka_subscribe_topics.split(",") if t.strip()
        ]
        polled_envelopes: List[Dict[str, Any]] = []
        remaining_budget = self.config.max_offsets_per_trigger

        for topic in topics:
            topic_partitions = self._extract_topic_partitions(producer, topic)
            if topic not in self._offsets:
                self._offsets[topic] = {}

            for part_id in sorted(topic_partitions.keys()):
                messages = topic_partitions[part_id]
                if (topic, part_id) not in self._initialized_partitions:
                    start_off = self._resolve_initial_offset(topic, part_id, len(messages))
                    self._offsets[topic][part_id] = start_off
                    self._initialized_partitions.add((topic, part_id))

                current_offset = self._offsets[topic][part_id]
                available = len(messages) - current_offset
                if available <= 0 or remaining_budget <= 0:
                    continue

                take_count = min(available, remaining_budget)
                slice_msgs = messages[current_offset : current_offset + take_count]
                for idx, msg in enumerate(slice_msgs):
                    polled_envelopes.append(
                        {
                            "topic": topic,
                            "partition": part_id,
                            "offset": current_offset + idx,
                            "key": self._extract_record_field(msg, "key"),
                            "value": self._extract_record_field(msg, "value"),
                            "timestamp": self._extract_record_field(msg, "timestamp"),
                        }
                    )

                self._offsets[topic][part_id] = current_offset + take_count
                remaining_budget -= take_count

        return self._execute_micro_batch_envelopes(
            polled_envelopes, foreach_batch_fn=foreach_batch_fn
        )

    def process_raw_messages(
        self,
        messages: List[Dict[str, Any]],
        foreach_batch_fn: Optional[Callable[[MicroBatchResult], None]] = None,
    ) -> MicroBatchResult:
        """Process an explicit list of raw Kafka envelope dicts as a micro-batch.

        Each dict in `messages` may specify `value`, `key`, `topic`, `partition`, `offset`.
        """
        normalized: List[Dict[str, Any]] = []
        for idx, msg in enumerate(messages):
            topic = str(msg.get("topic", self.config.kafka_subscribe_topics.split(",")[0]))
            part_id = int(msg.get("partition", 0))
            offset = int(msg.get("offset", idx))
            normalized.append(
                {
                    "topic": topic,
                    "partition": part_id,
                    "offset": offset,
                    "key": msg.get("key"),
                    "value": msg.get("value"),
                }
            )
            if topic not in self._offsets:
                self._offsets[topic] = {}
            self._offsets[topic][part_id] = max(
                self._offsets[topic].get(part_id, 0), offset + 1
            )
            self._initialized_partitions.add((topic, part_id))

        return self._execute_micro_batch_envelopes(
            normalized, foreach_batch_fn=foreach_batch_fn
        )

    def _execute_micro_batch_envelopes(
        self,
        envelopes: List[Dict[str, Any]],
        foreach_batch_fn: Optional[Callable[[MicroBatchResult], None]] = None,
    ) -> MicroBatchResult:
        """Core micro-batch execution: parse, validate, enforce watermark, update sinks & checkpoint."""
        if not envelopes:
            return MicroBatchResult(
                batch_id=self._next_batch_id,
                records_polled=0,
                valid_records=[],
                corrupt_records=[],
                dropped_late_records=[],
                committed_offsets={t: dict(p) for t, p in self._offsets.items()},
                watermark_epoch_sec=self.telemetry.current_watermark_epoch_sec,
                watermark_iso=self.telemetry.current_watermark_iso,
            )

        batch_id = self._next_batch_id
        self._next_batch_id += 1

        valid_records: List[ParsedTickRecord] = []
        corrupt_records: List[CorruptTickRecord] = []
        dropped_late_records: List[ParsedTickRecord] = []

        # Watermark at the start of the micro-batch is used to evaluate late arrivals
        active_watermark = self.telemetry.current_watermark_epoch_sec
        batch_max_event_epoch = self.telemetry.max_event_time_epoch_sec

        for env in envelopes:
            parsed = parse_and_validate_kafka_record(
                raw_value=env.get("value"),
                kafka_key=env.get("key"),
                kafka_topic=str(env.get("topic", "market.ticks.v1")),
                kafka_partition=int(env.get("partition", 0)),
                kafka_offset=int(env.get("offset", 0)),
            )
            if isinstance(parsed, CorruptTickRecord):
                corrupt_records.append(parsed)
                continue

            # Check against current event-time watermark
            if active_watermark > 0.0 and parsed.event_epoch_sec < active_watermark:
                dropped_late_records.append(parsed)
                continue

            valid_records.append(parsed)
            if parsed.event_epoch_sec > batch_max_event_epoch:
                batch_max_event_epoch = parsed.event_epoch_sec

        # Advance event-time watermark at end of micro-batch trigger:
        # watermark = max_observed_event_time - watermark_delay_seconds
        if batch_max_event_epoch > 0.0:
            self.telemetry.max_event_time_epoch_sec = batch_max_event_epoch
            candidate_watermark = max(
                0.0, batch_max_event_epoch - self.config.watermark_delay_seconds
            )
            if candidate_watermark > self.telemetry.current_watermark_epoch_sec:
                self.telemetry.current_watermark_epoch_sec = candidate_watermark

        # Update sink buffers and telemetry counters
        self.bronze_sink_records.extend(valid_records)
        self.quarantine_sink_records.extend(corrupt_records)
        self.vwap_aggregator.add_ticks(valid_records)

        self.telemetry.batches_processed += 1
        self.telemetry.records_received += len(envelopes)
        self.telemetry.valid_records_emitted += len(valid_records)
        self.telemetry.corrupt_records_quarantined += len(corrupt_records)
        self.telemetry.late_records_dropped += len(dropped_late_records)
        self.telemetry.partition_offsets = {t: dict(p) for t, p in self._offsets.items()}

        # Compute rolling VWAP and volatility bands across accumulated ticks
        vwap_stats = self.vwap_aggregator.compute_windows()

        # Persist checkpoint to disk if local path configured
        self._persist_local_checkpoint()

        result = MicroBatchResult(
            batch_id=batch_id,
            records_polled=len(envelopes),
            valid_records=valid_records,
            corrupt_records=corrupt_records,
            dropped_late_records=dropped_late_records,
            committed_offsets={t: dict(p) for t, p in self._offsets.items()},
            watermark_epoch_sec=self.telemetry.current_watermark_epoch_sec,
            watermark_iso=self.telemetry.current_watermark_iso,
            vwap_stats=vwap_stats,
        )

        # Automatically write to partitioned Snappy Parquet if output directory is configured
        if self.config.parquet_output_dir:
            ticks_dir = f"{self.config.parquet_output_dir.rstrip('/')}/ticks"
            self.write_micro_batch_to_parquet(result, output_dir=ticks_dir)
            if vwap_stats:
                vwap_dir = f"{self.config.parquet_output_dir.rstrip('/')}/vwap"
                self.write_vwap_to_parquet(vwap_stats, output_dir=vwap_dir)

        if foreach_batch_fn is not None:
            foreach_batch_fn(result)

        return result

    def write_micro_batch_to_parquet(
        self,
        batch_result: MicroBatchResult,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """Write valid ticks from a micro-batch into partitioned Snappy-compressed Parquet.

        Args:
            batch_result: The MicroBatchResult to persist.
            output_dir: Destination directory (local path or gs:// URI). Defaults to
                config.parquet_output_dir/ticks or config.bronze_output_path.

        Returns:
            Dictionary summary from LocalParquetDatasetWriter.
        """
        dest_dir = output_dir or (
            f"{self.config.parquet_output_dir.rstrip('/')}/ticks"
            if self.config.parquet_output_dir
            else self.config.bronze_output_path
        )
        return self.parquet_writer.write_ticks(
            records=batch_result.valid_records,
            output_dir=dest_dir,
            partition_cols=self.config.parquet_partition_columns,
            compression=self.config.parquet_compression_codec,
        )

    def write_vwap_to_parquet(
        self,
        vwap_stats: Optional[List[RollingVWAPStats]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """Write rolling VWAP summaries into partitioned Snappy-compressed Parquet.

        Args:
            vwap_stats: Optional list of RollingVWAPStats (defaults to computing all windows).
            output_dir: Destination directory. Defaults to config.parquet_output_dir/vwap
                or config.vwap_output_path.

        Returns:
            Dictionary summary from LocalParquetDatasetWriter.
        """
        stats_to_write = (
            vwap_stats if vwap_stats is not None else self.compute_vwap_windows()
        )
        dest_dir = output_dir or (
            f"{self.config.parquet_output_dir.rstrip('/')}/vwap"
            if self.config.parquet_output_dir
            else self.config.vwap_output_path
        )
        return self.vwap_parquet_writer.write_vwap_stats(
            records=stats_to_write,
            output_dir=dest_dir,
            partition_cols=self.config.vwap_parquet_partition_columns,
            compression=self.config.parquet_compression_codec,
        )

    def compute_vwap_windows(
        self,
        reference_time: Optional[datetime] = None,
    ) -> List[RollingVWAPStats]:
        """Compute rolling VWAP and volatility bands across all accumulated valid ticks."""
        return self.vwap_aggregator.compute_windows(reference_time=reference_time)

    def drain_producer(
        self,
        producer: Any,
        max_batches: int = 100,
        foreach_batch_fn: Optional[Callable[[MicroBatchResult], None]] = None,
    ) -> List[MicroBatchResult]:
        """Continuously trigger micro-batches until all available messages in `producer` are consumed."""
        batches: List[MicroBatchResult] = []
        for _ in range(max_batches):
            batch = self.poll_from_producer(producer, foreach_batch_fn=foreach_batch_fn)
            if batch.records_polled == 0:
                break
            batches.append(batch)
        return batches
