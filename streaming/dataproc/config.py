"""Dataproc PySpark Structured Streaming Configuration for Market Tick Consumption.

Provides enterprise configuration management, resource naming conventions aligned with
the Terraform GCP catalog, Spark SQL / Structured Streaming tuning parameters, and
Kafka consumer offset/rate-limiting controls.
"""

import argparse
from dataclasses import asdict, dataclass
import json
import os
import re
from typing import Any, Dict, List, Optional

ALLOWED_ENVIRONMENTS = {"dev", "staging", "prod"}
ALLOWED_SECURITY_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}


def parse_duration_to_seconds(duration_str: str) -> float:
    """Parse a Spark duration string (e.g. '30 seconds', '5 minutes', '500 milliseconds') into seconds.

    Args:
        duration_str: Human-readable Spark interval expression.

    Returns:
        Equivalent duration in float seconds.

    Raises:
        ValueError: If the duration string format is invalid or non-positive.
    """
    if not duration_str or not isinstance(duration_str, str):
        raise ValueError(f"Duration must be a non-empty string, got {duration_str!r}")

    cleaned = duration_str.strip().lower()
    match = re.match(
        r"^(\d+(?:\.\d+)?)\s*(ms|millisecond|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours)$",
        cleaned,
    )
    if not match:
        raise ValueError(
            f"Invalid Spark duration expression '{duration_str}'. "
            "Expected format like '5 seconds', '30 seconds', '500 milliseconds', or '1 minute'."
        )

    value = float(match.group(1))
    unit = match.group(2)

    if value <= 0:
        raise ValueError(f"Duration value must be strictly positive, got {value}")

    if unit in ("ms", "millisecond", "milliseconds"):
        return value / 1000.0
    if unit in ("s", "sec", "secs", "second", "seconds"):
        return value
    if unit in ("m", "min", "mins", "minute", "minutes"):
        return value * 60.0
    if unit in ("h", "hr", "hrs", "hour", "hours"):
        return value * 3600.0

    raise ValueError(f"Unsupported duration unit: {unit}")


@dataclass
class DataprocStreamingConfig:
    """Configuration container for the Dataproc PySpark Kafka Tick Streaming Engine."""

    project_id: str = "trading-risk-engine-dev"
    environment: str = "dev"
    region: str = "us-central1"
    cluster_name: Optional[str] = None
    app_name: Optional[str] = None

    # Kafka Consumer Connectivity & Rate Controls
    kafka_bootstrap_servers: str = "10.10.10.50:9092"
    kafka_subscribe_topics: str = "market.ticks.v1"
    kafka_dlq_topic: str = "market.ticks.deadletter"
    kafka_consumer_group_id: Optional[str] = None
    kafka_security_protocol: str = "PLAINTEXT"
    starting_offsets: str = "latest"
    max_offsets_per_trigger: int = 50000
    fail_on_data_loss: bool = False
    kafka_max_poll_records: int = 5000
    kafka_session_timeout_ms: int = 30000
    kafka_request_timeout_ms: int = 40000

    # Spark Structured Streaming & Checkpointing
    trigger_processing_time: str = "5 seconds"
    watermark_delay_threshold: str = "30 seconds"
    checkpoint_location: Optional[str] = None
    bronze_output_path: Optional[str] = None
    quarantine_output_path: Optional[str] = None
    output_mode: str = "append"

    # Rolling VWAP & Volatility Bands Tuning
    vwap_window_duration: str = "5 minutes"
    vwap_slide_duration: str = "1 minute"
    volatility_k_multiplier: float = 2.0
    volatility_spike_threshold_pct: float = 3.0
    vwap_output_path: Optional[str] = None
    vwap_checkpoint_location: Optional[str] = None

    # Spark Execution & Tuning
    shuffle_partitions: int = 12
    enable_adaptive_query_execution: bool = True
    backpressure_enabled: bool = True
    spark_kafka_package: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

    def __post_init__(self) -> None:
        """Resolve canonical GCP and Kafka resource defaults based on environment."""
        self.environment = self.environment.strip().lower()
        if not self.cluster_name:
            self.cluster_name = f"dp-tick-streaming-{self.environment}"
        if not self.app_name:
            self.app_name = f"trading-risk-tick-streaming-{self.environment}"
        if not self.kafka_consumer_group_id:
            self.kafka_consumer_group_id = f"cg-dataproc-tick-streaming-{self.environment}"
        if not self.checkpoint_location:
            self.checkpoint_location = (
                f"gs://trading-risk-dataflow-staging-{self.environment}/checkpoints/tick-streaming/"
            )
        if not self.bronze_output_path:
            self.bronze_output_path = f"gs://trading-risk-bronze-{self.environment}/ticks/"
        if not self.quarantine_output_path:
            self.quarantine_output_path = (
                f"gs://trading-risk-bronze-{self.environment}/quarantine/ticks/"
            )
        if not self.vwap_output_path:
            self.vwap_output_path = f"gs://trading-risk-bronze-{self.environment}/vwap/"
        if not self.vwap_checkpoint_location:
            self.vwap_checkpoint_location = (
                f"gs://trading-risk-dataflow-staging-{self.environment}/checkpoints/vwap/"
            )

    @property
    def watermark_delay_seconds(self) -> float:
        """Return watermark delay threshold converted to seconds."""
        return parse_duration_to_seconds(self.watermark_delay_threshold)

    @property
    def trigger_interval_seconds(self) -> float:
        """Return micro-batch trigger interval converted to seconds."""
        return parse_duration_to_seconds(self.trigger_processing_time)

    @property
    def vwap_window_duration_seconds(self) -> float:
        """Return rolling VWAP window duration converted to seconds."""
        return parse_duration_to_seconds(self.vwap_window_duration)

    @property
    def vwap_slide_duration_seconds(self) -> float:
        """Return rolling VWAP slide interval converted to seconds."""
        return parse_duration_to_seconds(self.vwap_slide_duration)

    def validate(self) -> None:
        """Validate all streaming configuration invariants.

        Raises:
            ValueError: If any parameter violates operational or domain constraints.
        """
        if self.environment not in ALLOWED_ENVIRONMENTS:
            raise ValueError(
                f"Invalid environment '{self.environment}'. Must be one of {sorted(ALLOWED_ENVIRONMENTS)}"
            )
        if not self.project_id or not self.project_id.strip():
            raise ValueError("project_id must be a non-empty string")
        if not self.kafka_bootstrap_servers or not self.kafka_bootstrap_servers.strip():
            raise ValueError("kafka_bootstrap_servers must be a non-empty host:port list")
        if not self.kafka_subscribe_topics or not self.kafka_subscribe_topics.strip():
            raise ValueError("kafka_subscribe_topics must be a non-empty topic name")

        if self.kafka_security_protocol not in ALLOWED_SECURITY_PROTOCOLS:
            raise ValueError(
                f"Invalid kafka_security_protocol '{self.kafka_security_protocol}'. "
                f"Must be one of {sorted(ALLOWED_SECURITY_PROTOCOLS)}"
            )

        if self.starting_offsets not in ("latest", "earliest"):
            # Must be a valid JSON partition offset map, e.g. {"market.ticks.v1":{"0":100}}
            try:
                parsed = json.loads(self.starting_offsets)
                if not isinstance(parsed, dict):
                    raise ValueError("starting_offsets JSON must be an object")
            except Exception as exc:
                raise ValueError(
                    f"starting_offsets must be 'latest', 'earliest', or valid JSON partition map: {exc}"
                ) from exc

        if self.max_offsets_per_trigger <= 0:
            raise ValueError(
                f"max_offsets_per_trigger must be > 0, got {self.max_offsets_per_trigger}"
            )
        if self.shuffle_partitions <= 0:
            raise ValueError(f"shuffle_partitions must be > 0, got {self.shuffle_partitions}")
        if self.kafka_max_poll_records <= 0:
            raise ValueError(
                f"kafka_max_poll_records must be > 0, got {self.kafka_max_poll_records}"
            )

        # Validate duration expressions
        _ = self.watermark_delay_seconds
        _ = self.trigger_interval_seconds
        vwap_win_sec = self.vwap_window_duration_seconds
        vwap_slide_sec = self.vwap_slide_duration_seconds

        if vwap_slide_sec <= 0:
            raise ValueError(f"vwap_slide_duration must be strictly positive, got {vwap_slide_sec}")
        if vwap_win_sec < vwap_slide_sec:
            raise ValueError(
                f"vwap_window_duration ({vwap_win_sec}s) must be >= vwap_slide_duration ({vwap_slide_sec}s)"
            )
        if self.volatility_k_multiplier <= 0:
            raise ValueError(
                f"volatility_k_multiplier must be > 0, got {self.volatility_k_multiplier}"
            )
        if self.volatility_spike_threshold_pct <= 0:
            raise ValueError(
                f"volatility_spike_threshold_pct must be > 0, got {self.volatility_spike_threshold_pct}"
            )

        if self.output_mode not in ("append", "update", "complete"):
            raise ValueError(
                f"Unsupported Spark output_mode '{self.output_mode}'. Must be append, update, or complete."
            )

    def to_spark_conf_dict(self) -> Dict[str, str]:
        """Generate SparkConf key-value properties for Dataproc cluster execution."""
        return {
            "spark.app.name": str(self.app_name),
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.adaptive.enabled": str(self.enable_adaptive_query_execution).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.streaming.backpressure.enabled": str(self.backpressure_enabled).lower(),
            "spark.sql.streaming.checkpointLocation": str(self.checkpoint_location),
            "spark.sql.streaming.schemaInference": "false",
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.jars.packages": self.spark_kafka_package,
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        }

    def to_kafka_read_options(self) -> Dict[str, str]:
        """Generate options dictionary for spark.readStream.format('kafka').options(**opts)."""
        return {
            "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
            "subscribe": self.kafka_subscribe_topics,
            "startingOffsets": self.starting_offsets,
            "maxOffsetsPerTrigger": str(self.max_offsets_per_trigger),
            "failOnDataLoss": str(self.fail_on_data_loss).lower(),
            "kafka.group.id": str(self.kafka_consumer_group_id),
            "kafka.security.protocol": self.kafka_security_protocol,
            "kafka.max.poll.records": str(self.kafka_max_poll_records),
            "kafka.session.timeout.ms": str(self.kafka_session_timeout_ms),
            "kafka.request.timeout.ms": str(self.kafka_request_timeout_ms),
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return asdict(self)

    @classmethod
    def from_env(cls) -> "DataprocStreamingConfig":
        """Load configuration from environment variables with fallback defaults."""
        env = os.environ.get("TRADING_RISK_ENV", "dev").strip().lower()
        return cls(
            project_id=os.environ.get("GCP_PROJECT_ID", f"trading-risk-engine-{env}"),
            environment=env,
            region=os.environ.get("GCP_REGION", "us-central1"),
            cluster_name=os.environ.get("DATAPROC_CLUSTER_NAME"),
            app_name=os.environ.get("SPARK_APP_NAME"),
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "10.10.10.50:9092"),
            kafka_subscribe_topics=os.environ.get("KAFKA_SUBSCRIBE_TOPICS", "market.ticks.v1"),
            kafka_dlq_topic=os.environ.get("KAFKA_DLQ_TOPIC", "market.ticks.deadletter"),
            kafka_consumer_group_id=os.environ.get("KAFKA_CONSUMER_GROUP_ID"),
            kafka_security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            starting_offsets=os.environ.get("SPARK_STARTING_OFFSETS", "latest"),
            max_offsets_per_trigger=int(os.environ.get("SPARK_MAX_OFFSETS_PER_TRIGGER", "50000")),
            fail_on_data_loss=os.environ.get("SPARK_FAIL_ON_DATA_LOSS", "false").lower() == "true",
            trigger_processing_time=os.environ.get("SPARK_TRIGGER_INTERVAL", "5 seconds"),
            watermark_delay_threshold=os.environ.get("SPARK_WATERMARK_DELAY", "30 seconds"),
            checkpoint_location=os.environ.get("SPARK_CHECKPOINT_LOCATION"),
            bronze_output_path=os.environ.get("GCS_BRONZE_OUTPUT_PATH"),
            quarantine_output_path=os.environ.get("GCS_QUARANTINE_OUTPUT_PATH"),
            vwap_window_duration=os.environ.get("SPARK_VWAP_WINDOW_DURATION", "5 minutes"),
            vwap_slide_duration=os.environ.get("SPARK_VWAP_SLIDE_DURATION", "1 minute"),
            volatility_k_multiplier=float(os.environ.get("SPARK_VOLATILITY_K_MULTIPLIER", "2.0")),
            volatility_spike_threshold_pct=float(
                os.environ.get("SPARK_VOLATILITY_SPIKE_THRESHOLD_PCT", "3.0")
            ),
            vwap_output_path=os.environ.get("GCS_VWAP_OUTPUT_PATH"),
            vwap_checkpoint_location=os.environ.get("SPARK_VWAP_CHECKPOINT_LOCATION"),
            shuffle_partitions=int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "12")),
        )

    @classmethod
    def from_cli_args(cls, argv: Optional[List[str]] = None) -> "DataprocStreamingConfig":
        """Parse command-line arguments into a validated DataprocStreamingConfig."""
        parser = argparse.ArgumentParser(
            description="Dataproc PySpark Structured Streaming Market Tick Consumer"
        )
        parser.add_argument("--project_id", default="trading-risk-engine-dev")
        parser.add_argument("--environment", default="dev", choices=sorted(ALLOWED_ENVIRONMENTS))
        parser.add_argument("--region", default="us-central1")
        parser.add_argument("--cluster_name", default=None)
        parser.add_argument("--app_name", default=None)
        parser.add_argument("--kafka_bootstrap_servers", default="10.10.10.50:9092")
        parser.add_argument("--kafka_subscribe_topics", default="market.ticks.v1")
        parser.add_argument("--kafka_dlq_topic", default="market.ticks.deadletter")
        parser.add_argument("--kafka_consumer_group_id", default=None)
        parser.add_argument(
            "--kafka_security_protocol",
            default="PLAINTEXT",
            choices=sorted(ALLOWED_SECURITY_PROTOCOLS),
        )
        parser.add_argument("--starting_offsets", default="latest")
        parser.add_argument("--max_offsets_per_trigger", type=int, default=50000)
        parser.add_argument("--fail_on_data_loss", action="store_true", default=False)
        parser.add_argument("--trigger_processing_time", default="5 seconds")
        parser.add_argument("--watermark_delay_threshold", default="30 seconds")
        parser.add_argument("--checkpoint_location", default=None)
        parser.add_argument("--bronze_output_path", default=None)
        parser.add_argument("--quarantine_output_path", default=None)
        parser.add_argument("--vwap_window_duration", default="5 minutes")
        parser.add_argument("--vwap_slide_duration", default="1 minute")
        parser.add_argument("--volatility_k_multiplier", type=float, default=2.0)
        parser.add_argument("--volatility_spike_threshold_pct", type=float, default=3.0)
        parser.add_argument("--vwap_output_path", default=None)
        parser.add_argument("--vwap_checkpoint_location", default=None)
        parser.add_argument("--shuffle_partitions", type=int, default=12)

        parsed, _ = parser.parse_known_args(argv)
        config = cls(
            project_id=parsed.project_id,
            environment=parsed.environment,
            region=parsed.region,
            cluster_name=parsed.cluster_name,
            app_name=parsed.app_name,
            kafka_bootstrap_servers=parsed.kafka_bootstrap_servers,
            kafka_subscribe_topics=parsed.kafka_subscribe_topics,
            kafka_dlq_topic=parsed.kafka_dlq_topic,
            kafka_consumer_group_id=parsed.kafka_consumer_group_id,
            kafka_security_protocol=parsed.kafka_security_protocol,
            starting_offsets=parsed.starting_offsets,
            max_offsets_per_trigger=parsed.max_offsets_per_trigger,
            fail_on_data_loss=parsed.fail_on_data_loss,
            trigger_processing_time=parsed.trigger_processing_time,
            watermark_delay_threshold=parsed.watermark_delay_threshold,
            checkpoint_location=parsed.checkpoint_location,
            bronze_output_path=parsed.bronze_output_path,
            quarantine_output_path=parsed.quarantine_output_path,
            vwap_window_duration=parsed.vwap_window_duration,
            vwap_slide_duration=parsed.vwap_slide_duration,
            volatility_k_multiplier=parsed.volatility_k_multiplier,
            volatility_spike_threshold_pct=parsed.volatility_spike_threshold_pct,
            vwap_output_path=parsed.vwap_output_path,
            vwap_checkpoint_location=parsed.vwap_checkpoint_location,
            shuffle_partitions=parsed.shuffle_partitions,
        )
        config.validate()
        return config
