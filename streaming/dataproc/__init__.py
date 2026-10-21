"""Dataproc PySpark Structured Streaming Package for Real-Time Market Tick Processing."""

from streaming.dataproc.config import DataprocStreamingConfig, parse_duration_to_seconds
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
from streaming.dataproc.vwap_aggregator import (
    ROLLING_VWAP_SCHEMA_DDL,
    ROLLING_VWAP_STRUCT_SPEC,
    RollingVWAPAggregator,
    RollingVWAPStats,
    RollingVWAPTelemetry,
    build_rolling_vwap_stream,
    get_pyspark_vwap_schema,
    start_vwap_writer,
)

__all__ = [
    "DataprocStreamingConfig",
    "parse_duration_to_seconds",
    "MARKET_TICK_SCHEMA_DDL",
    "MARKET_TICK_STRUCT_SPEC",
    "REQUIRED_TICK_FIELDS",
    "KAFKA_ENVELOPE_COLUMNS",
    "SparkFieldSpec",
    "SparkStructTypeSpec",
    "get_pyspark_tick_schema",
    "ParsedTickRecord",
    "CorruptTickRecord",
    "parse_and_validate_kafka_record",
    "StreamingQueryTelemetry",
    "MicroBatchResult",
    "SparkStreamPlanDescriptor",
    "PySparkKafkaTickConsumer",
    "ROLLING_VWAP_SCHEMA_DDL",
    "ROLLING_VWAP_STRUCT_SPEC",
    "get_pyspark_vwap_schema",
    "RollingVWAPStats",
    "RollingVWAPTelemetry",
    "RollingVWAPAggregator",
    "build_rolling_vwap_stream",
    "start_vwap_writer",
]
