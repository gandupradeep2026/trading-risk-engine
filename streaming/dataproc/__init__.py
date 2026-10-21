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
]
