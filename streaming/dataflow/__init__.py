"""Cloud Dataflow (Apache Beam) Financial Streaming Pipeline Package.

Provides production-grade pipeline construction, custom options, Pub/Sub IO integration,
JSON decoders with schema validation, Dead-Letter Queue (DLQ) routing, and metrics.
"""

from streaming.dataflow.decoders import (
    TAG_DEADLETTER,
    TAG_RAW_AUDIT,
    TAG_VALID_ORDERS,
    DeadLetterRecord,
    DecodeAndValidateOrders,
    DecodeAndValidateTradeOrderFn,
    FormatDeadLetterForPubSubFn,
)
from streaming.dataflow.options import RiskEnginePipelineOptions
from streaming.dataflow.pipeline import DataflowPipelineBundle, build_pipeline

__all__ = [
    "RiskEnginePipelineOptions",
    "DeadLetterRecord",
    "DecodeAndValidateTradeOrderFn",
    "FormatDeadLetterForPubSubFn",
    "DecodeAndValidateOrders",
    "DataflowPipelineBundle",
    "build_pipeline",
    "TAG_VALID_ORDERS",
    "TAG_DEADLETTER",
    "TAG_RAW_AUDIT",
]
