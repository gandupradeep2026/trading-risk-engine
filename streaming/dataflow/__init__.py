"""Cloud Dataflow (Apache Beam) Financial Streaming Pipeline Package.

Provides production-grade pipeline construction, custom options, Pub/Sub IO integration,
JSON decoders with schema validation, Dead-Letter Queue (DLQ) routing, 5-minute sliding
window transforms, custom DoFn anomaly detectors, and telemetry metrics.
"""

from streaming.dataflow.anomaly_detector import (
    TAG_RISK_ALERTS,
    TAG_WINDOW_STATS,
    DetectSlidingWindowDrawdownFn,
    EvaluateSingleOrderSpikeFn,
    FormatRiskAlertForPubSubFn,
    KeyByPortfolioAndTickerFn,
    RiskAlert,
    SlidingWindowAnomalyTransform,
    WindowDrawdownStats,
    classify_drawdown_severity,
)
from streaming.dataflow.cloudsql_sink import (
    TAG_CLOUDSQL_FAILED,
    TAG_CLOUDSQL_SUCCESS,
    CloudSqlConfig,
    CloudSqlDatabaseClient,
    WriteAlertsToCloudSql,
    WriteAlertsToCloudSqlFn,
)
from streaming.dataflow.decoders import (
    TAG_DEADLETTER,
    TAG_RAW_AUDIT,
    TAG_VALID_ORDERS,
    DeadLetterRecord,
    DecodeAndValidateOrders,
    DecodeAndValidateTradeOrderFn,
    FormatDeadLetterForPubSubFn,
)
from streaming.dataflow.gcs_sink import (
    TAG_GCS_DEADLETTER,
    TAG_GCS_RAW_ORDERS,
    FormatDeadLetterForGcsFn,
    FormatOrderForGcsFn,
    WriteRawDeadLetterToGcs,
    WriteRawOrdersToGcs,
)
from streaming.dataflow.options import RiskEnginePipelineOptions
from streaming.dataflow.pipeline import DataflowPipelineBundle, build_pipeline

__all__ = [
    "RiskEnginePipelineOptions",
    "DeadLetterRecord",
    "DecodeAndValidateTradeOrderFn",
    "FormatDeadLetterForPubSubFn",
    "DecodeAndValidateOrders",
    "RiskAlert",
    "WindowDrawdownStats",
    "KeyByPortfolioAndTickerFn",
    "DetectSlidingWindowDrawdownFn",
    "EvaluateSingleOrderSpikeFn",
    "FormatRiskAlertForPubSubFn",
    "SlidingWindowAnomalyTransform",
    "classify_drawdown_severity",
    "CloudSqlConfig",
    "CloudSqlDatabaseClient",
    "WriteAlertsToCloudSqlFn",
    "WriteAlertsToCloudSql",
    "FormatOrderForGcsFn",
    "FormatDeadLetterForGcsFn",
    "WriteRawOrdersToGcs",
    "WriteRawDeadLetterToGcs",
    "DataflowPipelineBundle",
    "build_pipeline",
    "TAG_VALID_ORDERS",
    "TAG_DEADLETTER",
    "TAG_RAW_AUDIT",
    "TAG_WINDOW_STATS",
    "TAG_RISK_ALERTS",
    "TAG_CLOUDSQL_SUCCESS",
    "TAG_CLOUDSQL_FAILED",
    "TAG_GCS_RAW_ORDERS",
    "TAG_GCS_DEADLETTER",
]
