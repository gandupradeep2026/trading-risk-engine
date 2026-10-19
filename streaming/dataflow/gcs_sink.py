"""Google Cloud Storage (GCS) Raw Order & Audit Sink (Day 10).

Provides:
- FormatOrderForGcsFn: Beam DoFn serializing TradeOrder instances into structured,
  audit-enriched JSON records for the Bronze cold tier.
- FormatDeadLetterForGcsFn: Beam DoFn formatting failed / unparseable deadletter
  records for regulatory audit retention in GCS.
- WriteRawOrdersToGcs: Composite Beam PTransform applying fixed time windowing
  (default: 60s) and writing partitioned JSON lines files to GCS Bronze or local paths.
- WriteRawDeadLetterToGcs: Composite Beam PTransform persisting deadletter audit records.
"""

from dataclasses import asdict
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, Generator, Optional, Union

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import window

from ingestion.pubsub_client import TradeOrder
from streaming.dataflow.decoders import DeadLetterRecord

logger = logging.getLogger(__name__)

TAG_GCS_RAW_ORDERS = "gcs_raw_orders"
TAG_GCS_DEADLETTER = "gcs_deadletter"


class FormatOrderForGcsFn(beam.DoFn):
    """Formats TradeOrder objects into newline-delimited JSON with ingestion metadata."""

    def __init__(self):
        super().__init__()
        self.orders_formatted = Metrics.counter("gcs_sink", "orders_formatted_for_gcs")
        self.bytes_formatted = Metrics.distribution("gcs_sink", "order_bytes_formatted_dist")

    def process(self, element: Any) -> Generator[str, None, None]:
        """Convert order element into serialized JSON line."""
        try:
            if hasattr(element, "to_dict") and callable(element.to_dict):
                order_dict = element.to_dict()
            elif hasattr(element, "__dataclass_fields__"):
                order_dict = asdict(element)
            elif isinstance(element, dict):
                order_dict = element
            else:
                raise TypeError(f"Unsupported order type for GCS sink: {type(element)}")

            # Enrich with cold-tier Bronze audit metadata
            record = {
                "order_id": str(order_dict.get("order_id", "")),
                "portfolio_id": str(order_dict.get("portfolio_id", "")),
                "ticker": str(order_dict.get("ticker", "")).upper(),
                "asset_class": str(order_dict.get("asset_class", "")).upper(),
                "side": str(order_dict.get("side", "")).upper(),
                "order_type": str(order_dict.get("order_type", "")).upper(),
                "quantity": float(order_dict.get("quantity", 0.0)),
                "price": float(order_dict.get("price", 0.0)),
                "notional_usd": float(order_dict.get("notional_usd", 0.0)),
                "timestamp": str(order_dict.get("timestamp", "")),
                "trader_id": str(order_dict.get("trader_id", "ALGO-EXEC-01")),
                "execution_status": str(order_dict.get("execution_status", "FILLED")).upper(),
                "sequence_number": int(order_dict.get("sequence_number", 1)),
                "_gcs_ingested_at": datetime.now(timezone.utc).isoformat(),
                "_tier": "BRONZE_RAW",
            }
            json_line = json.dumps(record, separators=(",", ":"))
            self.orders_formatted.inc()
            self.bytes_formatted.update(len(json_line.encode("utf-8")))
            yield json_line
        except Exception as exc:
            logger.error("Failed to format order for GCS sink: %s", exc)


class FormatDeadLetterForGcsFn(beam.DoFn):
    """Formats DeadLetterRecord instances into newline-delimited JSON with error diagnostics."""

    def __init__(self):
        super().__init__()
        self.deadletters_formatted = Metrics.counter("gcs_sink", "deadletters_formatted_for_gcs")

    def process(self, element: Any) -> Generator[str, None, None]:
        """Convert dead-letter record into serialized JSON line."""
        try:
            if hasattr(element, "to_dict") and callable(element.to_dict):
                dlq_dict = element.to_dict()
            elif hasattr(element, "__dataclass_fields__"):
                dlq_dict = asdict(element)
            elif isinstance(element, dict):
                dlq_dict = element
            else:
                raise TypeError(f"Unsupported deadletter type for GCS sink: {type(element)}")

            record = {
                "payload": str(dlq_dict.get("payload", "")),
                "error_type": str(dlq_dict.get("error_type", "UnknownError")),
                "error_message": str(dlq_dict.get("error_message", "")),
                "error_details": dlq_dict.get("error_details", {}),
                "timestamp": str(dlq_dict.get("timestamp", "")),
                "source_attributes": dlq_dict.get("source_attributes", {}),
                "ordering_key": dlq_dict.get("ordering_key"),
                "original_message_id": dlq_dict.get("original_message_id"),
                "_gcs_ingested_at": datetime.now(timezone.utc).isoformat(),
                "_tier": "BRONZE_DEADLETTER",
            }
            json_line = json.dumps(record, separators=(",", ":"))
            self.deadletters_formatted.inc()
            yield json_line
        except Exception as exc:
            logger.error("Failed to format deadletter record for GCS sink: %s", exc)


def _normalize_output_prefix(output_path: str, default_prefix: str) -> str:
    """Normalize GCS or local file path prefix ensuring directory slash handling."""
    normalized = output_path.strip().replace("\\", "/")
    if normalized.endswith("/"):
        return f"{normalized}{default_prefix}"
    return normalized


class WriteRawOrdersToGcs(beam.PTransform):
    """Composite PTransform windowing valid trade orders and writing JSON lines to GCS."""

    def __init__(
        self,
        output_path: str,
        window_seconds: int = 60,
        num_shards: int = 1,
        file_name_suffix: str = ".jsonl",
    ):
        super().__init__()
        self.output_path = output_path
        self.window_seconds = max(1, window_seconds)
        self.num_shards = num_shards
        self.file_name_suffix = file_name_suffix

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        """Apply fixed windowing, JSON serialization, and file writing."""
        prefix = _normalize_output_prefix(self.output_path, "orders-raw")
        return (
            pcoll
            | "WindowOrdersForGcs" >> beam.WindowInto(window.FixedWindows(self.window_seconds))
            | "FormatOrderForGcs" >> beam.ParDo(FormatOrderForGcsFn())
            | "WriteOrdersToGcsFiles"
            >> beam.io.WriteToText(
                file_path_prefix=prefix,
                file_name_suffix=self.file_name_suffix,
                num_shards=self.num_shards,
                append_trailing_newlines=True,
            )
        )


class WriteRawDeadLetterToGcs(beam.PTransform):
    """Composite PTransform windowing dead-letter records and writing JSON lines to GCS."""

    def __init__(
        self,
        output_path: str,
        window_seconds: int = 60,
        num_shards: int = 1,
        file_name_suffix: str = ".jsonl",
    ):
        super().__init__()
        self.output_path = output_path
        self.window_seconds = max(1, window_seconds)
        self.num_shards = num_shards
        self.file_name_suffix = file_name_suffix

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        """Apply fixed windowing, JSON serialization, and file writing."""
        prefix = _normalize_output_prefix(self.output_path, "deadletter-raw")
        return (
            pcoll
            | "WindowDeadLetterForGcs" >> beam.WindowInto(window.FixedWindows(self.window_seconds))
            | "FormatDeadLetterForGcs" >> beam.ParDo(FormatDeadLetterForGcsFn())
            | "WriteDeadLetterToGcsFiles"
            >> beam.io.WriteToText(
                file_path_prefix=prefix,
                file_name_suffix=self.file_name_suffix,
                num_shards=self.num_shards,
                append_trailing_newlines=True,
            )
        )
