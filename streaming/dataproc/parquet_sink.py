"""Production PySpark Streaming Sink for Partitioned Snappy-Compressed Parquet on GCS.

Provides:
- `ParquetSinkConfig`: Enterprise configuration container for partitioned columnar sinks
  with Snappy compression, file sizing (maxRecordsPerFile), and GCS direct committer settings.
- `ParquetSinkTelemetry`: Real-time operational metric tracker recording written records,
  created files, uncompressed vs compressed byte volumes, compression ratios, and Prometheus
  exposition formatting.
- `LocalParquetDatasetWriter`: Zero-cost hermetic local PyArrow dataset writer producing true
  partitioned Snappy Parquet files for offline testing, CI environments, and local simulation.
- `start_partitioned_parquet_sink`, `start_parquet_tick_sink`, `start_parquet_vwap_sink`, and
  `start_quarantine_json_sink`: PySpark Structured Streaming graph builders configuring
  `.format("parquet")`, `.option("compression", "snappy")`, `.partitionBy(...)`, and GCS paths.
- Parquet metadata inspection and verification utilities (`verify_snappy_parquet_file`,
  `read_partitioned_parquet_dataset`, `extract_partition_keys_from_path`).
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq

from streaming.dataproc.config import (
    ALLOWED_PARQUET_CODECS,
    DataprocStreamingConfig,
)
from streaming.dataproc.schema import ParsedTickRecord
from streaming.dataproc.vwap_aggregator import RollingVWAPStats

logger = logging.getLogger(__name__)

# Default Hive-style Partitioning Strategies
DEFAULT_PARQUET_TICK_PARTITIONS: List[str] = [
    "year",
    "month",
    "day",
    "asset_class",
]

DEFAULT_PARQUET_VWAP_PARTITIONS: List[str] = [
    "year",
    "month",
    "day",
    "asset_class",
]

# Explicit PyArrow Schemas matching Spark SQL DDL Schemas
TICK_PARQUET_ARROW_SCHEMA = pa.schema(
    [
        pa.field("trade_id", pa.string(), nullable=False),
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("asset_class", pa.string(), nullable=False),
        pa.field("bid", pa.float64(), nullable=False),
        pa.field("ask", pa.float64(), nullable=False),
        pa.field("last_price", pa.float64(), nullable=False),
        pa.field("volume", pa.float64(), nullable=False),
        pa.field("spread", pa.float64(), nullable=True),
        pa.field("mid_price", pa.float64(), nullable=True),
        pa.field("notional_usd", pa.float64(), nullable=True),
        pa.field("relative_spread_bps", pa.float64(), nullable=True),
        pa.field("timestamp", pa.string(), nullable=True),
        pa.field("event_timestamp", pa.string(), nullable=False),
        pa.field("event_epoch_sec", pa.float64(), nullable=False),
        pa.field("sequence_number", pa.int64(), nullable=True),
        pa.field("kafka_topic", pa.string(), nullable=True),
        pa.field("kafka_partition", pa.int32(), nullable=True),
        pa.field("kafka_offset", pa.int64(), nullable=True),
        pa.field("kafka_key", pa.string(), nullable=True),
        pa.field("ingested_at", pa.string(), nullable=True),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("month", pa.int32(), nullable=False),
        pa.field("day", pa.int32(), nullable=False),
        pa.field("hour", pa.int32(), nullable=False),
    ]
)

VWAP_PARQUET_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("asset_class", pa.string(), nullable=False),
        pa.field("window_start", pa.string(), nullable=False),
        pa.field("window_end", pa.string(), nullable=False),
        pa.field("window_start_epoch_sec", pa.float64(), nullable=True),
        pa.field("window_end_epoch_sec", pa.float64(), nullable=True),
        pa.field("tick_count", pa.int64(), nullable=False),
        pa.field("open_price", pa.float64(), nullable=False),
        pa.field("high_price", pa.float64(), nullable=False),
        pa.field("low_price", pa.float64(), nullable=False),
        pa.field("close_price", pa.float64(), nullable=False),
        pa.field("total_volume", pa.float64(), nullable=False),
        pa.field("total_notional", pa.float64(), nullable=False),
        pa.field("vwap", pa.float64(), nullable=False),
        pa.field("price_stddev", pa.float64(), nullable=False),
        pa.field("price_variance", pa.float64(), nullable=False),
        pa.field("volatility_multiplier", pa.float64(), nullable=False),
        pa.field("upper_band", pa.float64(), nullable=False),
        pa.field("lower_band", pa.float64(), nullable=False),
        pa.field("bandwidth_bps", pa.float64(), nullable=False),
        pa.field("percent_b", pa.float64(), nullable=False),
        pa.field("is_upper_breach", pa.bool_(), nullable=False),
        pa.field("is_lower_breach", pa.bool_(), nullable=False),
        pa.field("volatility_spike", pa.bool_(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("month", pa.int32(), nullable=False),
        pa.field("day", pa.int32(), nullable=False),
        pa.field("hour", pa.int32(), nullable=False),
        pa.field("computed_at", pa.string(), nullable=False),
    ]
)


def extract_partition_keys_from_path(file_path: Union[str, Path]) -> Dict[str, str]:
    """Extract Hive-style key=value partition tokens from a storage path.

    Example:
        `gs://bucket/ticks/year=2026/month=10/day=22/hour=11/asset_class=EQUITY/part-0.parquet`
        -> `{'year': '2026', 'month': '10', 'day': '22', 'hour': '11', 'asset_class': 'EQUITY'}`
    """
    path_str = str(file_path).replace("\\", "/")
    tokens = re.findall(r"([a-zA-Z0-9_]+)=([^/]+)", path_str)
    return {k: v for k, v in tokens}


def resolve_storage_path(
    uri: str,
    local_scratch_dir: Optional[Union[str, Path]] = None,
) -> Path:
    """Resolve a storage URI into a local path for hermetic offline testing.

    If URI starts with `gs://`, rewrites into a subpath within `local_scratch_dir`.
    Otherwise treats URI as a normal filesystem path.
    """
    if not uri:
        raise ValueError("Storage URI cannot be empty")

    cleaned = str(uri).strip()
    if cleaned.startswith("gs://"):
        # Strip gs:// prefix and normalize
        rel_path = cleaned[5:].strip("/")
        base_dir = (
            Path(local_scratch_dir)
            if local_scratch_dir
            else Path.cwd() / ".scratch" / "gcs_mock"
        )
        resolved = base_dir / rel_path
    else:
        resolved = Path(cleaned)

    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


@dataclass
class ParquetSinkConfig:
    """Configuration container for a partitioned Parquet streaming sink."""

    output_path: str
    checkpoint_location: str
    compression_codec: str = "snappy"
    partition_columns: List[str] = field(
        default_factory=lambda: list(DEFAULT_PARQUET_TICK_PARTITIONS)
    )
    max_records_per_file: int = 50000
    trigger_processing_time: str = "5 seconds"
    output_mode: str = "append"
    query_name: Optional[str] = None

    def validate(self) -> None:
        """Validate sink configuration invariants."""
        if not self.output_path or not self.output_path.strip():
            raise ValueError("output_path must be a non-empty string")
        if not self.checkpoint_location or not self.checkpoint_location.strip():
            raise ValueError("checkpoint_location must be a non-empty string")
        if self.compression_codec.lower() not in ALLOWED_PARQUET_CODECS:
            raise ValueError(
                f"Invalid compression_codec '{self.compression_codec}'. "
                f"Must be one of {sorted(ALLOWED_PARQUET_CODECS)}"
            )
        if self.max_records_per_file <= 0:
            raise ValueError(
                f"max_records_per_file must be > 0, got {self.max_records_per_file}"
            )
        if not self.partition_columns:
            raise ValueError("partition_columns must be a non-empty list of column names")
        if self.output_mode not in ("append", "complete", "update"):
            raise ValueError(
                f"Unsupported output_mode '{self.output_mode}'. Must be append, complete, or update."
            )

    @classmethod
    def from_streaming_config_for_ticks(
        cls,
        config: DataprocStreamingConfig,
        override_output_path: Optional[str] = None,
    ) -> "ParquetSinkConfig":
        """Factory creating a ParquetSinkConfig for the Bronze tick streaming sink."""
        config.validate()
        output_path = override_output_path or config.bronze_output_path
        checkpoint = f"{config.checkpoint_location.rstrip('/')}/bronze_parquet"
        sink_cfg = cls(
            output_path=str(output_path),
            checkpoint_location=checkpoint,
            compression_codec=config.parquet_compression_codec,
            partition_columns=list(config.parquet_partition_columns),
            max_records_per_file=config.parquet_max_records_per_file,
            trigger_processing_time=config.trigger_processing_time,
            output_mode=config.output_mode,
            query_name=f"{config.app_name}-bronze-parquet-sink",
        )
        sink_cfg.validate()
        return sink_cfg

    @classmethod
    def from_streaming_config_for_vwap(
        cls,
        config: DataprocStreamingConfig,
        override_output_path: Optional[str] = None,
    ) -> "ParquetSinkConfig":
        """Factory creating a ParquetSinkConfig for the Bronze rolling VWAP sink."""
        config.validate()
        output_path = override_output_path or config.vwap_output_path
        checkpoint = (
            config.vwap_checkpoint_location
            or f"{config.checkpoint_location.rstrip('/')}/vwap_parquet"
        )
        sink_cfg = cls(
            output_path=str(output_path),
            checkpoint_location=str(checkpoint),
            compression_codec=config.parquet_compression_codec,
            partition_columns=list(config.vwap_parquet_partition_columns),
            max_records_per_file=config.parquet_max_records_per_file,
            trigger_processing_time=config.trigger_processing_time,
            output_mode="append",
            query_name=f"{config.app_name}-vwap-parquet-sink",
        )
        sink_cfg.validate()
        return sink_cfg


@dataclass
class ParquetSinkTelemetry:
    """Operational telemetry store for Parquet file generation, sizing, and compression."""

    records_written: int = 0
    files_written: int = 0
    raw_bytes_uncompressed: int = 0
    compressed_bytes: int = 0
    batches_committed: int = 0
    partitions_created: Set[str] = field(default_factory=set)
    last_commit_timestamp: Optional[str] = None
    last_commit_latency_ms: float = 0.0

    @property
    def compression_ratio(self) -> float:
        """Calculate uncompressed-to-compressed data volume ratio."""
        if self.compressed_bytes > 0 and self.raw_bytes_uncompressed > 0:
            return round(self.raw_bytes_uncompressed / self.compressed_bytes, 4)
        return 1.0

    @property
    def avg_records_per_file(self) -> float:
        """Calculate average number of records contained per written Parquet file."""
        if self.files_written > 0:
            return round(self.records_written / self.files_written, 2)
        return 0.0

    def record_batch(
        self,
        num_records: int,
        num_files: int,
        raw_bytes: int,
        compressed_bytes: int,
        partitions: Iterable[str],
        latency_ms: float = 0.0,
    ) -> None:
        """Record batch metrics atomically."""
        self.records_written += num_records
        self.files_written += num_files
        self.raw_bytes_uncompressed += raw_bytes
        self.compressed_bytes += compressed_bytes
        self.batches_committed += 1
        for part in partitions:
            self.partitions_created.add(str(part))
        self.last_commit_timestamp = datetime.now(timezone.utc).isoformat()
        self.last_commit_latency_ms = round(latency_ms, 2)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize telemetry metrics to dictionary."""
        return {
            "records_written": self.records_written,
            "files_written": self.files_written,
            "raw_bytes_uncompressed": self.raw_bytes_uncompressed,
            "compressed_bytes": self.compressed_bytes,
            "batches_committed": self.batches_committed,
            "compression_ratio": self.compression_ratio,
            "avg_records_per_file": self.avg_records_per_file,
            "active_partitions_count": len(self.partitions_created),
            "partitions_created": sorted(self.partitions_created),
            "last_commit_timestamp": self.last_commit_timestamp,
            "last_commit_latency_ms": self.last_commit_latency_ms,
        }

    def format_prometheus(
        self,
        app_name: str = "trading-risk-tick-streaming",
        sink_name: str = "bronze_parquet",
    ) -> str:
        """Format metrics in standard Prometheus text exposition format."""
        lines = [
            f'# HELP dataproc_parquet_sink_records_written_total Cumulative count of records written to Parquet.',
            f'# TYPE dataproc_parquet_sink_records_written_total counter',
            f'dataproc_parquet_sink_records_written_total{{app="{app_name}",sink="{sink_name}"}} {self.records_written}',
            f'# HELP dataproc_parquet_sink_files_written_total Cumulative count of Parquet files committed.',
            f'# TYPE dataproc_parquet_sink_files_written_total counter',
            f'dataproc_parquet_sink_files_written_total{{app="{app_name}",sink="{sink_name}"}} {self.files_written}',
            f'# HELP dataproc_parquet_sink_compressed_bytes_total Cumulative compressed bytes on disk.',
            f'# TYPE dataproc_parquet_sink_compressed_bytes_total counter',
            f'dataproc_parquet_sink_compressed_bytes_total{{app="{app_name}",sink="{sink_name}"}} {self.compressed_bytes}',
            f'# HELP dataproc_parquet_sink_uncompressed_bytes_total Estimated uncompressed payload bytes.',
            f'# TYPE dataproc_parquet_sink_uncompressed_bytes_total counter',
            f'dataproc_parquet_sink_uncompressed_bytes_total{{app="{app_name}",sink="{sink_name}"}} {self.raw_bytes_uncompressed}',
            f'# HELP dataproc_parquet_sink_compression_ratio Current compression ratio (uncompressed/compressed).',
            f'# TYPE dataproc_parquet_sink_compression_ratio gauge',
            f'dataproc_parquet_sink_compression_ratio{{app="{app_name}",sink="{sink_name}"}} {self.compression_ratio}',
            f'# HELP dataproc_parquet_sink_active_partitions Number of distinct partition paths created.',
            f'# TYPE dataproc_parquet_sink_active_partitions gauge',
            f'dataproc_parquet_sink_active_partitions{{app="{app_name}",sink="{sink_name}"}} {len(self.partitions_created)}',
            f'# HELP dataproc_parquet_sink_last_commit_latency_ms Latency of most recent Parquet batch write in ms.',
            f'# TYPE dataproc_parquet_sink_last_commit_latency_ms gauge',
            f'dataproc_parquet_sink_last_commit_latency_ms{{app="{app_name}",sink="{sink_name}"}} {self.last_commit_latency_ms}',
            f'# HELP dataproc_parquet_sink_batches_committed_total Cumulative micro-batches committed to Parquet.',
            f'# TYPE dataproc_parquet_sink_batches_committed_total counter',
            f'dataproc_parquet_sink_batches_committed_total{{app="{app_name}",sink="{sink_name}"}} {self.batches_committed}',
        ]
        return "\n".join(lines) + "\n"


def verify_snappy_parquet_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Verify that a Parquet file exists, has valid row groups, and is Snappy-compressed.

    Args:
        file_path: Filesystem path to the Parquet file.

    Returns:
        Dict of metadata attributes (num_rows, num_columns, compression, serialized_size, columns).

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is not a valid Parquet file or any column is not SNAPPY-compressed.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found at: {path}")

    meta = pq.read_metadata(path)
    if meta.num_row_groups == 0 and meta.num_rows > 0:
        raise ValueError(f"Parquet file has rows ({meta.num_rows}) but 0 row groups: {path}")

    for rg_idx in range(meta.num_row_groups):
        rg = meta.row_group(rg_idx)
        for col_idx in range(meta.num_columns):
            col_meta = rg.column(col_idx)
            codec = str(col_meta.compression).upper()
            if codec != "SNAPPY":
                raise ValueError(
                    f"Column '{col_meta.path_in_schema}' in row group {rg_idx} of {path} "
                    f"is compressed with {codec}, expected SNAPPY."
                )

    return {
        "file_path": str(path),
        "num_rows": meta.num_rows,
        "num_columns": meta.num_columns,
        "num_row_groups": meta.num_row_groups,
        "columns": [meta.schema.names[i] for i in range(meta.num_columns)],
        "compression": "SNAPPY",
        "serialized_size_bytes": meta.serialized_size,
    }


def read_partitioned_parquet_dataset(
    dataset_path: Union[str, Path],
    columns: Optional[List[str]] = None,
) -> pa.Table:
    """Read a partitioned Parquet dataset back into a PyArrow Table.

    Automatically restores Hive partition columns (`year`, `month`, `day`, `hour`, `asset_class`).
    """
    path = Path(dataset_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset path does not exist: {path}")

    return pq.read_table(path, columns=columns)


class LocalParquetDatasetWriter:
    """Hermetic zero-cost local partitioned Parquet dataset writer using PyArrow.

    Produces byte-exact Snappy-compressed Parquet files organized into Hive-style partition
    directory hierarchies matching production GCP Cloud Storage layouts.
    """

    def __init__(self, scratch_dir: Optional[Union[str, Path]] = None) -> None:
        self.scratch_dir = Path(scratch_dir) if scratch_dir else None
        self.telemetry = ParquetSinkTelemetry()

    def write_ticks(
        self,
        records: List[ParsedTickRecord],
        output_dir: Union[str, Path],
        partition_cols: Optional[List[str]] = None,
        compression: str = "snappy",
    ) -> Dict[str, Any]:
        """Write a list of `ParsedTickRecord` instances into partitioned Snappy Parquet.

        Args:
            records: Validated tick records to persist.
            output_dir: Destination directory (local path or `gs://` URI).
            partition_cols: Ordered list of partition column names.
            compression: Compression codec (default: 'snappy').

        Returns:
            Dictionary summary with records written, files written, byte volumes, and paths.
        """
        start_time = time.perf_counter()
        target_dir = resolve_storage_path(str(output_dir), local_scratch_dir=self.scratch_dir)
        part_cols = (
            partition_cols if partition_cols is not None else DEFAULT_PARQUET_TICK_PARTITIONS
        )

        if not records:
            return {
                "records_written": 0,
                "files_written": 0,
                "raw_bytes": 0,
                "compressed_bytes": 0,
                "compression_ratio": 1.0,
                "partitions": [],
                "file_paths": [],
            }

        # Build PyArrow columnar dictionary
        data_dict: Dict[str, List[Any]] = {field.name: [] for field in TICK_PARQUET_ARROW_SCHEMA}
        for rec in records:
            data_dict["trade_id"].append(rec.trade_id)
            data_dict["ticker"].append(rec.ticker)
            data_dict["asset_class"].append(rec.asset_class)
            data_dict["bid"].append(float(rec.bid))
            data_dict["ask"].append(float(rec.ask))
            data_dict["last_price"].append(float(rec.last_price))
            data_dict["volume"].append(float(rec.volume))
            data_dict["spread"].append(float(rec.spread) if rec.spread is not None else None)
            data_dict["mid_price"].append(float(rec.mid_price) if rec.mid_price is not None else None)
            data_dict["notional_usd"].append(
                float(rec.notional_usd) if rec.notional_usd is not None else None
            )
            data_dict["relative_spread_bps"].append(
                float(rec.relative_spread_bps) if rec.relative_spread_bps is not None else None
            )
            data_dict["timestamp"].append(rec.event_timestamp)
            data_dict["event_timestamp"].append(rec.event_timestamp)
            data_dict["event_epoch_sec"].append(float(rec.event_epoch_sec))
            data_dict["sequence_number"].append(
                int(rec.sequence_number) if rec.sequence_number is not None else 0
            )
            data_dict["kafka_topic"].append(rec.kafka_topic)
            data_dict["kafka_partition"].append(int(rec.kafka_partition))
            data_dict["kafka_offset"].append(int(rec.kafka_offset))
            data_dict["kafka_key"].append(rec.kafka_key)
            data_dict["ingested_at"].append(rec.ingested_at)
            data_dict["year"].append(int(rec.year))
            data_dict["month"].append(int(rec.month))
            data_dict["day"].append(int(rec.day))
            data_dict["hour"].append(int(rec.hour))

        table = pa.Table.from_pydict(data_dict, schema=TICK_PARQUET_ARROW_SCHEMA)
        estimated_uncompressed = table.nbytes

        # Track pre-existing parquet files to identify newly created ones
        pre_files = set(target_dir.glob("**/*.parquet"))

        # Write dataset with Snappy compression and dictionary encoding
        pq.write_to_dataset(
            table,
            root_path=str(target_dir),
            partition_cols=part_cols,
            compression=compression.upper(),
            use_dictionary=True,
        )

        post_files = set(target_dir.glob("**/*.parquet"))
        new_files = sorted(post_files - pre_files) if post_files != pre_files else sorted(post_files)

        total_compressed_bytes = sum(f.stat().st_size for f in new_files)
        partition_paths = set()
        for f in new_files:
            tokens = extract_partition_keys_from_path(f)
            if tokens:
                partition_paths.add("/".join(f"{k}={v}" for k, v in tokens.items()))

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        self.telemetry.record_batch(
            num_records=len(records),
            num_files=len(new_files),
            raw_bytes=estimated_uncompressed,
            compressed_bytes=total_compressed_bytes,
            partitions=partition_paths,
            latency_ms=latency_ms,
        )

        return {
            "records_written": len(records),
            "files_written": len(new_files),
            "raw_bytes": estimated_uncompressed,
            "compressed_bytes": total_compressed_bytes,
            "compression_ratio": self.telemetry.compression_ratio,
            "partitions": sorted(partition_paths),
            "file_paths": [str(f) for f in new_files],
            "latency_ms": round(latency_ms, 2),
        }

    def write_vwap_stats(
        self,
        records: List[RollingVWAPStats],
        output_dir: Union[str, Path],
        partition_cols: Optional[List[str]] = None,
        compression: str = "snappy",
    ) -> Dict[str, Any]:
        """Write rolling VWAP and volatility band summaries into partitioned Snappy Parquet."""
        start_time = time.perf_counter()
        target_dir = resolve_storage_path(str(output_dir), local_scratch_dir=self.scratch_dir)
        part_cols = (
            partition_cols if partition_cols is not None else DEFAULT_PARQUET_VWAP_PARTITIONS
        )

        if not records:
            return {
                "records_written": 0,
                "files_written": 0,
                "raw_bytes": 0,
                "compressed_bytes": 0,
                "compression_ratio": 1.0,
                "partitions": [],
                "file_paths": [],
            }

        data_dict: Dict[str, List[Any]] = {field.name: [] for field in VWAP_PARQUET_ARROW_SCHEMA}
        for rec in records:
            data_dict["ticker"].append(rec.ticker)
            data_dict["asset_class"].append(rec.asset_class)
            data_dict["window_start"].append(rec.window_start)
            data_dict["window_end"].append(rec.window_end)
            data_dict["window_start_epoch_sec"].append(
                float(getattr(rec, "window_start_epoch_sec", 0.0))
            )
            data_dict["window_end_epoch_sec"].append(
                float(getattr(rec, "window_end_epoch_sec", 0.0))
            )
            data_dict["tick_count"].append(int(rec.tick_count))
            data_dict["open_price"].append(float(rec.open_price))
            data_dict["high_price"].append(float(rec.high_price))
            data_dict["low_price"].append(float(rec.low_price))
            data_dict["close_price"].append(float(rec.close_price))
            data_dict["total_volume"].append(float(rec.total_volume))
            data_dict["total_notional"].append(float(rec.total_notional))
            data_dict["vwap"].append(float(rec.vwap))
            data_dict["price_stddev"].append(float(rec.price_stddev))
            data_dict["price_variance"].append(float(rec.price_variance))
            data_dict["volatility_multiplier"].append(float(rec.volatility_multiplier))
            data_dict["upper_band"].append(float(rec.upper_band))
            data_dict["lower_band"].append(float(rec.lower_band))
            data_dict["bandwidth_bps"].append(float(rec.bandwidth_bps))
            data_dict["percent_b"].append(float(rec.percent_b))
            data_dict["is_upper_breach"].append(bool(rec.is_upper_breach))
            data_dict["is_lower_breach"].append(bool(rec.is_lower_breach))
            data_dict["volatility_spike"].append(bool(rec.volatility_spike))
            data_dict["year"].append(int(rec.year))
            data_dict["month"].append(int(rec.month))
            data_dict["day"].append(int(rec.day))
            data_dict["hour"].append(int(rec.hour))
            data_dict["computed_at"].append(rec.computed_at)

        table = pa.Table.from_pydict(data_dict, schema=VWAP_PARQUET_ARROW_SCHEMA)
        estimated_uncompressed = table.nbytes

        pre_files = set(target_dir.glob("**/*.parquet"))
        pq.write_to_dataset(
            table,
            root_path=str(target_dir),
            partition_cols=part_cols,
            compression=compression.upper(),
            use_dictionary=True,
        )
        post_files = set(target_dir.glob("**/*.parquet"))
        new_files = sorted(post_files - pre_files) if post_files != pre_files else sorted(post_files)

        total_compressed_bytes = sum(f.stat().st_size for f in new_files)
        partition_paths = set()
        for f in new_files:
            tokens = extract_partition_keys_from_path(f)
            if tokens:
                partition_paths.add("/".join(f"{k}={v}" for k, v in tokens.items()))

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        self.telemetry.record_batch(
            num_records=len(records),
            num_files=len(new_files),
            raw_bytes=estimated_uncompressed,
            compressed_bytes=total_compressed_bytes,
            partitions=partition_paths,
            latency_ms=latency_ms,
        )

        return {
            "records_written": len(records),
            "files_written": len(new_files),
            "raw_bytes": estimated_uncompressed,
            "compressed_bytes": total_compressed_bytes,
            "compression_ratio": self.telemetry.compression_ratio,
            "partitions": sorted(partition_paths),
            "file_paths": [str(f) for f in new_files],
            "latency_ms": round(latency_ms, 2),
        }


# =============================================================================
# PySpark Structured Streaming Graph Builders for Dataproc Cluster Execution
# =============================================================================


def start_partitioned_parquet_sink(
    streaming_df: Any,
    sink_config: ParquetSinkConfig,
) -> Any:
    """Configure and start a PySpark Structured Streaming partitioned Parquet query.

    Enforces Snappy compression, target GCS path, partition columns, maxRecordsPerFile,
    and checkpointing.
    """
    sink_config.validate()

    writer = (
        streaming_df.writeStream.format("parquet")
        .outputMode(sink_config.output_mode)
        .option("path", str(sink_config.output_path))
        .option("checkpointLocation", str(sink_config.checkpoint_location))
        .option("compression", str(sink_config.compression_codec))
        .option("maxRecordsPerFile", str(sink_config.max_records_per_file))
        .partitionBy(*sink_config.partition_columns)
        .trigger(processingTime=sink_config.trigger_processing_time)
    )

    if sink_config.query_name:
        writer = writer.queryName(sink_config.query_name)

    return writer.start()


def start_parquet_tick_sink(
    valid_ticks_df: Any,
    config: DataprocStreamingConfig,
    override_output_path: Optional[str] = None,
) -> Any:
    """Start the Bronze market tick partitioned Snappy Parquet streaming sink."""
    sink_cfg = ParquetSinkConfig.from_streaming_config_for_ticks(
        config=config, override_output_path=override_output_path
    )
    return start_partitioned_parquet_sink(valid_ticks_df, sink_cfg)


def start_parquet_vwap_sink(
    vwap_df: Any,
    config: DataprocStreamingConfig,
    override_output_path: Optional[str] = None,
) -> Any:
    """Start the rolling VWAP partitioned Snappy Parquet streaming sink."""
    sink_cfg = ParquetSinkConfig.from_streaming_config_for_vwap(
        config=config, override_output_path=override_output_path
    )
    return start_partitioned_parquet_sink(vwap_df, sink_cfg)


def start_quarantine_json_sink(
    corrupt_df: Any,
    config: DataprocStreamingConfig,
) -> Any:
    """Start the quarantined corrupt ticks JSON streaming sink to GCS Bronze."""
    quarantine_checkpoint = f"{config.checkpoint_location.rstrip('/')}/quarantine"
    return (
        corrupt_df.writeStream.format("json")
        .outputMode("append")
        .option("path", str(config.quarantine_output_path))
        .option("checkpointLocation", quarantine_checkpoint)
        .trigger(processingTime=config.trigger_processing_time)
        .queryName(f"{config.app_name}-quarantine-sink")
        .start()
    )
