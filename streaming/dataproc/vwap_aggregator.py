"""Dataproc PySpark Rolling VWAP & Volatility Band Streaming Engine.

Provides:
1. Mathematical models and Spark SQL DDL schemas for rolling Volume-Weighted Average Price
   (VWAP), Bollinger-style standard deviation volatility bands, bandwidth (bps), and %B indicators.
2. Production PySpark Structured Streaming graph builder (`build_rolling_vwap_stream`, `start_vwap_writer`)
   aggregating over sliding event-time windows (`groupBy(window(...), ticker, asset_class)`).
3. Hermetic zero-cost local execution engine (`RollingVWAPAggregator`) calculating chronologically
   rigorous sample standard deviation, VWAP, band breaches, volatility spikes, and Prometheus telemetry.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from streaming.dataproc.config import DataprocStreamingConfig
from streaming.dataproc.schema import (
    ParsedTickRecord,
    SparkFieldSpec,
    SparkStructTypeSpec,
    parse_and_validate_kafka_record,
)

logger = logging.getLogger(__name__)

# Explicit Spark SQL DDL Schema for Rolling VWAP aggregations
ROLLING_VWAP_SCHEMA_DDL: str = (
    "ticker STRING, "
    "asset_class STRING, "
    "window_start STRING, "
    "window_end STRING, "
    "tick_count LONG, "
    "open_price DOUBLE, "
    "high_price DOUBLE, "
    "low_price DOUBLE, "
    "close_price DOUBLE, "
    "total_volume DOUBLE, "
    "total_notional DOUBLE, "
    "vwap DOUBLE, "
    "price_stddev DOUBLE, "
    "price_variance DOUBLE, "
    "volatility_multiplier DOUBLE, "
    "upper_band DOUBLE, "
    "lower_band DOUBLE, "
    "bandwidth_bps DOUBLE, "
    "percent_b DOUBLE, "
    "is_upper_breach BOOLEAN, "
    "is_lower_breach BOOLEAN, "
    "volatility_spike BOOLEAN, "
    "year INT, "
    "month INT, "
    "day INT, "
    "hour INT, "
    "computed_at STRING"
)

ROLLING_VWAP_STRUCT_SPEC = SparkStructTypeSpec(
    fields=(
        SparkFieldSpec("ticker", "STRING", nullable=False),
        SparkFieldSpec("asset_class", "STRING", nullable=False),
        SparkFieldSpec("window_start", "STRING", nullable=False),
        SparkFieldSpec("window_end", "STRING", nullable=False),
        SparkFieldSpec("tick_count", "LONG", nullable=False),
        SparkFieldSpec("open_price", "DOUBLE", nullable=False),
        SparkFieldSpec("high_price", "DOUBLE", nullable=False),
        SparkFieldSpec("low_price", "DOUBLE", nullable=False),
        SparkFieldSpec("close_price", "DOUBLE", nullable=False),
        SparkFieldSpec("total_volume", "DOUBLE", nullable=False),
        SparkFieldSpec("total_notional", "DOUBLE", nullable=False),
        SparkFieldSpec("vwap", "DOUBLE", nullable=False),
        SparkFieldSpec("price_stddev", "DOUBLE", nullable=False),
        SparkFieldSpec("price_variance", "DOUBLE", nullable=False),
        SparkFieldSpec("volatility_multiplier", "DOUBLE", nullable=False),
        SparkFieldSpec("upper_band", "DOUBLE", nullable=False),
        SparkFieldSpec("lower_band", "DOUBLE", nullable=False),
        SparkFieldSpec("bandwidth_bps", "DOUBLE", nullable=False),
        SparkFieldSpec("percent_b", "DOUBLE", nullable=False),
        SparkFieldSpec("is_upper_breach", "BOOLEAN", nullable=False),
        SparkFieldSpec("is_lower_breach", "BOOLEAN", nullable=False),
        SparkFieldSpec("volatility_spike", "BOOLEAN", nullable=False),
        SparkFieldSpec("year", "INT", nullable=False),
        SparkFieldSpec("month", "INT", nullable=False),
        SparkFieldSpec("day", "INT", nullable=False),
        SparkFieldSpec("hour", "INT", nullable=False),
        SparkFieldSpec("computed_at", "STRING", nullable=False),
    )
)


def get_pyspark_vwap_schema() -> Any:
    """Return native pyspark.sql.types.StructType if PySpark is available, else SparkStructTypeSpec."""
    try:
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        type_map = {
            "STRING": StringType(),
            "DOUBLE": DoubleType(),
            "LONG": LongType(),
            "INT": IntegerType(),
            "BOOLEAN": BooleanType(),
        }
        return StructType(
            [
                StructField(f.name, type_map[f.data_type], nullable=f.nullable)
                for f in ROLLING_VWAP_STRUCT_SPEC.fields
            ]
        )
    except ImportError:
        return ROLLING_VWAP_STRUCT_SPEC


@dataclass
class RollingVWAPStats:
    """Microstructure aggregates and volatility bands for a specific rolling window."""

    ticker: str
    asset_class: str
    window_start: str
    window_end: str
    window_start_epoch_sec: float
    window_end_epoch_sec: float
    tick_count: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    total_volume: float
    total_notional: float
    vwap: float
    price_stddev: float
    price_variance: float
    volatility_multiplier: float
    upper_band: float
    lower_band: float
    bandwidth_bps: float
    percent_b: float
    is_upper_breach: bool
    is_lower_breach: bool
    volatility_spike: bool
    year: int
    month: int
    day: int
    hour: int
    computed_at: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert stats to JSON string."""
        return json.dumps(self.to_dict())


@dataclass
class RollingVWAPTelemetry:
    """Operational metrics and Prometheus telemetry for VWAP stream processing."""

    windows_computed: int = 0
    vwap_aggregations_emitted: int = 0
    upper_band_breaches: int = 0
    lower_band_breaches: int = 0
    volatility_spikes: int = 0
    latest_vwap_by_ticker: Dict[str, float] = field(default_factory=dict)
    latest_bandwidth_bps_by_ticker: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert telemetry to dictionary."""
        return {
            "windows_computed": self.windows_computed,
            "vwap_aggregations_emitted": self.vwap_aggregations_emitted,
            "upper_band_breaches": self.upper_band_breaches,
            "lower_band_breaches": self.lower_band_breaches,
            "volatility_spikes": self.volatility_spikes,
            "latest_vwap_by_ticker": dict(self.latest_vwap_by_ticker),
            "latest_bandwidth_bps_by_ticker": dict(self.latest_bandwidth_bps_by_ticker),
        }

    def format_prometheus(self, app_name: str = "trading-risk-tick-streaming") -> str:
        """Render Prometheus text exposition format metrics."""
        lines = [
            "# HELP dataproc_vwap_windows_computed_total Total sliding windows evaluated.",
            "# TYPE dataproc_vwap_windows_computed_total counter",
            f'dataproc_vwap_windows_computed_total{{app="{app_name}"}} {self.windows_computed}',
            "# HELP dataproc_vwap_aggregations_emitted_total Total VWAP window stats emitted.",
            "# TYPE dataproc_vwap_aggregations_emitted_total counter",
            f'dataproc_vwap_aggregations_emitted_total{{app="{app_name}"}} {self.vwap_aggregations_emitted}',
            "# HELP dataproc_vwap_upper_band_breaches_total Ticks exceeding upper volatility band.",
            "# TYPE dataproc_vwap_upper_band_breaches_total counter",
            f'dataproc_vwap_upper_band_breaches_total{{app="{app_name}"}} {self.upper_band_breaches}',
            "# HELP dataproc_vwap_lower_band_breaches_total Ticks breaching below lower volatility band.",
            "# TYPE dataproc_vwap_lower_band_breaches_total counter",
            f'dataproc_vwap_lower_band_breaches_total{{app="{app_name}"}} {self.lower_band_breaches}',
            "# HELP dataproc_vwap_volatility_spikes_total Windows where standard deviation exceeds spike threshold.",
            "# TYPE dataproc_vwap_volatility_spikes_total counter",
            f'dataproc_vwap_volatility_spikes_total{{app="{app_name}"}} {self.volatility_spikes}',
        ]
        for ticker, vwap_val in sorted(self.latest_vwap_by_ticker.items()):
            lines.append(
                f'dataproc_rolling_vwap_price{{app="{app_name}",ticker="{ticker}"}} {vwap_val:.4f}'
            )
        for ticker, bw_val in sorted(self.latest_bandwidth_bps_by_ticker.items()):
            lines.append(
                f'dataproc_volatility_bandwidth_bps{{app="{app_name}",ticker="{ticker}"}} {bw_val:.2f}'
            )
        return "\n".join(lines) + "\n"


class RollingVWAPAggregator:
    """Hermetic local rolling VWAP and volatility band aggregation engine."""

    def __init__(self, config: Optional[DataprocStreamingConfig] = None) -> None:
        self.config = config or DataprocStreamingConfig()
        self.config.validate()
        self.telemetry = RollingVWAPTelemetry()

        # Buffer: (ticker, asset_class) -> list of ParsedTickRecord ordered by event_epoch_sec
        self._tick_buffer: Dict[Tuple[str, str], List[ParsedTickRecord]] = {}

    def add_tick(
        self,
        tick: Union[ParsedTickRecord, Dict[str, Any], Any],
    ) -> Optional[ParsedTickRecord]:
        """Ingest a single tick into the aggregator buffer."""
        parsed: Optional[ParsedTickRecord] = None
        if isinstance(tick, ParsedTickRecord):
            parsed = tick
        else:
            res = parse_and_validate_kafka_record(raw_value=tick)
            if isinstance(res, ParsedTickRecord):
                parsed = res

        if parsed is None:
            return None

        key = (parsed.ticker, parsed.asset_class)
        if key not in self._tick_buffer:
            self._tick_buffer[key] = []
        self._tick_buffer[key].append(parsed)
        return parsed

    def add_ticks(
        self,
        ticks: Sequence[Union[ParsedTickRecord, Dict[str, Any], Any]],
    ) -> int:
        """Ingest multiple ticks into the aggregator buffer."""
        count = 0
        for t in ticks:
            if self.add_tick(t) is not None:
                count += 1
        return count

    def evict_older_than(self, threshold_epoch_sec: float) -> int:
        """Evict ticks older than `threshold_epoch_sec` to bound memory."""
        evicted = 0
        for key in list(self._tick_buffer.keys()):
            original_len = len(self._tick_buffer[key])
            self._tick_buffer[key] = [
                t for t in self._tick_buffer[key] if t.event_epoch_sec >= threshold_epoch_sec
            ]
            evicted += original_len - len(self._tick_buffer[key])
            if not self._tick_buffer[key]:
                del self._tick_buffer[key]
        return evicted

    def clear(self) -> None:
        """Clear all buffered ticks."""
        self._tick_buffer.clear()

    def compute_windows(
        self,
        reference_time: Optional[datetime] = None,
    ) -> List[RollingVWAPStats]:
        """Compute rolling VWAP and volatility bands across all buffered ticks.

        For each active `(ticker, asset_class)`, slices ticks into windows of size
        `vwap_window_duration_seconds` sliding by `vwap_slide_duration_seconds`.
        """
        results: List[RollingVWAPStats] = []
        window_size_sec = self.config.vwap_window_duration_seconds
        slide_sec = self.config.vwap_slide_duration_seconds
        k_multiplier = self.config.volatility_k_multiplier
        spike_threshold_pct = self.config.volatility_spike_threshold_pct
        now_iso = (reference_time or datetime.now(timezone.utc)).isoformat()

        for (ticker, asset_class), ticks in sorted(self._tick_buffer.items()):
            if not ticks:
                continue

            # Sort chronological
            sorted_ticks = sorted(ticks, key=lambda t: (t.event_epoch_sec, t.sequence_number))
            min_epoch = sorted_ticks[0].event_epoch_sec
            max_epoch = sorted_ticks[-1].event_epoch_sec

            # Determine window start alignments:
            # First window start is the floor of min_epoch aligned to slide_sec
            first_win_start = math.floor(min_epoch / slide_sec) * slide_sec
            last_win_start = math.floor(max_epoch / slide_sec) * slide_sec

            current_start = first_win_start
            while current_start <= last_win_start:
                current_end = current_start + window_size_sec

                # Find ticks in [current_start, current_end)
                win_ticks = [
                    t
                    for t in sorted_ticks
                    if current_start <= t.event_epoch_sec < current_end
                ]

                if win_ticks:
                    stat = self._calculate_window_stats(
                        ticker=ticker,
                        asset_class=asset_class,
                        ticks=win_ticks,
                        window_start_sec=current_start,
                        window_end_sec=current_end,
                        k_multiplier=k_multiplier,
                        spike_threshold_pct=spike_threshold_pct,
                        computed_at=now_iso,
                    )
                    results.append(stat)

                    # Update telemetry
                    self.telemetry.windows_computed += 1
                    self.telemetry.vwap_aggregations_emitted += 1
                    if stat.is_upper_breach:
                        self.telemetry.upper_band_breaches += 1
                    if stat.is_lower_breach:
                        self.telemetry.lower_band_breaches += 1
                    if stat.volatility_spike:
                        self.telemetry.volatility_spikes += 1
                    self.telemetry.latest_vwap_by_ticker[ticker] = stat.vwap
                    self.telemetry.latest_bandwidth_bps_by_ticker[ticker] = stat.bandwidth_bps

                current_start += slide_sec

        return results

    def _calculate_window_stats(
        self,
        ticker: str,
        asset_class: str,
        ticks: List[ParsedTickRecord],
        window_start_sec: float,
        window_end_sec: float,
        k_multiplier: float,
        spike_threshold_pct: float,
        computed_at: str,
    ) -> RollingVWAPStats:
        """Compute exact VWAP, sample standard deviation, and Bollinger bands for a set of ticks."""
        tick_count = len(ticks)
        open_price = round(ticks[0].last_price, 4)
        close_price = round(ticks[-1].last_price, 4)
        high_price = round(max(t.last_price for t in ticks), 4)
        low_price = round(min(t.last_price for t in ticks), 4)
        total_volume = round(sum(t.volume for t in ticks), 4)
        total_notional = round(sum(t.notional_usd for t in ticks), 4)

        vwap = (
            round(total_notional / total_volume, 4)
            if total_volume > 0
            else close_price
        )

        # Sample standard deviation: s = sqrt(sum((x - mean)^2) / (N - 1))
        if tick_count > 1:
            mean_price = sum(t.last_price for t in ticks) / float(tick_count)
            sample_variance = sum((t.last_price - mean_price) ** 2 for t in ticks) / float(
                tick_count - 1
            )
            price_stddev = round(math.sqrt(sample_variance), 4)
            price_variance = round(sample_variance, 6)
        else:
            price_stddev = 0.0
            price_variance = 0.0

        # Bollinger-style Volatility Bands: VWAP +/- k * sigma
        upper_band = round(vwap + (k_multiplier * price_stddev), 4)
        lower_band = round(vwap - (k_multiplier * price_stddev), 4)

        # Volatility Bandwidth in basis points (1 bp = 0.01% = 0.0001)
        if vwap > 0:
            bandwidth_bps = round(((upper_band - lower_band) / vwap) * 10000.0, 2)
        else:
            bandwidth_bps = 0.0

        # %B = (Last - Lower) / (Upper - Lower)
        band_diff = upper_band - lower_band
        if band_diff > 0:
            percent_b = round((close_price - lower_band) / band_diff, 4)
        else:
            percent_b = 0.5

        is_upper_breach = close_price > upper_band
        is_lower_breach = close_price < lower_band

        # Volatility spike: relative standard deviation exceeds threshold %
        volatility_pct = (price_stddev / vwap * 100.0) if vwap > 0 else 0.0
        volatility_spike = volatility_pct >= spike_threshold_pct

        dt_start = datetime.fromtimestamp(window_start_sec, tz=timezone.utc)
        dt_end = datetime.fromtimestamp(window_end_sec, tz=timezone.utc)

        return RollingVWAPStats(
            ticker=ticker,
            asset_class=asset_class,
            window_start=dt_start.isoformat(),
            window_end=dt_end.isoformat(),
            window_start_epoch_sec=window_start_sec,
            window_end_epoch_sec=window_end_sec,
            tick_count=tick_count,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            total_volume=total_volume,
            total_notional=total_notional,
            vwap=vwap,
            price_stddev=price_stddev,
            price_variance=price_variance,
            volatility_multiplier=k_multiplier,
            upper_band=upper_band,
            lower_band=lower_band,
            bandwidth_bps=bandwidth_bps,
            percent_b=percent_b,
            is_upper_breach=is_upper_breach,
            is_lower_breach=is_lower_breach,
            volatility_spike=volatility_spike,
            year=dt_end.year,
            month=dt_end.month,
            day=dt_end.day,
            hour=dt_end.hour,
            computed_at=computed_at,
        )


# -----------------------------------------------------------------------------
# Native PySpark Structured Streaming Window Aggregation Builders
# -----------------------------------------------------------------------------


def build_rolling_vwap_stream(
    valid_ticks_df: Any,
    config: DataprocStreamingConfig,
) -> Any:
    """Build the PySpark Structured Streaming rolling VWAP & volatility band aggregation DataFrame.

    Applies:
    - Grouping by `window(event_timestamp, window_duration, slide_duration)`, `ticker`, and `asset_class`.
    - Microstructure aggregation: count, open, high, low, close, volume, notional, stddev_samp.
    - Derived columns: vwap, upper_band, lower_band, bandwidth_bps, percent_b, is_upper_breach,
      is_lower_breach, volatility_spike, and temporal partition columns (year, month, day, hour).
    """
    try:
        from pyspark.sql import functions as F

        window_duration = config.vwap_window_duration
        slide_duration = config.vwap_slide_duration
        k_mult = config.volatility_k_multiplier
        spike_thresh = config.volatility_spike_threshold_pct

        # 1. Group by window and ticker
        windowed_df = (
            valid_ticks_df.groupBy(
                F.window(F.col("event_timestamp"), window_duration, slide_duration).alias("win"),
                F.col("ticker"),
                F.col("asset_class"),
            )
            .agg(
                F.count(F.lit(1)).alias("tick_count"),
                F.first("last_price").alias("open_price"),
                F.max("last_price").alias("high_price"),
                F.min("last_price").alias("low_price"),
                F.last("last_price").alias("close_price"),
                F.round(F.sum("volume"), 4).alias("total_volume"),
                F.round(F.sum("notional_usd"), 4).alias("total_notional"),
                F.stddev_samp("last_price").alias("raw_stddev"),
            )
        )

        # 2. Derive VWAP and Volatility Bands
        vwap_expr = F.when(
            F.col("total_volume") > 0,
            F.round(F.col("total_notional") / F.col("total_volume"), 4),
        ).otherwise(F.col("close_price"))

        stddev_expr = F.coalesce(F.round(F.col("raw_stddev"), 4), F.lit(0.0))
        variance_expr = F.round(F.pow(stddev_expr, 2), 6)

        upper_band_expr = F.round(vwap_expr + (F.lit(k_mult) * stddev_expr), 4)
        lower_band_expr = F.round(vwap_expr - (F.lit(k_mult) * stddev_expr), 4)

        band_diff = upper_band_expr - lower_band_expr
        bandwidth_bps_expr = F.when(
            vwap_expr > 0,
            F.round((band_diff / vwap_expr) * 10000.0, 2),
        ).otherwise(F.lit(0.0))

        percent_b_expr = F.when(
            band_diff > 0,
            F.round((F.col("close_price") - lower_band_expr) / band_diff, 4),
        ).otherwise(F.lit(0.5))

        upper_breach_expr = F.col("close_price") > upper_band_expr
        lower_breach_expr = F.col("close_price") < lower_band_expr

        volatility_spike_expr = F.when(
            vwap_expr > 0,
            (stddev_expr / vwap_expr * 100.0) >= F.lit(spike_thresh),
        ).otherwise(F.lit(False))

        return (
            windowed_df.select(
                F.col("ticker"),
                F.col("asset_class"),
                F.date_format(F.col("win.start"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias(
                    "window_start"
                ),
                F.date_format(F.col("win.end"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias(
                    "window_end"
                ),
                F.col("tick_count"),
                F.col("open_price"),
                F.col("high_price"),
                F.col("low_price"),
                F.col("close_price"),
                F.col("total_volume"),
                F.col("total_notional"),
                vwap_expr.alias("vwap"),
                stddev_expr.alias("price_stddev"),
                variance_expr.alias("price_variance"),
                F.lit(k_mult).alias("volatility_multiplier"),
                upper_band_expr.alias("upper_band"),
                lower_band_expr.alias("lower_band"),
                bandwidth_bps_expr.alias("bandwidth_bps"),
                percent_b_expr.alias("percent_b"),
                upper_breach_expr.alias("is_upper_breach"),
                lower_breach_expr.alias("is_lower_breach"),
                volatility_spike_expr.alias("volatility_spike"),
                F.year(F.col("win.end")).alias("year"),
                F.month(F.col("win.end")).alias("month"),
                F.dayofmonth(F.col("win.end")).alias("day"),
                F.hour(F.col("win.end")).alias("hour"),
                F.current_timestamp().alias("computed_at"),
            )
        )

    except ImportError:
        # Mock / non-JVM unit test fallback
        return valid_ticks_df.groupBy("ticker", "asset_class")


def start_vwap_writer(
    vwap_df: Any,
    config: DataprocStreamingConfig,
) -> Any:
    """Start the PySpark Structured Streaming query writing rolling VWAP Parquet to GCS."""
    checkpoint = str(config.vwap_checkpoint_location or f"{config.checkpoint_location.rstrip('/')}/vwap")
    output_path = str(config.vwap_output_path or f"{config.bronze_output_path.rstrip('/')}/vwap")

    return (
        vwap_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("year", "month", "day", "asset_class")
        .trigger(processingTime=config.trigger_processing_time)
        .queryName(f"{config.app_name}-vwap-sink")
        .start()
    )
