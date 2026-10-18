"""Dataflow Sliding Window Transforms & Custom DoFn Anomaly Detectors (Day 09).

Provides:
- RiskAlert: Enterprise risk anomaly data model aligned with Cloud SQL `risk_alerts` DDL.
- WindowDrawdownStats: Aggregated 5-minute sliding window microstructure & exposure metrics.
- KeyByPortfolioAndTickerFn: Deterministic composite key extractor (`portfolio_id#ticker`).
- DetectSlidingWindowDrawdownFn: Custom Beam DoFn computing chronological peak-to-trough
  drawdown, open-to-close drop, rolling VWAP, and multi-tier risk breach alerts.
- EvaluateSingleOrderSpikeFn: Zero-latency per-order fat-finger notional anomaly detector.
- SlidingWindowAnomalyTransform: Composite PTransform applying 5-minute sliding windows
  (duration=300s, period=60s), grouping by key, and branching into window stats and alerts.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple, Union
import uuid

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics
from apache_beam.transforms import window

from ingestion.pubsub_client import TradeOrder

logger = logging.getLogger(__name__)

# Tagged output identifiers for sliding window anomaly routing
TAG_WINDOW_STATS = "window_stats"
TAG_RISK_ALERTS = "risk_alerts"

VALID_SEVERITIES = ("LOW", "MEDIUM", "HIGH", "CRITICAL")
VALID_ALERT_STATUSES = ("ACTIVE", "ACKNOWLEDGED", "RESOLVED", "DISMISSED")


@dataclass
class RiskAlert:
    """Represents a high-priority operational risk anomaly alert.

    Schema maps 1:1 to `terraform/modules/cloudsql/init_schema.sql` (`risk_alerts` table).
    """

    alert_id: str
    portfolio_id: str
    ticker: str
    asset_class: str
    alert_type: str
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    metric_name: str
    threshold_value: float
    observed_value: float
    event_timestamp: str
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "ACTIVE"
    payload_json: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        portfolio_id: str,
        ticker: str,
        asset_class: str,
        alert_type: str,
        severity: str,
        metric_name: str,
        threshold_value: float,
        observed_value: float,
        event_timestamp: Optional[str] = None,
        payload_json: Optional[Dict[str, Any]] = None,
        alert_id: Optional[str] = None,
        status: str = "ACTIVE",
    ) -> "RiskAlert":
        """Factory constructor with invariant validation and UUID generation."""
        sev = severity.strip().upper()
        if sev not in VALID_SEVERITIES:
            raise ValueError(f"Invalid severity '{severity}'. Must be one of {VALID_SEVERITIES}.")

        st = status.strip().upper()
        if st not in VALID_ALERT_STATUSES:
            raise ValueError(f"Invalid alert status '{status}'. Must be one of {VALID_ALERT_STATUSES}.")

        now_iso = datetime.now(timezone.utc).isoformat()
        return cls(
            alert_id=alert_id or str(uuid.uuid4()),
            portfolio_id=portfolio_id.strip(),
            ticker=ticker.strip().upper(),
            asset_class=asset_class.strip().upper(),
            alert_type=alert_type.strip().upper(),
            severity=sev,
            metric_name=metric_name.strip(),
            threshold_value=round(float(threshold_value), 6),
            observed_value=round(float(observed_value), 6),
            event_timestamp=event_timestamp or now_iso,
            created_at=now_iso,
            status=st,
            payload_json=payload_json or {},
        )

    def validate(self) -> None:
        """Validate schema constraints against Cloud SQL PostgreSQL DDL rules."""
        if not self.alert_id or not self.alert_id.strip():
            raise ValueError("alert_id must be non-empty")
        if not self.portfolio_id or not self.portfolio_id.strip():
            raise ValueError("portfolio_id must be non-empty")
        if not self.ticker or not self.ticker.strip():
            raise ValueError("ticker must be non-empty")
        if self.severity not in VALID_SEVERITIES:
            raise ValueError(f"Invalid severity: {self.severity}")
        if self.status not in VALID_ALERT_STATUSES:
            raise ValueError(f"Invalid status: {self.status}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary representation."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize alert to JSON string."""
        return json.dumps(self.to_dict())

    def to_bytes(self) -> bytes:
        """Serialize alert to UTF-8 bytes."""
        return self.to_json().encode("utf-8")

    def to_pubsub_message(self) -> PubsubMessage:
        """Convert alert to a Cloud Pub/Sub message with routing attributes."""
        ordering_key = f"{self.portfolio_id}#{self.ticker}"
        return PubsubMessage(
            data=self.to_bytes(),
            attributes={
                "alert_id": self.alert_id,
                "portfolio_id": self.portfolio_id,
                "ticker": self.ticker,
                "asset_class": self.asset_class,
                "alert_type": self.alert_type,
                "severity": self.severity,
                "status": self.status,
            },
            ordering_key=ordering_key,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RiskAlert":
        """Reconstruct a RiskAlert from a dictionary."""
        return cls(
            alert_id=data["alert_id"],
            portfolio_id=data["portfolio_id"],
            ticker=data["ticker"],
            asset_class=data["asset_class"],
            alert_type=data["alert_type"],
            severity=data["severity"],
            metric_name=data["metric_name"],
            threshold_value=float(data["threshold_value"]),
            observed_value=float(data["observed_value"]),
            event_timestamp=data["event_timestamp"],
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
            status=data.get("status", "ACTIVE"),
            payload_json=data.get("payload_json", {}),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "RiskAlert":
        """Deserialize a RiskAlert from a JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class WindowDrawdownStats:
    """Quantitative metrics computed over a 5-minute sliding window per (portfolio_id, ticker)."""

    portfolio_id: str
    ticker: str
    asset_class: str
    window_start: str
    window_end: str
    order_count: int
    buy_count: int
    sell_count: int
    open_price: float
    close_price: float
    peak_price: float
    trough_price: float
    vwap: float
    max_drawdown_pct: float
    window_drop_pct: float
    total_notional_usd: float
    buy_notional_usd: float
    sell_notional_usd: float
    net_delta_usd: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert window summary statistics to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize window statistics to JSON string."""
        return json.dumps(self.to_dict())


def classify_drawdown_severity(observed_drop: float, threshold: float) -> str:
    """Classify risk breach severity based on multiple of configured drawdown threshold."""
    if threshold <= 0:
        return "HIGH"
    ratio = observed_drop / threshold
    if ratio >= 2.0:
        return "CRITICAL"
    if ratio >= 1.5:
        return "HIGH"
    if ratio >= 1.0:
        return "MEDIUM"
    return "LOW"


class KeyByPortfolioAndTickerFn(beam.DoFn):
    """Extracts composite key `portfolio_id#ticker` for group-by-key windowing."""

    def process(self, element: Any) -> Generator[Tuple[str, TradeOrder], None, None]:
        """Emit `(portfolio_id#ticker, TradeOrder)` tuple."""
        if isinstance(element, TradeOrder):
            order = element
        elif isinstance(element, dict):
            order = TradeOrder.from_json(json.dumps(element))
        else:
            return
        yield (f"{order.portfolio_id}#{order.ticker}", order)


class DetectSlidingWindowDrawdownFn(beam.DoFn):
    """Custom Beam DoFn evaluating 5-minute sliding windows for flash-crash drawdowns & exposure breaches.

    Calculates:
    1. Chronological peak-to-trough drawdown (`max_drawdown_pct`) and open-to-close drop (`window_drop_pct`).
    2. Volume-Weighted Average Price (`vwap`) across all fills in the sliding window.
    3. Cumulative gross notional exposure (`total_notional_usd`) and directional sell pressure.
    """

    def __init__(
        self,
        max_drop_threshold: float = 0.05,
        max_window_notional_usd: float = 2_500_000.0,
        sell_pressure_ratio_threshold: float = 0.85,
        sell_pressure_min_orders: int = 4,
        sell_pressure_notional_floor: float = 250_000.0,
    ) -> None:
        super().__init__()
        if max_drop_threshold <= 0 or max_drop_threshold >= 1.0:
            raise ValueError(f"max_drop_threshold must be in (0, 1), got {max_drop_threshold}")
        if max_window_notional_usd <= 0:
            raise ValueError(f"max_window_notional_usd must be > 0, got {max_window_notional_usd}")

        self.max_drop_threshold = float(max_drop_threshold)
        self.max_window_notional_usd = float(max_window_notional_usd)
        self.sell_pressure_ratio_threshold = float(sell_pressure_ratio_threshold)
        self.sell_pressure_min_orders = int(sell_pressure_min_orders)
        self.sell_pressure_notional_floor = float(sell_pressure_notional_floor)

        # Beam Metrics
        self.windows_evaluated = Metrics.counter("risk_anomaly_detector", "windows_evaluated")
        self.drawdown_alerts_counter = Metrics.counter("risk_anomaly_detector", "drawdown_alerts_emitted")
        self.notional_alerts_counter = Metrics.counter("risk_anomaly_detector", "notional_alerts_emitted")
        self.sell_pressure_alerts_counter = Metrics.counter("risk_anomaly_detector", "sell_pressure_alerts_emitted")
        self.max_drawdown_bps_dist = Metrics.distribution("risk_anomaly_detector", "max_drawdown_bps")

    def process(
        self,
        element: Any,
        window_param: Any = beam.DoFn.WindowParam,
    ) -> Generator[Any, None, None]:
        """Evaluate all orders within a sliding window for a `(portfolio_id#ticker)` key."""
        key, orders_iter = element
        orders: List[TradeOrder] = list(orders_iter)
        if not orders:
            return

        self.windows_evaluated.inc()

        # Sort chronologically by ISO timestamp then sequence_number
        orders.sort(key=lambda o: (o.timestamp, o.sequence_number))

        first_order = orders[0]
        last_order = orders[-1]
        portfolio_id = first_order.portfolio_id
        ticker = first_order.ticker
        asset_class = first_order.asset_class

        # Extract window bounds if running inside a windowed PCollection
        if window_param is not None and hasattr(window_param, "start") and hasattr(window_param, "end"):
            try:
                win_start_iso = datetime.fromtimestamp(float(window_param.start), tz=timezone.utc).isoformat()
                win_end_iso = datetime.fromtimestamp(float(window_param.end), tz=timezone.utc).isoformat()
            except Exception:
                win_start_iso = first_order.timestamp
                win_end_iso = last_order.timestamp
        else:
            win_start_iso = first_order.timestamp
            win_end_iso = last_order.timestamp

        # Compute chronological peak-to-trough drawdown & microstructure aggregates
        open_price = first_order.price
        close_price = last_order.price
        peak_price = first_order.price
        trough_price = first_order.price
        running_peak = first_order.price
        max_drawdown_pct = 0.0

        total_qty = 0.0
        total_pv = 0.0
        total_notional = 0.0
        buy_notional = 0.0
        sell_notional = 0.0
        buy_count = 0
        sell_count = 0

        for o in orders:
            px = float(o.price)
            qty = float(o.quantity)
            notional = float(o.notional_usd)

            if px > peak_price:
                peak_price = px
            if px < trough_price:
                trough_price = px

            if px > running_peak:
                running_peak = px
            elif running_peak > 0:
                dd = (running_peak - px) / running_peak
                if dd > max_drawdown_pct:
                    max_drawdown_pct = dd

            total_qty += qty
            total_pv += px * qty
            total_notional += notional

            if o.side == "BUY":
                buy_count += 1
                buy_notional += notional
            else:
                sell_count += 1
                sell_notional += notional

        vwap = round(total_pv / total_qty, 4) if total_qty > 0 else close_price
        window_drop_pct = max(0.0, (open_price - close_price) / open_price) if open_price > 0 else 0.0
        effective_drop_pct = max(max_drawdown_pct, window_drop_pct)

        self.max_drawdown_bps_dist.update(int(round(effective_drop_pct * 10000)))

        stats = WindowDrawdownStats(
            portfolio_id=portfolio_id,
            ticker=ticker,
            asset_class=asset_class,
            window_start=win_start_iso,
            window_end=win_end_iso,
            order_count=len(orders),
            buy_count=buy_count,
            sell_count=sell_count,
            open_price=round(open_price, 4),
            close_price=round(close_price, 4),
            peak_price=round(peak_price, 4),
            trough_price=round(trough_price, 4),
            vwap=vwap,
            max_drawdown_pct=round(max_drawdown_pct, 6),
            window_drop_pct=round(window_drop_pct, 6),
            total_notional_usd=round(total_notional, 2),
            buy_notional_usd=round(buy_notional, 2),
            sell_notional_usd=round(sell_notional, 2),
            net_delta_usd=round(buy_notional - sell_notional, 2),
        )

        # Always emit window statistics to the main output
        yield stats

        common_payload = {
            "key": key,
            "window_start": win_start_iso,
            "window_end": win_end_iso,
            "order_count": len(orders),
            "open_price": stats.open_price,
            "close_price": stats.close_price,
            "peak_price": stats.peak_price,
            "trough_price": stats.trough_price,
            "vwap": stats.vwap,
            "max_drawdown_pct": stats.max_drawdown_pct,
            "window_drop_pct": stats.window_drop_pct,
            "total_notional_usd": stats.total_notional_usd,
            "first_order_id": first_order.order_id,
            "last_order_id": last_order.order_id,
        }

        # Rule 1: 5-Minute Sliding Window Flash Crash / Price Drop Detection
        if effective_drop_pct >= self.max_drop_threshold:
            self.drawdown_alerts_counter.inc()
            severity = classify_drawdown_severity(effective_drop_pct, self.max_drop_threshold)
            alert = RiskAlert.create(
                portfolio_id=portfolio_id,
                ticker=ticker,
                asset_class=asset_class,
                alert_type="FLASH_CRASH_DRAWDOWN",
                severity=severity,
                metric_name="rolling_5m_drawdown_pct",
                threshold_value=self.max_drop_threshold,
                observed_value=effective_drop_pct,
                event_timestamp=last_order.timestamp,
                payload_json=common_payload,
            )
            yield pvalue.TaggedOutput(TAG_RISK_ALERTS, alert)

        # Rule 2: Cumulative Window Notional Exposure Breach
        if total_notional >= self.max_window_notional_usd:
            self.notional_alerts_counter.inc()
            ratio = total_notional / self.max_window_notional_usd
            severity = "CRITICAL" if ratio >= 2.0 else "HIGH"
            alert = RiskAlert.create(
                portfolio_id=portfolio_id,
                ticker=ticker,
                asset_class=asset_class,
                alert_type="NOTIONAL_EXPOSURE_BREACH",
                severity=severity,
                metric_name="rolling_5m_notional_usd",
                threshold_value=self.max_window_notional_usd,
                observed_value=total_notional,
                event_timestamp=last_order.timestamp,
                payload_json=common_payload,
            )
            yield pvalue.TaggedOutput(TAG_RISK_ALERTS, alert)

        # Rule 3: Concentrated Institutional Sell-Off Pressure
        if len(orders) >= self.sell_pressure_min_orders and total_notional >= self.sell_pressure_notional_floor:
            sell_ratio = sell_notional / total_notional if total_notional > 0 else 0.0
            if sell_ratio >= self.sell_pressure_ratio_threshold:
                self.sell_pressure_alerts_counter.inc()
                alert = RiskAlert.create(
                    portfolio_id=portfolio_id,
                    ticker=ticker,
                    asset_class=asset_class,
                    alert_type="CONCENTRATED_SELL_PRESSURE",
                    severity="HIGH" if sell_ratio >= 0.95 else "MEDIUM",
                    metric_name="rolling_5m_sell_notional_ratio",
                    threshold_value=self.sell_pressure_ratio_threshold,
                    observed_value=sell_ratio,
                    event_timestamp=last_order.timestamp,
                    payload_json={**common_payload, "sell_ratio": round(sell_ratio, 4)},
                )
                yield pvalue.TaggedOutput(TAG_RISK_ALERTS, alert)


class EvaluateSingleOrderSpikeFn(beam.DoFn):
    """Zero-latency DoFn detecting fat-finger single-order notional limit breaches."""

    def __init__(self, single_order_notional_limit_usd: float = 1_000_000.0) -> None:
        super().__init__()
        if single_order_notional_limit_usd <= 0:
            raise ValueError("single_order_notional_limit_usd must be positive")
        self.limit_usd = float(single_order_notional_limit_usd)
        self.fat_finger_alerts = Metrics.counter("risk_anomaly_detector", "fat_finger_alerts_emitted")

    def process(self, order: Any) -> Generator[Any, None, None]:
        """Pass through valid order and emit RiskAlert if single-order notional breaches limit."""
        yield order
        if order.notional_usd >= self.limit_usd:
            self.fat_finger_alerts.inc()
            severity = "CRITICAL" if order.notional_usd >= 2.0 * self.limit_usd else "HIGH"
            alert = RiskAlert.create(
                portfolio_id=order.portfolio_id,
                ticker=order.ticker,
                asset_class=order.asset_class,
                alert_type="SINGLE_ORDER_FAT_FINGER",
                severity=severity,
                metric_name="single_order_notional_usd",
                threshold_value=self.limit_usd,
                observed_value=order.notional_usd,
                event_timestamp=order.timestamp,
                payload_json={
                    "order_id": order.order_id,
                    "side": order.side,
                    "order_type": order.order_type,
                    "quantity": order.quantity,
                    "price": order.price,
                    "trader_id": order.trader_id,
                },
            )
            yield pvalue.TaggedOutput(TAG_RISK_ALERTS, alert)


class FormatRiskAlertForPubSubFn(beam.DoFn):
    """Formats a RiskAlert into a PubsubMessage for downstream notification topics."""

    def process(self, element: Any) -> Generator[PubsubMessage, None, None]:
        """Convert RiskAlert to PubsubMessage with attributes and ordering key."""
        if isinstance(element, RiskAlert):
            yield element.to_pubsub_message()
        elif isinstance(element, dict):
            yield RiskAlert.from_dict(element).to_pubsub_message()


class SlidingWindowAnomalyTransform(beam.PTransform):
    """Composite PTransform applying 5-minute sliding windows and custom DoFn anomaly detection."""

    def __init__(
        self,
        window_duration_seconds: int = 300,
        window_slide_seconds: int = 60,
        max_drop_threshold: float = 0.05,
        max_window_notional_usd: float = 2_500_000.0,
    ) -> None:
        super().__init__()
        self.window_duration_seconds = int(window_duration_seconds)
        self.window_slide_seconds = int(window_slide_seconds)
        self.max_drop_threshold = float(max_drop_threshold)
        self.max_window_notional_usd = float(max_window_notional_usd)

    def expand(self, pcoll: Any) -> Any:
        """Apply SlidingWindows, key by portfolio#ticker, group, and detect drawdown anomalies."""
        return (
            pcoll
            | "ApplySlidingWindow"
            >> beam.WindowInto(
                window.SlidingWindows(
                    size=self.window_duration_seconds,
                    period=self.window_slide_seconds,
                )
            )
            | "KeyByPortfolioAndTicker" >> beam.ParDo(KeyByPortfolioAndTickerFn())
            | "GroupOrdersByWindowKey" >> beam.GroupByKey()
            | "DetectWindowAnomalies"
            >> beam.ParDo(
                DetectSlidingWindowDrawdownFn(
                    max_drop_threshold=self.max_drop_threshold,
                    max_window_notional_usd=self.max_window_notional_usd,
                )
            ).with_outputs(TAG_RISK_ALERTS, main=TAG_WINDOW_STATS)
        )
