"""Zero-Cost Unit and Beam Integration Tests for Dataflow Sliding Window Anomaly Detection (Day 09).

Validates:
- RiskAlert: schema invariants, Cloud SQL DDL alignment, severity levels, JSON roundtrip,
  and PubsubMessage conversion.
- KeyByPortfolioAndTickerFn: composite key extraction (`portfolio_id#ticker`).
- DetectSlidingWindowDrawdownFn:
  - Accurate VWAP, peak/trough, net delta, and chronological drawdown calculations.
  - 5-minute rolling price drop / flash-crash detection (`FLASH_CRASH_DRAWDOWN`).
  - Dynamic severity scaling (`MEDIUM`, `HIGH`, `CRITICAL`) by threshold multiple.
  - Out-of-order event arrival resilience via chronological timestamp + sequence sorting.
  - Cumulative 5-minute window notional breach detection (`NOTIONAL_EXPOSURE_BREACH`).
  - Concentrated institutional sell-off detection (`CONCENTRATED_SELL_PRESSURE`).
- EvaluateSingleOrderSpikeFn: zero-latency single-order fat-finger limit breach detection.
- SlidingWindowAnomalyTransform & Full Pipeline Integration (`DirectRunner`):
  - End-to-end 5-minute sliding window (`duration=300s`, `period=60s`) execution over
    event-time stamped trade order streams.
"""

from datetime import datetime, timezone
import json
from typing import Any, Dict, List
import pytest

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from ingestion.pubsub_client import TradeOrder
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
from streaming.dataflow.options import RiskEnginePipelineOptions
from streaming.dataflow.pipeline import build_pipeline


# -----------------------------------------------------------------------------
# 1. RISK ALERT DATA MODEL & SEVERITY CLASSIFIER TESTS
# -----------------------------------------------------------------------------
class TestRiskAlertModel:
    """Validate RiskAlert schema, invariants, and serialization."""

    def test_risk_alert_creation_and_validation(self) -> None:
        """Verify valid RiskAlert creation and Cloud SQL field alignment."""
        alert = RiskAlert.create(
            portfolio_id="PORT-ALPHA",
            ticker="nvda",
            asset_class="equity",
            alert_type="FLASH_CRASH_DRAWDOWN",
            severity="critical",
            metric_name="rolling_5m_drawdown_pct",
            threshold_value=0.05,
            observed_value=0.1125,
            event_timestamp="2026-10-18T12:04:00+00:00",
            payload_json={"peak_price": 140.0, "trough_price": 124.25},
        )
        alert.validate()
        assert alert.ticker == "NVDA"
        assert alert.asset_class == "EQUITY"
        assert alert.severity == "CRITICAL"
        assert alert.status == "ACTIVE"
        assert alert.threshold_value == 0.05
        assert alert.observed_value == 0.1125

    def test_risk_alert_rejects_invalid_severity_or_status(self) -> None:
        """Verify invalid severity or status raises ValueError."""
        with pytest.raises(ValueError, match="Invalid severity"):
            RiskAlert.create(
                portfolio_id="PORT-1",
                ticker="AAPL",
                asset_class="EQUITY",
                alert_type="FLASH_CRASH_DRAWDOWN",
                severity="EXTREME",
                metric_name="drawdown",
                threshold_value=0.05,
                observed_value=0.08,
            )

        with pytest.raises(ValueError, match="Invalid alert status"):
            RiskAlert.create(
                portfolio_id="PORT-1",
                ticker="AAPL",
                asset_class="EQUITY",
                alert_type="FLASH_CRASH_DRAWDOWN",
                severity="HIGH",
                metric_name="drawdown",
                threshold_value=0.05,
                observed_value=0.08,
                status="DELETED",
            )

    def test_risk_alert_json_and_pubsub_roundtrip(self) -> None:
        """Verify JSON serialization roundtrip and PubsubMessage formatting."""
        alert = RiskAlert.create(
            portfolio_id="PORT-MACRO",
            ticker="BTCUSD",
            asset_class="CRYPTO",
            alert_type="NOTIONAL_EXPOSURE_BREACH",
            severity="HIGH",
            metric_name="rolling_5m_notional_usd",
            threshold_value=2500000.0,
            observed_value=3200000.0,
        )
        json_str = alert.to_json()
        reconstructed = RiskAlert.from_json(json_str)
        assert reconstructed.alert_id == alert.alert_id
        assert reconstructed.portfolio_id == "PORT-MACRO"
        assert reconstructed.observed_value == 3200000.0

        pubsub_msg = alert.to_pubsub_message()
        assert isinstance(pubsub_msg, PubsubMessage)
        assert pubsub_msg.ordering_key == "PORT-MACRO#BTCUSD"
        assert pubsub_msg.attributes["severity"] == "HIGH"
        assert pubsub_msg.attributes["alert_type"] == "NOTIONAL_EXPOSURE_BREACH"

    @pytest.mark.parametrize(
        "observed,threshold,expected_sev",
        [
            (0.052, 0.05, "MEDIUM"),
            (0.076, 0.05, "HIGH"),
            (0.105, 0.05, "CRITICAL"),
            (0.020, 0.05, "LOW"),
        ],
    )
    def test_classify_drawdown_severity(
        self, observed: float, threshold: float, expected_sev: str
    ) -> None:
        """Verify severity scaling by threshold multiple."""
        assert classify_drawdown_severity(observed, threshold) == expected_sev


# -----------------------------------------------------------------------------
# 2. KEY EXTRACTION & SLIDING WINDOW DRAWDOWN DO-FN UNIT TESTS
# -----------------------------------------------------------------------------
class TestDetectSlidingWindowDrawdownFn:
    """Validate custom DoFn anomaly detection logic in isolation."""

    @staticmethod
    def _make_order(
        order_id: str,
        price: float,
        quantity: float = 100.0,
        side: str = "BUY",
        timestamp: str = "2026-10-18T12:00:00+00:00",
        seq: int = 1,
        portfolio_id: str = "PORT-ALPHA",
        ticker: str = "NVDA",
    ) -> TradeOrder:
        return TradeOrder.create(
            order_id=order_id,
            portfolio_id=portfolio_id,
            ticker=ticker,
            asset_class="EQUITY",
            side=side,
            order_type="LIMIT",
            quantity=quantity,
            price=price,
            timestamp=timestamp,
            sequence_number=seq,
        )

    def test_key_by_portfolio_and_ticker_fn(self) -> None:
        """Verify KeyByPortfolioAndTickerFn extracts composite key."""
        key_fn = KeyByPortfolioAndTickerFn()
        order = self._make_order("ORD-1", 140.0, portfolio_id="PORT-ARB", ticker="MSFT")
        out = list(key_fn.process(order))
        assert len(out) == 1
        assert out[0][0] == "PORT-ARB#MSFT"
        assert out[0][1].order_id == "ORD-1"

    def test_normal_window_emits_stats_without_alerts(self) -> None:
        """Verify benign price fluctuations (< 5% drop) emit WindowDrawdownStats and 0 alerts."""
        detector = DetectSlidingWindowDrawdownFn(max_drop_threshold=0.05)
        orders = [
            self._make_order("ORD-1", 100.0, quantity=50.0, side="BUY", timestamp="2026-10-18T12:00:10+00:00", seq=1),
            self._make_order("ORD-2", 101.0, quantity=50.0, side="BUY", timestamp="2026-10-18T12:01:10+00:00", seq=2),
            self._make_order("ORD-3", 99.5, quantity=100.0, side="SELL", timestamp="2026-10-18T12:02:10+00:00", seq=3),
        ]

        results = list(detector.process(("PORT-ALPHA#NVDA", orders), window_param=None))
        assert len(results) == 1
        stats = results[0]
        assert isinstance(stats, WindowDrawdownStats)
        assert stats.order_count == 3
        assert stats.buy_count == 2
        assert stats.sell_count == 1
        assert stats.open_price == 100.0
        assert stats.close_price == 99.5
        assert stats.peak_price == 101.0
        assert stats.trough_price == 99.5
        # Peak-to-trough: (101.0 - 99.5) / 101.0 = 0.014851 (< 0.05)
        assert stats.max_drawdown_pct == pytest.approx(0.014851, abs=1e-5)
        # VWAP: (100*50 + 101*50 + 99.5*100) / 200 = 20000 / 200 = 100.0
        assert stats.vwap == 100.0

    def test_5min_flash_crash_triggers_drawdown_alert(self) -> None:
        """Verify >= 5% rolling price drop emits FLASH_CRASH_DRAWDOWN alert."""
        detector = DetectSlidingWindowDrawdownFn(max_drop_threshold=0.05)
        orders = [
            self._make_order("ORD-1", 200.0, quantity=20.0, timestamp="2026-10-18T12:00:00+00:00", seq=1),
            self._make_order("ORD-2", 210.0, quantity=20.0, timestamp="2026-10-18T12:01:00+00:00", seq=2),
            self._make_order("ORD-3", 189.0, quantity=20.0, timestamp="2026-10-18T12:03:00+00:00", seq=3),  # 10% drop from 210 peak
        ]

        results = list(detector.process(("PORT-ALPHA#NVDA", orders), window_param=None))
        stats_list = [r for r in results if isinstance(r, WindowDrawdownStats)]
        alerts_list = [
            r.value for r in results if isinstance(r, pvalue.TaggedOutput) and r.tag == TAG_RISK_ALERTS
        ]

        assert len(stats_list) == 1
        assert stats_list[0].max_drawdown_pct == pytest.approx(0.10, abs=1e-5)

        assert len(alerts_list) == 1
        alert: RiskAlert = alerts_list[0]
        assert alert.alert_type == "FLASH_CRASH_DRAWDOWN"
        assert alert.severity == "CRITICAL"  # 0.10 >= 2 * 0.05
        assert alert.observed_value == pytest.approx(0.10, abs=1e-5)
        assert alert.payload_json["peak_price"] == 210.0
        assert alert.payload_json["trough_price"] == 189.0

    def test_chronological_sorting_prevents_false_drawdown_on_rally(self) -> None:
        """Verify out-of-order arrival of a price rally does not trigger a false drawdown."""
        detector = DetectSlidingWindowDrawdownFn(max_drop_threshold=0.05)
        # Chronologically price goes UP from 100 -> 120 (+20% rally, 0% drawdown),
        # but elements arrive out of order in the iterable:
        orders_out_of_order = [
            self._make_order("ORD-LATE", 120.0, timestamp="2026-10-18T12:04:00+00:00", seq=2),
            self._make_order("ORD-EARLY", 100.0, timestamp="2026-10-18T12:00:00+00:00", seq=1),
        ]

        results = list(detector.process(("PORT-ALPHA#NVDA", orders_out_of_order), window_param=None))
        assert len(results) == 1
        stats = results[0]
        assert isinstance(stats, WindowDrawdownStats)
        assert stats.open_price == 100.0
        assert stats.close_price == 120.0
        assert stats.max_drawdown_pct == 0.0
        assert stats.window_drop_pct == 0.0

    def test_window_notional_exposure_breach_alert(self) -> None:
        """Verify cumulative window notional >= threshold triggers NOTIONAL_EXPOSURE_BREACH."""
        detector = DetectSlidingWindowDrawdownFn(
            max_drop_threshold=0.05,
            max_window_notional_usd=1_000_000.0,
        )
        # 3 orders of $400,000 each = $1,200,000 (> $1,000,000 limit)
        orders = [
            self._make_order(f"ORD-{i}", 400.0, quantity=1000.0, side="BUY", seq=i)
            for i in range(1, 4)
        ]

        results = list(detector.process(("PORT-ALPHA#NVDA", orders), window_param=None))
        alerts = [r.value for r in results if isinstance(r, pvalue.TaggedOutput)]
        assert len(alerts) == 1
        assert alerts[0].alert_type == "NOTIONAL_EXPOSURE_BREACH"
        assert alerts[0].observed_value == 1_200_000.0

    def test_concentrated_sell_pressure_alert(self) -> None:
        """Verify heavy one-sided sell flow triggers CONCENTRATED_SELL_PRESSURE."""
        detector = DetectSlidingWindowDrawdownFn(
            max_drop_threshold=0.10,  # keep price drop below threshold
            max_window_notional_usd=10_000_000.0,
            sell_pressure_ratio_threshold=0.85,
            sell_pressure_min_orders=4,
            sell_pressure_notional_floor=200_000.0,
        )
        orders = [
            self._make_order(f"ORD-SELL-{i}", 100.0, quantity=1000.0, side="SELL", seq=i)
            for i in range(1, 5)
        ]  # 4 sell orders totaling $400,000 (100% sell ratio)

        results = list(detector.process(("PORT-ALPHA#NVDA", orders), window_param=None))
        alerts = [r.value for r in results if isinstance(r, pvalue.TaggedOutput)]
        assert len(alerts) == 1
        assert alerts[0].alert_type == "CONCENTRATED_SELL_PRESSURE"
        assert alerts[0].severity == "HIGH"
        assert alerts[0].observed_value == 1.0


# -----------------------------------------------------------------------------
# 3. SINGLE ORDER FAT-FINGER DETECTOR TESTS
# -----------------------------------------------------------------------------
class TestEvaluateSingleOrderSpikeFn:
    """Validate zero-latency single-order fat-finger anomaly detection."""

    def test_single_order_fat_finger_detection(self) -> None:
        """Verify orders exceeding single-order notional limit emit SINGLE_ORDER_FAT_FINGER."""
        fn = EvaluateSingleOrderSpikeFn(single_order_notional_limit_usd=500_000.0)

        normal_order = TradeOrder.create("PORT-1", "AAPL", "EQUITY", "BUY", "LIMIT", 100.0, 200.0)
        res_normal = list(fn.process(normal_order))
        assert len(res_normal) == 1
        assert isinstance(res_normal[0], TradeOrder)

        whale_order = TradeOrder.create("PORT-1", "AAPL", "EQUITY", "BUY", "MARKET", 6000.0, 200.0)  # $1.2M
        res_whale = list(fn.process(whale_order))
        assert len(res_whale) == 2
        tagged = res_whale[1]
        assert isinstance(tagged, pvalue.TaggedOutput)
        assert tagged.tag == TAG_RISK_ALERTS
        assert tagged.value.alert_type == "SINGLE_ORDER_FAT_FINGER"
        assert tagged.value.severity == "CRITICAL"


# -----------------------------------------------------------------------------
# 4. APACHE BEAM SLIDING WINDOW PIPELINE INTEGRATION TESTS
# -----------------------------------------------------------------------------
class TestSlidingWindowBeamPipelineIntegration:
    """End-to-end Apache Beam DirectRunner tests for 5-minute sliding window anomaly detection."""

    def test_beam_sliding_window_detects_5min_flash_crash(self) -> None:
        """Verify SlidingWindowAnomalyTransform detects a 5-min price crash across event time."""
        # Create raw JSON events within a single 5-minute window:
        # NVDA crashes from $140.00 -> $126.00 (10.0% drop >= 5% threshold)
        # AAPL stays stable $200.00 -> $199.00 (0.5% drop < 5% threshold)
        raw_events = [
            json.dumps({
                "order_id": "ORD-NVDA-1",
                "portfolio_id": "PORT-ALPHA",
                "ticker": "NVDA",
                "asset_class": "EQUITY",
                "side": "BUY",
                "order_type": "LIMIT",
                "quantity": 50.0,
                "price": 140.0,
                "timestamp": "2026-10-18T12:00:05+00:00",
                "sequence_number": 1,
            }).encode("utf-8"),
            json.dumps({
                "order_id": "ORD-NVDA-2",
                "portfolio_id": "PORT-ALPHA",
                "ticker": "NVDA",
                "asset_class": "EQUITY",
                "side": "SELL",
                "order_type": "MARKET",
                "quantity": 80.0,
                "price": 126.0,
                "timestamp": "2026-10-18T12:00:45+00:00",
                "sequence_number": 2,
            }).encode("utf-8"),
            json.dumps({
                "order_id": "ORD-AAPL-1",
                "portfolio_id": "PORT-ALPHA",
                "ticker": "AAPL",
                "asset_class": "EQUITY",
                "side": "BUY",
                "order_type": "LIMIT",
                "quantity": 40.0,
                "price": 200.0,
                "timestamp": "2026-10-18T12:00:10+00:00",
                "sequence_number": 1,
            }).encode("utf-8"),
            json.dumps({
                "order_id": "ORD-AAPL-2",
                "portfolio_id": "PORT-ALPHA",
                "ticker": "AAPL",
                "asset_class": "EQUITY",
                "side": "BUY",
                "order_type": "LIMIT",
                "quantity": 40.0,
                "price": 199.0,
                "timestamp": "2026-10-18T12:00:50+00:00",
                "sequence_number": 2,
            }).encode("utf-8"),
        ]

        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
            "--max_anomaly_drop_threshold=0.05",
        ])
        opts.enable_deadletter_sink = False

        with BeamTestPipeline() as p:
            input_pcoll = p | "CreateOrderEvents" >> beam.Create(raw_events)
            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=input_pcoll)

            assert bundle.window_stats is not None
            assert bundle.risk_alerts is not None

            # Extract (ticker, alert_type, severity) tuples across all 5 overlapping 5-min/1-min windows
            global_alerts = (
                bundle.risk_alerts
                | "RewindowToGlobal" >> beam.WindowInto(beam.window.GlobalWindows())
                | "MapAlertSummary" >> beam.Map(lambda a: (a.ticker, a.alert_type, a.severity))
            )
            distinct_alerts = global_alerts | "DeduplicateWindowSlides" >> beam.Distinct()

            assert_that(
                distinct_alerts,
                equal_to([("NVDA", "FLASH_CRASH_DRAWDOWN", "CRITICAL")]),
                label="VerifyOnlyNvdaTriggersFlashCrash",
            )
            assert_that(
                global_alerts,
                equal_to([("NVDA", "FLASH_CRASH_DRAWDOWN", "CRITICAL")] * 5),
                label="VerifyFiveOverlappingSlidingWindowsEmit",
            )

