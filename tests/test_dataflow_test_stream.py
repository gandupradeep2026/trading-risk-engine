"""Comprehensive Apache Beam Unit Tests Using TestStream and Mock Events (Day 11).

Validates:
1. Mock Event Generation & Synthetic Corruption Fixtures:
   - Schema adherence, USD notional calculations, timestamp handling, and PubsubMessage attributes.
   - Exhaustive synthetic corruptions (syntax errors, unicode decoding errors, missing fields,
     non-positive prices/volumes, invalid sides/order types/timestamps, non-dict payloads).
   - Quantitative scenario generators: flash crash sequences, fat-finger single orders,
     concentrated sell-side pressure, and cumulative notional breaches.
2. StreamScenarioBuilder & TestStream Mechanics:
   - Watermark strict monotonicity enforcement, processing time advancement, and TimestampedValue emission.
   - Prepackaged scenario builders (happy path, flash crash, DLQ corruption, multi-portfolio).
3. Apache Beam TestStream Decoders & DLQ Routing:
   - Stream decoding of valid orders, UTF-8 JSON handling, and Pub/Sub attribute retention.
   - Interleaved valid and corrupted streams with deterministic dead-letter tagging (TAG_DEADLETTER).
   - Beam metric counter verification (valid_orders_decoded, messages_received, deadletter_emitted).
4. Sliding Window Anomaly Detection with TestStream:
   - 5-minute sliding window (300s size, 60s slide) flash crash detection across multiple slide firings.
   - Out-of-order event arrival within the sliding window, verifying chronological timestamp sorting.
   - Cumulative 5-minute window notional exposure limit breach ($2.5M threshold).
   - Concentrated institutional sell pressure detection (>= 85% sell notional).
   - Zero-latency single-order fat-finger spike detection.
5. Multi-Portfolio / Multi-Asset Market Stream Isolation:
   - Concurrent streaming of NVDA, AAPL, MSFT, and BTCUSD across PORT-ALPHA, PORT-BETA, PORT-GAMMA.
   - Verification that ticker-level anomalies do not cross-pollinate window stats or alerts.
   - Market quiescence handling (watermark jumps during idle trading periods).
   - High-throughput event burst handling without element drops.
6. End-to-End Multi-Output Pipeline Integration:
   - Full pipeline execution under TestStream feeding valid orders, flash crashes, and corrupt payloads.
   - Routing clean orders to GCS raw files, dead-letter records to GCS DLQ, and risk alerts to SQLite Cloud SQL.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Set
import uuid

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import MetricsFilter
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
import pytest

# Prevent pytest from attempting to collect Apache Beam's TestStream as a test class
TestStream.__test__ = False

from ingestion.pubsub_client import TradeOrder
from streaming.dataflow.anomaly_detector import (
    TAG_RISK_ALERTS,
    TAG_WINDOW_STATS,
    EvaluateSingleOrderSpikeFn,
    RiskAlert,
)
from streaming.dataflow.cloudsql_sink import CloudSqlDatabaseClient
from streaming.dataflow.decoders import (
    TAG_DEADLETTER,
    TAG_VALID_ORDERS,
    DeadLetterRecord,
    DecodeAndValidateOrders,
)
from streaming.dataflow.options import RiskEnginePipelineOptions
from streaming.dataflow.pipeline import build_pipeline
from streaming.dataflow.testing import (
    CorruptionType,
    StreamScenarioBuilder,
    build_dlq_corruption_scenario,
    build_flash_crash_scenario,
    build_happy_path_scenario,
    build_multi_portfolio_scenario,
    contains_subset,
    create_corrupt_message,
    create_fat_finger_event,
    create_flash_crash_event_sequence,
    create_mock_order_bytes,
    create_mock_order_dict,
    create_mock_pubsub_message,
    create_multi_asset_portfolio_stream,
    create_notional_breach_event_sequence,
    create_sell_pressure_event_sequence,
    elements_satisfy,
    has_count,
    matches_set,
)


# -----------------------------------------------------------------------------
# 1. MOCK EVENT FACTORY & CORRUPTION TESTS
# -----------------------------------------------------------------------------
class TestMockEventGenerationAndCorruption:
    """Validate mock financial event generation, corruption types, and quantitative sequences."""

    def test_mock_order_dict_default_and_custom_fields(self) -> None:
        """Verify mock order creation produces valid dictionaries with auto-calculated notional."""
        order = create_mock_order_dict(
            order_id="ORD-TEST-001",
            portfolio_id="PORT-BETA",
            ticker="msft",
            quantity=50.0,
            price=400.0,
            timestamp="2026-10-18T12:00:00Z",
            sequence_number=10,
        )
        assert order["order_id"] == "ORD-TEST-001"
        assert order["portfolio_id"] == "PORT-BETA"
        assert order["ticker"] == "MSFT"
        assert order["asset_class"] == "EQUITY"
        assert order["side"] == "BUY"
        assert order["quantity"] == 50.0
        assert order["price"] == 400.0
        assert order["notional_usd"] == 20000.0
        assert order["sequence_number"] == 10
        assert "2026-10-18T12:00:00" in order["timestamp"]

    def test_mock_order_bytes_and_pubsub_message(self) -> None:
        """Verify UTF-8 bytes and PubsubMessage wrappers with ordering keys."""
        order_bytes = create_mock_order_bytes(ticker="AAPL", price=220.0, quantity=10.0)
        assert isinstance(order_bytes, bytes)
        decoded = json.loads(order_bytes.decode("utf-8"))
        assert decoded["ticker"] == "AAPL"
        assert decoded["notional_usd"] == 2200.0

        ps_msg = create_mock_pubsub_message(decoded, attributes={"env": "dev"})
        assert isinstance(ps_msg, PubsubMessage)
        assert ps_msg.attributes["env"] == "dev"
        assert ps_msg.attributes["ticker"] == "AAPL"
        assert ps_msg.ordering_key == "PORT-ALPHA#AAPL"

    @pytest.mark.parametrize(
        "corr_type",
        [
            CorruptionType.JSON_SYNTAX_ERROR,
            CorruptionType.UNICODE_DECODE_ERROR,
            CorruptionType.MISSING_FIELD,
            CorruptionType.NON_POSITIVE_PRICE,
            CorruptionType.NON_POSITIVE_QUANTITY,
            CorruptionType.INVALID_SIDE,
            CorruptionType.INVALID_ORDER_TYPE,
            CorruptionType.INVALID_TIMESTAMP,
            CorruptionType.NOT_A_DICT,
        ],
    )
    def test_all_corruption_types_produce_expected_payloads(self, corr_type: CorruptionType) -> None:
        """Verify each corruption type generates corrupt bytes and can wrap in PubsubMessage."""
        raw = create_corrupt_message(corr_type, missing_field="price")
        assert isinstance(raw, bytes)

        msg = create_corrupt_message(corr_type, as_pubsub_message=True)
        assert isinstance(msg, PubsubMessage)
        assert msg.data == raw

    def test_flash_crash_sequence_microstructure(self) -> None:
        """Verify flash crash generator creates accelerating descending prices to trough."""
        seq = create_flash_crash_event_sequence(
            portfolio_id="PORT-ALPHA",
            ticker="NVDA",
            base_price=150.0,
            drop_pct=0.10,
            start_timestamp_sec=1792324800.0,
            duration_seconds=120,
            num_orders=5,
        )
        assert len(seq) == 5
        assert seq[0]["price"] == 150.0
        assert seq[0]["side"] == "BUY"
        assert seq[-1]["price"] == 135.0  # 150 * (1 - 0.10)
        assert seq[-1]["side"] == "SELL"

        # Verify strictly increasing sequence numbers and timestamps
        for i in range(len(seq) - 1):
            assert seq[i]["sequence_number"] < seq[i + 1]["sequence_number"]
            assert seq[i]["timestamp"] < seq[i + 1]["timestamp"]

    def test_quantitative_anomaly_generators(self) -> None:
        """Verify fat finger, sell pressure, and notional breach generators."""
        fat = create_fat_finger_event(quantity=20000.0, price=300.0)
        assert fat["notional_usd"] == 6_000_000.0
        assert fat["trader_id"] == "ROGUE-TRADER-99"

        sell_seq = create_sell_pressure_event_sequence(num_orders=5)
        total_sell = sum(o["notional_usd"] for o in sell_seq if o["side"] == "SELL")
        total_all = sum(o["notional_usd"] for o in sell_seq)
        assert (total_sell / total_all) > 0.85

        notional_seq = create_notional_breach_event_sequence(target_cumulative_notional=3_500_000.0)
        assert sum(o["notional_usd"] for o in notional_seq) >= 3_500_000.0


# -----------------------------------------------------------------------------
# 2. STREAM SCENARIO BUILDER TESTS
# -----------------------------------------------------------------------------
class TestStreamScenarioBuilder:
    """Validate StreamScenarioBuilder fluent API, watermark progression, and validation."""

    def test_builder_enforces_monotonic_watermarks(self) -> None:
        """Verify builder rejects non-monotonic or negative watermark advancement."""
        builder = StreamScenarioBuilder(start_watermark_sec=100.0)
        assert builder.current_watermark == 100.0

        builder.advance_watermark_to(150.0)
        assert builder.current_watermark == 150.0

        with pytest.raises(ValueError, match="Watermark must strictly advance monotonically"):
            builder.advance_watermark_to(150.0)

        with pytest.raises(ValueError, match="Watermark must strictly advance monotonically"):
            builder.advance_watermark_to(120.0)

        with pytest.raises(ValueError, match="Watermark delta must be strictly positive"):
            builder.advance_watermark_by(-5.0)

    def test_builder_fluent_chain_and_build(self) -> None:
        """Verify fluent method chaining produces an executable TestStream."""
        base_ts = 1792324800.0
        builder = StreamScenarioBuilder(start_watermark_sec=base_ts)
        ts = (
            builder.add_order(order_id="ORD-1", event_timestamp=base_ts + 10.0)
            .advance_processing_time(5.0)
            .advance_watermark_by(30.0)
            .add_order(order_id="ORD-2", event_timestamp=base_ts + 40.0)
            .build()
        )
        assert isinstance(ts, TestStream)

    def test_prepackaged_scenarios_build_successfully(self) -> None:
        """Verify all prepackaged scenario helper functions build valid TestStreams."""
        s1 = build_happy_path_scenario(num_orders=5)
        s2 = build_flash_crash_scenario()
        s3 = build_dlq_corruption_scenario()
        s4 = build_multi_portfolio_scenario()
        for s in [s1, s2, s3, s4]:
            assert isinstance(s, TestStream)


# -----------------------------------------------------------------------------
# 3. APACHE BEAM TESTSTREAM DECODERS & DLQ ROUTING TESTS
# -----------------------------------------------------------------------------
class TestBeamTestStreamDecodersAndDLQ:
    """Validate stream decoding, timestamp extraction, and DLQ routing using TestStream."""

    def test_test_stream_happy_path_decoding(self) -> None:
        """Verify TestStream feeding on-time orders decodes valid TradeOrders cleanly."""
        base_ts = 1792324800.0
        stream = (
            StreamScenarioBuilder(start_watermark_sec=base_ts)
            .add_order(order_id="ORD-HP-1", ticker="AAPL", price=220.0, event_timestamp=base_ts + 10.0)
            .advance_watermark_to(base_ts + 30.0)
            .add_order(order_id="ORD-HP-2", ticker="AAPL", price=221.0, event_timestamp=base_ts + 40.0)
            .advance_watermark_to(base_ts + 60.0)
            .build()
        )

        with BeamTestPipeline() as p:
            raw = p | stream
            outputs = raw | "DecodeAndValidate" >> DecodeAndValidateOrders()
            valid_orders = outputs[TAG_VALID_ORDERS] | "ExtractId" >> beam.Map(lambda o: o.order_id)
            deadletter = outputs[TAG_DEADLETTER]

            assert_that(valid_orders, equal_to(["ORD-HP-1", "ORD-HP-2"]), label="VerifyValidOrderIds")
            assert_that(deadletter, equal_to([]), label="VerifyEmptyDeadLetter")

    def test_test_stream_pubsub_message_attributes_preserved(self) -> None:
        """Verify PubsubMessage data and attributes are processed via TestStream."""
        base_ts = 1792324800.0
        ps_msg = create_mock_pubsub_message(
            order_id="ORD-PS-ATTR",
            portfolio_id="PORT-ALPHA",
            ticker="NVDA",
            quantity=100.0,
            price=140.0,
            timestamp=base_ts + 10.0,
            attributes={"client_id": "CLIENT-007"},
        )

        stream = (
            StreamScenarioBuilder(start_watermark_sec=base_ts)
            .add_order(ps_msg, event_timestamp=base_ts + 10.0, as_pubsub_message=True)
            .advance_watermark_to(base_ts + 60.0)
            .build()
        )

        with BeamTestPipeline() as p:
            raw = p | stream
            outputs = raw | "DecodeOrders" >> DecodeAndValidateOrders()
            orders = outputs[TAG_VALID_ORDERS] | "MapOrder" >> beam.Map(lambda o: (o.order_id, o.ticker, o.notional_usd))

            assert_that(orders, equal_to([("ORD-PS-ATTR", "NVDA", 14000.0)]))

    def test_test_stream_interleaved_dlq_routing(self) -> None:
        """Verify mixed stream with valid orders and 8 distinct corruption types routes to DLQ."""
        base_ts = 1792324800.0
        stream = build_dlq_corruption_scenario(base_ts=base_ts)

        with BeamTestPipeline() as p:
            raw = p | stream
            outputs = raw | "DecodeMixedStream" >> DecodeAndValidateOrders()

            valid_ids = outputs[TAG_VALID_ORDERS] | "MapValidIds" >> beam.Map(lambda o: o.order_id)
            dlq_errors = outputs[TAG_DEADLETTER] | "MapDlqTypes" >> beam.Map(lambda r: r.error_type)

            # Exactly 3 valid orders were in the scenario
            assert_that(valid_ids, equal_to(["ORD-VALID-1", "ORD-VALID-2", "ORD-VALID-3"]), label="CheckValidOrders")

            # Exactly 8 corrupt messages across error types
            assert_that(
                dlq_errors,
                contains_subset([
                    "JSONDecodeError",
                    "UnicodeDecodeError",
                    "MissingFieldError",
                    "ValidationError",
                ]),
                label="CheckDLQErrorTypes",
            )
            assert_that(dlq_errors, has_count(9), label="CheckDLQCount")

    def test_test_stream_metrics_counter_accumulation(self) -> None:
        """Verify pipeline metrics counters committed during TestStream execution."""
        base_ts = 1792324800.0
        builder = StreamScenarioBuilder(start_watermark_sec=base_ts)
        builder.add_order(order_id="ORD-M-1", event_timestamp=base_ts + 10.0)
        builder.add_order(order_id="ORD-M-2", event_timestamp=base_ts + 20.0)
        builder.add_corrupt_payload(CorruptionType.JSON_SYNTAX_ERROR, event_timestamp=base_ts + 30.0)
        builder.advance_watermark_to(base_ts + 60.0)
        stream = builder.build()

        pipeline = BeamTestPipeline()
        raw = pipeline | stream
        outputs = raw | "DecodeWithMetrics" >> DecodeAndValidateOrders()
        _ = outputs[TAG_VALID_ORDERS] | "PassValid" >> beam.Map(lambda o: o)
        _ = outputs[TAG_DEADLETTER] | "PassDlq" >> beam.Map(lambda d: d)

        result = pipeline.run()
        result.wait_until_finish()

        # Query committed metrics
        valid_query = result.metrics().query(MetricsFilter().with_name("valid_orders_decoded"))
        dlq_query = result.metrics().query(MetricsFilter().with_name("deadletter_emitted"))
        recv_query = result.metrics().query(MetricsFilter().with_name("messages_received"))

        assert len(valid_query["counters"]) > 0 and valid_query["counters"][0].committed >= 1
        assert len(dlq_query["counters"]) > 0 and dlq_query["counters"][0].committed >= 1
        assert len(recv_query["counters"]) > 0 and recv_query["counters"][0].committed >= 1


# -----------------------------------------------------------------------------
# 4. SLIDING WINDOW ANOMALY DETECTION WITH TESTSTREAM
# -----------------------------------------------------------------------------
class TestBeamTestStreamSlidingWindowAnomalies:
    """Validate 5-minute sliding window drawdown, out-of-order events, and quantitative limits."""

    def test_test_stream_flash_crash_across_sliding_windows(self) -> None:
        """Verify TestStream flash crash sequence emits RiskAlerts across sliding windows."""
        base_ts = 1792324800.0  # 12:00:00
        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
            "--max_anomaly_drop_threshold=0.05",
        ])
        opts.enable_deadletter_sink = False
        opts.enable_cloudsql_sink = False
        opts.enable_gcs_raw_sink = False
        opts.enable_gcs_deadletter_sink = False

        stream = build_flash_crash_scenario(
            portfolio_id="PORT-ALPHA",
            ticker="NVDA",
            base_price=140.0,
            drop_pct=0.10,  # 10% drop > 5% threshold
            base_ts=base_ts,
        )

        with BeamTestPipeline() as p:
            raw_input = p | stream
            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=raw_input)
            assert bundle.risk_alerts is not None
            assert bundle.window_stats is not None

            # Verify flash crash alerts emitted with CRITICAL severity
            alert_tuples = bundle.risk_alerts | "MapAlertInfo" >> beam.Map(
                lambda a: (a.portfolio_id, a.ticker, a.alert_type, a.severity)
            )
            assert_that(
                alert_tuples,
                elements_satisfy(
                    lambda t: t == ("PORT-ALPHA", "NVDA", "FLASH_CRASH_DRAWDOWN", "CRITICAL"),
                    description="All emitted alerts must match NVDA FLASH_CRASH_DRAWDOWN CRITICAL",
                ),
                label="VerifyAlertTypes",
            )
            assert_that(
                alert_tuples,
                matches_set({("PORT-ALPHA", "NVDA", "FLASH_CRASH_DRAWDOWN", "CRITICAL")}),
                label="VerifyDistinctAlertSet",
            )

    def test_test_stream_out_of_order_arrival_chronological_sorting(self) -> None:
        """Verify out-of-order events arriving before watermark are chronologically sorted.

        Event sequence:
        t=10s: $140 (Buy)
        t=40s: $126 (Sell) - 10% drop
        t=25s: $133 (Sell) - arrives LAST, but event-time is between t=10 and t=40
        """
        base_ts = 1792324800.0
        builder = StreamScenarioBuilder(start_watermark_sec=base_ts)
        builder.add_out_of_order_scenario(
            portfolio_id="PORT-ALPHA",
            ticker="NVDA",
            base_ts=base_ts,
        )
        builder.advance_watermark_to(base_ts + 60.0)
        builder.advance_watermark_to(base_ts + 360.0)
        stream = builder.build()

        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
            "--max_anomaly_drop_threshold=0.05",
        ])
        opts.enable_deadletter_sink = False
        opts.enable_cloudsql_sink = False
        opts.enable_gcs_raw_sink = False
        opts.enable_gcs_deadletter_sink = False

        with BeamTestPipeline() as p:
            raw_input = p | stream
            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=raw_input)
            assert bundle.window_stats is not None

            stats_summary = bundle.window_stats | "ExtractStats" >> beam.Map(
                lambda s: (s.ticker, s.open_price, s.close_price, s.peak_price, s.trough_price, s.order_count)
            )
            # Open must be 140, Close must be 126, Peak 140, Trough 126, Count 3
            assert_that(
                stats_summary,
                matches_set({("NVDA", 140.0, 126.0, 140.0, 126.0, 3)}),
                label="VerifyChronologicalSorting",
            )

    def test_test_stream_cumulative_notional_exposure_breach(self) -> None:
        """Verify rolling 5-minute cumulative notional breach alert under TestStream."""
        base_ts = 1792324800.0
        orders = create_notional_breach_event_sequence(
            portfolio_id="PORT-BETA",
            ticker="MSFT",
            base_price=400.0,
            target_cumulative_notional=3_200_000.0,  # > $2.5M
            start_timestamp_sec=base_ts + 10.0,
            num_orders=4,
        )

        stream = (
            StreamScenarioBuilder(start_watermark_sec=base_ts)
            .add_orders(orders)
            .advance_watermark_to(base_ts + 120.0)
            .advance_watermark_to(base_ts + 360.0)
            .build()
        )

        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
            "--max_window_notional_usd=2500000.0",
        ])
        opts.enable_deadletter_sink = False
        opts.enable_cloudsql_sink = False
        opts.enable_gcs_raw_sink = False
        opts.enable_gcs_deadletter_sink = False

        with BeamTestPipeline() as p:
            raw_input = p | stream
            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=raw_input)
            assert bundle.risk_alerts is not None

            notional_alerts = bundle.risk_alerts | "FilterNotionalAlerts" >> beam.Filter(
                lambda a: a.alert_type == "NOTIONAL_EXPOSURE_BREACH"
            ) | "MapNotionalAlert" >> beam.Map(lambda a: (a.portfolio_id, a.ticker, a.alert_type))

            assert_that(
                notional_alerts,
                matches_set({("PORT-BETA", "MSFT", "NOTIONAL_EXPOSURE_BREACH")}),
                label="VerifyNotionalBreachAlert",
            )

    def test_test_stream_concentrated_sell_pressure_alert(self) -> None:
        """Verify concentrated institutional sell pressure alert under TestStream."""
        base_ts = 1792324800.0
        orders = create_sell_pressure_event_sequence(
            portfolio_id="PORT-GAMMA",
            ticker="AAPL",
            base_price=220.0,
            start_timestamp_sec=base_ts + 10.0,
            num_orders=5,
        )

        stream = (
            StreamScenarioBuilder(start_watermark_sec=base_ts)
            .add_orders(orders)
            .advance_watermark_to(base_ts + 120.0)
            .advance_watermark_to(base_ts + 360.0)
            .build()
        )

        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
        ])
        opts.enable_deadletter_sink = False
        opts.enable_cloudsql_sink = False
        opts.enable_gcs_raw_sink = False
        opts.enable_gcs_deadletter_sink = False

        with BeamTestPipeline() as p:
            raw_input = p | stream
            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=raw_input)
            assert bundle.risk_alerts is not None

            sell_alerts = bundle.risk_alerts | "FilterSellAlerts" >> beam.Filter(
                lambda a: a.alert_type == "CONCENTRATED_SELL_PRESSURE"
            ) | "MapSellAlert" >> beam.Map(lambda a: (a.portfolio_id, a.ticker, a.alert_type))

            assert_that(
                sell_alerts,
                matches_set({("PORT-GAMMA", "AAPL", "CONCENTRATED_SELL_PRESSURE")}),
                label="VerifySellPressureAlert",
            )

    def test_test_stream_single_order_fat_finger_detection(self) -> None:
        """Verify zero-latency fat-finger detection directly on valid orders stream."""
        base_ts = 1792324800.0
        fat_order = create_fat_finger_event(
            portfolio_id="PORT-DELTA",
            ticker="TSLA",
            quantity=25000.0,
            price=250.0,  # $6.25M > $5M limit
            timestamp_sec=base_ts + 15.0,
        )

        stream = (
            StreamScenarioBuilder(start_watermark_sec=base_ts)
            .add_order(fat_order)
            .advance_watermark_to(base_ts + 60.0)
            .build()
        )

        with BeamTestPipeline() as p:
            raw = p | stream
            outputs = raw | "DecodeOrders" >> DecodeAndValidateOrders()
            spike_outputs = (
                outputs[TAG_VALID_ORDERS]
                | "EvaluateFatFinger"
                >> beam.ParDo(EvaluateSingleOrderSpikeFn(single_order_notional_limit_usd=5_000_000.0)).with_outputs(
                    TAG_RISK_ALERTS, main=TAG_VALID_ORDERS
                )
            )
            fat_alerts = (
                spike_outputs[TAG_RISK_ALERTS]
                | "MapFatAlert" >> beam.Map(lambda a: (a.portfolio_id, a.ticker, a.alert_type, a.severity))
            )

            assert_that(
                fat_alerts,
                equal_to([("PORT-DELTA", "TSLA", "SINGLE_ORDER_FAT_FINGER", "HIGH")]),
                label="VerifyFatFingerSpike",
            )


# -----------------------------------------------------------------------------
# 5. MULTI-PORTFOLIO & MARKET STREAM DYNAMICS
# -----------------------------------------------------------------------------
class TestBeamTestStreamMultiPortfolioAndMarketDynamics:
    """Validate multi-ticker isolation, quiescent market intervals, and burst traffic."""

    def test_test_stream_multi_portfolio_isolation(self) -> None:
        """Verify interleaved multi-portfolio stream routes and alerts each ticker independently."""
        base_ts = 1792324800.0
        stream = build_multi_portfolio_scenario(base_ts=base_ts)

        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
            "--max_anomaly_drop_threshold=0.05",
            "--max_window_notional_usd=2500000.0",
        ])
        opts.enable_deadletter_sink = False
        opts.enable_cloudsql_sink = False
        opts.enable_gcs_raw_sink = False
        opts.enable_gcs_deadletter_sink = False

        with BeamTestPipeline() as p:
            raw_input = p | stream
            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=raw_input)
            assert bundle.risk_alerts is not None

            alert_types = bundle.risk_alerts | "MapAlertTypes" >> beam.Map(
                lambda a: (a.portfolio_id, a.ticker, a.alert_type)
            )

            # NVDA had flash crash, MSFT had notional breach, BTCUSD had sell concentration
            assert_that(
                alert_types,
                matches_set({
                    ("PORT-ALPHA", "NVDA", "FLASH_CRASH_DRAWDOWN"),
                    ("PORT-BETA", "MSFT", "NOTIONAL_EXPOSURE_BREACH"),
                    ("PORT-GAMMA", "BTCUSD", "CONCENTRATED_SELL_PRESSURE"),
                }),
                label="VerifyMultiPortfolioAlerts",
            )

            # AAPL was stable and must NEVER appear in risk alerts
            aapl_alerts = bundle.risk_alerts | "FilterAAPL" >> beam.Filter(lambda a: a.ticker == "AAPL")
            assert_that(aapl_alerts, equal_to([]), label="VerifyAaplHasZeroAlerts")

    def test_test_stream_quiescent_market_gap_produces_no_spurious_alerts(self) -> None:
        """Verify watermark advancing over a 15-minute gap without trades emits zero spurious alerts."""
        base_ts = 1792324800.0
        builder = StreamScenarioBuilder(start_watermark_sec=base_ts)

        # 2 normal orders in first 30 seconds
        builder.add_order(order_id="ORD-Q-1", ticker="AAPL", price=220.0, event_timestamp=base_ts + 10.0)
        builder.add_order(order_id="ORD-Q-2", ticker="AAPL", price=220.1, event_timestamp=base_ts + 20.0)
        builder.advance_watermark_to(base_ts + 60.0)

        # 15-minute gap with no trades
        builder.advance_watermark_to(base_ts + 300.0)
        builder.advance_watermark_to(base_ts + 600.0)
        builder.advance_watermark_to(base_ts + 900.0)
        stream = builder.build()

        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
            "--max_anomaly_drop_threshold=0.05",
        ])
        opts.enable_deadletter_sink = False
        opts.enable_cloudsql_sink = False
        opts.enable_gcs_raw_sink = False
        opts.enable_gcs_deadletter_sink = False

        with BeamTestPipeline() as p:
            raw_input = p | stream
            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=raw_input)
            assert bundle.risk_alerts is not None

            assert_that(bundle.risk_alerts, equal_to([]), label="VerifyZeroSpuriousAlerts")

    def test_test_stream_burst_traffic_processing(self) -> None:
        """Verify high-frequency burst (20 orders within 2s) processes without drops."""
        base_ts = 1792324800.0
        builder = StreamScenarioBuilder(start_watermark_sec=base_ts)

        burst_orders = []
        for i in range(20):
            burst_orders.append(
                create_mock_order_dict(
                    order_id=f"ORD-BURST-{i + 1}",
                    ticker="NVDA",
                    price=140.0 + (i * 0.05),
                    quantity=10.0,
                    timestamp=base_ts + (i * 0.1),
                    sequence_number=i + 1,
                )
            )
        builder.add_orders(burst_orders)
        builder.advance_watermark_to(base_ts + 10.0)
        builder.advance_watermark_to(base_ts + 60.0)
        stream = builder.build()

        with BeamTestPipeline() as p:
            raw = p | stream
            outputs = raw | "DecodeBurst" >> DecodeAndValidateOrders()
            order_ids = outputs[TAG_VALID_ORDERS] | "MapBurstIds" >> beam.Map(lambda o: o.order_id)

            assert_that(order_ids, has_count(20), label="VerifyAllBurstOrdersProcessed")


# -----------------------------------------------------------------------------
# 6. END-TO-END MULTI-OUTPUT PIPELINE WITH TESTSTREAM
# -----------------------------------------------------------------------------
class TestBeamTestStreamEndToEndPipeline:
    """Validate full Dataflow pipeline with TestStream, SQLite Cloud SQL sink, and GCS formatting."""

    def test_test_stream_end_to_end_multi_sink_pipeline(self, tmp_path: Path) -> None:
        """Verify full pipeline: valid orders to GCS, alerts to SQLite, and corrupts to DLQ."""
        base_ts = 1792324800.0
        sqlite_db = str(tmp_path / "stream_risk_test.db")
        gcs_orders_dir = str(tmp_path / "gcs_raw" / "orders")
        gcs_dlq_dir = str(tmp_path / "gcs_dlq" / "dlq")

        # 1. Build mixed TestStream
        builder = StreamScenarioBuilder(start_watermark_sec=base_ts)

        # Normal AAPL order
        builder.add_order(
            order_id="ORD-E2E-AAPL-1",
            ticker="AAPL",
            price=220.0,
            quantity=50.0,
            event_timestamp=base_ts + 10.0,
        )

        # Flash crash NVDA orders
        builder.add_flash_crash_sequence(
            portfolio_id="PORT-ALPHA",
            ticker="NVDA",
            base_price=140.0,
            drop_pct=0.10,
            start_timestamp_sec=base_ts + 15.0,
            duration_seconds=60,
            num_orders=4,
        )

        # Corrupted payload
        builder.add_corrupt_payload(
            CorruptionType.JSON_SYNTAX_ERROR,
            event_timestamp=base_ts + 30.0,
            order_id="ORD-E2E-CORRUPT",
        )

        # Advance watermark past window slide
        builder.advance_watermark_to(base_ts + 60.0)
        builder.advance_watermark_to(base_ts + 360.0)
        stream = builder.build()

        # 2. Configure pipeline options
        opts = RiskEnginePipelineOptions([
            "--project_id=trading-risk-engine-dev",
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
            "--max_anomaly_drop_threshold=0.05",
            "--enable_cloudsql_sink",
            f"--cloudsql_sqlite_path={sqlite_db}",
            "--cloudsql_driver=sqlite3",
            "--cloudsql_table=risk_alerts",
            "--cloudsql_batch_size=1",
            "--enable_gcs_raw_sink",
            f"--gcs_output_path={gcs_orders_dir}",
            "--gcs_window_seconds=60",
            "--gcs_num_shards=1",
            "--enable_gcs_deadletter_sink",
            f"--gcs_deadletter_output_path={gcs_dlq_dir}",
        ])
        opts.resolve_defaults()
        opts.deadletter_topic = None
        opts.output_alerts_topic = None
        opts.enable_deadletter_sink = False
        opts.validate_options(is_test=True)

        # Initialize SQLite database
        client = CloudSqlDatabaseClient.from_options(opts)
        client.initialize_schema()

        # 3. Execute pipeline
        pipeline = BeamTestPipeline()
        raw_input = pipeline | stream
        bundle = build_pipeline(
            opts,
            pipeline=pipeline,
            test_input_pcoll=raw_input,
            cloudsql_client=client,
        )
        assert bundle.cloudsql_alerts_written is not None
        assert bundle.gcs_orders_written is not None
        assert bundle.gcs_deadletter_written is not None

        result = pipeline.run()
        result.wait_until_finish()

        # 4. Verify SQLite Database Entries
        conn = sqlite3.connect(sqlite_db)
        cursor = conn.cursor()
        cursor.execute("SELECT ticker, alert_type, severity, count(*) FROM risk_alerts GROUP BY ticker, alert_type, severity")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) > 0
        ticker, alert_type, severity, count = rows[0]
        assert ticker == "NVDA"
        assert alert_type == "FLASH_CRASH_DRAWDOWN"
        assert severity == "CRITICAL"
        assert count >= 1

        # 5. Verify GCS Output Files Created
        order_files = list((tmp_path / "gcs_raw").glob("orders*"))
        dlq_files = list((tmp_path / "gcs_dlq").glob("dlq*"))
        assert len(order_files) >= 1
        assert len(dlq_files) >= 1

        # Check content of written raw order lines
        with open(order_files[0], "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
            tickers = set(item["ticker"] for item in lines)
            assert "AAPL" in tickers or "NVDA" in tickers
            assert all(item.get("_tier") == "BRONZE_RAW" for item in lines)

        # Check content of written deadletter file
        with open(dlq_files[0], "r", encoding="utf-8") as f:
            dlq_lines = [json.loads(line) for line in f if line.strip()]
            assert len(dlq_lines) >= 1
            assert dlq_lines[0].get("error_type") == "JSONDecodeError"
            assert dlq_lines[0].get("_tier") == "BRONZE_DEADLETTER"
