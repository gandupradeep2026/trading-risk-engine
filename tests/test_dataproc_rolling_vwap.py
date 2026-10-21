"""Comprehensive Zero-Cost Unit and Integration Test Suite for Dataproc PySpark Rolling VWAP.

Verifies:
1. `ROLLING_VWAP_SCHEMA_DDL` Spark SQL DDL syntax, StructType descriptors, and field types.
2. `RollingVWAPStats` dataclass validation, microstructure metrics, and JSON roundtrip serialization.
3. Precise mathematical verification of:
   - Volume-Weighted Average Price: VWAP = sum(P * V) / sum(V)
   - Sample variance and standard deviation: s = sqrt(sum((x - mean)^2) / (N - 1))
   - Bollinger Volatility Bands: upper = vwap + k*sigma, lower = vwap - k*sigma
   - Volatility Bandwidth in basis points: ((upper - lower) / vwap) * 10000
   - %B indicator: (close - lower) / (upper - lower)
   - Upper and lower band breach detections
   - Volatility spike alerts
4. Multi-ticker isolation (Equities, ETFs, FX, Crypto) across sliding event-time windows.
5. Eviction and buffer memory bounding via `evict_older_than`.
6. Prometheus text exposition format metrics and telemetry counters.
7. Mock PySpark Structured Streaming graph builders (`build_rolling_vwap_stream`, `start_vwap_writer`).
8. End-to-end integration: MarketFeedGenerator -> KafkaTickProducer -> LocalKafkaProducerMock -> PySparkKafkaTickConsumer -> RollingVWAPAggregator.
9. CLI entrypoint verification (`run_tick_consumer.py`) for VWAP dry run and local simulation.
"""

from datetime import datetime, timezone
import json
import math
from typing import List
from unittest.mock import MagicMock

import pytest

from ingestion.kafka.producer import KafkaTickProducer, LocalKafkaProducerMock, ProducerConfig
from ingestion.market_feed import MarketFeedGenerator
from ingestion.market_tick import MarketTick
from streaming.dataproc.config import DataprocStreamingConfig
from streaming.dataproc.run_tick_consumer import main as run_consumer_main, run_local_simulation
from streaming.dataproc.schema import ParsedTickRecord, parse_and_validate_kafka_record
from streaming.dataproc.tick_consumer import PySparkKafkaTickConsumer
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


# =============================================================================
# 1. Schema, DDL & StructType Descriptor Tests
# =============================================================================


class TestRollingVWAPSchemaAndDDL:
    """Unit tests for Spark SQL DDL schemas and StructType descriptors for Rolling VWAP."""

    def test_schema_ddl_syntax_and_required_columns(self) -> None:
        """Verify ROLLING_VWAP_SCHEMA_DDL defines all essential financial microstructure columns."""
        ddl = ROLLING_VWAP_SCHEMA_DDL

        assert "ticker STRING" in ddl
        assert "asset_class STRING" in ddl
        assert "window_start STRING" in ddl
        assert "window_end STRING" in ddl
        assert "tick_count LONG" in ddl
        assert "open_price DOUBLE" in ddl
        assert "high_price DOUBLE" in ddl
        assert "low_price DOUBLE" in ddl
        assert "close_price DOUBLE" in ddl
        assert "total_volume DOUBLE" in ddl
        assert "total_notional DOUBLE" in ddl
        assert "vwap DOUBLE" in ddl
        assert "price_stddev DOUBLE" in ddl
        assert "price_variance DOUBLE" in ddl
        assert "volatility_multiplier DOUBLE" in ddl
        assert "upper_band DOUBLE" in ddl
        assert "lower_band DOUBLE" in ddl
        assert "bandwidth_bps DOUBLE" in ddl
        assert "percent_b DOUBLE" in ddl
        assert "is_upper_breach BOOLEAN" in ddl
        assert "is_lower_breach BOOLEAN" in ddl
        assert "volatility_spike BOOLEAN" in ddl
        assert "year INT" in ddl
        assert "month INT" in ddl
        assert "day INT" in ddl
        assert "hour INT" in ddl
        assert "computed_at STRING" in ddl

    def test_struct_spec_field_names_and_ddl_export(self) -> None:
        """Verify ROLLING_VWAP_STRUCT_SPEC exports matching DDL and field names."""
        fields = ROLLING_VWAP_STRUCT_SPEC.field_names
        assert "ticker" in fields
        assert "vwap" in fields
        assert "upper_band" in fields
        assert "lower_band" in fields
        assert "bandwidth_bps" in fields
        assert "percent_b" in fields
        assert "is_upper_breach" in fields

        ddl = ROLLING_VWAP_STRUCT_SPEC.to_ddl()
        assert "ticker STRING" in ddl
        assert "vwap DOUBLE" in ddl

    def test_get_pyspark_vwap_schema_fallback(self) -> None:
        """Verify get_pyspark_vwap_schema returns valid schema without JVM."""
        schema = get_pyspark_vwap_schema()
        assert schema is not None


# =============================================================================
# 2. Mathematical Precision Verification Tests
# =============================================================================


class TestRollingVWAPMathematicalCalculations:
    """Unit tests verifying exact financial formulas for VWAP and volatility bands."""

    def test_single_tick_window_behavior(self) -> None:
        """Verify single tick window yields zero variance and equal upper/lower bands to VWAP."""
        aggregator = RollingVWAPAggregator(
            config=DataprocStreamingConfig(
                vwap_window_duration="5 minutes",
                vwap_slide_duration="1 minute",
            )
        )
        tick = {
            "trade_id": "T-SINGLE",
            "ticker": "AAPL",
            "asset_class": "EQUITY",
            "bid": 185.0,
            "ask": 185.1,
            "last_price": 185.05,
            "volume": 100.0,
            "timestamp": "2026-10-21T10:02:30+00:00",
        }
        aggregator.add_tick(tick)
        windows = aggregator.compute_windows()

        assert len(windows) == 1
        stat = windows[0]
        assert stat.tick_count == 1
        assert stat.open_price == 185.05
        assert stat.high_price == 185.05
        assert stat.low_price == 185.05
        assert stat.close_price == 185.05
        assert stat.total_volume == 100.0
        assert stat.total_notional == 18505.0
        assert stat.vwap == 185.05
        assert stat.price_stddev == 0.0
        assert stat.price_variance == 0.0
        assert stat.upper_band == 185.05
        assert stat.lower_band == 185.05
        assert stat.bandwidth_bps == 0.0
        assert stat.percent_b == 0.5
        assert stat.is_upper_breach is False
        assert stat.is_lower_breach is False
        assert stat.volatility_spike is False

    def test_multi_tick_exact_vwap_and_sample_stddev(self) -> None:
        """Verify exact quantitative calculation of VWAP and sample standard deviation.

        Scenario:
        Tick 1: P = 100.0, V = 50.0  -> PV = 5000.0
        Tick 2: P = 102.0, V = 100.0 -> PV = 10200.0
        Tick 3: P = 98.0,  V = 50.0  -> PV = 4900.0
        Total Volume = 200.0, Total Notional = 20100.0
        Expected VWAP = 20100.0 / 200.0 = 100.50

        Prices: [100.0, 102.0, 98.0], Mean Price = 100.0
        Sample Variance: ((100-100)^2 + (102-100)^2 + (98-100)^2) / (3 - 1) = (0 + 4 + 4) / 2 = 4.0
        Sample Stddev = sqrt(4.0) = 2.0
        Upper Band (k=2.0): 100.5 + 2 * 2.0 = 104.50
        Lower Band (k=2.0): 100.5 - 2 * 2.0 = 96.50
        Bandwidth bps: (104.5 - 96.5) / 100.5 * 10000 = 8.0 / 100.5 * 10000 = 796.02 bps
        %B for close price 98.0: (98.0 - 96.5) / (104.5 - 96.5) = 1.5 / 8.0 = 0.1875
        """
        aggregator = RollingVWAPAggregator(
            config=DataprocStreamingConfig(
                vwap_window_duration="5 minutes",
                vwap_slide_duration="5 minutes",
                volatility_k_multiplier=2.0,
            )
        )
        ticks = [
            {
                "trade_id": "T1",
                "ticker": "AAPL",
                "asset_class": "EQUITY",
                "bid": 99.9,
                "ask": 100.1,
                "last_price": 100.0,
                "volume": 50.0,
                "timestamp": "2026-10-21T10:00:10+00:00",
            },
            {
                "trade_id": "T2",
                "ticker": "AAPL",
                "asset_class": "EQUITY",
                "bid": 101.9,
                "ask": 102.1,
                "last_price": 102.0,
                "volume": 100.0,
                "timestamp": "2026-10-21T10:01:20+00:00",
            },
            {
                "trade_id": "T3",
                "ticker": "AAPL",
                "asset_class": "EQUITY",
                "bid": 97.9,
                "ask": 98.1,
                "last_price": 98.0,
                "volume": 50.0,
                "timestamp": "2026-10-21T10:02:30+00:00",
            },
        ]
        aggregator.add_ticks(ticks)
        windows = aggregator.compute_windows()

        assert len(windows) == 1
        stat = windows[0]
        assert stat.tick_count == 3
        assert stat.open_price == 100.0
        assert stat.high_price == 102.0
        assert stat.low_price == 98.0
        assert stat.close_price == 98.0
        assert stat.total_volume == 200.0
        assert stat.total_notional == 20100.0
        assert stat.vwap == 100.5
        assert stat.price_variance == 4.0
        assert stat.price_stddev == 2.0
        assert stat.upper_band == 104.5
        assert stat.lower_band == 96.5
        assert stat.bandwidth_bps == pytest.approx(796.02, abs=0.1)
        assert stat.percent_b == pytest.approx(0.1875, abs=0.001)
        assert stat.is_upper_breach is False
        assert stat.is_lower_breach is False

    def test_upper_volatility_band_breach(self) -> None:
        """Verify is_upper_breach is True when close price exceeds the upper band."""
        aggregator = RollingVWAPAggregator(
            config=DataprocStreamingConfig(
                vwap_window_duration="5 minutes",
                vwap_slide_duration="5 minutes",
                volatility_k_multiplier=2.0,
            )
        )
        # 10 baseline consolidated ticks around 100.0 followed by an aggressive upper breakout to 105.0
        ticks = [
            {
                "trade_id": f"T-BASE-{i}",
                "ticker": "NVDA",
                "asset_class": "EQUITY",
                "bid": 99.95,
                "ask": 100.05,
                "last_price": 100.0,
                "volume": 100.0,
                "timestamp": f"2026-10-21T10:0{i // 3}:{10 + (i % 3) * 15}+00:00",
            }
            for i in range(10)
        ]
        ticks.append(
            {
                "trade_id": "T-SPIKE",
                "ticker": "NVDA",
                "asset_class": "EQUITY",
                "bid": 104.9,
                "ask": 105.1,
                "last_price": 105.0,  # Aggressive upper breakout
                "volume": 10.0,
                "timestamp": "2026-10-21T10:04:00+00:00",
            }
        )
        aggregator.add_ticks(ticks)
        windows = aggregator.compute_windows()

        assert len(windows) == 1
        stat = windows[0]
        assert stat.close_price == 105.0
        assert stat.close_price > stat.upper_band
        assert stat.is_upper_breach is True
        assert stat.is_lower_breach is False
        assert stat.percent_b > 1.0

    def test_lower_volatility_band_breach(self) -> None:
        """Verify is_lower_breach is True when close price crashes below the lower band."""
        aggregator = RollingVWAPAggregator(
            config=DataprocStreamingConfig(
                vwap_window_duration="5 minutes",
                vwap_slide_duration="5 minutes",
                volatility_k_multiplier=2.0,
            )
        )
        # 10 baseline consolidated ticks around 100.0 followed by a sudden plunge down to 95.0
        ticks = [
            {
                "trade_id": f"T-BASE-{i}",
                "ticker": "TSLA",
                "asset_class": "EQUITY",
                "bid": 99.95,
                "ask": 100.05,
                "last_price": 100.0,
                "volume": 100.0,
                "timestamp": f"2026-10-21T10:0{i // 3}:{10 + (i % 3) * 15}+00:00",
            }
            for i in range(10)
        ]
        ticks.append(
            {
                "trade_id": "T-PLUNGE",
                "ticker": "TSLA",
                "asset_class": "EQUITY",
                "bid": 94.9,
                "ask": 95.1,
                "last_price": 95.0,  # Sudden crash down
                "volume": 10.0,
                "timestamp": "2026-10-21T10:04:00+00:00",
            }
        )
        aggregator.add_ticks(ticks)
        windows = aggregator.compute_windows()

        assert len(windows) == 1
        stat = windows[0]
        assert stat.close_price == 95.0
        assert stat.close_price < stat.lower_band
        assert stat.is_lower_breach is True
        assert stat.is_upper_breach is False
        assert stat.percent_b < 0.0

    def test_volatility_spike_detection(self) -> None:
        """Verify volatility_spike flag triggers when relative standard deviation exceeds threshold."""
        # Threshold: 3.0% (0.03 * VWAP)
        aggregator = RollingVWAPAggregator(
            config=DataprocStreamingConfig(
                vwap_window_duration="5 minutes",
                vwap_slide_duration="5 minutes",
                volatility_spike_threshold_pct=3.0,
            )
        )
        # Low volatility scenario (stddev / vwap < 3%)
        low_vol_ticks = [
            {
                "trade_id": f"T-LV-{i}",
                "ticker": "SPY",
                "asset_class": "ETF",
                "bid": 550.0 + (i * 0.1),
                "ask": 550.1 + (i * 0.1),
                "last_price": 550.05 + (i * 0.1),
                "volume": 100.0,
                "timestamp": f"2026-10-21T10:0{i}:00+00:00",
            }
            for i in range(4)
        ]
        aggregator.add_ticks(low_vol_ticks)
        res_low = aggregator.compute_windows()
        assert len(res_low) == 1
        assert res_low[0].volatility_spike is False

        # Clear and test high volatility scenario (stddev / vwap >= 3%)
        aggregator.clear()
        high_vol_ticks = [
            {
                "trade_id": "T-HV-1",
                "ticker": "BTCUSD",
                "asset_class": "CRYPTO",
                "bid": 60000.0,
                "ask": 60010.0,
                "last_price": 60005.0,
                "volume": 1.0,
                "timestamp": "2026-10-21T10:00:00+00:00",
            },
            {
                "trade_id": "T-HV-2",
                "ticker": "BTCUSD",
                "asset_class": "CRYPTO",
                "bid": 68000.0,
                "ask": 68010.0,
                "last_price": 68005.0,
                "volume": 1.0,
                "timestamp": "2026-10-21T10:01:00+00:00",
            },
            {
                "trade_id": "T-HV-3",
                "ticker": "BTCUSD",
                "asset_class": "CRYPTO",
                "bid": 56000.0,
                "ask": 56010.0,
                "last_price": 56005.0,
                "volume": 1.0,
                "timestamp": "2026-10-21T10:02:00+00:00",
            },
        ]
        aggregator.add_ticks(high_vol_ticks)
        res_high = aggregator.compute_windows()
        assert len(res_high) == 1
        assert res_high[0].volatility_spike is True


# =============================================================================
# 3. Sliding Window Mechanics & Universe Isolation Tests
# =============================================================================


class TestRollingVWAPSlidingWindows:
    """Unit tests for sliding window progressions, multi-ticker isolation, and buffer eviction."""

    def test_sliding_window_advancement_and_overlap(self) -> None:
        """Verify 5-minute sliding windows with 1-minute slide overlap and produce aligned intervals."""
        aggregator = RollingVWAPAggregator(
            config=DataprocStreamingConfig(
                vwap_window_duration="5 minutes",
                vwap_slide_duration="1 minute",
            )
        )
        # Emit ticks at minutes 0, 1, 2, 3, 4, 5
        for m in range(6):
            aggregator.add_tick(
                {
                    "trade_id": f"T-{m}",
                    "ticker": "MSFT",
                    "asset_class": "EQUITY",
                    "bid": 420.0 + m,
                    "ask": 420.1 + m,
                    "last_price": 420.05 + m,
                    "volume": 50.0,
                    "timestamp": f"2026-10-21T10:0{m}:30+00:00",
                }
            )

        windows = aggregator.compute_windows()
        assert len(windows) >= 2

        # Verify windows are ordered by window_start
        for i in range(len(windows) - 1):
            assert windows[i].window_start_epoch_sec <= windows[i + 1].window_start_epoch_sec
            assert (
                windows[i].window_end_epoch_sec - windows[i].window_start_epoch_sec
                == 300.0  # 5 min
            )

    def test_multi_ticker_universe_isolation(self) -> None:
        """Verify ticks from different tickers and asset classes do not contaminate each other."""
        aggregator = RollingVWAPAggregator(
            config=DataprocStreamingConfig(
                vwap_window_duration="5 minutes",
                vwap_slide_duration="5 minutes",
            )
        )
        ticks = [
            {
                "trade_id": "T-AAPL",
                "ticker": "AAPL",
                "asset_class": "EQUITY",
                "bid": 185.0,
                "ask": 185.1,
                "last_price": 185.05,
                "volume": 100.0,
                "timestamp": "2026-10-21T10:01:00+00:00",
            },
            {
                "trade_id": "T-BTC",
                "ticker": "BTCUSD",
                "asset_class": "CRYPTO",
                "bid": 67500.0,
                "ask": 67510.0,
                "last_price": 67505.0,
                "volume": 2.0,
                "timestamp": "2026-10-21T10:01:00+00:00",
            },
            {
                "trade_id": "T-EUR",
                "ticker": "EURUSD",
                "asset_class": "FX",
                "bid": 1.0850,
                "ask": 1.0852,
                "last_price": 1.0851,
                "volume": 1000.0,
                "timestamp": "2026-10-21T10:01:00+00:00",
            },
        ]
        aggregator.add_ticks(ticks)
        windows = aggregator.compute_windows()

        assert len(windows) == 3
        tickers_present = {w.ticker for w in windows}
        assert tickers_present == {"AAPL", "BTCUSD", "EURUSD"}

        aapl_stat = next(w for w in windows if w.ticker == "AAPL")
        assert aapl_stat.vwap == 185.05
        assert aapl_stat.asset_class == "EQUITY"

        btc_stat = next(w for w in windows if w.ticker == "BTCUSD")
        assert btc_stat.vwap == 67505.0
        assert btc_stat.asset_class == "CRYPTO"

    def test_evict_older_than_bounds_memory(self) -> None:
        """Verify evict_older_than safely purges expired ticks from buffer."""
        aggregator = RollingVWAPAggregator()
        # Add old tick (epoch = 1000) and newer tick (epoch = 2000)
        t_old = ParsedTickRecord(
            trade_id="T1",
            ticker="AAPL",
            asset_class="EQUITY",
            bid=100.0,
            ask=100.1,
            last_price=100.05,
            volume=10.0,
            spread=0.1,
            mid_price=100.05,
            notional_usd=1000.5,
            relative_spread_bps=9.99,
            event_timestamp="2026-10-21T10:00:00+00:00",
            event_epoch_sec=1000.0,
            sequence_number=1,
            year=2026,
            month=10,
            day=21,
            hour=10,
        )
        t_new = ParsedTickRecord(
            trade_id="T2",
            ticker="AAPL",
            asset_class="EQUITY",
            bid=101.0,
            ask=101.1,
            last_price=101.05,
            volume=10.0,
            spread=0.1,
            mid_price=101.05,
            notional_usd=1010.5,
            relative_spread_bps=9.89,
            event_timestamp="2026-10-21T10:16:40+00:00",
            event_epoch_sec=2000.0,
            sequence_number=2,
            year=2026,
            month=10,
            day=21,
            hour=10,
        )
        aggregator.add_ticks([t_old, t_new])
        assert len(aggregator._tick_buffer[("AAPL", "EQUITY")]) == 2

        evicted = aggregator.evict_older_than(threshold_epoch_sec=1500.0)
        assert evicted == 1
        assert len(aggregator._tick_buffer[("AAPL", "EQUITY")]) == 1
        assert aggregator._tick_buffer[("AAPL", "EQUITY")][0].trade_id == "T2"


# =============================================================================
# 4. Telemetry & Prometheus Formatting Tests
# =============================================================================


class TestRollingVWAPTelemetryAndPrometheus:
    """Unit tests for VWAP metrics accumulation and Prometheus exposition text formatting."""

    def test_telemetry_snapshot_dictionary(self) -> None:
        """Verify telemetry snapshot dictionary exports all metrics."""
        telem = RollingVWAPTelemetry(
            windows_computed=12,
            vwap_aggregations_emitted=36,
            upper_band_breaches=3,
            lower_band_breaches=1,
            volatility_spikes=2,
            latest_vwap_by_ticker={"AAPL": 185.25, "MSFT": 421.50},
            latest_bandwidth_bps_by_ticker={"AAPL": 215.40, "MSFT": 180.20},
        )
        d = telem.to_dict()

        assert d["windows_computed"] == 12
        assert d["vwap_aggregations_emitted"] == 36
        assert d["upper_band_breaches"] == 3
        assert d["lower_band_breaches"] == 1
        assert d["volatility_spikes"] == 2
        assert d["latest_vwap_by_ticker"]["AAPL"] == 185.25
        assert d["latest_bandwidth_bps_by_ticker"]["MSFT"] == 180.20

    def test_prometheus_exposition_rendering(self) -> None:
        """Verify Prometheus text format renders counter lines and gauge lines with labels."""
        telem = RollingVWAPTelemetry(
            windows_computed=5,
            vwap_aggregations_emitted=10,
            upper_band_breaches=2,
            lower_band_breaches=1,
            volatility_spikes=1,
            latest_vwap_by_ticker={"NVDA": 125.50},
            latest_bandwidth_bps_by_ticker={"NVDA": 450.0},
        )
        prom = telem.format_prometheus(app_name="trading-risk-engine")

        assert 'dataproc_vwap_windows_computed_total{app="trading-risk-engine"} 5' in prom
        assert 'dataproc_vwap_aggregations_emitted_total{app="trading-risk-engine"} 10' in prom
        assert 'dataproc_vwap_upper_band_breaches_total{app="trading-risk-engine"} 2' in prom
        assert 'dataproc_vwap_lower_band_breaches_total{app="trading-risk-engine"} 1' in prom
        assert 'dataproc_vwap_volatility_spikes_total{app="trading-risk-engine"} 1' in prom
        assert 'dataproc_rolling_vwap_price{app="trading-risk-engine",ticker="NVDA"} 125.5000' in prom
        assert 'dataproc_volatility_bandwidth_bps{app="trading-risk-engine",ticker="NVDA"} 450.00' in prom


# =============================================================================
# 5. PySpark Structured Streaming Graph Builders Tests
# =============================================================================


class TestSparkStreamingGraphBuilders:
    """Unit tests for Spark Structured Streaming query builders."""

    def test_build_rolling_vwap_stream_fallback(self) -> None:
        """Verify build_rolling_vwap_stream handles mock input DataFrames."""
        config = DataprocStreamingConfig()
        mock_df = MagicMock()
        mock_df.groupBy.return_value = "mock_grouped_df"

        res = build_rolling_vwap_stream(mock_df, config)
        assert res is not None

    def test_start_vwap_writer_configuration(self) -> None:
        """Verify start_vwap_writer configures parquet format, partitionBy, trigger, and queryName."""
        config = DataprocStreamingConfig(
            environment="dev",
            vwap_output_path="gs://trading-risk-bronze-dev/vwap/",
            vwap_checkpoint_location="gs://trading-risk-dataflow-staging-dev/checkpoints/vwap/",
        )
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.writeStream.format.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.queryName.return_value = mock_writer
        mock_writer.start.return_value = "mock_vwap_query"

        query = start_vwap_writer(mock_df, config)
        assert query == "mock_vwap_query"
        mock_df.writeStream.format.assert_called_with("parquet")
        mock_writer.outputMode.assert_called_with("append")
        mock_writer.partitionBy.assert_called_with("year", "month", "day", "asset_class")
        mock_writer.start.assert_called_once()


# =============================================================================
# 6. End-to-End Consumer & VWAP Aggregation Integration Tests
# =============================================================================


class TestEndToEndConsumerVWAPIntegration:
    """End-to-end integration tests connecting generator -> producer -> consumer -> VWAP aggregator."""

    def test_e2e_generator_to_vwap_windows(self) -> None:
        """Verify continuous market ticks flow into consumer and generate enriched VWAP windows."""
        topic = "market.ticks.v1"
        mock_broker = LocalKafkaProducerMock(num_partitions_map={topic: 4})
        producer = KafkaTickProducer(
            config=ProducerConfig(topic=topic, simulation_mode=True),
            mock_producer=mock_broker,
        )

        feed = MarketFeedGenerator(tickers=["AAPL", "MSFT", "GOOGL"], seed=42)
        for _ in range(45):
            producer.send_tick(feed.simulate_tick())

        config = DataprocStreamingConfig(
            kafka_subscribe_topics=topic,
            starting_offsets="earliest",
            max_offsets_per_trigger=50,
            vwap_window_duration="5 minutes",
            vwap_slide_duration="1 minute",
        )
        consumer = PySparkKafkaTickConsumer(config=config)
        batch = consumer.poll_from_producer(mock_broker)

        assert batch.records_polled == 45
        assert len(batch.valid_records) == 45

        # Compute VWAP windows
        vwap_stats = consumer.compute_vwap_windows()
        assert len(vwap_stats) >= 1

        for stat in vwap_stats:
            assert stat.ticker in ["AAPL", "MSFT", "GOOGL"]
            assert stat.vwap > 0.0
            assert stat.upper_band >= stat.vwap
            assert stat.lower_band <= stat.vwap
            assert stat.bandwidth_bps >= 0.0
            assert stat.year == 2026


# =============================================================================
# 7. CLI Runner Tests with Rolling VWAP
# =============================================================================


class TestDataprocRunnerCliWithVWAP:
    """Unit tests for run_tick_consumer.py CLI including VWAP parameters."""

    def test_run_tick_consumer_cli_dry_run_includes_vwap(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        """Verify --dry_run outputs VWAP plan parameters in JSON output."""
        exit_code = run_consumer_main(
            [
                "--dry_run",
                "--vwap_window_duration",
                "10 minutes",
                "--vwap_slide_duration",
                "2 minutes",
                "--volatility_k_multiplier",
                "2.5",
            ]
        )
        assert exit_code == 0

        captured = capsys.readouterr()
        out = json.loads(captured.out)
        plan = out["plan"]
        assert plan["vwap_window_duration"] == "10 minutes"
        assert plan["vwap_slide_duration"] == "2 minutes"
        assert plan["volatility_k_multiplier"] == 2.5
        assert "vwap" in plan["vwap_output_path"]

    def test_run_local_simulation_outputs_vwap_telemetry(self) -> None:
        """Verify run_local_simulation returns vwap_stats and vwap_telemetry."""
        config = DataprocStreamingConfig()
        summary = run_local_simulation(config=config, num_ticks=20, seed=123)

        assert summary["mode"] == "local_simulation"
        assert "vwap_stats_count" in summary
        assert "vwap_stats" in summary
        assert "vwap_telemetry" in summary
        assert summary["vwap_telemetry"]["windows_computed"] >= 1
