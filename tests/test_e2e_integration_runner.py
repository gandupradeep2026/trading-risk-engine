"""Zero-Cost Unit & Integration Test Suite for End-to-End Test Runner (Day 29).

Validates:
1. Configuration Models & Enums: PipelineStage, ExecutionScenario, StageStatus, E2ERunnerConfig, E2EPipelineSummary.
2. Synthetic Data Generator: Multi-asset ticks, trade orders, flash crash drawdowns, fat finger spikes, DLQ poison pills.
3. Individual Pipeline Stages:
   - Stage 1: Ingestion (Kafka mock, Pub/Sub mock, DLQ routing)
   - Stage 2: Streaming (Dataflow decoders, anomaly detectors, Cloud SQL SQLite sink, Parquet sink)
   - Stage 3: Lakehouse Batch (Delta Bronze, Silver cleansing, Gold VaR analytics)
   - Stage 4: Data Quality (Great Expectations checkpoint validation suites)
   - Stage 5: DWH Replication (BigQuery Star Schema, Cloud SQL replication, fact tables, MVs)
   - Stage 6: dbt Analytics (15 models in topological order, 84+ schema tests in DuckDB)
   - Stage 7: Monitoring & Observability (Pub/Sub lag, DLQ monitor, Slack Block Kit)
4. End-to-End Data Lineage & Reconciliation:
   - Record count conservation across all architectural tiers
   - Gross notional USD reconciliation
   - Anomaly alert conservation
   - DLQ forensic classification
   - Value-at-Risk parity
   - dbt mart schema test compliance
   - SLA latency adherence
5. Full E2E Pipeline Orchestration: Happy Path and Full E2E execution.
6. Operational Telemetry & Prometheus OpenMetrics Exposition.
7. CLI Runner & Report Exports: Dry-run, stage filtering, Markdown, JSON, and Prometheus exports.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Dict, List
import pytest

from e2e.cli import build_parser, parse_stages_arg, run_e2e_cli
from e2e.config import (
    AssertionResult,
    E2EPipelineSummary,
    E2ERunnerConfig,
    ExecutionScenario,
    PipelineStage,
    StageExecutionResult,
    StageStatus,
)
from e2e.pipeline_runner import E2EIntegrationRunner
from e2e.synthetic_generator import SyntheticE2EDataBatch, SyntheticE2EGenerator
from e2e.telemetry import E2ETelemetry
from e2e.validator import E2EDataLineageValidator

# =============================================================================
# 1. CONFIGURATION MODELS & ENUMS TESTS
# =============================================================================


class TestE2EConfiguration:
    """Validate E2E configuration models, environment loading, and serialization."""

    def test_default_configuration(self) -> None:
        cfg = E2ERunnerConfig()
        assert cfg.environment == "dev"
        assert cfg.scenario == ExecutionScenario.FULL_E2E
        assert len(cfg.stages) == 7
        assert cfg.scale_factor == 1.0
        assert cfg.inject_flash_crash is True
        assert cfg.inject_fat_finger is True
        assert cfg.inject_corrupted_orders is True
        assert cfg.inject_corrupted_ticks is True
        assert cfg.sla_total_seconds == 60.0

    def test_from_env_loading(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("E2E_SCENARIO", "anomaly_injection")
        monkeypatch.setenv("E2E_SCALE_FACTOR", "2.5")
        monkeypatch.setenv("ENVIRONMENT", "staging")
        monkeypatch.setenv("E2E_INJECT_CORRUPTED_ORDERS", "false")

        cfg = E2ERunnerConfig.from_env()
        assert cfg.scenario == ExecutionScenario.ANOMALY_INJECTION
        assert cfg.scale_factor == 2.5
        assert cfg.environment == "staging"
        assert cfg.inject_corrupted_orders is False

    def test_scenario_enums(self) -> None:
        assert ExecutionScenario.HAPPY_PATH.value == "happy_path"
        assert ExecutionScenario.ANOMALY_INJECTION.value == "anomaly_injection"
        assert ExecutionScenario.CORRUPTED_DLQ.value == "corrupted_dlq"
        assert ExecutionScenario.FULL_E2E.value == "full_e2e"

    def test_pipeline_stage_enums(self) -> None:
        assert PipelineStage.INGESTION.value == "ingestion"
        assert PipelineStage.STREAMING.value == "streaming"
        assert PipelineStage.LAKEHOUSE_BATCH.value == "lakehouse_batch"
        assert PipelineStage.DATA_QUALITY.value == "data_quality"
        assert PipelineStage.DWH_REPLICATION.value == "dwh_replication"
        assert PipelineStage.DBT_ANALYTICS.value == "dbt_analytics"
        assert PipelineStage.MONITORING_OBSERVABILITY.value == "monitoring_observability"

    def test_stage_execution_result_serialization(self) -> None:
        res = StageExecutionResult(
            stage=PipelineStage.STREAMING,
            status=StageStatus.PASSED,
            duration_sec=1.2345,
            records_in=50,
            records_out=45,
            metrics={"valid_orders": 45, "dlq_orders": 5},
        )
        assert res.passed is True
        d = res.to_dict()
        assert d["stage"] == "streaming"
        assert d["status"] == "PASSED"
        assert d["duration_sec"] == 1.2345
        assert d["records_in"] == 50
        assert d["records_out"] == 45
        assert d["metrics"]["valid_orders"] == 45

    def test_assertion_result_serialization(self) -> None:
        ar = AssertionResult(
            name="order_conservation",
            category="RECORD_CONSERVATION",
            passed=True,
            expected=100,
            actual=100,
            message="Counts match",
        )
        d = ar.to_dict()
        assert d["name"] == "order_conservation"
        assert d["category"] == "RECORD_CONSERVATION"
        assert d["passed"] is True
        assert d["expected"] == 100
        assert d["actual"] == 100

    def test_pipeline_summary_markdown_and_json(self) -> None:
        summary = E2EPipelineSummary(
            scenario=ExecutionScenario.FULL_E2E,
            environment="dev",
            target_date="2026-11-09",
            success=True,
            total_duration_sec=3.456,
            stage_results={
                "ingestion": StageExecutionResult(
                    stage=PipelineStage.INGESTION,
                    status=StageStatus.PASSED,
                    duration_sec=0.5,
                    records_in=100,
                    records_out=100,
                    metrics={"produced": 100},
                )
            },
            validation_results=[
                AssertionResult(
                    name="order_count_check",
                    category="RECORD_CONSERVATION",
                    passed=True,
                    expected=100,
                    actual=100,
                    message="All orders matched",
                )
            ],
            reconciliation_metrics={"total_notional": 1250000.50},
        )

        md = summary.to_markdown()
        assert "# Trading Risk Engine - End-to-End Integration Test Report" in md
        assert "✅ PASSED" in md
        assert "`full_e2e`" in md
        assert "`order_count_check`" in md
        assert "$1,250,000.50" in md

        js = summary.to_json()
        data = json.loads(js)
        assert data["scenario"] == "full_e2e"
        assert data["success"] is True
        assert len(data["stages"]) == 1
        assert data["validations"]["passed"] == 1


# =============================================================================
# 2. SYNTHETIC DATA GENERATOR TESTS
# =============================================================================


class TestSyntheticDataGenerator:
    """Validate synthetic market tick, trade order, anomaly, and DLQ generation."""

    def test_synthetic_generator_reproducibility(self) -> None:
        gen1 = SyntheticE2EGenerator(seed=123)
        gen2 = SyntheticE2EGenerator(seed=123)
        b1 = gen1.generate(E2ERunnerConfig(scale_factor=0.5))
        b2 = gen2.generate(E2ERunnerConfig(scale_factor=0.5))

        assert len(b1.market_ticks) == len(b2.market_ticks)
        assert len(b1.trade_orders) == len(b2.trade_orders)
        assert b1.expected_total_notional_usd == b2.expected_total_notional_usd

    def test_clean_order_generation(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        batch = gen.generate(E2ERunnerConfig(scale_factor=0.5))

        assert len(batch.trade_orders) > 0
        for o in batch.trade_orders:
            assert o.order_id.startswith("ORD-E2E-")
            assert o.side in ("BUY", "SELL")
            assert o.order_type in ("MARKET", "LIMIT")
            assert o.price > 0
            assert o.quantity > 0
            assert o.notional_usd == round(o.price * o.quantity, 2)
            assert o.portfolio_id.startswith("PORTFOLIO_")

    def test_clean_market_tick_generation(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        batch = gen.generate(E2ERunnerConfig(scale_factor=0.5))

        assert len(batch.market_ticks) > 0
        for t in batch.market_ticks:
            assert t.ticker in ("AAPL", "MSFT", "GOOGL", "NVDA", "SPY", "EURUSD", "BTCUSD", "XAUUSD")
            assert t.bid > 0
            assert t.ask > 0
            assert t.bid <= t.ask
            assert t.volume > 0

    def test_flash_crash_injection(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        cfg = E2ERunnerConfig(scenario=ExecutionScenario.ANOMALY_INJECTION)
        batch = gen.generate(cfg)

        assert len(batch.flash_crash_orders) == 5
        prices = [o.price for o in batch.flash_crash_orders]
        assert prices == [200.0, 196.0, 192.0, 188.0, 185.0]
        drop_pct = ((prices[0] - prices[-1]) / prices[0]) * 100.0
        assert drop_pct == 7.5
        assert batch.expected_flash_crash_alerts_count == 1

    def test_fat_finger_injection(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        cfg = E2ERunnerConfig(scenario=ExecutionScenario.ANOMALY_INJECTION)
        batch = gen.generate(cfg)

        assert len(batch.fat_finger_orders) == 1
        ff = batch.fat_finger_orders[0]
        assert ff.notional_usd >= 5_000_000.0
        assert ff.price == 500.0
        assert ff.quantity == 12500
        assert ff.notional_usd == 6_250_000.0
        assert batch.expected_fat_finger_alerts_count == 1

    def test_corrupted_order_generation(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        cfg = E2ERunnerConfig(scenario=ExecutionScenario.CORRUPTED_DLQ)
        batch = gen.generate(cfg)

        assert len(batch.corrupted_orders) == 4
        error_types = [o["error_type"] for o in batch.corrupted_orders]
        assert "NON_POSITIVE_PRICE" in error_types
        assert "NON_POSITIVE_QUANTITY" in error_types
        assert "INVALID_SIDE" in error_types
        assert "JSON_DECODE_ERROR" in error_types

    def test_corrupted_tick_generation(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        cfg = E2ERunnerConfig(scenario=ExecutionScenario.CORRUPTED_DLQ)
        batch = gen.generate(cfg)

        assert len(batch.corrupted_ticks) == 3
        error_types = [t["error_type"] for t in batch.corrupted_ticks]
        assert "CROSSED_QUOTE" in error_types
        assert "NEGATIVE_PRICE" in error_types
        assert "NEGATIVE_VOLUME" in error_types

    def test_portfolio_limits_generation(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        batch = gen.generate(E2ERunnerConfig())

        assert len(batch.portfolio_limits) == 3
        pids = [lim["portfolio_id"] for lim in batch.portfolio_limits]
        assert "PORTFOLIO_ALPHA" in pids
        assert "PORTFOLIO_BETA" in pids
        assert "PORTFOLIO_GAMMA" in pids

    def test_scale_factor_volumetric_scaling(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        b_small = gen.generate(E2ERunnerConfig(scale_factor=0.2))
        b_large = gen.generate(E2ERunnerConfig(scale_factor=1.0))

        assert len(b_large.trade_orders) > len(b_small.trade_orders)
        assert len(b_large.market_ticks) > len(b_small.market_ticks)


# =============================================================================
# 3. INDIVIDUAL PIPELINE STAGES TESTS
# =============================================================================


class TestE2EPipelineStages:
    """Validate each of the seven stages executed within the runner."""

    @pytest.fixture
    def runner(self) -> E2EIntegrationRunner:
        cfg = E2ERunnerConfig(
            environment="dev",
            scenario=ExecutionScenario.FULL_E2E,
            scale_factor=0.3,
        )
        r = E2EIntegrationRunner(config=cfg)
        yield r
        r.close()

    def test_stage_ingestion_execution(self, runner: E2EIntegrationRunner) -> None:
        res = runner.run_stage_ingestion()
        assert res.passed is True
        assert res.stage == PipelineStage.INGESTION
        assert res.records_out > 0
        assert runner.kafka_mock is not None
        assert runner.pubsub_mock is not None
        assert runner.kafka_mock.get_total_records("market.ticks.v1") > 0
        assert runner.kafka_mock.get_total_records("market.ticks.deadletter") == len(runner.batch.corrupted_ticks)

    def test_stage_streaming_execution(self, runner: E2EIntegrationRunner) -> None:
        runner.run_stage_ingestion()
        res = runner.run_stage_streaming()
        assert res.passed is True
        assert res.stage == PipelineStage.STREAMING
        assert res.metrics["valid_orders_decoded"] == runner.batch.expected_valid_orders_count
        assert res.metrics["dlq_orders_emitted"] == runner.batch.expected_dlq_orders_count
        assert res.metrics["flash_crash_alerts_emitted"] == 1
        assert res.metrics["fat_finger_alerts_emitted"] == 1
        assert res.metrics["alerts_stored_cloudsql"] == 2
        assert res.metrics["parquet_ticks_written"] == runner.batch.expected_valid_ticks_count

    def test_stage_lakehouse_batch_execution(self, runner: E2EIntegrationRunner) -> None:
        runner.run_stage_ingestion()
        runner.run_stage_streaming()
        res = runner.run_stage_lakehouse_batch()
        assert res.passed is True
        assert res.stage == PipelineStage.LAKEHOUSE_BATCH
        assert res.metrics["silver_orders_count"] == runner.batch.expected_valid_orders_count
        assert res.metrics["silver_ticks_count"] == runner.batch.expected_valid_ticks_count
        assert res.metrics["gold_portfolio_var_count"] > 0

    def test_stage_data_quality_execution(self, runner: E2EIntegrationRunner) -> None:
        runner.run_stage_ingestion()
        runner.run_stage_streaming()
        runner.run_stage_lakehouse_batch()
        res = runner.run_stage_data_quality()
        assert res.passed is True
        assert res.stage == PipelineStage.DATA_QUALITY
        assert res.metrics["suites_evaluated"] == 3
        assert res.metrics["all_passed"] is True
        assert res.metrics["expectations_failed"] == 0

    def test_stage_dwh_replication_execution(self, runner: E2EIntegrationRunner) -> None:
        runner.run_stage_ingestion()
        runner.run_stage_streaming()
        runner.run_stage_lakehouse_batch()
        runner.run_stage_data_quality()
        res = runner.run_stage_dwh_replication()
        assert res.passed is True
        assert res.stage == PipelineStage.DWH_REPLICATION
        assert res.metrics["fact_trade_orders_count"] == runner.batch.expected_valid_orders_count
        assert res.metrics["fact_market_ticks_count"] == runner.batch.expected_valid_ticks_count
        assert res.metrics["fact_risk_alerts_count"] >= 2
        assert res.metrics["fact_risk_stats_count"] > 0
        assert res.metrics["fact_orders_total_notional"] > 0

    def test_stage_dbt_analytics_execution(self, runner: E2EIntegrationRunner) -> None:
        runner.run_stage_ingestion()
        runner.run_stage_streaming()
        runner.run_stage_lakehouse_batch()
        runner.run_stage_data_quality()
        runner.run_stage_dwh_replication()
        res = runner.run_stage_dbt_analytics()
        assert res.passed is True
        assert res.stage == PipelineStage.DBT_ANALYTICS
        assert res.metrics["models_executed"] == 15
        assert res.metrics["models_failed"] == 0
        assert res.metrics["tests_passed"] > 0
        assert res.metrics["tests_failed"] == 0
        assert res.metrics["dbt_orders_total_notional"] > 0

    def test_stage_monitoring_observability_execution(self, runner: E2EIntegrationRunner) -> None:
        runner.run_stage_ingestion()
        runner.run_stage_streaming()
        res = runner.run_stage_monitoring_observability()
        assert res.passed is True
        assert res.stage == PipelineStage.MONITORING_OBSERVABILITY
        assert res.metrics["dlq_records_scanned"] == len(runner.batch.corrupted_orders) + len(
            runner.batch.corrupted_ticks
        )
        assert res.metrics["slack_alerts_rendered"] == 1


# =============================================================================
# 4. DATA LINEAGE & MATHEMATICAL RECONCILIATION TESTS
# =============================================================================


class TestE2EDataLineageValidation:
    """Validate mathematical invariant assertion logic across the validator."""

    def test_record_conservation_validation_success(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        batch = gen.generate(E2ERunnerConfig(scale_factor=0.3))
        validator = E2EDataLineageValidator()

        stage_results = {
            "ingestion": StageExecutionResult(
                stage=PipelineStage.INGESTION,
                status=StageStatus.PASSED,
                metrics={"orders_produced": batch.expected_valid_orders_count},
            ),
            "streaming": StageExecutionResult(
                stage=PipelineStage.STREAMING,
                status=StageStatus.PASSED,
                metrics={
                    "valid_orders_decoded": batch.expected_valid_orders_count,
                    "dlq_orders_emitted": batch.expected_dlq_orders_count,
                },
            ),
            "lakehouse_batch": StageExecutionResult(
                stage=PipelineStage.LAKEHOUSE_BATCH,
                status=StageStatus.PASSED,
                metrics={
                    "silver_orders_count": batch.expected_valid_orders_count,
                    "silver_ticks_count": batch.expected_valid_ticks_count,
                },
            ),
            "dwh_replication": StageExecutionResult(
                stage=PipelineStage.DWH_REPLICATION,
                status=StageStatus.PASSED,
                metrics={
                    "fact_trade_orders_count": batch.expected_valid_orders_count,
                    "fact_market_ticks_count": batch.expected_valid_ticks_count,
                },
            ),
        }

        assertions = validator.validate_record_conservation(batch, stage_results)
        assert len(assertions) == 6
        assert all(a.passed for a in assertions)

    def test_record_conservation_catches_leakage(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        batch = gen.generate(E2ERunnerConfig(scale_factor=0.3))
        validator = E2EDataLineageValidator()

        stage_results = {
            "dwh_replication": StageExecutionResult(
                stage=PipelineStage.DWH_REPLICATION,
                status=StageStatus.PASSED,
                metrics={
                    "fact_trade_orders_count": batch.expected_valid_orders_count - 2,  # 2 missing orders!
                    "fact_market_ticks_count": batch.expected_valid_ticks_count,
                },
            ),
        }

        assertions = validator.validate_record_conservation(batch, stage_results)
        failed = [a for a in assertions if not a.passed]
        assert len(failed) == 1
        assert failed[0].name == "bigquery_fact_orders_conservation"

    def test_notional_reconciliation_validation(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        batch = gen.generate(E2ERunnerConfig(scale_factor=0.3))
        validator = E2EDataLineageValidator()

        stage_results = {
            "dwh_replication": StageExecutionResult(
                stage=PipelineStage.DWH_REPLICATION,
                status=StageStatus.PASSED,
                metrics={"fact_orders_total_notional": batch.expected_total_notional_usd},
            ),
            "dbt_analytics": StageExecutionResult(
                stage=PipelineStage.DBT_ANALYTICS,
                status=StageStatus.PASSED,
                metrics={"dbt_orders_total_notional": batch.expected_total_notional_usd},
            ),
        }

        assertions = validator.validate_notional_reconciliation(batch, stage_results)
        assert len(assertions) == 2
        assert all(a.passed for a in assertions)

    def test_anomaly_detection_integrity(self) -> None:
        gen = SyntheticE2EGenerator(seed=42)
        batch = gen.generate(E2ERunnerConfig(scenario=ExecutionScenario.ANOMALY_INJECTION))
        validator = E2EDataLineageValidator()

        stage_results = {
            "streaming": StageExecutionResult(
                stage=PipelineStage.STREAMING,
                status=StageStatus.PASSED,
                metrics={"flash_crash_alerts_emitted": 1, "fat_finger_alerts_emitted": 1},
            ),
            "dwh_replication": StageExecutionResult(
                stage=PipelineStage.DWH_REPLICATION,
                status=StageStatus.PASSED,
                metrics={"fact_risk_alerts_count": 2},
            ),
        }

        assertions = validator.validate_anomaly_detection_integrity(batch, stage_results)
        assert len(assertions) == 3
        assert all(a.passed for a in assertions)

    def test_sla_performance_validation(self) -> None:
        validator = E2EDataLineageValidator(E2ERunnerConfig(sla_total_seconds=5.0, sla_stage_seconds=2.0))
        stage_results = {
            "ingestion": StageExecutionResult(stage=PipelineStage.INGESTION, duration_sec=0.4),
            "streaming": StageExecutionResult(stage=PipelineStage.STREAMING, duration_sec=1.1),
        }

        # Passing run
        assertions = validator.validate_sla_performance(stage_results, total_duration_sec=2.5)
        assert all(a.passed for a in assertions)

        # Breach total SLA
        breach_assertions = validator.validate_sla_performance(stage_results, total_duration_sec=6.5)
        failed = [a for a in breach_assertions if not a.passed]
        assert len(failed) == 1
        assert failed[0].name == "total_pipeline_sla_compliance"


# =============================================================================
# 5. FULL END-TO-END PIPELINE RUNNER TESTS
# =============================================================================


class TestEndToEndPipelineExecution:
    """Validate full end-to-end integration pipeline execution across scenarios."""

    def test_full_e2e_pipeline_execution(self) -> None:
        """Full institutional pipeline: ingestion -> streaming -> batch -> DQ -> DWH -> dbt -> monitoring."""
        cfg = E2ERunnerConfig(
            scenario=ExecutionScenario.FULL_E2E,
            scale_factor=0.3,
            sla_total_seconds=60.0,
        )
        runner = E2EIntegrationRunner(config=cfg)
        try:
            summary = runner.run_all()
            assert summary.success is True
            assert summary.failed_assertions == 0
            assert summary.passed_assertions >= 12
            assert len(summary.stage_results) == 7
            for stage_name, res in summary.stage_results.items():
                assert res.passed is True, f"Stage {stage_name} failed: {res.error_message}"

            # Verify reconciliation metrics populated
            rm = summary.reconciliation_metrics
            assert rm["ingested_orders_count"] > 0
            assert rm["ingested_total_notional_usd"] == rm["bigquery_orders_notional_usd"]
            assert rm["bigquery_orders_notional_usd"] == rm["dbt_orders_notional_usd"]
            assert rm["corrupted_dlq_orders_count"] == 4
            assert rm["corrupted_dlq_ticks_count"] == 3
        finally:
            runner.close()

    def test_happy_path_scenario_execution(self) -> None:
        """Happy path scenario: clean order flow, zero anomalies, zero DLQ records."""
        cfg = E2ERunnerConfig(
            scenario=ExecutionScenario.HAPPY_PATH,
            scale_factor=0.2,
            inject_flash_crash=False,
            inject_fat_finger=False,
            inject_corrupted_orders=False,
            inject_corrupted_ticks=False,
        )
        runner = E2EIntegrationRunner(config=cfg)
        try:
            summary = runner.run_all()
            assert summary.success is True
            assert summary.reconciliation_metrics["corrupted_dlq_orders_count"] == 0
            assert summary.reconciliation_metrics["corrupted_dlq_ticks_count"] == 0
        finally:
            runner.close()

    def test_partial_stage_execution(self) -> None:
        """Execute only a subset of pipeline stages (ingestion, streaming)."""
        cfg = E2ERunnerConfig(
            scenario=ExecutionScenario.FULL_E2E,
            stages=[PipelineStage.INGESTION, PipelineStage.STREAMING],
            scale_factor=0.2,
        )
        runner = E2EIntegrationRunner(config=cfg)
        try:
            summary = runner.run_all()
            assert summary.success is True
            assert len(summary.stage_results) == 2
            assert "ingestion" in summary.stage_results
            assert "streaming" in summary.stage_results
            assert "lakehouse_batch" not in summary.stage_results
        finally:
            runner.close()


# =============================================================================
# 6. TELEMETRY & OPENMETRICS TESTS
# =============================================================================


class TestE2ETelemetryAndOpenMetrics:
    """Validate operational metrics recording and OpenMetrics formatting."""

    def test_telemetry_recording_and_counters(self) -> None:
        tel = E2ETelemetry()
        tel.record_run("full_e2e", success=True, duration_sec=4.5)
        tel.record_stage("streaming", duration_sec=1.2, records_in=100, records_out=95)
        tel.record_assertion("RECORD_CONSERVATION", passed=True)
        tel.record_assertion("NOTIONAL_RECONCILIATION", passed=False)
        tel.record_sla_violation()

        assert tel.runs_total[("full_e2e", "SUCCESS")] == 1
        assert tel.stage_durations["streaming"] == 1.2
        assert tel.records_processed[("streaming", "records_in")] == 100
        assert tel.assertions_evaluated[("RECORD_CONSERVATION", "PASS")] == 1
        assert tel.assertions_evaluated[("NOTIONAL_RECONCILIATION", "FAIL")] == 1
        assert tel.sla_violations_total == 1

    def test_prometheus_metrics_formatting(self) -> None:
        tel = E2ETelemetry()
        tel.record_run("full_e2e", success=True, duration_sec=2.3)
        tel.record_stage("ingestion", duration_sec=0.5, records_in=50, records_out=50)

        prom = tel.format_prometheus()
        assert "# HELP e2e_pipeline_runs_total" in prom
        assert "# TYPE e2e_pipeline_runs_total counter" in prom
        assert 'e2e_pipeline_runs_total{scenario="full_e2e",status="SUCCESS"} 1' in prom
        assert "# HELP e2e_stage_duration_seconds" in prom
        assert 'e2e_stage_duration_seconds{stage="ingestion"} 0.5000' in prom
        assert "e2e_sla_violations_total 0" in prom


# =============================================================================
# 7. COMMAND-LINE INTERFACE (CLI) TESTS
# =============================================================================


class TestE2ECommandLineInterface:
    """Validate CLI argument parsing, dry-run mode, and export flags."""

    def test_cli_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        assert args.scenario == "full_e2e"
        assert args.stages == "all"
        assert args.scale_factor == 1.0
        assert args.environment == "dev"
        assert args.dry_run is False

    def test_parse_stages_arg(self) -> None:
        all_st = parse_stages_arg("all")
        assert len(all_st) == 7

        subset = parse_stages_arg("ingestion,streaming,lakehouse_batch")
        assert len(subset) == 3
        assert PipelineStage.INGESTION in subset
        assert PipelineStage.STREAMING in subset

        with pytest.raises(ValueError, match="Unknown stage 'invalid_stage'"):
            parse_stages_arg("ingestion,invalid_stage")

    def test_cli_dry_run_mode(self, capsys: pytest.CaptureFixture) -> None:
        code = run_e2e_cli(["--dry-run", "--scenario", "full_e2e", "--scale-factor", "0.5"])
        assert code == 0
        captured = capsys.readouterr().out
        data = json.loads(captured)
        assert data["dry_run"] is True
        assert data["scenario"] == "full_e2e"
        assert "synthetic_batch" in data
        assert data["synthetic_batch"]["expected_valid_orders"] > 0

    def test_cli_invalid_scenario_rejection(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            run_e2e_cli(["--scenario", "invalid_scenario"])
        assert exc_info.value.code == 2

    def test_cli_execution_with_report_exports(self, tmp_path: Path) -> None:
        json_file = tmp_path / "summary.json"
        md_file = tmp_path / "summary.md"
        prom_file = tmp_path / "metrics.prom"

        code = run_e2e_cli(
            [
                "--scenario",
                "happy_path",
                "--scale-factor",
                "0.2",
                "--stages",
                "ingestion,streaming",
                "--json-report",
                str(json_file),
                "--markdown-report",
                str(md_file),
                "--export-prometheus",
                str(prom_file),
            ]
        )
        assert code == 0
        assert json_file.exists()
        assert md_file.exists()
        assert prom_file.exists()

        json_content = json.loads(json_file.read_text(encoding="utf-8"))
        assert json_content["scenario"] == "happy_path"
        assert "ingestion" in json_content["stages"]

        md_content = md_file.read_text(encoding="utf-8")
        assert "# Trading Risk Engine - End-to-End Integration Test Report" in md_content

        prom_content = prom_file.read_text(encoding="utf-8")
        assert "e2e_pipeline_runs_total" in prom_content
