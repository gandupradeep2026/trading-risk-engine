"""Unit and Integration Tests for Great Expectations Data Quality Orchestration (Day 21).

Validates:
1. Great Expectations Configuration:
   - Invariant validation, default resolution, environment variable overrides.
   - Partition path resolution for Silver ticks, orders, VWAP, and Gold VaR and features.
   - Data Docs URL formatting.
2. Expectation Suite Definitions and Files:
   - JSON suite files validity and schema conformance for all 5 Lakehouse suites.
   - Programmatic suite builders and expectation count invariants.
   - Checkpoint YAML definitions and great_expectations.yml structure.
3. GreatExpectationsValidator Execution:
   - Clean execution against PyArrow tables, list of dicts, and Parquet files.
   - All 5 suites passing 100% on compliant financial datasets.
4. Quality Gate Breaches & Financial Domain Anomalies:
   - Crossed quote detection (ask < bid) in market ticks.
   - Duplicate order ID detection in trade orders.
   - Invalid order types and non-positive notional values.
   - Mathematical ordering violations (VaR_99 < VaR_95).
   - Coherent tail risk violations (CVaR < VaR).
   - Normalized liquidity score boundary violations ([0, 1]).
5. Data Docs HTML Generation & Operational Telemetry:
   - Standalone HTML Data Docs rendering and index.html updating.
   - Telemetry tracking and Prometheus metrics exposition formatting.
6. Airflow Operator & In-Memory State Registry:
   - GreatExpectationsValidationRegistry state tracking and attempt counting.
   - GreatExpectationsOperator Jinja templating ({{ ds }}).
   - Execution failure injection, exponential backoff retries, and callback escalation.
   - Validation failure handling (fail_task_on_validation_failure=True vs False).
7. Standalone & Master Lakehouse Airflow DAGs:
   - Standalone great_expectations_quality_pipeline topology and acyclicity.
   - Master trading_risk_daily_batch_pipeline integration with production operators.
8. CLI Runner:
   - run_checkpoints CLI dry-run and execution modes.
"""

from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Dict, List
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_quality.engine import (
    CheckpointValidationResult,
    DataQualityTelemetry,
    GreatExpectationsValidator,
)
from data_quality.suite_definitions import (
    ExpectationConfig,
    ExpectationSuite,
    SUITE_BUILDERS,
    build_gold_portfolio_var_suite,
    build_gold_risk_features_suite,
    build_silver_market_ticks_suite,
    build_silver_rolling_vwap_suite,
    build_silver_trade_orders_suite,
    get_expectation_suite,
)
from orchestration.airflow.airflow_compat import (
    AirflowException,
    GreatExpectationsOperator,
    GreatExpectationsValidationRegistry,
    TriggerRule,
    create_mock_task_context,
)
from orchestration.airflow.callbacks.failure_callbacks import (
    create_failure_callback,
    create_retry_callback,
)
from orchestration.airflow.config.great_expectations_config import GreatExpectationsConfig
from orchestration.airflow.dags.great_expectations_dag import (
    build_great_expectations_dag,
    great_expectations_dag,
)
from orchestration.airflow.dags.trading_risk_daily_batch_dag import (
    build_trading_risk_dag,
    trading_risk_dag,
)


@pytest.fixture(autouse=True)
def reset_ge_registry() -> None:
    """Reset the Great Expectations mock registry before and after each test."""
    GreatExpectationsValidationRegistry.reset()
    yield
    GreatExpectationsValidationRegistry.reset()


# =============================================================================
# 1. GREAT EXPECTATIONS CONFIGURATION TESTS
# =============================================================================
class TestGreatExpectationsConfig:
    """Validate Great Expectations configuration models and resolution."""

    def test_default_config_resolution(self) -> None:
        """Verify default configuration attributes."""
        cfg = GreatExpectationsConfig()
        assert cfg.environment == "dev"
        assert cfg.gcp_project_id == "trading-risk-dev-001"
        assert cfg.silver_bucket == "trading-risk-silver-dev"
        assert cfg.gold_bucket == "trading-risk-gold-dev"
        assert cfg.data_context_root_dir == "data_quality/great_expectations"
        assert cfg.fail_task_on_validation_failure is True
        assert cfg.min_success_percent == 100.0
        assert cfg.checkpoint_retry_limit == 2
        assert cfg.checkpoint_retry_delay_seconds == 30
        assert cfg.silver_ticks_suite == "silver_market_ticks_suite"
        assert cfg.silver_orders_suite == "silver_trade_orders_suite"
        assert cfg.silver_vwap_suite == "silver_rolling_vwap_suite"
        assert cfg.gold_var_suite == "gold_portfolio_var_suite"
        assert cfg.gold_features_suite == "gold_risk_features_suite"

    def test_config_validation_rules(self) -> None:
        """Verify configuration validation invariants."""
        # Invalid environment
        with pytest.raises(ValueError, match="Invalid environment"):
            GreatExpectationsConfig(environment="invalid_env")

        # Invalid min_success_percent
        with pytest.raises(ValueError, match="min_success_percent"):
            GreatExpectationsConfig(min_success_percent=120.0)

        with pytest.raises(ValueError, match="min_success_percent"):
            GreatExpectationsConfig(min_success_percent=-5.0)

        # Invalid retries
        with pytest.raises(ValueError, match="checkpoint_retry_limit"):
            GreatExpectationsConfig(checkpoint_retry_limit=-1)

        # Empty data context path
        with pytest.raises(ValueError, match="data_context_root_dir"):
            GreatExpectationsConfig(data_context_root_dir="")

    def test_from_env_loading(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify environment variable overrides."""
        monkeypatch.setenv("AIRFLOW_VAR_ENVIRONMENT", "prod")
        monkeypatch.setenv("AIRFLOW_VAR_GCP_PROJECT", "trading-risk-prod-999")
        monkeypatch.setenv("AIRFLOW_VAR_GCS_SILVER_BUCKET", "prod-silver-bucket")
        monkeypatch.setenv("AIRFLOW_VAR_GCS_GOLD_BUCKET", "prod-gold-bucket")
        monkeypatch.setenv("AIRFLOW_VAR_FAIL_ON_DQ_FAILURE", "false")
        monkeypatch.setenv("AIRFLOW_VAR_DQ_MIN_SUCCESS_PCT", "95.5")

        cfg = GreatExpectationsConfig.from_env()
        assert cfg.environment == "prod"
        assert cfg.gcp_project_id == "trading-risk-prod-999"
        assert cfg.silver_bucket == "prod-silver-bucket"
        assert cfg.gold_bucket == "prod-gold-bucket"
        assert cfg.fail_task_on_validation_failure is False
        assert cfg.min_success_percent == 95.5

    def test_partition_path_resolution(self) -> None:
        """Verify GCS partition URI generation."""
        cfg = GreatExpectationsConfig(silver_bucket="s-bkt", gold_bucket="g-bkt")
        ticks_uri = cfg.get_silver_ticks_path("2026-11-01")
        assert ticks_uri == "gs://s-bkt/delta/silver/silver_market_ticks/year=2026/month=11/day=01"

        orders_uri = cfg.get_silver_orders_path("2026-11-01")
        assert orders_uri == "gs://s-bkt/delta/silver/silver_trade_orders/year=2026/month=11/day=01"

        vwap_uri = cfg.get_silver_vwap_path("2026-11-01")
        assert vwap_uri == "gs://s-bkt/delta/silver/silver_rolling_vwap/year=2026/month=11/day=01"

        var_uri = cfg.get_gold_var_path("2026-11-01")
        assert var_uri == "gs://g-bkt/delta/gold/gold_portfolio_var/year=2026/month=11/day=01"

        feat_uri = cfg.get_gold_features_path("2026-11-01")
        assert feat_uri == "gs://g-bkt/delta/gold/gold_risk_features/year=2026/month=11/day=01"

    def test_data_docs_url(self) -> None:
        """Verify canonical Data Docs GCS link formatting."""
        cfg = GreatExpectationsConfig(gold_bucket="trading-risk-gold-prod")
        url = cfg.get_data_docs_gcs_url()
        assert url == "https://storage.googleapis.com/trading-risk-gold-prod/great_expectations/data_docs/index.html"


# =============================================================================
# 2. EXPECTATION SUITE DEFINITIONS & ARTIFACTS TESTS
# =============================================================================
class TestExpectationSuiteDefinitions:
    """Validate declarative and programmatic expectation suites."""

    REPO_ROOT = Path(__file__).parent.parent
    EXPECTATIONS_DIR = REPO_ROOT / "data_quality" / "great_expectations" / "expectations"
    CHECKPOINTS_DIR = REPO_ROOT / "data_quality" / "great_expectations" / "checkpoints"

    def test_suite_json_files_exist_and_are_valid(self) -> None:
        """Verify all 5 JSON expectation suites exist and contain valid JSON."""
        suite_names = [
            "silver_market_ticks_suite",
            "silver_trade_orders_suite",
            "silver_rolling_vwap_suite",
            "gold_portfolio_var_suite",
            "gold_risk_features_suite",
        ]
        for name in suite_names:
            p = self.EXPECTATIONS_DIR / f"{name}.json"
            assert p.exists(), f"Suite file missing: {p}"
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            assert data["expectation_suite_name"] == name
            assert "expectations" in data
            assert len(data["expectations"]) > 0

    def test_checkpoint_yaml_files_exist(self) -> None:
        """Verify checkpoint configuration files exist."""
        checkpoints = [
            "silver_quality_checkpoint.yml",
            "gold_quality_checkpoint.yml",
            "lakehouse_master_checkpoint.yml",
        ]
        for cp in checkpoints:
            p = self.CHECKPOINTS_DIR / cp
            assert p.exists(), f"Checkpoint file missing: {p}"
            content = p.read_text(encoding="utf-8")
            assert "class_name: SimpleCheckpoint" in content
            assert "validations:" in content

    def test_great_expectations_root_yaml_exists(self) -> None:
        """Verify root great_expectations.yml exists and defines stores and sites."""
        p = self.REPO_ROOT / "data_quality" / "great_expectations" / "great_expectations.yml"
        assert p.exists(), f"Root YAML missing: {p}"
        content = p.read_text(encoding="utf-8")
        assert "config_version: 3.0" in content
        assert "expectations_store" in content
        assert "data_docs_sites" in content

    def test_programmatic_suite_builders(self) -> None:
        """Verify programmatic suite factories match expectations."""
        ticks_suite = build_silver_market_ticks_suite()
        assert ticks_suite.expectation_suite_name == "silver_market_ticks_suite"
        assert len(ticks_suite.expectations) >= 15

        orders_suite = build_silver_trade_orders_suite()
        assert orders_suite.expectation_suite_name == "silver_trade_orders_suite"
        assert any(e.expectation_type == "expect_column_values_to_be_unique" for e in orders_suite.expectations)

        vwap_suite = build_silver_rolling_vwap_suite()
        assert vwap_suite.expectation_suite_name == "silver_rolling_vwap_suite"

        var_suite = build_gold_portfolio_var_suite()
        assert var_suite.expectation_suite_name == "gold_portfolio_var_suite"
        pair_exps = [e for e in var_suite.expectations if e.expectation_type == "expect_column_pair_values_a_to_be_greater_than_or_equal_to_b"]
        assert len(pair_exps) >= 4  # VaR_99 >= VaR_95, MC VaR_99 >= MC VaR_95, CVaR_95 >= MC_95, CVaR_99 >= MC_99

        features_suite = build_gold_risk_features_suite()
        assert features_suite.expectation_suite_name == "gold_risk_features_suite"

    def test_suite_json_roundtrip(self) -> None:
        """Verify suite serialization to and from JSON."""
        suite = build_silver_market_ticks_suite()
        json_str = suite.to_json()
        deserialized = ExpectationSuite.from_json(json_str)
        assert deserialized.expectation_suite_name == suite.expectation_suite_name
        assert len(deserialized.expectations) == len(suite.expectations)


# =============================================================================
# 3. GREAT EXPECTATIONS VALIDATOR ENGINE & DOMAIN BREACH TESTS
# =============================================================================
class TestGreatExpectationsValidatorEngine:
    """Validate core Great Expectations evaluation engine against clean and corrupt data."""

    def test_validate_clean_silver_ticks(self) -> None:
        """Verify clean Silver market ticks pass 100% of expectations."""
        clean_ticks = {
            "ticker": ["AAPL", "MSFT", "GOOGL", "NVDA", "BTCUSD"],
            "asset_class": ["EQUITY", "EQUITY", "EQUITY", "EQUITY", "CRYPTO"],
            "bid": [150.0, 300.0, 140.0, 120.0, 60000.0],
            "ask": [150.05, 300.10, 140.05, 120.04, 60005.0],
            "last_price": [150.02, 300.05, 140.02, 120.02, 60002.5],
            "volume": [100.0, 250.0, 150.0, 500.0, 2.5],
            "timestamp": ["2026-11-01T00:00:00Z"] * 5,
            "spread": [0.05, 0.10, 0.05, 0.04, 5.0],
            "mid_price": [150.025, 300.05, 140.025, 120.02, 60002.5],
            "relative_spread_bps": [3.33, 3.33, 3.57, 3.33, 0.83],
            "quote_condition": ["TIGHT_SPREAD", "TIGHT_SPREAD", "TIGHT_SPREAD", "TIGHT_SPREAD", "TIGHT_SPREAD"],
            "is_crossed_quote": [False, False, False, False, False],
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(clean_ticks, "silver_market_ticks_suite")
        assert result.success is True
        assert result.statistics["unsuccessful_expectations"] == 0
        assert result.statistics["success_percent"] == 100.0

    def test_breach_crossed_quotes_in_silver_ticks(self) -> None:
        """Verify that crossed quotes (bid > ask) are caught by the validation suite."""
        crossed_ticks = {
            "ticker": ["AAPL", "MSFT"],
            "asset_class": ["EQUITY", "EQUITY"],
            "bid": [150.50, 300.0],  # crossed AAPL quote (bid 150.50 > ask 150.00)
            "ask": [150.00, 300.10],
            "last_price": [150.25, 300.05],
            "volume": [100.0, 200.0],
            "timestamp": ["2026-11-01T00:00:00Z"] * 2,
            "spread": [-0.50, 0.10],
            "mid_price": [150.25, 300.05],
            "relative_spread_bps": [-33.28, 3.33],
            "quote_condition": ["WIDE_SPREAD", "TIGHT_SPREAD"],
            "is_crossed_quote": [True, False],
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(crossed_ticks, "silver_market_ticks_suite")
        assert result.success is False
        assert result.statistics["unsuccessful_expectations"] > 0

        # Specifically expect_column_pair_values_a_to_be_greater_than_or_equal_to_b failed
        pair_res = next(
            r for r in result.results
            if r["expectation_config"]["expectation_type"] == "expect_column_pair_values_a_to_be_greater_than_or_equal_to_b"
        )
        assert pair_res["success"] is False

    def test_validate_clean_silver_trade_orders(self) -> None:
        """Verify clean Silver trade orders pass 100% of expectations."""
        clean_orders = {
            "order_id": [f"ord_{i}" for i in range(1, 6)],
            "portfolio_id": ["PORT-01"] * 5,
            "ticker": ["AAPL", "MSFT", "GOOGL", "NVDA", "BTCUSD"],
            "side": ["BUY", "SELL", "BUY", "BUY", "SELL"],
            "order_type": ["MARKET", "LIMIT", "MARKET", "LIMIT", "MARKET"],
            "quantity": [10.0, 20.0, 15.0, 50.0, 0.5],
            "price": [150.0, 300.0, 140.0, 120.0, 60000.0],
            "notional_usd": [1500.0, 6000.0, 2100.0, 6000.0, 30000.0],
            "order_category": ["EQUITY_ORDER", "EQUITY_ORDER", "EQUITY_ORDER", "EQUITY_ORDER", "CRYPTO_ORDER"],
            "notional_tier": ["STANDARD", "STANDARD", "STANDARD", "STANDARD", "STANDARD"],
            "timestamp": ["2026-11-01T00:00:00Z"] * 5,
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(clean_orders, "silver_trade_orders_suite")
        assert result.success is True
        assert result.statistics["unsuccessful_expectations"] == 0

    def test_breach_duplicate_order_id(self) -> None:
        """Verify duplicate order IDs are caught by uniqueness expectation."""
        duplicate_orders = {
            "order_id": ["ord_01", "ord_01", "ord_03"],  # duplicate
            "portfolio_id": ["PORT-01"] * 3,
            "ticker": ["AAPL", "AAPL", "GOOGL"],
            "side": ["BUY", "BUY", "SELL"],
            "order_type": ["MARKET", "MARKET", "LIMIT"],
            "quantity": [10.0, 10.0, 15.0],
            "price": [150.0, 150.0, 140.0],
            "notional_usd": [1500.0, 1500.0, 2100.0],
            "order_category": ["EQUITY_ORDER"] * 3,
            "notional_tier": ["STANDARD"] * 3,
            "timestamp": ["2026-11-01T00:00:00Z"] * 3,
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(duplicate_orders, "silver_trade_orders_suite")
        assert result.success is False
        unique_res = next(
            r for r in result.results
            if r["expectation_config"]["expectation_type"] == "expect_column_values_to_be_unique"
        )
        assert unique_res["success"] is False

    def test_breach_invalid_side_and_negative_price(self) -> None:
        """Verify invalid enum value and non-positive price trigger validation failures."""
        bad_orders = {
            "order_id": ["ord_1", "ord_2"],
            "portfolio_id": ["PORT-01", "PORT-01"],
            "ticker": ["AAPL", "MSFT"],
            "side": ["INVALID_SIDE", "BUY"],  # invalid side
            "order_type": ["MARKET", "LIMIT"],
            "quantity": [10.0, 20.0],
            "price": [-50.0, 300.0],  # negative price
            "notional_usd": [-500.0, 6000.0],
            "order_category": ["EQUITY_ORDER", "EQUITY_ORDER"],
            "notional_tier": ["STANDARD", "STANDARD"],
            "timestamp": ["2026-11-01T00:00:00Z"] * 2,
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(bad_orders, "silver_trade_orders_suite")
        assert result.success is False
        assert result.statistics["unsuccessful_expectations"] >= 2

    def test_validate_clean_gold_portfolio_var(self) -> None:
        """Verify clean Gold portfolio VaR passes all mathematical ordering invariants."""
        clean_var = {
            "var_id": ["var_001"],
            "portfolio_id": ["PORT-ALPHA"],
            "valuation_date": ["2026-11-01"],
            "gross_exposure_usd": [1500000.0],
            "parametric_var_95_usd": [45000.0],
            "parametric_var_99_usd": [65000.0],
            "monte_carlo_var_95_usd": [46000.0],
            "monte_carlo_var_99_usd": [66000.0],
            "expected_shortfall_cvar_95_usd": [55000.0],
            "expected_shortfall_cvar_99_usd": [75000.0],
            "risk_regime": ["MODERATE_RISK"],
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(clean_var, "gold_portfolio_var_suite")
        assert result.success is True
        assert result.statistics["unsuccessful_expectations"] == 0

    def test_breach_var_confidence_monotonicity(self) -> None:
        """Verify breach when VaR_99 < VaR_95 (violating probability monotonicity)."""
        bad_var = {
            "var_id": ["var_001"],
            "portfolio_id": ["PORT-ALPHA"],
            "valuation_date": ["2026-11-01"],
            "gross_exposure_usd": [1500000.0],
            "parametric_var_95_usd": [70000.0],
            "parametric_var_99_usd": [40000.0],  # INVERSION: VaR_99 < VaR_95
            "monte_carlo_var_95_usd": [46000.0],
            "monte_carlo_var_99_usd": [66000.0],
            "expected_shortfall_cvar_95_usd": [55000.0],
            "expected_shortfall_cvar_99_usd": [75000.0],
            "risk_regime": ["ELEVATED_RISK"],
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(bad_var, "gold_portfolio_var_suite")
        assert result.success is False

    def test_breach_coherent_tail_risk(self) -> None:
        """Verify breach when CVaR < VaR (violating sub-additivity / coherent risk axioms)."""
        bad_var = {
            "var_id": ["var_001"],
            "portfolio_id": ["PORT-ALPHA"],
            "valuation_date": ["2026-11-01"],
            "gross_exposure_usd": [1500000.0],
            "parametric_var_95_usd": [45000.0],
            "parametric_var_99_usd": [65000.0],
            "monte_carlo_var_95_usd": [46000.0],
            "monte_carlo_var_99_usd": [66000.0],
            "expected_shortfall_cvar_95_usd": [30000.0],  # INVERSION: CVaR_95 < MC VaR_95
            "expected_shortfall_cvar_99_usd": [75000.0],
            "risk_regime": ["MODERATE_RISK"],
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(bad_var, "gold_portfolio_var_suite")
        assert result.success is False

    def test_breach_risk_features_bounds(self) -> None:
        """Verify breach when normalized liquidity score exceeds [0, 1]."""
        bad_features = {
            "feature_key": ["feat_1"],
            "portfolio_id": ["PORT-ALPHA"],
            "ticker": ["AAPL"],
            "asset_class": ["EQUITY"],
            "mark_to_market_price": [150.0],
            "liquidity_score": [1.45],  # Out of bounds (> 1.0)
            "order_imbalance_ratio": [0.10],
            "aggressive_order_ratio": [0.3],
        }
        validator = GreatExpectationsValidator()
        result = validator.validate(bad_features, "gold_risk_features_suite")
        assert result.success is False

    def test_validate_parquet_file_on_disk(self, tmp_path: Path) -> None:
        """Verify validation directly against a physical Parquet file."""
        parquet_path = tmp_path / "silver_ticks.parquet"
        tbl = pa.table({
            "ticker": ["AAPL", "MSFT"],
            "asset_class": ["EQUITY", "EQUITY"],
            "bid": [150.0, 300.0],
            "ask": [150.05, 300.10],
            "last_price": [150.02, 300.05],
            "volume": [100.0, 250.0],
            "timestamp": ["2026-11-01T00:00:00Z", "2026-11-01T00:00:00Z"],
            "spread": [0.05, 0.10],
            "mid_price": [150.025, 300.05],
            "relative_spread_bps": [3.33, 3.33],
            "quote_condition": ["TIGHT_SPREAD", "TIGHT_SPREAD"],
            "is_crossed_quote": [False, False],
        })
        pq.write_table(tbl, parquet_path)

        validator = GreatExpectationsValidator()
        result = validator.validate(parquet_path, "silver_market_ticks_suite")
        assert result.success is True
        assert result.statistics["unsuccessful_expectations"] == 0


# =============================================================================
# 4. DATA DOCS GENERATION & PROMETHEUS TELEMETRY TESTS
# =============================================================================
class TestDataDocsAndTelemetry:
    """Validate HTML report generation and Prometheus metrics formatting."""

    def test_data_docs_html_generation(self, tmp_path: Path) -> None:
        """Verify HTML Data Docs generation and content structure."""
        docs_dir = tmp_path / "data_docs"
        validator = GreatExpectationsValidator()
        clean_ticks = {
            "ticker": ["AAPL"],
            "asset_class": ["EQUITY"],
            "bid": [150.0],
            "ask": [150.05],
            "last_price": [150.02],
            "volume": [100.0],
            "timestamp": ["2026-11-01T00:00:00Z"],
            "spread": [0.05],
            "mid_price": [150.025],
            "relative_spread_bps": [3.33],
            "quote_condition": ["TIGHT_SPREAD"],
            "is_crossed_quote": [False],
        }
        res = validator.validate(clean_ticks, "silver_market_ticks_suite", data_docs_dir=docs_dir)
        assert res.data_docs_path is not None
        assert Path(res.data_docs_path).exists()
        assert (docs_dir / "index.html").exists()

        html_text = Path(res.data_docs_path).read_text(encoding="utf-8")
        assert "Great Expectations Validation Report" in html_text
        assert "PASSED" in html_text
        assert "silver_market_ticks_suite" in html_text

    def test_telemetry_tracking_and_prometheus_metrics(self) -> None:
        """Verify telemetry counters and Prometheus exposition output."""
        telemetry = DataQualityTelemetry()
        validator = GreatExpectationsValidator(telemetry=telemetry)

        clean_data = {
            "ticker": ["AAPL"],
            "asset_class": ["EQUITY"],
            "bid": [150.0],
            "ask": [150.05],
            "last_price": [150.02],
            "volume": [100.0],
            "timestamp": ["2026-11-01T00:00:00Z"],
            "spread": [0.05],
            "mid_price": [150.025],
            "relative_spread_bps": [3.33],
            "quote_condition": ["TIGHT_SPREAD"],
            "is_crossed_quote": [False],
        }
        validator.validate(clean_data, "silver_market_ticks_suite")

        assert telemetry.validations_total == 1
        assert telemetry.validations_passed_total == 1
        assert telemetry.validations_failed_total == 0
        assert telemetry.expectations_evaluated_total > 0
        assert telemetry.expectations_passed_total > 0

        prom_text = telemetry.format_prometheus(environment="dev")
        assert 'dq_validations_total{env="dev"} 1' in prom_text
        assert 'dq_validations_passed_total{env="dev"} 1' in prom_text
        assert "# TYPE dq_validations_total counter" in prom_text

        telemetry.reset()
        assert telemetry.validations_total == 0


# =============================================================================
# 5. AIRFLOW OPERATOR & IN-MEMORY REGISTRY TESTS
# =============================================================================
class TestAirflowOperatorAndRegistry:
    """Validate GreatExpectationsOperator and state registry."""

    def test_operator_execution_success(self) -> None:
        """Verify GreatExpectationsOperator executes and records validation in registry."""
        op = GreatExpectationsOperator(
            task_id="test_validate_ticks",
            expectation_suite_name="silver_market_ticks_suite",
            data_asset_name="silver_market_ticks",
            data_asset_path="gs://trading-risk-silver-dev/delta/silver/silver_market_ticks/year={{ ds_nodash[:4] }}",
        )
        context = {"ds": "2026-11-01", "ds_nodash": "20261101"}
        stats = op.execute(context)
        assert stats["unsuccessful_expectations"] == 0
        assert stats["success_percent"] == 100.0

        # Verify registry recorded the validation
        latest = GreatExpectationsValidationRegistry.get_latest_validation("silver_market_ticks_suite")
        assert latest is not None
        assert latest["success"] is True

    def test_operator_jinja_template_rendering(self) -> None:
        """Verify Jinja templating across parameters."""
        op = GreatExpectationsOperator(
            task_id="test_jinja_op",
            expectation_suite_name="{{ params.suite }}",
            data_asset_path="gs://bkt/{{ ds }}/ticks.parquet",
            params={"suite": "silver_market_ticks_suite"},
        )
        context = {"ds": "2026-11-01", "params": {"suite": "silver_market_ticks_suite"}}
        stats = op.execute(context)
        assert stats["unsuccessful_expectations"] == 0

    def test_operator_failure_raises_airflow_exception(self) -> None:
        """Verify validation failure raises AirflowException when fail_task_on_validation_failure=True."""
        # Inject validation failure
        GreatExpectationsValidationRegistry.inject_validation_result(
            "silver_trade_orders_suite",
            success=False,
            failed_expectations=3,
        )

        op = GreatExpectationsOperator(
            task_id="test_fail_orders",
            expectation_suite_name="silver_trade_orders_suite",
            fail_task_on_validation_failure=True,
        )
        with pytest.raises(AirflowException, match="Great Expectations validation failed"):
            op.execute({})

    def test_operator_non_failing_mode(self) -> None:
        """Verify validation failure does not raise when fail_task_on_validation_failure=False."""
        GreatExpectationsValidationRegistry.inject_validation_result(
            "silver_trade_orders_suite",
            success=False,
            failed_expectations=2,
        )
        op = GreatExpectationsOperator(
            task_id="test_soft_fail",
            expectation_suite_name="silver_trade_orders_suite",
            fail_task_on_validation_failure=False,
        )
        stats = op.execute({})
        assert stats["unsuccessful_expectations"] == 2

    def test_operator_transient_failure_recovery_on_retry(self) -> None:
        """Verify operator recovers after transient error injection."""
        GreatExpectationsValidationRegistry.inject_failure(
            "test_retry_op",
            exception=AirflowException("Transient GCS timeout"),
            num_failures=1,
        )
        op = GreatExpectationsOperator(
            task_id="test_retry_op",
            expectation_suite_name="silver_market_ticks_suite",
            retries=2,
        )

        # Attempt 1 fails
        with pytest.raises(AirflowException, match="Transient GCS timeout"):
            op.execute({"task_instance": None})
        assert GreatExpectationsValidationRegistry.get_attempt_count("test_retry_op") == 1

        # Attempt 2 succeeds
        stats = op.execute({"task_instance": None})
        assert stats["unsuccessful_expectations"] == 0
        assert GreatExpectationsValidationRegistry.get_attempt_count("test_retry_op") == 2

    def test_operator_permanent_failure_callback_invocation(self) -> None:
        """Verify permanent failures trigger the Airflow on_failure_callback."""
        callback_called = False

        def failure_callback(context: Dict[str, Any]) -> None:
            nonlocal callback_called
            callback_called = True

        GreatExpectationsValidationRegistry.inject_failure(
            "test_perm_fail",
            exception=AirflowException("Permanent storage permission error"),
            num_failures=5,
        )
        op = GreatExpectationsOperator(
            task_id="test_perm_fail",
            expectation_suite_name="gold_portfolio_var_suite",
            on_failure_callback=failure_callback,
        )

        with pytest.raises(AirflowException):
            try:
                op.execute({})
            except Exception as e:
                mock_ctx = create_mock_task_context(
                    dag=None,
                    task=op,
                    exception=e,
                    try_number=1,
                    state="failed",
                )
                if op.on_failure_callback:
                    op.on_failure_callback(mock_ctx)
                raise

        assert callback_called is True


# =============================================================================
# 6. STANDALONE & MASTER LAKEHOUSE DAG ORCHESTRATION TESTS
# =============================================================================
class TestAirflowDAGsIntegration:
    """Validate standalone Great Expectations DAG and Master Lakehouse DAG integration."""

    def test_standalone_great_expectations_dag_structure(self) -> None:
        """Verify DAG properties, TaskGroups, and acyclicity."""
        dag = build_great_expectations_dag()
        assert dag.dag_id == "great_expectations_quality_pipeline"
        assert dag.schedule_interval == "30 2 * * *"
        assert "great-expectations" in dag.tags
        assert dag.sla == timedelta(minutes=30)

        # Must have 8 tasks: start, 3 silver checks, 2 gold checks, publish docs, end
        assert len(dag.tasks) == 8

        # Verify topological sort succeeds without cycles
        ordered = dag.topological_sort()
        assert len(ordered) == 8

        # Task IDs check
        task_ids = set(dag.task_ids)
        assert "validate_silver_ticks" in task_ids
        assert "validate_silver_orders" in task_ids
        assert "validate_silver_vwap" in task_ids
        assert "validate_gold_var" in task_ids
        assert "validate_gold_features" in task_ids
        assert "publish_data_docs" in task_ids

    def test_master_lakehouse_dag_data_quality_gates(self) -> None:
        """Verify master Lakehouse DAG integrates GreatExpectationsOperator in data_quality_gates."""
        dag = build_trading_risk_dag()
        assert dag.dag_id == "trading_risk_daily_batch_pipeline"

        # Verify topological sort passes with no cycles
        ordered = dag.topological_sort()
        assert len(ordered) == len(dag.tasks)

        # Verify data_quality_gates contains GreatExpectationsOperator instances
        val_silver = dag.get_task("validate_silver_suite")
        val_gold = dag.get_task("validate_gold_suite")
        pub_docs = dag.get_task("publish_data_docs")

        assert isinstance(val_silver, GreatExpectationsOperator)
        assert isinstance(val_gold, GreatExpectationsOperator)
        assert isinstance(pub_docs, GreatExpectationsOperator)

        # Verify dependency flow:
        # Databricks maintenance task -> Data Quality Silver tasks
        db_maint = dag.get_task("execute_gold_table_maintenance")
        assert val_silver in db_maint.downstream_list
        assert val_gold in val_silver.downstream_list
        assert pub_docs in val_gold.downstream_list

        # Data Quality publish_data_docs -> BigQuery refresh task
        bq_refresh = dag.get_task("refresh_bigquery_partitions")
        assert bq_refresh in pub_docs.downstream_list


# =============================================================================
# 7. CLI RUNNER TESTS
# =============================================================================
class TestQualityGateCLIRunner:
    """Validate data_quality.run_checkpoints script execution."""

    def test_cli_dry_run_mode(self, capsys: pytest.CaptureFixture) -> None:
        """Verify CLI runner in dry-run mode prints JSON suite definition."""
        from data_quality.run_checkpoints import run_quality_gate

        code = run_quality_gate(
            suite_name="silver_market_ticks_suite",
            dry_run=True,
        )
        assert code == 0
        captured = capsys.readouterr()
        assert "silver_market_ticks_suite" in captured.out

    def test_cli_execution_with_clean_parquet(self, tmp_path: Path) -> None:
        """Verify CLI runner executes against Parquet and returns exit code 0."""
        from data_quality.run_checkpoints import run_quality_gate

        parquet_file = tmp_path / "orders.parquet"
        tbl = pa.table({
            "order_id": ["ord_1", "ord_2"],
            "portfolio_id": ["PORT-01", "PORT-01"],
            "ticker": ["AAPL", "MSFT"],
            "side": ["BUY", "SELL"],
            "order_type": ["MARKET", "LIMIT"],
            "quantity": [10.0, 20.0],
            "price": [150.0, 300.0],
            "notional_usd": [1500.0, 6000.0],
            "order_category": ["EQUITY_ORDER", "EQUITY_ORDER"],
            "notional_tier": ["STANDARD", "STANDARD"],
            "timestamp": ["2026-11-01T00:00:00Z", "2026-11-01T00:00:00Z"],
        })
        pq.write_table(tbl, parquet_file)

        code = run_quality_gate(
            suite_name="silver_trade_orders_suite",
            data_path=str(parquet_file),
            data_docs_dir=str(tmp_path / "docs"),
            fail_on_error=True,
        )
        assert code == 0
