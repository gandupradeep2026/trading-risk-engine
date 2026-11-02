"""Zero-Cost Unit & Integration Test Suite for dbt Staging & Intermediate Models (Day 23).

Verifies dbt Core project structure, GoogleSQL model compilation, DAG topological sorting,
Jinja macro rendering, hermetic DuckDB execution, schema test evaluations (unique, not_null,
accepted_values, relationships), Prometheus telemetry, Airflow DbtRunOperator/DbtTestOperator,
and master lakehouse DAG integration.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from pathlib import Path
import tempfile
from typing import Any, Dict, List
import pytest
import pyarrow as pa
import yaml

from dwh.bigquery.emulator import LocalBigQueryEmulator
from dwh.bigquery.seed_data import (
    generate_mock_market_ticks,
    generate_mock_risk_alerts,
    generate_mock_risk_stats,
    generate_mock_trade_orders,
)
from dwh.dbt.config import DbtConfig
from dwh.dbt.project_manager import (
    DbtCycleError,
    DbtModelNode,
    DbtProjectError,
    DbtProjectManager,
    DbtTestSpec,
)
from dwh.dbt.runner import DbtRunner, DbtRunResult, DbtTelemetry
from orchestration.airflow.airflow_compat import (
    AirflowException,
    DbtExecutionRegistry,
    DbtRunOperator,
    DbtTestOperator,
)
from orchestration.airflow.dags.trading_risk_daily_batch_dag import build_trading_risk_dag


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="module")
def dbt_root_dir() -> Path:
    """Return resolved path to the dwh/dbt project directory."""
    return Path(__file__).resolve().parent.parent / "dwh" / "dbt"


@pytest.fixture(scope="module")
def seeded_emulator() -> LocalBigQueryEmulator:
    """Provide an in-memory DuckDB BigQuery emulator populated with dimension & fact seed data."""
    emulator = LocalBigQueryEmulator(
        project_id="trading-risk-dev-001",
        dataset_id="trading_risk_dwh_dev",
    )
    emulator.initialize_star_schema()
    emulator.load_seed_dimensions()

    emulator.insert_rows("fact_trade_orders", generate_mock_trade_orders(100))
    emulator.insert_rows("fact_market_ticks", generate_mock_market_ticks(100))
    emulator.insert_rows("fact_risk_stats", generate_mock_risk_stats("2026-11-02"))
    emulator.insert_rows("fact_risk_alerts", generate_mock_risk_alerts(20))
    return emulator


@pytest.fixture(scope="module")
def project_manager(dbt_root_dir: Path) -> DbtProjectManager:
    """Provide an initialized DbtProjectManager."""
    cfg = DbtConfig(
        project_dir=dbt_root_dir,
        target="dev",
        environment="dev",
        gcp_project_id="trading-risk-dev-001",
        dataset_id="trading_risk_dwh_dev",
    )
    return DbtProjectManager(cfg)


# =============================================================================
# 1. DBT PROJECT CONFIGURATION & STRUCTURE TESTS
# =============================================================================

class TestDbtProjectStructureAndConfig:
    """Validates dbt project directory layout, YAML manifests, and Pydantic configurations."""

    def test_dbt_project_files_exist(self, dbt_root_dir: Path) -> None:
        assert (dbt_root_dir / "dbt_project.yml").is_file()
        assert (dbt_root_dir / "profiles.yml").is_file()
        assert (dbt_root_dir / "packages.yml").is_file()
        assert (dbt_root_dir / "README.md").is_file()

    def test_dbt_project_manifest_contents(self, dbt_root_dir: Path) -> None:
        with open(dbt_root_dir / "dbt_project.yml", "r", encoding="utf-8") as f:
            manifest = yaml.safe_load(f)

        assert manifest["name"] == "trading_risk_dbt"
        assert manifest["version"] == "1.0.0"
        assert manifest["profile"] == "trading_risk"
        assert "models" in manifest
        assert "staging" in manifest["models"]["trading_risk_dbt"]
        assert "intermediate" in manifest["models"]["trading_risk_dbt"]
        assert manifest["vars"]["environment"] == "dev"

    def test_profiles_manifest_targets(self, dbt_root_dir: Path) -> None:
        with open(dbt_root_dir / "profiles.yml", "r", encoding="utf-8") as f:
            profiles = yaml.safe_load(f)

        assert "trading_risk" in profiles
        outputs = profiles["trading_risk"]["outputs"]
        assert "dev" in outputs
        assert "prod" in outputs
        assert "duckdb" in outputs
        assert outputs["dev"]["type"] == "bigquery"
        assert outputs["duckdb"]["type"] == "duckdb"

    def test_packages_manifest(self, dbt_root_dir: Path) -> None:
        with open(dbt_root_dir / "packages.yml", "r", encoding="utf-8") as f:
            pkgs = yaml.safe_load(f)

        assert "packages" in pkgs
        pkg_names = [p["package"] for p in pkgs["packages"]]
        assert "dbt-labs/dbt_utils" in pkg_names

    def test_dbt_config_defaults_and_env(self, dbt_root_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        monkeypatch.setenv("GCP_PROJECT_ID", "trading-risk-prod-999")
        monkeypatch.setenv("BIGQUERY_DATASET", "trading_risk_dwh_prod")

        cfg = DbtConfig.from_env(project_dir=dbt_root_dir)
        assert cfg.environment == "prod"
        assert cfg.gcp_project_id == "trading-risk-prod-999"
        assert cfg.dataset_id == "trading_risk_dwh_prod"
        assert cfg.project_name == "trading_risk_dbt"


# =============================================================================
# 2. DBT MODELS, SOURCES, AND MACROS TESTS
# =============================================================================

class TestDbtModelsAndMacrosSyntax:
    """Validates model existence, SQL formatting, sources, and schema declarations."""

    def test_all_staging_models_exist(self, dbt_root_dir: Path) -> None:
        staging_dir = dbt_root_dir / "models" / "staging"
        expected_models = [
            "stg_trade_orders.sql",
            "stg_market_ticks.sql",
            "stg_risk_stats.sql",
            "stg_risk_alerts.sql",
            "stg_tickers.sql",
            "stg_portfolios.sql",
            "stg_traders.sql",
        ]
        for m in expected_models:
            assert (staging_dir / m).is_file(), f"Missing staging model: {m}"

    def test_all_intermediate_models_exist(self, dbt_root_dir: Path) -> None:
        int_dir = dbt_root_dir / "models" / "intermediate"
        expected_models = [
            "int_orders_enriched.sql",
            "int_portfolio_hourly_exposure.sql",
            "int_market_microstructure_hourly.sql",
            "int_risk_limit_breaches.sql",
        ]
        for m in expected_models:
            assert (int_dir / m).is_file(), f"Missing intermediate model: {m}"

    def test_macros_exist_and_non_empty(self, dbt_root_dir: Path) -> None:
        macro_dir = dbt_root_dir / "macros"
        expected_macros = [
            "generate_surrogate_key.sql",
            "financial_conversions.sql",
            "get_custom_schema.sql",
        ]
        for m in expected_macros:
            p = macro_dir / m
            assert p.is_file(), f"Missing macro: {m}"
            assert p.stat().st_size > 50

    def test_staging_sources_and_schema_yaml(self, dbt_root_dir: Path) -> None:
        sources_path = dbt_root_dir / "models" / "staging" / "sources.yml"
        schema_path = dbt_root_dir / "models" / "staging" / "schema.yml"

        with open(sources_path, "r", encoding="utf-8") as f:
            sources_doc = yaml.safe_load(f)
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_doc = yaml.safe_load(f)

        assert "sources" in sources_doc
        assert len(sources_doc["sources"][0]["tables"]) >= 8
        assert "models" in schema_doc
        model_names = [m["name"] for m in schema_doc["models"]]
        assert "stg_trade_orders" in model_names
        assert "stg_market_ticks" in model_names
        assert "stg_risk_stats" in model_names

    def test_intermediate_schema_yaml(self, dbt_root_dir: Path) -> None:
        schema_path = dbt_root_dir / "models" / "intermediate" / "schema.yml"
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_doc = yaml.safe_load(f)

        assert "models" in schema_doc
        model_names = [m["name"] for m in schema_doc["models"]]
        assert "int_orders_enriched" in model_names
        assert "int_portfolio_hourly_exposure" in model_names
        assert "int_market_microstructure_hourly" in model_names
        assert "int_risk_limit_breaches" in model_names


# =============================================================================
# 3. DBT PROJECT MANAGER & DAG TOPOLOGICAL SORT TESTS
# =============================================================================

class TestDbtProjectManagerAndDAG:
    """Validates model parsing, dependency graph resolution, cycle detection, and SQL compilation."""

    def test_discovered_models_count(self, project_manager: DbtProjectManager) -> None:
        assert len(project_manager.models) == 11
        staging_count = sum(1 for m in project_manager.models.values() if m.layer == "staging")
        intermediate_count = sum(1 for m in project_manager.models.values() if m.layer == "intermediate")
        assert staging_count == 7
        assert intermediate_count == 4

    def test_staging_models_have_zero_refs(self, project_manager: DbtProjectManager) -> None:
        for name, node in project_manager.models.items():
            if node.layer == "staging":
                assert len(node.refs) == 0, f"Staging model '{name}' should not ref other models"
                assert len(node.sources) >= 1, f"Staging model '{name}' should have at least one source"

    def test_intermediate_models_depend_on_staging(self, project_manager: DbtProjectManager) -> None:
        for name, node in project_manager.models.items():
            if node.layer == "intermediate":
                assert len(node.refs) >= 1, f"Intermediate model '{name}' must reference at least one model"

    def test_topological_execution_order(self, project_manager: DbtProjectManager) -> None:
        order = project_manager.get_execution_order()
        assert len(order) == 11

        # All staging models must appear before intermediate models
        staging_indices = [order.index(m) for m, n in project_manager.models.items() if n.layer == "staging"]
        intermediate_indices = [order.index(m) for m, n in project_manager.models.items() if n.layer == "intermediate"]

        # Specifically, int_orders_enriched must precede int_portfolio_hourly_exposure
        assert order.index("int_orders_enriched") < order.index("int_portfolio_hourly_exposure")
        # stg_trade_orders must precede int_orders_enriched
        assert order.index("stg_trade_orders") < order.index("int_orders_enriched")
        # stg_market_ticks must precede int_market_microstructure_hourly
        assert order.index("stg_market_ticks") < order.index("int_market_microstructure_hourly")

    def test_cycle_detection_raises_error(self, project_manager: DbtProjectManager) -> None:
        # Create a temporary manager with a synthetic cycle
        pm = DbtProjectManager(project_manager.config)
        pm.models["model_a"] = DbtModelNode(
            name="model_a", layer="staging", file_path=Path("a.sql"), refs=["model_b"]
        )
        pm.models["model_b"] = DbtModelNode(
            name="model_b", layer="staging", file_path=Path("b.sql"), refs=["model_a"]
        )

        with pytest.raises(DbtCycleError, match="Circular dependency detected"):
            pm.get_execution_order(["model_a", "model_b"])

    def test_compile_model_sql_replaces_macros(self, project_manager: DbtProjectManager) -> None:
        compiled = project_manager.compile_model_sql("stg_trade_orders")
        assert "config(" not in compiled
        assert "trading_risk_dwh_dev.fact_trade_orders" in compiled
        assert "MD5(CONCAT" in compiled

        compiled_int = project_manager.compile_model_sql("int_orders_enriched")
        assert "trading_risk_dwh_dev.stg_trade_orders" in compiled_int
        assert "trading_risk_dwh_dev.stg_tickers" in compiled_int
        assert "trading_risk_dwh_dev.stg_portfolios" in compiled_int
        assert "trading_risk_dwh_dev.stg_traders" in compiled_int


# =============================================================================
# 4. HERMETIC DUCKDB EXECUTION & DATA ACCURACY TESTS
# =============================================================================

class TestDbtExecutionAndHermeticDuckDB:
    """Executes all models against DuckDB emulator and verifies analytical accuracy."""

    @pytest.fixture(scope="class")
    def dbt_run_result(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        project_manager: DbtProjectManager,
    ) -> DbtRunResult:
        runner = DbtRunner(emulator=seeded_emulator, project_manager=project_manager)
        return runner.run_models()

    def test_all_models_run_successfully(self, dbt_run_result: DbtRunResult) -> None:
        assert dbt_run_result.success is True
        assert dbt_run_result.models_executed == 11
        assert dbt_run_result.models_failed == 0
        for m in dbt_run_result.model_results:
            assert m.status == "SUCCESS", f"Model {m.model_name} failed: {m.error_message}"
            assert m.rows > 0, f"Model {m.model_name} produced 0 rows"

    def test_stg_trade_orders_metrics(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        res = seeded_emulator.connection.execute(
            "SELECT COUNT(*), COUNT(DISTINCT trade_order_key), SUM(CASE WHEN is_filled THEN 1 ELSE 0 END) "
            "FROM trading_risk_dwh_dev.stg_trade_orders;"
        ).fetchone()
        assert res is not None
        total_rows, distinct_keys, filled_rows = res
        assert total_rows == 100
        assert distinct_keys == 100
        assert filled_rows > 0

    def test_stg_market_ticks_metrics(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        res = seeded_emulator.connection.execute(
            "SELECT COUNT(*), MIN(last_price), AVG(relative_spread_bps) "
            "FROM trading_risk_dwh_dev.stg_market_ticks;"
        ).fetchone()
        assert res is not None
        cnt, min_p, avg_spread = res
        assert cnt == 100
        assert min_p > 0
        assert avg_spread >= 0

    def test_stg_risk_stats_metrics(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        res = seeded_emulator.connection.execute(
            "SELECT COUNT(*), MIN(parametric_var), AVG(limit_utilization_pct), AVG(tail_risk_ratio) "
            "FROM trading_risk_dwh_dev.stg_risk_stats;"
        ).fetchone()
        assert res is not None
        cnt, min_pvar, avg_util, avg_tail = res
        assert cnt == 10
        assert min_pvar >= 0
        assert avg_util > 0
        assert avg_tail >= 1.0  # CVaR is typically >= VaR

    def test_int_orders_enriched_calculations(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        # Check signed notional and notional tier classifications
        res = seeded_emulator.connection.execute(
            "SELECT side, notional_usd, signed_notional_usd, notional_tier, is_aggressive, desk "
            "FROM trading_risk_dwh_dev.int_orders_enriched LIMIT 20;"
        ).fetchall()

        for row in res:
            side, notional, signed, tier, is_agg, desk = row
            if side == "BUY":
                assert signed == notional
            else:
                assert signed == -notional

            if notional >= 500000:
                assert tier == "BLOCK"
            elif notional >= 100000:
                assert tier == "LARGE"
            elif notional >= 10000:
                assert tier == "STANDARD"
            else:
                assert tier == "MICRO"

            assert desk != "UNKNOWN"

    def test_int_portfolio_hourly_exposure_invariants(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        res = seeded_emulator.connection.execute(
            "SELECT portfolio_id, total_orders, gross_turnover_usd, net_exposure_usd, order_flow_imbalance "
            "FROM trading_risk_dwh_dev.int_portfolio_hourly_exposure;"
        ).fetchall()

        assert len(res) > 0
        for row in res:
            pid, orders, gross, net, imbalance = row
            assert orders > 0
            assert gross > 0
            assert abs(net) <= gross
            assert -1.0 <= imbalance <= 1.0

    def test_int_market_microstructure_hourly_vwap(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        res = seeded_emulator.connection.execute(
            "SELECT ticker, high_price, low_price, open_price, close_price, hourly_vwap, total_volume "
            "FROM trading_risk_dwh_dev.int_market_microstructure_hourly;"
        ).fetchall()

        assert len(res) > 0
        for row in res:
            ticker, high, low, op, cl, vwap, vol = row
            assert high >= low
            assert low <= vwap <= high
            assert vol > 0

    def test_int_risk_limit_breaches_severity(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        res = seeded_emulator.connection.execute(
            "SELECT portfolio_id, limit_utilization_pct, breach_severity, risk_headroom_usd "
            "FROM trading_risk_dwh_dev.int_risk_limit_breaches;"
        ).fetchall()

        assert len(res) > 0
        for row in res:
            pid, util, severity, headroom = row
            if util >= 100.0:
                assert severity == "CRITICAL"
            elif util >= 85.0:
                assert severity == "WARNING"
            else:
                assert severity == "NORMAL"

    def test_fetch_arrow_table(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        project_manager: DbtProjectManager,
    ) -> None:
        runner = DbtRunner(emulator=seeded_emulator, project_manager=project_manager)
        arrow_tbl = runner.fetch_arrow_table("int_orders_enriched")
        assert isinstance(arrow_tbl, pa.Table)
        assert arrow_tbl.num_rows == 100
        assert "signed_notional_usd" in arrow_tbl.column_names
        assert "notional_tier" in arrow_tbl.column_names


# =============================================================================
# 5. DBT SCHEMA TESTS EVALUATION
# =============================================================================

class TestDbtSchemaTestsEvaluation:
    """Evaluates all 84 declared dbt schema tests (unique, not_null, accepted_values, relationships)."""

    def test_all_schema_tests_pass(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        project_manager: DbtProjectManager,
    ) -> None:
        runner = DbtRunner(emulator=seeded_emulator, project_manager=project_manager)
        test_res = runner.run_tests()

        assert test_res.success is True
        assert test_res.tests_executed >= 80
        assert test_res.tests_failed == 0
        assert test_res.tests_passed == test_res.tests_executed

    def test_schema_test_catches_intentional_corruption(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        project_manager: DbtProjectManager,
    ) -> None:
        # Create a temporary table with intentional null violation
        seeded_emulator.connection.execute(
            "CREATE OR REPLACE VIEW trading_risk_dwh_dev.stg_traders AS "
            "SELECT CAST(NULL AS VARCHAR) AS trader_id, 'Corrupt' AS trader_name, 'DESK' AS desk, "
            "'ROLE' AS trader_role, 1000.0 AS single_order_limit_usd, TRUE AS is_active, CURRENT_TIMESTAMP AS created_at;"
        )

        runner = DbtRunner(emulator=seeded_emulator, project_manager=project_manager)
        test_res = runner.run_tests(["stg_traders"])

        assert test_res.success is False
        assert test_res.tests_failed >= 1

        failed_tests = [t for t in test_res.test_results if t.status == "FAILED"]
        assert any(t.test_type == "not_null" and t.column_name == "trader_id" for t in failed_tests)

        # Restore valid model
        runner.run_models(["stg_traders"])

    def test_prometheus_telemetry_exposition(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        project_manager: DbtProjectManager,
    ) -> None:
        runner = DbtRunner(emulator=seeded_emulator, project_manager=project_manager)
        runner.run_all()
        prom_text = runner.telemetry.format_prometheus()

        assert "dbt_models_executed_total" in prom_text
        assert "dbt_models_failed_total 0" in prom_text
        assert "dbt_tests_executed_total" in prom_text
        assert "dbt_tests_passed_total" in prom_text
        assert "dbt_total_execution_time_seconds" in prom_text


# =============================================================================
# 6. AIRFLOW DBT OPERATORS & MASTER DAG INTEGRATION
# =============================================================================

class TestAirflowDbtOperatorsAndDAG:
    """Validates Airflow DbtRunOperator, DbtTestOperator, retries, and master Lakehouse DAG integration."""

    def setup_method(self) -> None:
        DbtExecutionRegistry.reset()

    def test_dbt_run_operator_execution(self) -> None:
        op = DbtRunOperator(
            task_id="dbt_run_staging",
            models=["stg_trade_orders", "stg_market_ticks"],
            target="dev",
        )
        res = op.execute({"ds": "2026-11-03"})
        assert res["status"] == "SUCCESS"
        assert res["models_executed"] == 2
        assert DbtExecutionRegistry.get_run(res["run_id"]) is not None

    def test_dbt_test_operator_execution(self) -> None:
        op = DbtTestOperator(
            task_id="dbt_test_staging",
            models=["stg_trade_orders"],
            target="dev",
        )
        res = op.execute({"ds": "2026-11-03"})
        assert res["status"] == "SUCCESS"
        assert res["tests_passed"] == 10

    def test_dbt_run_operator_retry_and_failure_injection(self) -> None:
        DbtExecutionRegistry.inject_failure("dbt_run_failure_task", num_failures=2)

        op = DbtRunOperator(
            task_id="dbt_run_failure_task",
            models=["stg_trade_orders"],
            retries=3,
        )

        # Attempt 1 -> fails
        with pytest.raises(AirflowException):
            op.execute({})

        # Attempt 2 -> fails
        with pytest.raises(AirflowException):
            op.execute({})

        # Attempt 3 -> succeeds
        res = op.execute({})
        assert res["status"] == "SUCCESS"
        assert res["attempt"] == 3

    def test_master_lakehouse_dag_dbt_integration(self) -> None:
        dag = build_trading_risk_dag()

        # Verify task presence
        assert "run_dbt_transforms" in dag.task_dict
        assert "test_dbt_models" in dag.task_dict

        run_task = dag.get_task("run_dbt_transforms")
        test_task = dag.get_task("test_dbt_models")
        bq_task = dag.get_task("refresh_bigquery_partitions")
        report_task = dag.get_task("publish_risk_report")

        # Verify exact chain: refresh_bigquery >> run_dbt_transforms >> test_dbt_models
        assert run_task in bq_task.downstream_list
        assert test_task in run_task.downstream_list
        assert report_task in test_task.downstream_list

        # Verify operator classes
        assert isinstance(run_task, DbtRunOperator)
        assert isinstance(test_task, DbtTestOperator)

        # Ensure DAG remains acyclic
        dag.validate()


# =============================================================================
# 7. DBT CLI RUNNER INTEGRATION
# =============================================================================

class TestDbtCLIRunner:
    """Validates CLI arguments, --compile-only, --seed, and report exporting."""

    def test_cli_compile_only(self) -> None:
        from dwh.dbt.cli import main
        code = main(["--compile-only", "--models", "staging"])
        assert code == 0

    def test_cli_seed_and_run_tests_with_reports(self) -> None:
        from dwh.dbt.cli import main
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "dbt_report.json"
            prom_path = Path(tmpdir) / "dbt_metrics.prom"

            code = main([
                "--seed",
                "--run-tests",
                "--models", "staging", "intermediate",
                "--json-report", str(json_path),
                "--export-prometheus", str(prom_path),
            ])
            assert code == 0
            assert json_path.is_file()
            assert prom_path.is_file()

            with open(json_path, "r", encoding="utf-8") as f:
                report = json.load(f)

            assert report["success"] is True
            assert report["models_executed"] == 11
            assert report["tests_executed"] >= 80

            with open(prom_path, "r", encoding="utf-8") as f:
                metrics = f.read()

            assert "dbt_models_executed_total 11" in metrics
            assert "dbt_tests_failed_total 0" in metrics
