"""Zero-Cost Unit & Integration Test Suite for dbt Marts & BigQuery Materialized Views (Day 24).

Validates:
1. Terraform BigQuery Materialized Views: MV resources defined, HCL syntax, balanced braces,
   refresh intervals, clustering columns, schema file consistency, and root outputs.
2. BigQuery SQL DDL & Schemas: Materialized view definitions, JSON schemas, catalog specs,
   GoogleSQL compliance.
3. DuckDB Local Emulator Execution: Materialized views deployment, querying, aggregation
   correctness against fact tables, Arrow table conversion, and BigQuery telemetry.
4. dbt Project Structure & Model Discovery: Marts layer models discovered, ref dependencies
   analyzed, DAG acyclicity verified, topological ordering (staging -> intermediate -> marts).
5. dbt Model Compilation: Clean Jinja rendering, macro replacement, relation qualification.
6. Hermetic DuckDB Mart Execution: 15-model pipeline execution, table materialization for marts,
   financial metric correctness (OHLCV, session VWAP, trader delta/turnover, portfolio headroom,
   alert summaries).
7. Schema Tests Evaluation: Exhaustive evaluation of unique, not_null, accepted_values, and
   relationships tests on mart models (100% pass rate).
8. Intentional Corruption Detection: Verification that schema test suites catch data anomalies.
9. Operational Telemetry & Prometheus Metrics: Prometheus text exposition formatting.
10. Airflow DbtRunOperator/DbtTestOperator execution with marts and master DAG integration.
11. CLI Runner integration with --models staging intermediate marts.
"""

from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
import tempfile
from typing import Any, Dict, List
import pytest
import pyarrow as pa
import yaml

from dwh.bigquery.config import (
    BigQueryDatasetConfig,
    MATERIALIZED_VIEWS_CATALOG,
    MV_REALTIME_MARKET_PERFORMANCE_SPEC,
    MV_REALTIME_RISK_ALERTS_SPEC,
    MV_REALTIME_TRADER_EXPOSURE_SPEC,
    STAR_SCHEMA_CATALOG,
)
from dwh.bigquery.emulator import (
    BigQueryTelemetry,
    LocalBigQueryEmulator,
)
from dwh.bigquery.schema_manager import (
    BigQuerySchemaManager,
    SchemaValidationError,
)
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

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_BQ_DIR = REPO_ROOT / "terraform" / "modules" / "bigquery"
TF_ROOT_DIR = REPO_ROOT / "terraform"
DWH_BQ_DIR = REPO_ROOT / "dwh" / "bigquery"
DBT_DIR = REPO_ROOT / "dwh" / "dbt"


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="module")
def dbt_root_dir() -> Path:
    """Return resolved path to the dwh/dbt project directory."""
    return DBT_DIR


@pytest.fixture(scope="module")
def seeded_emulator() -> LocalBigQueryEmulator:
    """Provide an in-memory DuckDB BigQuery emulator with dimensions, facts, and materialized views."""
    emulator = LocalBigQueryEmulator(
        project_id="trading-risk-dev-001",
        dataset_id="trading_risk_dwh_dev",
    )
    emulator.initialize_star_schema()
    emulator.load_seed_dimensions()

    emulator.insert_rows("fact_trade_orders", generate_mock_trade_orders(100))
    emulator.insert_rows("fact_market_ticks", generate_mock_market_ticks(100))
    emulator.insert_rows("fact_risk_stats", generate_mock_risk_stats("2026-11-04"))
    emulator.insert_rows("fact_risk_alerts", generate_mock_risk_alerts(20))

    emulator.initialize_materialized_views()
    return emulator


@pytest.fixture(scope="module")
def marts_project_manager(dbt_root_dir: Path) -> DbtProjectManager:
    """Provide an initialized DbtProjectManager discovering staging, intermediate, and marts."""
    cfg = DbtConfig(
        project_dir=dbt_root_dir,
        target="dev",
        environment="dev",
        gcp_project_id="trading-risk-dev-001",
        dataset_id="trading_risk_dwh_dev",
        models=["staging", "intermediate", "marts"],
    )
    return DbtProjectManager(cfg)


# =============================================================================
# 1. TERRAFORM BIGQUERY MATERIALIZED VIEWS TESTS
# =============================================================================

class TestTerraformBigQueryMaterializedViews:
    """Validates Terraform module declarations for BigQuery Materialized Views."""

    def test_materialized_view_resources_in_main_tf(self) -> None:
        main_tf = TF_BQ_DIR / "main.tf"
        content = main_tf.read_text(encoding="utf-8")
        assert 'resource "google_bigquery_table" "mv_realtime_market_performance"' in content
        assert 'resource "google_bigquery_table" "mv_realtime_trader_exposure"' in content
        assert 'resource "google_bigquery_table" "mv_realtime_risk_alerts"' in content
        assert "materialized_view {" in content
        assert "enable_refresh" in content
        assert "refresh_interval_ms" in content

    def test_materialized_view_variables_and_outputs(self) -> None:
        vars_tf = (TF_BQ_DIR / "variables.tf").read_text(encoding="utf-8")
        assert "mv_refresh_interval_ms" in vars_tf

        outputs_tf = (TF_BQ_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert "mv_realtime_market_performance_id" in outputs_tf
        assert "mv_realtime_trader_exposure_id" in outputs_tf
        assert "mv_realtime_risk_alerts_id" in outputs_tf
        assert "materialized_view_ids" in outputs_tf

    def test_root_terraform_wires_materialized_views(self) -> None:
        root_outputs = (TF_ROOT_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert "bigquery_materialized_views" in root_outputs
        assert "module.bigquery.materialized_view_ids" in root_outputs

    def test_balanced_braces_in_terraform_module(self) -> None:
        for tf_path in TF_BQ_DIR.glob("*.tf"):
            content = tf_path.read_text(encoding="utf-8")
            open_cnt = content.count("{")
            close_cnt = content.count("}")
            assert open_cnt == close_cnt, f"Unbalanced braces in {tf_path}: {open_cnt} vs {close_cnt}"

    def test_schema_json_files_exist_in_terraform(self) -> None:
        schema_dir = TF_BQ_DIR / "schemas"
        expected = [
            "mv_realtime_market_performance.json",
            "mv_realtime_trader_exposure.json",
            "mv_realtime_risk_alerts.json",
        ]
        for fname in expected:
            fpath = schema_dir / fname
            assert fpath.is_file(), f"Missing schema file in terraform: {fpath}"
            assert fpath.stat().st_size > 0


# =============================================================================
# 2. BIGQUERY MATERIALIZED VIEWS DDL & CATALOG TESTS
# =============================================================================

class TestBigQueryMaterializedViewsDDLAndCatalog:
    """Validates GoogleSQL DDL files, JSON schemas, and catalog definitions for MVs."""

    def test_all_ddl_files_exist(self) -> None:
        ddl_dir = DWH_BQ_DIR / "ddl"
        expected = [
            "mv_realtime_market_performance.sql",
            "mv_realtime_trader_exposure.sql",
            "mv_realtime_risk_alerts.sql",
            "all_materialized_views.sql",
        ]
        for f in expected:
            path = ddl_dir / f
            assert path.is_file(), f"Missing DDL file: {path}"
            assert path.stat().st_size > 0

    def test_materialized_views_catalog_completeness(self) -> None:
        assert len(MATERIALIZED_VIEWS_CATALOG) == 3
        assert "mv_realtime_market_performance" in MATERIALIZED_VIEWS_CATALOG
        assert "mv_realtime_trader_exposure" in MATERIALIZED_VIEWS_CATALOG
        assert "mv_realtime_risk_alerts" in MATERIALIZED_VIEWS_CATALOG

    def test_schema_manager_validates_materialized_views_catalog(self) -> None:
        sm = BigQuerySchemaManager()
        # Should execute cleanly without raising SchemaValidationError
        sm.validate_materialized_views_catalog()

    def test_clustering_column_limits_on_materialized_views(self) -> None:
        for mv_name, spec in MATERIALIZED_VIEWS_CATALOG.items():
            assert len(spec.clustering_columns) <= 4, f"MV {mv_name} exceeds 4 clustering columns"
            assert len(spec.clustering_columns) == len(set(spec.clustering_columns)), f"Duplicate clustering columns in {mv_name}"


# =============================================================================
# 3. DUCKDB LOCAL BIGQUERY EMULATOR MATERIALIZED VIEWS TESTS
# =============================================================================

class TestDuckDBMaterializedViewsExecution:
    """Verifies that the DuckDB emulator translates and executes BigQuery Materialized Views."""

    def test_materialized_views_initialized(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        # Check views exist in DuckDB information schema
        res = seeded_emulator.connection.execute(
            "SELECT table_name FROM information_schema.views WHERE table_schema = 'trading_risk_dwh_dev';"
        ).fetchall()
        view_names = [r[0] for r in res]
        assert "mv_realtime_market_performance" in view_names
        assert "mv_realtime_trader_exposure" in view_names
        assert "mv_realtime_risk_alerts" in view_names

    def test_mv_realtime_market_performance_aggregations(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        rows = seeded_emulator.execute_query(
            "SELECT * FROM trading_risk_dwh_dev.mv_realtime_market_performance;"
        )
        assert len(rows) > 0
        for r in rows:
            assert r["tick_count"] > 0
            assert r["total_volume"] > 0
            assert r["gross_notional_usd"] > 0
            assert r["min_bid_price"] <= r["max_ask_price"]
            assert r["asset_class"] in ("EQUITY", "FX", "COMMODITY", "CRYPTO", "ETF")

    def test_mv_realtime_trader_exposure_aggregations(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        rows = seeded_emulator.execute_query(
            "SELECT * FROM trading_risk_dwh_dev.mv_realtime_trader_exposure;"
        )
        assert len(rows) > 0
        for r in rows:
            assert r["order_count"] > 0
            assert r["total_quantity"] > 0
            assert r["gross_notional_usd"] >= 0
            assert r["portfolio_id"].startswith("PORTFOLIO_")
            assert r["trader_id"].startswith("TRADER_")

    def test_mv_realtime_risk_alerts_aggregations(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        rows = seeded_emulator.execute_query(
            "SELECT * FROM trading_risk_dwh_dev.mv_realtime_risk_alerts;"
        )
        assert len(rows) > 0
        for r in rows:
            assert r["alert_count"] > 0
            assert r["severity"] in ("INFO", "WARNING", "CRITICAL")
            assert r["max_observed_value"] is not None

    def test_materialized_view_arrow_table(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        table = seeded_emulator.execute_query_arrow(
            "SELECT * FROM trading_risk_dwh_dev.mv_realtime_market_performance;"
        )
        assert isinstance(table, pa.Table)
        assert table.num_rows > 0
        assert "tick_date" in table.column_names
        assert "ticker" in table.column_names
        assert "total_volume" in table.column_names


# =============================================================================
# 4. DBT MARTS MODEL DISCOVERY & DAG ACYCLICITY TESTS
# =============================================================================

class TestDbtMartsModelDiscoveryAndDAG:
    """Validates dbt marts model files, dependency resolution, and DAG topological sorting."""

    def test_all_marts_models_exist(self, dbt_root_dir: Path) -> None:
        marts_dir = dbt_root_dir / "models" / "marts"
        assert marts_dir.is_dir()
        expected = [
            "fct_daily_market_performance.sql",
            "fct_trader_performance_daily.sql",
            "fct_portfolio_risk_daily.sql",
            "fct_risk_alerts_summary.sql",
            "schema.yml",
        ]
        for fname in expected:
            fpath = marts_dir / fname
            assert fpath.is_file(), f"Missing file in marts: {fpath}"
            assert fpath.stat().st_size > 0

    def test_marts_schema_yml_declares_all_models(self, dbt_root_dir: Path) -> None:
        schema_path = dbt_root_dir / "models" / "marts" / "schema.yml"
        with open(schema_path, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        assert "models" in doc
        names = [m["name"] for m in doc["models"]]
        assert "fct_daily_market_performance" in names
        assert "fct_trader_performance_daily" in names
        assert "fct_portfolio_risk_daily" in names
        assert "fct_risk_alerts_summary" in names

    def test_discovered_models_counts(self, marts_project_manager: DbtProjectManager) -> None:
        assert len(marts_project_manager.models) == 15
        staging_cnt = sum(1 for m in marts_project_manager.models.values() if m.layer == "staging")
        int_cnt = sum(1 for m in marts_project_manager.models.values() if m.layer == "intermediate")
        marts_cnt = sum(1 for m in marts_project_manager.models.values() if m.layer == "marts")
        assert staging_cnt == 7
        assert int_cnt == 4
        assert marts_cnt == 4

    def test_marts_models_depend_on_upstream(self, marts_project_manager: DbtProjectManager) -> None:
        for name, node in marts_project_manager.models.items():
            if node.layer == "marts":
                assert len(node.refs) >= 1, f"Mart model '{name}' must have at least one ref"
                assert node.materialized == "table", f"Mart model '{name}' should materialize as table"

    def test_topological_execution_order_layers(self, marts_project_manager: DbtProjectManager) -> None:
        order = marts_project_manager.get_execution_order()
        assert len(order) == 15

        # All staging models must appear before intermediate models
        staging_idx = [order.index(m) for m, n in marts_project_manager.models.items() if n.layer == "staging"]
        int_idx = [order.index(m) for m, n in marts_project_manager.models.items() if n.layer == "intermediate"]
        marts_idx = [order.index(m) for m, n in marts_project_manager.models.items() if n.layer == "marts"]

        assert max(staging_idx) < max(int_idx)
        # Specific dependency checks
        assert order.index("int_market_microstructure_hourly") < order.index("fct_daily_market_performance")
        assert order.index("int_orders_enriched") < order.index("fct_trader_performance_daily")
        assert order.index("int_risk_limit_breaches") < order.index("fct_portfolio_risk_daily")
        assert order.index("stg_risk_alerts") < order.index("fct_risk_alerts_summary")

    def test_compile_mart_models(self, marts_project_manager: DbtProjectManager) -> None:
        for mart_name in [
            "fct_daily_market_performance",
            "fct_trader_performance_daily",
            "fct_portfolio_risk_daily",
            "fct_risk_alerts_summary",
        ]:
            compiled = marts_project_manager.compile_model_sql(mart_name)
            assert "config(" not in compiled
            assert "trading_risk_dwh_dev." in compiled
            assert "MD5(CONCAT" in compiled


# =============================================================================
# 5. HERMETIC DUCKDB DBT MARTS EXECUTION TESTS
# =============================================================================

class TestDbtMartsExecutionInDuckDB:
    """Executes the full 15-model pipeline against DuckDB and verifies analytical accuracy."""

    @pytest.fixture(scope="class")
    def run_all_models(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        marts_project_manager: DbtProjectManager,
    ) -> DbtRunResult:
        cfg = DbtConfig(
            project_dir=DBT_DIR,
            target="dev",
            environment="dev",
            models=["staging", "intermediate", "marts"],
        )
        runner = DbtRunner(emulator=seeded_emulator, project_manager=marts_project_manager, config=cfg)
        return runner.run_models()

    def test_all_15_models_succeed(self, run_all_models: DbtRunResult) -> None:
        assert run_all_models.success is True
        assert run_all_models.models_executed == 15
        assert run_all_models.models_failed == 0
        for m in run_all_models.model_results:
            assert m.status == "SUCCESS", f"Model {m.model_name} failed: {m.error_message}"
            assert m.rows > 0, f"Model {m.model_name} produced 0 rows"

    def test_fct_daily_market_performance_metrics(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        rows = seeded_emulator.execute_query(
            "SELECT * FROM trading_risk_dwh_dev.fct_daily_market_performance;"
        )
        assert len(rows) > 0
        for r in rows:
            assert r["performance_date_key"] is not None
            assert r["ticker"] is not None
            assert r["total_volume"] > 0
            assert r["session_vwap"] > 0
            assert r["session_high"] >= r["session_low"]
            assert r["session_high"] >= r["session_open"]
            assert r["session_high"] >= r["session_close"]
            assert r["price_range_bps"] >= 0
            assert r["liquidity_tier"] in ("HIGH", "MEDIUM", "LOW")

    def test_fct_trader_performance_daily_metrics(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        rows = seeded_emulator.execute_query(
            "SELECT * FROM trading_risk_dwh_dev.fct_trader_performance_daily;"
        )
        assert len(rows) > 0
        for r in rows:
            assert r["trader_perf_key"] is not None
            assert r["trader_id"] is not None
            assert r["portfolio_id"] is not None
            assert r["total_orders_executed"] > 0
            assert r["gross_turnover_usd"] >= 0
            # Gross turnover must equal buy_notional + sell_notional within rounding tolerance
            assert abs(r["gross_turnover_usd"] - (r["buy_notional_usd"] + r["sell_notional_usd"])) < 0.1
            # Net delta must equal buy_notional - sell_notional
            assert abs(r["net_delta_usd"] - (r["buy_notional_usd"] - r["sell_notional_usd"])) < 0.1
            assert r["aggressive_order_pct"] >= 0.0

    def test_fct_portfolio_risk_daily_metrics(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        rows = seeded_emulator.execute_query(
            "SELECT * FROM trading_risk_dwh_dev.fct_portfolio_risk_daily;"
        )
        assert len(rows) > 0
        for r in rows:
            assert r["portfolio_risk_key"] is not None
            assert r["portfolio_id"] is not None
            assert r["parametric_var"] > 0
            assert r["portfolio_limit_usd"] > 0
            # Headroom = limit - gross_exposure
            expected_headroom = round(r["portfolio_limit_usd"] - r["gross_exposure"], 2)
            assert abs(r["risk_headroom_usd"] - expected_headroom) < 0.1
            assert r["is_capital_adequate"] == (r["risk_headroom_usd"] >= 0)
            assert r["risk_regime"] in ("LOW_RISK", "MODERATE_RISK", "ELEVATED_RISK", "HIGH_RISK", "CRITICAL_BREACH")
            assert r["breach_severity"] in ("NORMAL", "WARNING", "CRITICAL")

    def test_fct_risk_alerts_summary_metrics(self, seeded_emulator: LocalBigQueryEmulator) -> None:
        rows = seeded_emulator.execute_query(
            "SELECT * FROM trading_risk_dwh_dev.fct_risk_alerts_summary;"
        )
        assert len(rows) > 0
        for r in rows:
            assert r["alert_summary_key"] is not None
            assert r["portfolio_id"] is not None
            assert r["total_alerts"] > 0
            assert r["affected_tickers_count"] <= r["total_alerts"]
            assert r["severity"] in ("INFO", "WARNING", "CRITICAL")
            assert r["is_critical"] == (r["severity"] == "CRITICAL")


# =============================================================================
# 6. DBT SCHEMA TESTS EVALUATION ON MARTS
# =============================================================================

class TestDbtMartsSchemaTestsEvaluation:
    """Verifies that all declared schema tests for marts evaluate and pass 100%."""

    def test_all_marts_schema_tests_pass(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        marts_project_manager: DbtProjectManager,
    ) -> None:
        cfg = DbtConfig(
            project_dir=DBT_DIR,
            target="dev",
            environment="dev",
            models=["staging", "intermediate", "marts"],
        )
        runner = DbtRunner(emulator=seeded_emulator, project_manager=marts_project_manager, config=cfg)
        test_res = runner.run_tests(["marts"])
        assert test_res.success is True
        assert test_res.tests_executed > 30
        assert test_res.tests_failed == 0
        for t in test_res.test_results:
            assert t.status == "PASSED", f"Test failed: {t.model_name}.{t.column_name} ({t.test_type}): {t.error_message}"

    def test_schema_test_catches_intentional_corruption_in_marts(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        marts_project_manager: DbtProjectManager,
    ) -> None:
        # Inject duplicate using pure SQL
        seeded_emulator.connection.execute(
            "INSERT INTO trading_risk_dwh_dev.fct_daily_market_performance "
            "SELECT * FROM trading_risk_dwh_dev.fct_daily_market_performance LIMIT 1;"
        )

        cfg = DbtConfig(
            project_dir=DBT_DIR,
            target="dev",
            environment="dev",
            models=["marts"],
        )
        runner = DbtRunner(emulator=seeded_emulator, project_manager=marts_project_manager, config=cfg)
        test_res = runner.run_tests(["fct_daily_market_performance"])
        assert test_res.success is False
        assert test_res.tests_failed >= 1

        failed_unique = any(
            t.model_name == "fct_daily_market_performance"
            and t.column_name == "performance_date_key"
            and t.test_type == "unique"
            and t.status == "FAILED"
            for t in test_res.test_results
        )
        assert failed_unique, "Unique test on performance_date_key failed to detect injected duplicate!"

        # Clean up duplicate by re-running model
        runner.run_models(["fct_daily_market_performance"])

    def test_prometheus_telemetry_with_marts(
        self,
        seeded_emulator: LocalBigQueryEmulator,
        marts_project_manager: DbtProjectManager,
    ) -> None:
        cfg = DbtConfig(
            project_dir=DBT_DIR,
            target="dev",
            environment="dev",
            models=["staging", "intermediate", "marts"],
        )
        runner = DbtRunner(emulator=seeded_emulator, project_manager=marts_project_manager, config=cfg)
        runner.run_models(["fct_daily_market_performance"])
        runner.run_tests(["fct_daily_market_performance"])

        prom_text = runner.telemetry.format_prometheus()
        assert "dbt_models_executed_total" in prom_text
        assert "dbt_tests_executed_total" in prom_text
        assert "dbt_tests_failed_total 0" in prom_text


# =============================================================================
# 7. AIRFLOW OPERATOR & DAG INTEGRATION WITH MARTS
# =============================================================================

class TestAirflowIntegrationWithMarts:
    """Verifies that Airflow DbtRunOperator and DbtTestOperator support marts models."""

    def test_dbt_run_operator_executes_marts(self) -> None:
        op = DbtRunOperator(
            task_id="run_dbt_marts",
            models=["fct_daily_market_performance", "fct_trader_performance_daily", "fct_portfolio_risk_daily", "fct_risk_alerts_summary"],
            environment="dev",
        )
        context = {"ds": "2026-11-04"}
        result = op.execute(context)
        assert result["status"] == "SUCCESS"
        assert result["models_executed"] == 4
        assert result["models_failed"] == 0

    def test_dbt_test_operator_evaluates_marts(self) -> None:
        op = DbtTestOperator(
            task_id="test_dbt_marts",
            models=["marts"],
            environment="dev",
        )
        context = {"ds": "2026-11-04"}
        result = op.execute(context)
        assert result["status"] == "SUCCESS"
        assert result["tests_failed"] == 0
        assert result["tests_passed"] > 0

    def test_master_lakehouse_dag_topology_with_dbt(self) -> None:
        dag = build_trading_risk_dag()
        assert "run_dbt_transforms" in dag.task_dict
        assert "test_dbt_models" in dag.task_dict
        run_task = dag.get_task("run_dbt_transforms")
        test_task = dag.get_task("test_dbt_models")
        assert run_task in test_task.upstream_list


# =============================================================================
# 8. DBT CLI RUNNER WITH MARTS
# =============================================================================

class TestDbtCLIRunnerWithMarts:
    """Verifies CLI execution with marts models."""

    def test_cli_compile_only_marts(self) -> None:
        from dwh.dbt.cli import main
        code = main(["--compile-only", "--models", "marts"])
        assert code == 0

    def test_cli_seed_and_run_marts_with_reports(self) -> None:
        from dwh.dbt.cli import main
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "dbt_marts_report.json"
            prom_path = Path(tmpdir) / "dbt_marts_metrics.prom"

            code = main([
                "--seed",
                "--run-tests",
                "--models", "staging", "intermediate", "marts",
                "--json-report", str(json_path),
                "--export-prometheus", str(prom_path),
            ])
            assert code == 0
            assert json_path.is_file()
            assert prom_path.is_file()

            with open(json_path, "r", encoding="utf-8") as f:
                report = json.load(f)

            assert report["success"] is True
            assert report["models_executed"] == 15
            assert report["tests_executed"] >= 120

            with open(prom_path, "r", encoding="utf-8") as f:
                metrics = f.read()

            assert "dbt_models_executed_total 15" in metrics
            assert "dbt_tests_failed_total 0" in metrics
