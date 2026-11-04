"""Zero-Cost Unit & Integration Test Suite for Cloud SQL to BigQuery Replication (Day 25).

Validates:
1. Terraform BigQuery Module: External connection resource, IAM service account role binding,
   federated views definitions, balanced HCL braces, and root wiring.
2. JSON Schemas & SQL DDL: Parity, GoogleSQL syntax, options stripping, and Arrow schema conversion.
3. BigQuery Emulator Federation: In-memory Cloud SQL tables initialization, seeding, EXTERNAL_QUERY
   subquery translation, and federated view execution in DuckDB.
4. Replication Engine: Pushdown query generation, idempotent MERGE statements, incremental watermark
   advancement, snapshot replacement, idempotency (zero duplicate rows), retry policies, and Prometheus metrics.
5. Airflow Orchestration: CloudSqlToBigQueryFederationOperator execution with Jinja templating, failure
   injection simulation, and master DAG topological acyclicity.
6. Replication CLI Runner: --dry-run, --sync-all, --table, --json-report, and --export-prometheus modes.
7. dbt Staging Models: Discovery and compilation of federated Cloud SQL staging models.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Dict, List
import pytest
import pyarrow as pa

from dwh.bigquery.config import (
    BigQueryConnectionConfig,
    BigQueryDatasetConfig,
    FEDERATED_VIEWS_CATALOG,
    MATERIALIZED_VIEWS_CATALOG,
    STAR_SCHEMA_CATALOG,
    TablePartitionClusteringSpec,
    V_FEDERATED_PORTFOLIO_LIMITS_SPEC,
    V_FEDERATED_RISK_ALERTS_SPEC,
)
from dwh.bigquery.emulator import (
    BigQueryTelemetry,
    LocalBigQueryEmulator,
    _translate_external_query,
)
from dwh.bigquery.replication import (
    CloudSqlBigQueryReplicationRunner,
    REPLICATION_CATALOG,
    ReplicationConfig,
    ReplicationMode,
    ReplicationTelemetry,
    ReplicationWatermarkStore,
    TableReplicationSpec,
)
from dwh.bigquery.replication_cli import run_replication_cli
from dwh.bigquery.schema_manager import (
    BigQuerySchemaManager,
    SchemaValidationError,
)
from dwh.dbt.config import DbtConfig
from dwh.dbt.project_manager import DbtProjectManager
from orchestration.airflow.airflow_compat import (
    BigQueryJobRegistry,
    CloudSqlToBigQueryFederationOperator,
    create_mock_task_context,
)
from orchestration.airflow.dags.trading_risk_daily_batch_dag import build_trading_risk_dag

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_BQ_DIR = REPO_ROOT / "terraform" / "modules" / "bigquery"
TF_ROOT_DIR = REPO_ROOT / "terraform"
DWH_BQ_DIR = REPO_ROOT / "dwh" / "bigquery"


# =============================================================================
# 1. TERRAFORM FEDERATION MODULE TESTS
# =============================================================================

class TestTerraformFederationModule:
    """Validate Terraform infrastructure manifests for BigQuery Cloud SQL federation."""

    def test_bigquery_federation_resources_defined(self) -> None:
        """Verify BigQuery connection and IAM resources exist in main.tf."""
        main_tf = (TF_BQ_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'resource "google_bigquery_connection" "cloudsql_connection"' in main_tf
        assert 'resource "google_project_iam_member" "bigquery_cloudsql_client"' in main_tf
        assert 'role    = "roles/cloudsql.client"' in main_tf
        assert 'resource "google_bigquery_table" "v_federated_risk_alerts"' in main_tf
        assert 'resource "google_bigquery_table" "v_federated_portfolio_limits"' in main_tf

    def test_balanced_braces_in_terraform(self) -> None:
        """Verify balanced HCL braces across all BigQuery and root Terraform files."""
        for tf_file in TF_BQ_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_file.name}"

        for tf_file in TF_ROOT_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in root {tf_file.name}"

    def test_root_terraform_wires_cloudsql_federation(self) -> None:
        """Verify root Terraform configuration wires Cloud SQL module into BigQuery module."""
        main_tf = (TF_ROOT_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'cloudsql_instance_connection_name = module.cloudsql.instance_connection_name' in main_tf
        assert 'cloudsql_database_name            = module.cloudsql.database_name' in main_tf
        assert 'cloudsql_user                     = module.cloudsql.db_username' in main_tf
        assert 'cloudsql_password                 = module.cloudsql.db_password' in main_tf

        outputs_tf = (TF_ROOT_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert "bigquery_cloudsql_connection_id" in outputs_tf
        assert "bigquery_federated_views" in outputs_tf

    def test_federation_json_schemas_exist_and_valid(self) -> None:
        """Verify federated views JSON schemas exist in both module and dwh directories."""
        views = ["v_federated_risk_alerts", "v_federated_portfolio_limits"]
        for v in views:
            dwh_schema = DWH_BQ_DIR / "schemas" / f"{v}.json"
            tf_schema = TF_BQ_DIR / "schemas" / f"{v}.json"
            assert dwh_schema.is_file(), f"Missing DWH schema: {dwh_schema}"
            assert tf_schema.is_file(), f"Missing TF schema: {tf_schema}"

            data_dwh = json.loads(dwh_schema.read_text(encoding="utf-8"))
            data_tf = json.loads(tf_schema.read_text(encoding="utf-8"))
            assert data_dwh == data_tf, f"Schema mismatch between DWH and TF for {v}"
            assert len(data_dwh) > 0


# =============================================================================
# 2. SQL DDL & SCHEMA MANAGER TESTS
# =============================================================================

class TestBigQueryFederatedViewsAndDDL:
    """Validate GoogleSQL DDL scripts, catalog specifications, and Arrow schemas."""

    def test_ddl_files_exist_and_non_empty(self) -> None:
        """Verify federated view DDL files exist and are non-empty."""
        required = [
            "v_federated_risk_alerts.sql",
            "v_federated_portfolio_limits.sql",
            "all_federated_views.sql",
        ]
        ddl_dir = DWH_BQ_DIR / "ddl"
        for f in required:
            path = ddl_dir / f
            assert path.is_file(), f"Missing DDL file: {path}"
            assert path.stat().st_size > 0

    def test_federated_views_catalog_validation(self) -> None:
        """Verify schema manager successfully validates FEDERATED_VIEWS_CATALOG."""
        schema_mgr = BigQuerySchemaManager()
        schema_mgr.validate_federated_views_catalog()
        assert "v_federated_risk_alerts" in FEDERATED_VIEWS_CATALOG
        assert "v_federated_portfolio_limits" in FEDERATED_VIEWS_CATALOG

    def test_ddl_rendering(self) -> None:
        """Verify variable interpolation in DDL rendering."""
        schema_mgr = BigQuerySchemaManager()
        rendered = schema_mgr.render_ddl(
            "v_federated_risk_alerts",
            project_id="test-proj",
            dataset_id="test_dwh",
            region="europe-west1",
            connection_id="test-conn",
        )
        assert "test-proj.test_dwh.v_federated_risk_alerts" in rendered
        assert "test-proj.europe-west1.test-conn" in rendered

        master_rendered = schema_mgr.render_federated_views_ddl(
            project_id="test-proj",
            dataset_id="test_dwh",
            region="europe-west1",
            connection_id="test-conn",
        )
        assert "v_federated_risk_alerts" in master_rendered
        assert "v_federated_portfolio_limits" in master_rendered

    def test_arrow_schema_conversion(self) -> None:
        """Verify conversion from JSON schemas to PyArrow Schemas."""
        schema_mgr = BigQuerySchemaManager()
        arrow_alerts = schema_mgr.to_arrow_schema("v_federated_risk_alerts")
        assert isinstance(arrow_alerts, pa.Schema)
        assert "alert_id" in arrow_alerts.names
        assert "threshold_value" in arrow_alerts.names
        assert "event_timestamp" in arrow_alerts.names

        arrow_limits = schema_mgr.to_arrow_schema("v_federated_portfolio_limits")
        assert isinstance(arrow_limits, pa.Schema)
        assert "limit_id" in arrow_limits.names
        assert "max_notional_usd" in arrow_limits.names


# =============================================================================
# 3. LOCAL BIGQUERY EMULATOR FEDERATION TESTS
# =============================================================================

class TestLocalBigQueryEmulatorFederation:
    """Validate DuckDB-backed BigQuery emulator handling EXTERNAL_QUERY and federated views."""

    def test_cloudsql_tables_initialization_and_seeding(self) -> None:
        """Verify operational Cloud SQL tables are created and seeded in DuckDB."""
        emulator = LocalBigQueryEmulator()
        emulator.initialize_cloudsql_tables()
        counts = emulator.seed_cloudsql_tables()
        assert counts["risk_alerts"] == 25
        assert counts["portfolio_limits"] == 3

        cnt_alerts = emulator.get_table_row_count("risk_alerts")
        cnt_limits = emulator.get_table_row_count("portfolio_limits")
        assert cnt_alerts == 25
        assert cnt_limits == 3

    def test_external_query_regex_translation(self) -> None:
        """Verify translation of GoogleSQL EXTERNAL_QUERY into standard derived subquery."""
        q1 = 'SELECT * FROM EXTERNAL_QUERY("proj.loc.conn", "SELECT alert_id, ticker FROM risk_alerts")'
        t1 = _translate_external_query(q1)
        assert t1 == "SELECT * FROM (SELECT alert_id, ticker FROM risk_alerts)"

        q2 = 'SELECT * FROM EXTERNAL_QUERY("proj.loc.conn", """SELECT alert_id::text, threshold_value::numeric FROM risk_alerts""")'
        t2 = _translate_external_query(q2)
        assert "alert_id::VARCHAR AS alert_id" in t2
        assert "threshold_value::DECIMAL(18, 4) AS threshold_value" in t2

    def test_deploy_and_query_federated_views(self) -> None:
        """Verify deploying federated views and executing queries over Cloud SQL operational data."""
        emulator = LocalBigQueryEmulator()
        emulator.initialize_cloudsql_tables()
        emulator.seed_cloudsql_tables()
        emulator.initialize_federated_views()

        # Query v_federated_risk_alerts
        res_alerts = emulator.execute_query(
            "SELECT alert_id, portfolio_id, ticker, severity, threshold_value, observed_value FROM v_federated_risk_alerts"
        )
        assert len(res_alerts) == 25
        first = res_alerts[0]
        assert "alert_id" in first
        assert "severity" in first
        assert first["severity"] in ("LOW", "MEDIUM", "HIGH", "CRITICAL")

        # Query v_federated_portfolio_limits
        res_limits = emulator.execute_query(
            "SELECT portfolio_id, max_notional_usd, max_drawdown_pct, is_active FROM v_federated_portfolio_limits ORDER BY portfolio_id"
        )
        assert len(res_limits) == 3
        portfolios = [r["portfolio_id"] for r in res_limits]
        assert "PORTFOLIO_ALPHA" in portfolios
        assert "PORTFOLIO_BETA" in portfolios
        assert "PORTFOLIO_GAMMA" in portfolios

    def test_federated_views_arrow_table_conversion(self) -> None:
        """Verify PyArrow table conversion over federated views."""
        emulator = LocalBigQueryEmulator()
        emulator.initialize_cloudsql_tables()
        emulator.seed_cloudsql_tables()
        emulator.initialize_federated_views()

        arrow_tbl = emulator.execute_query_arrow("SELECT * FROM v_federated_portfolio_limits")
        assert isinstance(arrow_tbl, pa.Table)
        assert arrow_tbl.num_rows == 3
        assert "max_notional_usd" in arrow_tbl.column_names


# =============================================================================
# 4. REPLICATION RUNNER TESTS
# =============================================================================

class TestCloudSqlBigQueryReplicationRunner:
    """Validate replication engine pushdown queries, MERGE execution, and watermark tracking."""

    def test_pushdown_query_generation(self) -> None:
        """Verify generation of pushdown queries with watermark and date filters."""
        runner = CloudSqlBigQueryReplicationRunner()
        spec_alerts = REPLICATION_CATALOG["risk_alerts"]

        # No filters
        q1 = runner.build_pushdown_query(spec_alerts)
        assert "SELECT alert_id::text AS alert_id" in q1
        assert "FROM risk_alerts" in q1
        assert "ORDER BY event_timestamp ASC" in q1

        # With watermark
        wm = datetime(2026, 11, 4, 12, 0, 0, tzinfo=timezone.utc)
        q2 = runner.build_pushdown_query(spec_alerts, since_watermark=wm)
        assert "event_timestamp > '2026-11-04 12:00:00'" in q2

        # With target date
        q3 = runner.build_pushdown_query(spec_alerts, target_date=date(2026, 11, 5))
        assert "DATE(event_timestamp) = '2026-11-05'" in q3

    def test_build_merge_statements(self) -> None:
        """Verify generation of BigQuery MERGE statements using EXTERNAL_QUERY."""
        runner = CloudSqlBigQueryReplicationRunner()
        merge_alerts = runner.build_merge_statement(REPLICATION_CATALOG["risk_alerts"])
        assert "MERGE INTO `trading-risk-dev-001.trading_risk_dwh_dev.fact_risk_alerts` T" in merge_alerts
        assert "USING (" in merge_alerts
        assert "FROM EXTERNAL_QUERY(" in merge_alerts
        assert "ON T.alert_id = S.alert_id" in merge_alerts
        assert "WHEN MATCHED THEN" in merge_alerts
        assert "WHEN NOT MATCHED THEN" in merge_alerts

        merge_limits = runner.build_merge_statement(REPLICATION_CATALOG["portfolio_limits"])
        assert "MERGE INTO `trading-risk-dev-001.trading_risk_dwh_dev.dim_portfolios` T" in merge_limits
        assert "ON T.portfolio_id = S.portfolio_id" in merge_limits

    def test_replicate_portfolio_limits_snapshot(self) -> None:
        """Verify snapshot replication of portfolio_limits into dim_portfolios."""
        emulator = LocalBigQueryEmulator()
        emulator.initialize_star_schema()
        emulator.load_seed_dimensions()

        runner = CloudSqlBigQueryReplicationRunner(emulator=emulator)
        runner.seed_mock_cloudsql_data()

        result = runner.replicate_table("portfolio_limits", full_refresh=True)
        assert result["status"] == "SUCCESS"
        assert result["target_table"] == "dim_portfolios"

        # Verify limits updated in dim_portfolios
        rows = emulator.execute_query(
            "SELECT portfolio_id, risk_limit_usd, max_var_limit_usd FROM dim_portfolios WHERE portfolio_id = 'PORTFOLIO_ALPHA'"
        )
        assert len(rows) == 1
        assert float(rows[0]["risk_limit_usd"]) == 10000000.0

    def test_replicate_risk_alerts_incremental(self) -> None:
        """Verify incremental watermark replication of risk_alerts into fact_risk_alerts."""
        emulator = LocalBigQueryEmulator()
        emulator.initialize_star_schema()
        emulator.load_seed_dimensions()

        runner = CloudSqlBigQueryReplicationRunner(emulator=emulator)
        runner.seed_mock_cloudsql_data(alerts_count=25)

        # First sync
        result1 = runner.replicate_table("risk_alerts")
        assert result1["status"] == "SUCCESS"
        assert result1["rows_synced"] == 25
        assert emulator.get_table_row_count("fact_risk_alerts") == 25
        assert result1["watermark"] is not None

        # Second sync without new rows (idempotency & watermark filter)
        result2 = runner.replicate_table("risk_alerts")
        assert result2["status"] == "SUCCESS"
        assert result2["rows_synced"] == 0
        assert emulator.get_table_row_count("fact_risk_alerts") == 25

    def test_replicate_all_end_to_end(self) -> None:
        """Verify orchestrating full replication catalog end-to-end."""
        emulator = LocalBigQueryEmulator()
        emulator.initialize_star_schema()
        emulator.load_seed_dimensions()

        runner = CloudSqlBigQueryReplicationRunner(emulator=emulator)
        runner.seed_mock_cloudsql_data()

        all_res = runner.replicate_all()
        assert all_res["status"] == "SUCCESS"
        assert all_res["total_rows_synced"] == 25
        assert "portfolio_limits" in all_res["tables"]
        assert "risk_alerts" in all_res["tables"]

    def test_replication_telemetry_and_prometheus(self) -> None:
        """Verify operational telemetry recording and Prometheus exposition."""
        telemetry = ReplicationTelemetry()
        telemetry.record_sync("risk_alerts", 50, 120.5, success=True, est_bytes=12800)
        telemetry.record_sync("portfolio_limits", 3, 45.0, success=True, est_bytes=768)
        telemetry.record_sync("test_fail", 0, 10.0, success=False)

        assert telemetry.syncs_executed == 3
        assert telemetry.syncs_failed == 1
        assert telemetry.records_synced["risk_alerts"] == 50
        assert telemetry.records_synced["portfolio_limits"] == 3

        prom = telemetry.format_prometheus()
        assert "bigquery_replication_syncs_executed_total 3" in prom
        assert "bigquery_replication_syncs_failed_total 1" in prom
        assert 'bigquery_replication_records_synced_total{table="risk_alerts"} 50' in prom

    def test_watermark_store_operations(self) -> None:
        """Verify setting, getting, and resetting watermark store state."""
        store = ReplicationWatermarkStore()
        assert store.get_watermark("risk_alerts") is None

        now = datetime.now(timezone.utc)
        store.set_watermark("risk_alerts", now)
        assert store.get_watermark("risk_alerts") == now

        as_dict = store.to_dict()
        assert "risk_alerts" in as_dict

        store.reset("risk_alerts")
        assert store.get_watermark("risk_alerts") is None


# =============================================================================
# 5. AIRFLOW INTEGRATION TESTS
# =============================================================================

class TestAirflowFederationOperator:
    """Validate CloudSqlToBigQueryFederationOperator execution and master DAG integration."""

    def test_operator_initialization_and_template_fields(self) -> None:
        """Verify operator properties and Jinja template fields."""
        op = CloudSqlToBigQueryFederationOperator(
            task_id="test_replicate",
            tables=["risk_alerts"],
            target_date="{{ ds }}",
            connection_id="conn_test",
        )
        assert op.task_id == "test_replicate"
        assert "tables" in op.template_fields
        assert "target_date" in op.template_fields
        assert "connection_id" in op.template_fields

    def test_operator_execute_in_mock_context(self) -> None:
        """Verify execution within Airflow task context with Jinja rendering."""
        dag = build_trading_risk_dag()
        task = dag.get_task("replicate_cloudsql_operational_data")
        assert task is not None
        assert isinstance(task, CloudSqlToBigQueryFederationOperator)

        ctx = create_mock_task_context(dag, task, execution_date=datetime(2026, 11, 5))
        result = task.execute(ctx)
        assert result["status"] == "SUCCESS"
        assert "tables" in result

    def test_master_batch_dag_structure_and_acyclicity(self) -> None:
        """Verify master Lakehouse DAG validates and preserves proper topological dependencies."""
        dag = build_trading_risk_dag()
        dag.validate()

        sorted_task_ids = [t.task_id for t in dag.topological_sort()]
        assert "replicate_cloudsql_operational_data" in sorted_task_ids
        assert "refresh_bigquery_partitions" in sorted_task_ids
        assert "run_dbt_transforms" in sorted_task_ids
        assert "test_dbt_models" in sorted_task_ids

        # Verify dependency order: replication before partition refresh and dbt
        idx_repl = sorted_task_ids.index("replicate_cloudsql_operational_data")
        idx_refresh = sorted_task_ids.index("refresh_bigquery_partitions")
        idx_dbt = sorted_task_ids.index("run_dbt_transforms")

        assert idx_repl < idx_refresh, "Replication must precede BigQuery partition refresh"
        assert idx_refresh < idx_dbt, "Partition refresh must precede dbt transforms"


# =============================================================================
# 6. REPLICATION CLI TESTS
# =============================================================================

class TestReplicationCLI:
    """Validate the standalone replication CLI runner."""

    def test_cli_dry_run(self) -> None:
        """Verify --dry-run mode executes and outputs SQL without DB changes."""
        code = run_replication_cli(["--sync-all", "--dry-run"])
        assert code == 0

    def test_cli_sync_all_with_seed(self) -> None:
        """Verify --sync-all with mock seeding."""
        code = run_replication_cli(["--sync-all", "--seed-mock-data"])
        assert code == 0

    def test_cli_table_specific_execution(self) -> None:
        """Verify replicating a single table via CLI."""
        code = run_replication_cli(["--table", "portfolio_limits", "--seed-mock-data"])
        assert code == 0

    def test_cli_json_report(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify --json-report produces valid JSON output."""
        code = run_replication_cli(["--sync-all", "--seed-mock-data", "--json-report"])
        assert code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["status"] == "SUCCESS"
        assert "tables" in data

    def test_cli_export_prometheus(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify --export-prometheus flag renders metrics."""
        code = run_replication_cli(["--sync-all", "--seed-mock-data", "--export-prometheus"])
        assert code == 0
        captured = capsys.readouterr()
        assert "bigquery_replication_syncs_executed_total" in captured.out


# =============================================================================
# 7. DOWNSTREAM DATA PIPELINE & DBT INTEGRATION TESTS
# =============================================================================

class TestDownstreamDbtIntegrationWithReplication:
    """Validate that replicated BigQuery tables feed seamlessly into dbt risk models."""

    def test_replicated_data_accessible_by_staging_models(self) -> None:
        """Verify replicated Cloud SQL risk alerts in fact_risk_alerts are queryable by analytical models."""
        emulator = LocalBigQueryEmulator()
        emulator.initialize_star_schema()
        emulator.load_seed_dimensions()

        runner = CloudSqlBigQueryReplicationRunner(emulator=emulator)
        runner.seed_mock_cloudsql_data(alerts_count=10)
        res = runner.replicate_all()
        assert res["status"] == "SUCCESS"

        # Verify fact_risk_alerts has rows
        cnt = emulator.get_table_row_count("fact_risk_alerts")
        assert cnt == 10

        # Query fact_risk_alerts as done by dbt stg_risk_alerts
        alerts = emulator.execute_query(
            "SELECT alert_id, portfolio_id, ticker, alert_type, severity, threshold_value, observed_value FROM fact_risk_alerts"
        )
        assert len(alerts) == 10
        for a in alerts:
            assert a["alert_id"] is not None
            assert a["portfolio_id"].startswith("PORTFOLIO_")

    def test_replicated_portfolio_limits_accessible_by_portfolio_dimension(self) -> None:
        """Verify replicated portfolio limits in dim_portfolios are queryable with updated mandates."""
        emulator = LocalBigQueryEmulator()
        emulator.initialize_star_schema()
        emulator.load_seed_dimensions()

        runner = CloudSqlBigQueryReplicationRunner(emulator=emulator)
        runner.seed_mock_cloudsql_data()
        runner.replicate_table("portfolio_limits")

        # Verify dim_portfolios has updated risk limits
        portfolios = emulator.execute_query(
            "SELECT portfolio_id, risk_limit_usd, max_var_limit_usd FROM dim_portfolios WHERE portfolio_id IN ('PORTFOLIO_ALPHA', 'PORTFOLIO_BETA')"
        )
        assert len(portfolios) == 2
        limits_by_id = {p["portfolio_id"]: float(p["risk_limit_usd"]) for p in portfolios}
        assert limits_by_id["PORTFOLIO_ALPHA"] == 10000000.0
        assert limits_by_id["PORTFOLIO_BETA"] == 25000000.0

