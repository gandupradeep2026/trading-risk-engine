"""BigQuery Star Schema, DDL, and Partition/Clustering Tests (Day 22).

Validates:
1. Terraform BigQuery Module: Dataset definition, region pinning, access controls,
   labels, deletion protection, partition policies, and clustering rules (<= 4 cols).
2. SQL DDL & Schema Consistency: Valid GoogleSQL DDL, BigQuery types, JSON schemas,
   column parity between DDL and JSON schemas.
3. Star Schema Catalog & Constraints: Invariant validation for clustering limits,
   partition granularity, and mandatory WHERE partition filtering.
4. Zero-Cost Local BigQuery Emulator: DuckDB-backed execution of DDL, dimension seeding,
   synthetic fact loading, star schema analytical joins, partition filter enforcement,
   and Prometheus telemetry.
5. Airflow Master DAG Integration: BigQueryInsertJobOperator execution, Jinja templating,
   job registry tracking, and failure handling.
6. CLI DDL Runner: dry-run, validate-only, and local emulation execution modes.
"""

from datetime import date, datetime
import json
from pathlib import Path
import re
import pytest
import pyarrow as pa

from dwh.bigquery.config import (
    BigQueryDatasetConfig,
    STAR_SCHEMA_CATALOG,
    TablePartitionClusteringSpec,
    DIM_TICKERS_SPEC,
    DIM_PORTFOLIOS_SPEC,
    DIM_TRADERS_SPEC,
    DIM_DATE_SPEC,
    FACT_TRADE_ORDERS_SPEC,
    FACT_MARKET_TICKS_SPEC,
    FACT_RISK_STATS_SPEC,
    FACT_RISK_ALERTS_SPEC,
)
from dwh.bigquery.schema_manager import (
    BigQuerySchemaManager,
    SchemaValidationError,
    ALLOWED_BIGQUERY_TYPES,
)
from dwh.bigquery.emulator import (
    BigQueryPartitionFilterRequiredError,
    BigQueryTelemetry,
    LocalBigQueryEmulator,
)
from dwh.bigquery.seed_data import (
    generate_mock_market_ticks,
    generate_mock_risk_alerts,
    generate_mock_risk_stats,
    generate_mock_trade_orders,
    get_dim_date_records,
    get_dim_portfolios_records,
    get_dim_tickers_records,
    get_dim_traders_records,
)
from dwh.bigquery.ddl_runner import run_ddl_tool
from orchestration.airflow.airflow_compat import (
    AirflowException,
    BigQueryInsertJobOperator,
    BigQueryJobRegistry,
)
from orchestration.airflow.dags.trading_risk_daily_batch_dag import build_trading_risk_dag

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_BQ_DIR = REPO_ROOT / "terraform" / "modules" / "bigquery"
TF_ROOT_DIR = REPO_ROOT / "terraform"
DWH_BQ_DIR = REPO_ROOT / "dwh" / "bigquery"


# =============================================================================
# 1. TERRAFORM BIGQUERY MODULE TESTS
# =============================================================================

class TestTerraformBigQueryModule:
    """Validate BigQuery Terraform module and root integration."""

    def test_bigquery_module_files_exist(self) -> None:
        """Verify all required BigQuery Terraform module files exist."""
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf"]
        for f in required:
            path = TF_BQ_DIR / f
            assert path.is_file(), f"Missing Terraform file: {path}"
            assert path.stat().st_size > 0, f"File is empty: {path}"

    def test_all_json_schemas_exist_in_terraform_module(self) -> None:
        """Verify all 8 star-schema JSON schemas exist in the Terraform module."""
        schema_dir = TF_BQ_DIR / "schemas"
        assert schema_dir.is_dir()
        tables = [
            "dim_tickers", "dim_portfolios", "dim_traders", "dim_date",
            "fact_trade_orders", "fact_market_ticks", "fact_risk_stats", "fact_risk_alerts"
        ]
        for tbl in tables:
            schema_file = schema_dir / f"{tbl}.json"
            assert schema_file.is_file(), f"Missing schema in Terraform module: {schema_file}"
            assert schema_file.stat().st_size > 0

    def test_balanced_braces_in_bigquery_module(self) -> None:
        """Verify balanced HCL braces across all BigQuery Terraform files."""
        for tf_file in TF_BQ_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_file.name}"

    def test_root_terraform_wires_bigquery_module(self) -> None:
        """Verify root Terraform configuration instantiates and wires module.bigquery."""
        main_tf = (TF_ROOT_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'module "bigquery"' in main_tf
        assert 'source = "./modules/bigquery"' in main_tf
        assert "composer_service_account_email = module.composer.composer_service_account_email" in main_tf

        outputs_tf = (TF_ROOT_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert "bigquery_dataset_id" in outputs_tf
        assert "bigquery_table_ids" in outputs_tf

        vars_tf = (TF_ROOT_DIR / "variables.tf").read_text(encoding="utf-8")
        assert "bigquery_dataset_id" in vars_tf

        dev_tfvars = (TF_ROOT_DIR / "environments" / "dev.tfvars").read_text(encoding="utf-8")
        assert "trading_risk_dwh_dev" in dev_tfvars

        prod_tfvars = (TF_ROOT_DIR / "environments" / "prod.tfvars").read_text(encoding="utf-8")
        assert "trading_risk_dwh_prod" in prod_tfvars

    def test_fact_table_partitioning_and_cost_controls(self) -> None:
        """Verify fact tables enforce day-level partitioning and partition filtering."""
        main_tf = (TF_BQ_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'field                    = "trade_date"' in main_tf
        assert 'field                    = "tick_date"' in main_tf
        assert 'field                    = "valuation_date"' in main_tf
        assert 'field                    = "alert_date"' in main_tf
        assert 'type                     = "DAY"' in main_tf
        assert "require_partition_filter = var.require_partition_filter" in main_tf

    def test_clustering_specifications_and_limits(self) -> None:
        """Verify clustering definitions conform to BigQuery constraints (max 4 cols)."""
        main_tf = (TF_BQ_DIR / "main.tf").read_text(encoding="utf-8")
        # Find all clustering blocks
        cluster_matches = re.findall(r'clustering\s*=\s*\[(.*?)\]', main_tf)
        assert len(cluster_matches) >= 6
        for m in cluster_matches:
            cols = [c.strip().strip('"') for c in m.split(",") if c.strip()]
            assert len(cols) <= 4, f"Clustering list exceeds BigQuery limit of 4: {cols}"
            assert len(cols) == len(set(cols)), f"Duplicate clustering column: {cols}"


# =============================================================================
# 2. SQL DDL & JSON SCHEMA CONSISTENCY TESTS
# =============================================================================

class TestBigQuerySchemaFilesAndDDL:
    """Validate JSON schemas, SQL DDL files, and schema parity."""

    def test_all_json_schema_files_exist_and_are_valid(self) -> None:
        """Verify all 8 JSON schemas parse cleanly and use supported BigQuery types."""
        sm = BigQuerySchemaManager()
        for tbl in STAR_SCHEMA_CATALOG.keys():
            schema = sm.load_json_schema(tbl)
            assert isinstance(schema, list)
            assert len(schema) > 0
            for field_def in schema:
                assert "name" in field_def
                assert "type" in field_def
                assert field_def["type"] in ALLOWED_BIGQUERY_TYPES
                assert field_def.get("mode", "NULLABLE") in {"REQUIRED", "NULLABLE", "REPEATED"}

    def test_all_ddl_files_exist_and_non_empty(self) -> None:
        """Verify all 8 DDL files and all_star_schema.sql exist."""
        sm = BigQuerySchemaManager()
        for tbl in list(STAR_SCHEMA_CATALOG.keys()) + ["all_star_schema"]:
            ddl = sm.load_raw_ddl(tbl)
            assert len(ddl.strip()) > 0
            assert "CREATE OR REPLACE TABLE" in ddl

    def test_ddl_and_schema_parity(self) -> None:
        """Verify exact column name and ordering parity between DDL and JSON schemas."""
        sm = BigQuerySchemaManager()
        sm.validate_catalog()  # Validates parity across all 8 tables without raising

    def test_master_ddl_rendering(self) -> None:
        """Verify all_star_schema.sql renders with interpolated project and dataset."""
        sm = BigQuerySchemaManager()
        rendered = sm.render_master_ddl("my-test-proj", "my_test_dwh")
        assert "my-test-proj.my_test_dwh.dim_tickers" in rendered
        assert "my-test-proj.my_test_dwh.fact_trade_orders" in rendered
        assert "${project_id}" not in rendered
        assert "${dataset_id}" not in rendered


# =============================================================================
# 3. STAR SCHEMA CATALOG & CONFIGURATION TESTS
# =============================================================================

class TestBigQueryConfigAndCatalog:
    """Validate star schema specification invariants and configuration models."""

    def test_catalog_completeness(self) -> None:
        """Verify all 8 core tables are registered in the star schema catalog."""
        expected = {
            "dim_tickers", "dim_portfolios", "dim_traders", "dim_date",
            "fact_trade_orders", "fact_market_ticks", "fact_risk_stats", "fact_risk_alerts"
        }
        assert set(STAR_SCHEMA_CATALOG.keys()) == expected

    def test_clustering_limit_validation(self) -> None:
        """Verify TablePartitionClusteringSpec rejects more than 4 clustering columns."""
        with pytest.raises(ValueError, match="maximum of 4 clustering columns"):
            TablePartitionClusteringSpec(
                table_name="invalid_table",
                clustering_columns=["c1", "c2", "c3", "c4", "c5"]
            )

    def test_clustering_duplicate_column_rejection(self) -> None:
        """Verify TablePartitionClusteringSpec rejects duplicate clustering columns."""
        with pytest.raises(ValueError, match="must be distinct"):
            TablePartitionClusteringSpec(
                table_name="invalid_table",
                clustering_columns=["colA", "colB", "colA"]
            )

    def test_partition_consistency_validation(self) -> None:
        """Verify require_partition_filter=True requires partition_column to be set."""
        with pytest.raises(ValueError, match="require_partition_filter=True but partition_column is not specified"):
            TablePartitionClusteringSpec(
                table_name="invalid_table",
                require_partition_filter=True,
                partition_column=None,
            )

    def test_dataset_config_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify BigQueryDatasetConfig parses environment variables correctly."""
        monkeypatch.setenv("ENVIRONMENT", "prod")
        monkeypatch.setenv("GCP_PROJECT_ID", "trading-risk-prod-custom")
        monkeypatch.setenv("BIGQUERY_DATASET_ID", "custom_prod_dwh")
        monkeypatch.setenv("GCP_REGION", "us-east4")

        cfg = BigQueryDatasetConfig.from_env()
        assert cfg.environment == "prod"
        assert cfg.project_id == "trading-risk-prod-custom"
        assert cfg.dataset_id == "custom_prod_dwh"
        assert cfg.location == "us-east4"
        assert cfg.delete_contents_on_destroy is False
        assert cfg.get_full_table_id("fact_trade_orders") == "trading-risk-prod-custom.custom_prod_dwh.fact_trade_orders"


# =============================================================================
# 4. ZERO-COST LOCAL BIGQUERY EMULATOR TESTS
# =============================================================================

class TestBigQueryLocalEmulator:
    """Validate DuckDB-backed local BigQuery emulator execution and query behavior."""

    @pytest.fixture
    def emulator(self) -> LocalBigQueryEmulator:
        emu = LocalBigQueryEmulator(
            project_id="test-risk-project", dataset_id="test_risk_dwh"
        )
        emu.initialize_star_schema()
        yield emu
        emu.close()

    def test_initialize_star_schema_creates_all_tables(self, emulator: LocalBigQueryEmulator) -> None:
        """Verify all 8 star-schema tables are initialized with row count 0."""
        for tbl in STAR_SCHEMA_CATALOG.keys():
            cnt = emulator.get_table_row_count(tbl)
            assert cnt == 0
            cols = emulator.get_table_columns(tbl)
            assert len(cols) > 0

    def test_load_seed_dimensions(self, emulator: LocalBigQueryEmulator) -> None:
        """Verify loading master seed dimensions populates tickers, portfolios, traders, and dates."""
        counts = emulator.load_seed_dimensions(
            date_start=date(2026, 1, 1), date_end=date(2026, 1, 31)
        )
        assert counts["dim_tickers"] == 17
        assert counts["dim_portfolios"] == 5
        assert counts["dim_traders"] == 5
        assert counts["dim_date"] == 31

        assert emulator.get_table_row_count("dim_tickers") == 17
        assert emulator.get_table_row_count("dim_portfolios") == 5
        assert emulator.get_table_row_count("dim_traders") == 5
        assert emulator.get_table_row_count("dim_date") == 31

    def test_insert_and_query_fact_trade_orders(self, emulator: LocalBigQueryEmulator) -> None:
        """Verify inserting synthetic trade orders and querying with partition filter."""
        orders = generate_mock_trade_orders(count=40, trade_date="2026-11-02")
        inserted = emulator.insert_rows("fact_trade_orders", orders)
        assert inserted == 40

        # Query with partition filter
        query = (
            f"SELECT portfolio_id, COUNT(*) AS num_orders, SUM(notional_usd) AS total_notional "
            f"FROM {emulator.dataset_id}.fact_trade_orders "
            f"WHERE trade_date = '2026-11-02' "
            f"GROUP BY portfolio_id ORDER BY total_notional DESC;"
        )
        rows = emulator.execute_query(query)
        assert len(rows) > 0
        assert sum(r["num_orders"] for r in rows) == 40
        assert all(r["total_notional"] > 0 for r in rows)

    def test_partition_filter_enforcement_raises_on_missing_filter(
        self, emulator: LocalBigQueryEmulator
    ) -> None:
        """Verify queries against require_partition_filter=True tables fail without WHERE partition filter."""
        # Missing WHERE clause entirely
        with pytest.raises(BigQueryPartitionFilterRequiredError, match="require_partition_filter = TRUE"):
            emulator.execute_query(f"SELECT * FROM {emulator.dataset_id}.fact_trade_orders;")

        # WHERE clause exists but does not filter on trade_date
        with pytest.raises(BigQueryPartitionFilterRequiredError, match="without a filter over partition column 'trade_date'"):
            emulator.execute_query(
                f"SELECT * FROM {emulator.dataset_id}.fact_trade_orders WHERE portfolio_id = 'PORTFOLIO_ALPHA';"
            )

        # fact_market_ticks missing tick_date
        with pytest.raises(BigQueryPartitionFilterRequiredError, match="partition column 'tick_date'"):
            emulator.execute_query(
                f"SELECT * FROM {emulator.dataset_id}.fact_market_ticks WHERE asset_class = 'EQUITY';"
            )

        # fact_risk_stats missing valuation_date
        with pytest.raises(BigQueryPartitionFilterRequiredError, match="partition column 'valuation_date'"):
            emulator.execute_query(
                f"SELECT * FROM {emulator.dataset_id}.fact_risk_stats WHERE risk_regime = 'LOW_RISK';"
            )

    def test_partition_filter_enforcement_passes_with_valid_filter(
        self, emulator: LocalBigQueryEmulator
    ) -> None:
        """Verify queries succeed when the partition column is present in the WHERE clause."""
        res = emulator.execute_query(
            f"SELECT COUNT(*) AS c FROM {emulator.dataset_id}.fact_trade_orders WHERE trade_date = '2026-11-02';"
        )
        assert res[0]["c"] == 0

    def test_star_schema_analytical_join_turnover_by_asset_class(
        self, emulator: LocalBigQueryEmulator
    ) -> None:
        """Verify star-schema analytical join between fact_trade_orders and dimensions."""
        emulator.load_seed_dimensions(date_start=date(2026, 11, 1), date_end=date(2026, 11, 5))
        orders = generate_mock_trade_orders(count=60, trade_date="2026-11-02")
        emulator.insert_rows("fact_trade_orders", orders)

        # Join fact_trade_orders with dim_tickers, dim_portfolios, dim_traders, dim_date
        query = f"""
            SELECT
                d.day_name,
                t.asset_class,
                p.desk,
                tr.role AS trader_role,
                COUNT(f.trade_order_id) AS exec_count,
                SUM(f.notional_usd) AS total_turnover_usd,
                SUM(f.fee_usd) AS total_fees_usd
            FROM {emulator.dataset_id}.fact_trade_orders f
            INNER JOIN {emulator.dataset_id}.dim_tickers t ON f.ticker = t.ticker
            INNER JOIN {emulator.dataset_id}.dim_portfolios p ON f.portfolio_id = p.portfolio_id
            INNER JOIN {emulator.dataset_id}.dim_traders tr ON f.trader_id = tr.trader_id
            INNER JOIN {emulator.dataset_id}.dim_date d ON f.date_key = d.date_key
            WHERE f.trade_date = '2026-11-02'
            GROUP BY d.day_name, t.asset_class, p.desk, tr.role
            ORDER BY total_turnover_usd DESC;
        """
        rows = emulator.execute_query(query)
        assert len(rows) > 0
        total_exec = sum(r["exec_count"] for r in rows)
        assert total_exec == 60
        assert all(r["day_name"] == "Monday" for r in rows)

    def test_star_schema_analytical_join_portfolio_risk_headroom(
        self, emulator: LocalBigQueryEmulator
    ) -> None:
        """Verify join between fact_risk_stats and dim_portfolios computing risk headroom."""
        emulator.load_seed_dimensions(date_start=date(2026, 11, 1), date_end=date(2026, 11, 5))
        stats = generate_mock_risk_stats(valuation_date="2026-11-02")
        emulator.insert_rows("fact_risk_stats", stats)

        query = f"""
            SELECT
                p.portfolio_name,
                p.desk,
                r.confidence_level,
                r.risk_regime,
                r.parametric_var_usd,
                p.max_var_limit_usd,
                (p.max_var_limit_usd - r.parametric_var_usd) AS var_headroom_usd,
                r.limit_utilization_pct
            FROM {emulator.dataset_id}.fact_risk_stats r
            INNER JOIN {emulator.dataset_id}.dim_portfolios p ON r.portfolio_id = p.portfolio_id
            WHERE r.valuation_date = '2026-11-02' AND r.confidence_level = 0.99
            ORDER BY r.limit_utilization_pct DESC;
        """
        rows = emulator.execute_query(query)
        assert len(rows) == 5
        for r in rows:
            assert r["parametric_var_usd"] > 0
            assert r["max_var_limit_usd"] > 0

    def test_arrow_table_query_execution(self, emulator: LocalBigQueryEmulator) -> None:
        """Verify execute_query_arrow returns an Apache PyArrow Table."""
        orders = generate_mock_trade_orders(count=20, trade_date="2026-11-02")
        emulator.insert_rows("fact_trade_orders", orders)

        arrow_table = emulator.execute_query_arrow(
            f"SELECT ticker, side, price, notional_usd FROM {emulator.dataset_id}.fact_trade_orders WHERE trade_date = '2026-11-02';"
        )
        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 20
        assert "ticker" in arrow_table.column_names

    def test_bigquery_telemetry_and_prometheus_metrics(
        self, emulator: LocalBigQueryEmulator
    ) -> None:
        """Verify BigQuery telemetry tracks query counts, errors, and Prometheus formatting."""
        # Successful query
        emulator.execute_query(
            f"SELECT 1 AS val WHERE 'trade_date' = 'trade_date';",
            bypass_partition_filter=True,
        )
        # Attempted query violating partition filter
        with pytest.raises(BigQueryPartitionFilterRequiredError):
            emulator.execute_query(f"SELECT * FROM {emulator.dataset_id}.fact_trade_orders;")

        prom = emulator.telemetry.format_prometheus()
        assert "bigquery_queries_executed_total" in prom
        assert "bigquery_partition_filter_violations_total 1" in prom


# =============================================================================
# 5. AIRFLOW INTEGRATION & REGISTRY TESTS
# =============================================================================

class TestBigQueryAirflowIntegration:
    """Validate BigQuery Airflow operator execution and master lakehouse DAG wiring."""

    def setup_method(self) -> None:
        BigQueryJobRegistry.reset()

    def test_bigquery_insert_job_operator_execution(self) -> None:
        """Verify BigQueryInsertJobOperator records job execution in registry."""
        op = BigQueryInsertJobOperator(
            task_id="test_bq_refresh",
            configuration={
                "query": {
                    "query": "SELECT * FROM fact_risk_stats WHERE valuation_date = '{{ ds }}'",
                    "useLegacySql": False,
                }
            },
            location="us-central1",
        )
        context = {"ds": "2026-11-02", "params": {}}
        job_id = op.execute(context)
        assert job_id.startswith("bq_job_test_bq_refresh_")

        record = BigQueryJobRegistry.get_job(job_id)
        assert record is not None
        assert record["status"] == "DONE"
        assert "2026-11-02" in record["configuration"]["query"]["query"]

    def test_bigquery_operator_injected_failure(self) -> None:
        """Verify BigQueryJobRegistry failure injection triggers AirflowException."""
        task_id = "failing_bq_task"
        BigQueryJobRegistry.inject_failure(
            task_id=task_id,
            exception=AirflowException("Simulated BigQuery quota exceeded"),
            num_failures=1,
        )

        op = BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={"query": {"query": "SELECT 1"}},
        )
        with pytest.raises(AirflowException, match="Simulated BigQuery quota exceeded"):
            op.execute({"ds": "2026-11-02"})

        # Second attempt succeeds
        job_id = op.execute({"ds": "2026-11-02"})
        assert job_id is not None

    def test_master_lakehouse_dag_includes_bigquery_refresh(self) -> None:
        """Verify master lakehouse batch DAG has refresh_bigquery_partitions wired."""
        dag = build_trading_risk_dag()
        task = dag.get_task("refresh_bigquery_partitions")
        assert isinstance(task, BigQueryInsertJobOperator)
        assert "fact_risk_stats" in task.configuration["query"]["query"]
        assert task.location == "us-central1"
        assert task.retries == 3

        # Upstream: Great Expectations data docs
        pub_docs = dag.get_task("publish_data_docs")
        assert task in pub_docs.downstream_list

        # Downstream: run_dbt
        run_dbt = dag.get_task("run_dbt_transforms")
        assert run_dbt in task.downstream_list


# =============================================================================
# 6. CLI DDL RUNNER TESTS
# =============================================================================

class TestBigQueryDDLCLIRunner:
    """Validate DDL CLI tool dry-run, validate-only, and local emulation execution."""

    def test_cli_runner_validate_only(self) -> None:
        """Verify CLI runner --validate_only executes and reports SUCCESS."""
        summary = run_ddl_tool(validate_only=True)
        assert summary["status"] == "SUCCESS"
        assert summary["validation"] == "PASSED"
        assert summary["mode"] == "validate_only"

    def test_cli_runner_dry_run(self) -> None:
        """Verify CLI runner --dry_run renders master DDL."""
        summary = run_ddl_tool(dry_run=True)
        assert summary["status"] == "SUCCESS"
        assert summary["mode"] == "dry_run"
        assert summary["rendered_ddl_bytes"] > 0
        assert len(summary["tables_to_create"]) == 8

    def test_cli_runner_local_emulator_with_seed(self, tmp_path: Path) -> None:
        """Verify CLI runner deploys to emulator, seeds dimensions, and writes JSON report."""
        report_file = tmp_path / "ddl_summary.json"
        summary = run_ddl_tool(
            local_emulator=True,
            seed_dimensions=True,
            output_json=str(report_file),
        )
        assert summary["status"] == "SUCCESS"
        assert summary["seed_counts"]["dim_tickers"] == 17
        assert summary["tables"]["dim_tickers"]["row_count"] == 17
        assert summary["tables"]["fact_trade_orders"]["column_count"] == 21
        assert report_file.is_file()
