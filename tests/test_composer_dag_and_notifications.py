"""Cloud Composer (Apache Airflow) DAG Scaffolding & Notification Hook Tests (Day 15).

Validates:
1. Cloud Composer Terraform Module:
   - Private environment configuration, zero-trust network attachment.
   - Least-privilege IAM service account (`sa-composer`) with Dataproc, GCS, BigQuery, and Cloud SQL roles.
   - Airflow software config overrides, PyPI packages, and environment variables.
   - Root Terraform wiring and dev/prod environment configurations.
2. Airflow Environment Configuration:
   - Typed dataclass configuration, default resolution, environment variable overrides, validation.
3. Airflow Compatibility Layer & DAG Scaffolding:
   - Pure-Python zero-cost emulation of DAG, BaseOperator, EmptyOperator, TaskGroup, chain, and cycle detection.
   - Canonical DAG topology: schedule, catchup=False, tags, SLA, TaskGroups, and end-to-end dependency chain.
4. GCP & Enterprise Connections Manager:
   - Generation of 6 canonical connections (`google_cloud_default`, `gcp_dataproc_default`,
     `gcp_cloudsql_postgres`, `databricks_default`, `slack_alerts_connection`, `pagerduty_alerts_connection`).
   - Airflow URI formatting, `AIRFLOW_CONN_*` environment variables, and connections JSON import format.
5. Slack Notification Hook:
   - Slack Block Kit message formatting with severity coloring, metadata fields, and action buttons.
   - Resilient HTTP transport with exponential backoff on HTTP 429 (rate limits) and 5xx errors.
6. PagerDuty Notification Hook:
   - Events API v2 payload formatting, deduplication keys, links, and retry dynamics.
7. Notification Dispatcher & Airflow Callbacks:
   - Severity-based routing policies (Slack for all, PagerDuty for CRITICAL).
   - `on_failure_callback`, `on_retry_callback`, `on_success_callback`, and `sla_miss_callback` execution.
"""

from datetime import datetime, timedelta
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Tuple
import pytest

from orchestration.airflow.airflow_compat import (
    AirflowDagCycleException,
    AirflowException,
    BaseOperator,
    DAG,
    EmptyOperator,
    MockTaskInstance,
    TaskGroup,
    chain,
    create_mock_task_context,
)
from orchestration.airflow.callbacks.failure_callbacks import (
    create_failure_callback,
    create_retry_callback,
    create_sla_miss_callback,
    create_success_callback,
)
from orchestration.airflow.config.airflow_config import AirflowEnvironmentConfig
from orchestration.airflow.connections.connection_manager import (
    AirflowConnection,
    GcpConnectionManager,
)
from orchestration.airflow.dags.trading_risk_daily_batch_dag import (
    build_trading_risk_dag,
    trading_risk_dag,
)
from orchestration.airflow.hooks.notification_dispatcher import (
    DispatcherTelemetry,
    NotificationDispatcher,
)
from orchestration.airflow.hooks.pagerduty_hook import (
    PagerDutyAction,
    PagerDutyHook,
    PagerDutySeverity,
)
from orchestration.airflow.hooks.slack_webhook_hook import (
    AlertSeverity,
    SlackNotificationHook,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_COMPOSER_DIR = REPO_ROOT / "terraform" / "modules" / "composer"
TF_ROOT_DIR = REPO_ROOT / "terraform"
TF_ENV_DIR = REPO_ROOT / "terraform" / "environments"


# =============================================================================
# 1. TERRAFORM CLOUD COMPOSER MODULE VALIDATION
# =============================================================================
class TestTerraformComposerModule:
    """Validate Cloud Composer 2 Terraform module configuration, security, and IAM."""

    def test_composer_module_files_exist(self) -> None:
        """Verify all essential composer Terraform module files exist."""
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf"]
        for filename in required:
            file_path = TF_COMPOSER_DIR / filename
            assert file_path.is_file(), f"Missing required file: {file_path}"
            assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

    def test_balanced_braces_in_composer_module(self) -> None:
        """Verify balanced HCL block braces in composer module."""
        for tf_file in TF_COMPOSER_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_file.name}"

    def test_root_terraform_wires_composer_module(self) -> None:
        """Verify root terraform/main.tf, variables.tf, and outputs.tf wire the composer module."""
        root_main = (TF_ROOT_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'module "composer"' in root_main
        assert 'source = "./modules/composer"' in root_main
        assert "subnetwork_id                   = module.network.data_services_subnet_id" in root_main

        root_vars = (TF_ROOT_DIR / "variables.tf").read_text(encoding="utf-8")
        assert 'variable "composer_image_version"' in root_vars
        assert 'variable "composer_environment_size"' in root_vars
        assert 'variable "composer_master_ipv4_cidr"' in root_vars

        root_outputs = (TF_ROOT_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert 'output "composer_environment_id"' in root_outputs
        assert 'output "composer_airflow_uri"' in root_outputs
        assert 'output "composer_dag_gcs_prefix"' in root_outputs
        assert 'output "composer_service_account_email"' in root_outputs

    def test_private_environment_config_enforced(self) -> None:
        """Verify Cloud Composer private IP environment configuration."""
        main_tf = (TF_COMPOSER_DIR / "main.tf").read_text(encoding="utf-8")
        assert "private_environment_config" in main_tf
        assert "enable_private_endpoint              = true" in main_tf
        assert "enable_privately_used_public_ips     = false" in main_tf
        assert "master_ipv4_cidr_block               = var.composer_master_ipv4_cidr" in main_tf
        assert "cloud_sql_ipv4_cidr_block            = var.composer_cloudsql_ipv4_cidr" in main_tf

    def test_shielded_and_tagged_node_config(self) -> None:
        """Verify node config attaches to correct subnetwork, service account, and tags."""
        main_tf = (TF_COMPOSER_DIR / "main.tf").read_text(encoding="utf-8")
        assert "node_config" in main_tf
        assert "network         = var.network_id" in main_tf
        assert "subnetwork      = var.subnetwork_id" in main_tf
        assert "service_account = google_service_account.composer_sa.email" in main_tf
        assert 'tags            = ["trading-risk", "composer", "airflow"]' in main_tf

    def test_iam_service_account_and_roles(self) -> None:
        """Verify dedicated least-privilege service account and IAM roles."""
        main_tf = (TF_COMPOSER_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'resource "google_service_account" "composer_sa"' in main_tf
        assert 'role    = "roles/composer.worker"' in main_tf
        assert 'role    = "roles/dataproc.editor"' in main_tf
        assert 'role    = "roles/storage.objectAdmin"' in main_tf
        assert 'role    = "roles/bigquery.jobUser"' in main_tf
        assert 'role    = "roles/bigquery.dataEditor"' in main_tf
        assert 'role    = "roles/secretmanager.secretAccessor"' in main_tf
        assert 'role    = "roles/cloudsql.client"' in main_tf

    def test_airflow_overrides_and_pypi_packages(self) -> None:
        """Verify essential Airflow config overrides and required PyPI packages."""
        main_tf = (TF_COMPOSER_DIR / "main.tf").read_text(encoding="utf-8")
        assert '"core-dags_are_paused_at_creation" = "true"' in main_tf
        assert '"core-load_examples"               = "false"' in main_tf
        assert '"core-max_active_tasks_per_dag"    = "16"' in main_tf
        assert "apache-airflow-providers-google" in main_tf
        assert "apache-airflow-providers-databricks" in main_tf
        assert "slack-sdk" in main_tf
        assert "pydantic" in main_tf

    def test_environment_tfvars_definitions(self) -> None:
        """Verify dev.tfvars and prod.tfvars define composer settings."""
        dev_vars = (TF_ENV_DIR / "dev.tfvars").read_text(encoding="utf-8")
        assert 'composer_environment_size       = "ENVIRONMENT_SIZE_SMALL"' in dev_vars
        assert 'composer_master_ipv4_cidr       = "172.16.0.0/28"' in dev_vars

        prod_vars = (TF_ENV_DIR / "prod.tfvars").read_text(encoding="utf-8")
        assert 'composer_environment_size       = "ENVIRONMENT_SIZE_MEDIUM"' in prod_vars
        assert 'composer_master_ipv4_cidr       = "172.16.1.0/28"' in prod_vars


# =============================================================================
# 2. AIRFLOW CONFIGURATION TESTS
# =============================================================================
class TestAirflowConfig:
    """Validate strongly typed AirflowEnvironmentConfig loader and rules."""

    def test_config_defaults(self) -> None:
        """Verify default configuration values."""
        cfg = AirflowEnvironmentConfig()
        assert cfg.environment == "dev"
        assert cfg.gcp_project_id == "trading-risk-dev-001"
        assert cfg.bronze_bucket == "trading-risk-bronze-dev"
        assert cfg.silver_bucket == "trading-risk-silver-dev"
        assert cfg.gold_bucket == "trading-risk-gold-dev"
        assert cfg.batch_sla_minutes == 60
        assert cfg.default_retries == 3
        assert cfg.default_retry_delay_seconds == 300

    def test_config_validation_rules(self) -> None:
        """Verify invalid settings raise ValueError."""
        with pytest.raises(ValueError, match="environment cannot be empty"):
            AirflowEnvironmentConfig(environment="")

        with pytest.raises(ValueError, match="gcp_project_id cannot be empty"):
            AirflowEnvironmentConfig(gcp_project_id="")

        with pytest.raises(ValueError, match="GCS bronze, silver, and gold buckets"):
            AirflowEnvironmentConfig(bronze_bucket="")

        with pytest.raises(ValueError, match="batch_sla_minutes must be positive"):
            AirflowEnvironmentConfig(batch_sla_minutes=0)

        with pytest.raises(ValueError, match="default_retries cannot be negative"):
            AirflowEnvironmentConfig(default_retries=-1)

    def test_config_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify environment variable overrides."""
        monkeypatch.setenv("AIRFLOW_VAR_ENVIRONMENT", "prod")
        monkeypatch.setenv("AIRFLOW_VAR_GCP_PROJECT", "trading-risk-prod-custom")
        monkeypatch.setenv("AIRFLOW_VAR_BATCH_SLA_MINUTES", "45")
        monkeypatch.setenv("AIRFLOW_VAR_DEFAULT_RETRIES", "5")

        cfg = AirflowEnvironmentConfig.from_env()
        assert cfg.environment == "prod"
        assert cfg.gcp_project_id == "trading-risk-prod-custom"
        assert cfg.batch_sla_minutes == 45
        assert cfg.default_retries == 5

    def test_config_to_dict(self) -> None:
        """Verify serialization to dictionary."""
        cfg = AirflowEnvironmentConfig()
        d = cfg.to_dict()
        assert isinstance(d, dict)
        assert d["environment"] == "dev"
        assert d["batch_sla_minutes"] == 60


# =============================================================================
# 3. AIRFLOW COMPATIBILITY LAYER & DAG SCENARIOS
# =============================================================================
class TestAirflowCompatAndDAG:
    """Validate Airflow compatibility layer, operators, task groups, and cycle detection."""

    def test_compat_empty_operator_init_and_properties(self) -> None:
        """Verify operator properties and default parameter propagation."""
        op = EmptyOperator(
            task_id="test_task",
            owner="risk-team",
            retries=4,
            retry_delay=timedelta(seconds=120),
            execution_timeout=timedelta(minutes=30),
        )
        assert op.task_id == "test_task"
        assert op.owner == "risk-team"
        assert op.retries == 4
        assert op.retry_delay == timedelta(seconds=120)
        assert op.execution_timeout == timedelta(minutes=30)
        assert op.execute({}) is None

    def test_compat_bitshift_linear_dependencies(self) -> None:
        """Verify linear dependency chaining via bitshift operator."""
        t1 = EmptyOperator(task_id="t1")
        t2 = EmptyOperator(task_id="t2")
        t3 = EmptyOperator(task_id="t3")

        t1 >> t2 >> t3

        assert t2.task_id in t1.downstream_task_ids
        assert t1.task_id in t2.upstream_task_ids
        assert t3.task_id in t2.downstream_task_ids
        assert t2.task_id in t3.upstream_task_ids

    def test_compat_bitshift_fanout_and_fanin(self) -> None:
        """Verify fanout and fanin list bitshift chaining."""
        start = EmptyOperator(task_id="start")
        branch_a = EmptyOperator(task_id="branch_a")
        branch_b = EmptyOperator(task_id="branch_b")
        end = EmptyOperator(task_id="end")

        start >> [branch_a, branch_b]
        branch_a >> end
        branch_b >> end

        assert "branch_a" in start.downstream_task_ids
        assert "branch_b" in start.downstream_task_ids
        assert "start" in branch_a.upstream_task_ids
        assert "start" in branch_b.upstream_task_ids
        assert "end" in branch_a.downstream_task_ids
        assert "end" in branch_b.downstream_task_ids
        assert "branch_a" in end.upstream_task_ids
        assert "branch_b" in end.upstream_task_ids

    def test_compat_task_group_nesting_and_roots_leaves(self) -> None:
        """Verify TaskGroup context manager, child tasks, roots, and leaves."""
        with TaskGroup("group_1") as tg:
            g_t1 = EmptyOperator(task_id="g_t1")
            g_t2 = EmptyOperator(task_id="g_t2")
            g_t1 >> g_t2

        assert len(tg.get_all_tasks()) == 2
        assert [t.task_id for t in tg.roots] == ["g_t1"]
        assert [t.task_id for t in tg.leaves] == ["g_t2"]

        # Connect task to group and group to task
        pre = EmptyOperator(task_id="pre")
        post = EmptyOperator(task_id="post")

        pre >> tg >> post

        assert "g_t1" in pre.downstream_task_ids
        assert "post" in g_t2.downstream_task_ids
        assert "g_t2" in post.upstream_task_ids

    def test_compat_dag_context_and_task_registration(self) -> None:
        """Verify DAG context manager automatically binds created tasks."""
        with DAG(dag_id="test_dag") as dag:
            t1 = EmptyOperator(task_id="t1")
            t2 = EmptyOperator(task_id="t2")
            t1 >> t2

        assert len(dag.tasks) == 2
        assert dag.has_task("t1")
        assert dag.has_task("t2")
        assert dag.get_task("t1") is t1
        assert dag.roots == [t1]
        assert dag.leaves == [t2]

    def test_compat_dag_cycle_detection(self) -> None:
        """Verify cycle detection identifies circular dependencies and raises exception."""
        dag = DAG(dag_id="cycle_dag")
        t1 = EmptyOperator(task_id="t1", dag=dag)
        t2 = EmptyOperator(task_id="t2", dag=dag)
        t3 = EmptyOperator(task_id="t3", dag=dag)

        t1 >> t2 >> t3 >> t1

        with pytest.raises(AirflowDagCycleException, match="Cycle detected in DAG 'cycle_dag'"):
            dag.validate()

    def test_compat_topological_sort(self) -> None:
        """Verify topological sort returns tasks in valid execution order."""
        with DAG(dag_id="sort_dag") as dag:
            t1 = EmptyOperator(task_id="t1")
            t2 = EmptyOperator(task_id="t2")
            t3 = EmptyOperator(task_id="t3")
            t1 >> t2 >> t3

        sorted_tasks = dag.topological_sort()
        task_ids = [t.task_id for t in sorted_tasks]
        assert task_ids == ["t1", "t2", "t3"]

    def test_canonical_trading_risk_dag_structure(self) -> None:
        """Verify production trading_risk_daily_batch_pipeline structure and task hierarchy."""
        dag = trading_risk_dag
        assert dag.dag_id == "trading_risk_daily_batch_pipeline"
        assert dag.schedule_interval == "0 1 * * *"
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert "market-risk" in dag.tags
        assert "lakehouse" in dag.tags
        assert dag.sla == timedelta(minutes=60)

        # Validate tasks presence
        expected_tasks = {
            "start_pipeline",
            "verify_bronze_partitions",
            "create_ephemeral_cluster",
            "submit_pyspark_job",
            "teardown_cluster",
            "execute_silver_cleansing",
            "execute_gold_var_calculation",
            "validate_silver_suite",
            "validate_gold_suite",
            "refresh_bigquery_partitions",
            "run_dbt_transforms",
            "publish_risk_report",
            "end_pipeline",
        }
        actual_tasks = set(dag.task_dict.keys())
        assert expected_tasks.issubset(actual_tasks)

        # Validate roots and leaves
        assert [t.task_id for t in dag.roots] == ["start_pipeline"]
        assert [t.task_id for t in dag.leaves] == ["end_pipeline"]

        # Validate topological sorting runs cleanly without cycles
        ordered = dag.topological_sort()
        assert len(ordered) == len(actual_tasks)
        assert ordered[0].task_id == "start_pipeline"
        assert ordered[-1].task_id == "end_pipeline"


# =============================================================================
# 4. GCP & ENTERPRISE CONNECTIONS MANAGER TESTS
# =============================================================================
class TestGcpConnectionManager:
    """Validate canonical Airflow connection generation and export formats."""

    def test_canonical_connections_created(self) -> None:
        """Verify the 6 canonical connections are generated with expected properties."""
        mgr = GcpConnectionManager()
        conns = mgr.get_all_connections()
        assert len(conns) == 6

        ids = [c.conn_id for c in conns]
        assert "google_cloud_default" in ids
        assert "gcp_dataproc_default" in ids
        assert "gcp_cloudsql_postgres" in ids
        assert "databricks_default" in ids
        assert "slack_alerts_connection" in ids
        assert "pagerduty_alerts_connection" in ids

    def test_connection_uri_generation(self) -> None:
        """Verify Airflow standard URI serialization."""
        conn = AirflowConnection(
            conn_id="test_postgres",
            conn_type="postgres",
            host="10.10.30.5",
            port=5432,
            schema="trading_risk_db",
            login="risk_user",
            password="secret_password",
            extra={"sslmode": "require"},
        )
        uri = conn.to_uri()
        assert uri.startswith("postgres://risk_user:secret_password@10.10.30.5:5432/trading_risk_db?")
        assert "sslmode" in uri

    def test_connection_env_var_generation(self) -> None:
        """Verify AIRFLOW_CONN_* environment variable mapping."""
        mgr = GcpConnectionManager()
        env_vars = mgr.generate_env_vars()
        assert "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT" in env_vars
        assert "AIRFLOW_CONN_GCP_DATAPROC_DEFAULT" in env_vars
        assert "AIRFLOW_CONN_GCP_CLOUDSQL_POSTGRES" in env_vars
        assert "AIRFLOW_CONN_DATABRICKS_DEFAULT" in env_vars
        assert "AIRFLOW_CONN_SLACK_ALERTS_CONNECTION" in env_vars
        assert "AIRFLOW_CONN_PAGERDUTY_ALERTS_CONNECTION" in env_vars

    def test_connection_cli_commands(self) -> None:
        """Verify CLI add command output."""
        mgr = GcpConnectionManager()
        conn = mgr.get_connection("google_cloud_default")
        cmd = conn.to_cli_command()
        assert cmd.startswith("airflow connections add 'google_cloud_default' --conn-uri 'google-cloud-platform://")

    def test_export_json_and_roundtrip(self, tmp_path: Path) -> None:
        """Verify JSON export matches Airflow connections import structure."""
        mgr = GcpConnectionManager()
        export_file = tmp_path / "connections.json"
        json_str = mgr.export_json(export_file)

        assert export_file.is_file()
        data = json.loads(json_str)
        assert "google_cloud_default" in data
        assert data["google_cloud_default"]["conn_type"] == "google-cloud-platform"
        assert "databricks_default" in data
        assert data["databricks_default"]["conn_type"] == "databricks"

    def test_export_cli_script_syntax(self) -> None:
        """Verify generated bash script is valid and non-empty."""
        mgr = GcpConnectionManager()
        script = mgr.export_cli_script()
        assert script.startswith("#!/usr/bin/env bash")
        assert "set -euo pipefail" in script
        assert "airflow connections add" in script

    def test_connection_manager_validate_all(self) -> None:
        """Verify validation passes for canonical connections and rejects invalid entries."""
        mgr = GcpConnectionManager()
        mgr.validate_all()

        # Register invalid connection
        bad_conn = AirflowConnection(conn_id="", conn_type="http")
        mgr.register(bad_conn)
        with pytest.raises(ValueError, match="Connection conn_id cannot be empty"):
            mgr.validate_all()


# =============================================================================
# 5. SLACK NOTIFICATION HOOK TESTS
# =============================================================================
class TestSlackNotificationHook:
    """Validate Slack Block Kit alert formatting and resilient HTTP delivery."""

    def test_block_kit_payload_structure(self) -> None:
        """Verify Block Kit payload sections, colors, emojis, and actions."""
        hook = SlackNotificationHook()
        payload = hook.build_block_kit_payload(
            severity=AlertSeverity.CRITICAL,
            title="PySpark Rollup Stage Failed",
            dag_id="trading_risk_daily_batch_pipeline",
            task_id="dataproc_batch_aggregation.submit_pyspark_job",
            execution_date="2026-10-23T01:00:00Z",
            environment="prod",
            try_number=3,
            max_tries=3,
            error_message="SparkException: Job aborted due to stage failure: Task 4 in stage 2.0 failed 4 times",
            log_url="https://composer.cloud.google.com/airflow/log?task_id=submit_pyspark_job",
            duration_sec=312.4,
        )

        assert payload["channel"] == "#risk-alerts"
        attachment = payload["attachments"][0]
        assert attachment["color"] == "#E01E5A"  # Red for critical

        blocks = attachment["blocks"]
        assert len(blocks) >= 3
        assert blocks[0]["type"] == "header"
        assert "[CRITICAL] PySpark Rollup Stage Failed" in blocks[0]["text"]["text"]

        # Check section fields
        fields = blocks[1]["fields"]
        field_texts = [f["text"] for f in fields]
        assert any("`PROD`" in t for t in field_texts)
        assert any("`3 of 3`" in t for t in field_texts)
        assert any("`312.4s`" in t for t in field_texts)

        # Check action button
        actions = blocks[-1]["elements"]
        assert actions[0]["text"]["text"] == "View Task Log"
        assert actions[0]["url"].startswith("https://composer.cloud.google.com")

    def test_block_kit_error_message_truncation(self) -> None:
        """Verify long error messages are truncated to stay within Slack block limits."""
        hook = SlackNotificationHook()
        massive_error = "E" * 5000
        payload = hook.build_block_kit_payload(
            severity=AlertSeverity.WARNING,
            title="Warning",
            dag_id="dag",
            task_id="task",
            execution_date="2026-10-23",
            error_message=massive_error,
        )
        error_block = payload["attachments"][0]["blocks"][2]
        assert "...[truncated]" in error_block["text"]["text"]
        assert len(error_block["text"]["text"]) < 3100

    def test_successful_notification_dispatch(self) -> None:
        """Verify 200 OK HTTP transport updates telemetry and returns True."""
        call_count = 0

        def mock_transport(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal call_count
            call_count += 1
            return 200, b"ok"

        hook = SlackNotificationHook(http_transport=mock_transport)
        success = hook.send_notification(
            severity=AlertSeverity.INFO,
            title="Pipeline Started",
            dag_id="dag",
            task_id="start",
            execution_date="2026-10-23",
        )

        assert success is True
        assert call_count == 1
        assert hook.telemetry.notifications_succeeded == 1
        assert hook.telemetry.notifications_failed == 0

    def test_transient_rate_limit_429_backoff_retry(self) -> None:
        """Verify HTTP 429 rate limit triggers backoff retry and succeeds."""
        attempts = 0

        def mock_transport(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                return 429, b"rate_limited"
            return 200, b"ok"

        hook = SlackNotificationHook(
            retry_delay_sec=0.01,
            http_transport=mock_transport,
        )
        success = hook.send_notification(
            severity=AlertSeverity.WARNING,
            title="Retry Test",
            dag_id="dag",
            task_id="task",
            execution_date="2026-10-23",
        )

        assert success is True
        assert attempts == 2
        assert hook.telemetry.retries_performed == 1
        assert hook.telemetry.notifications_succeeded == 1

    def test_transient_500_server_error_backoff_retry(self) -> None:
        """Verify HTTP 500 error triggers backoff retry."""
        attempts = 0

        def mock_transport(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                return 500, b"internal_server_error"
            return 200, b"ok"

        hook = SlackNotificationHook(
            retry_delay_sec=0.01,
            http_transport=mock_transport,
        )
        success = hook.send_notification(
            severity=AlertSeverity.CRITICAL,
            title="Server Error Retry",
            dag_id="dag",
            task_id="task",
            execution_date="2026-10-23",
        )

        assert success is True
        assert attempts == 3
        assert hook.telemetry.retries_performed == 2

    def test_permanent_400_client_error(self) -> None:
        """Verify HTTP 400 Bad Request fails fast without retries."""
        attempts = 0

        def mock_transport(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal attempts
            attempts += 1
            return 400, b"invalid_payload"

        hook = SlackNotificationHook(
            max_retries=3,
            retry_delay_sec=0.01,
            http_transport=mock_transport,
        )
        success = hook.send_notification(
            severity=AlertSeverity.CRITICAL,
            title="Client Error",
            dag_id="dag",
            task_id="task",
            execution_date="2026-10-23",
        )

        assert success is False
        assert attempts == 1  # No retry on 400
        assert hook.telemetry.notifications_failed == 1

    def test_exhausted_retries(self) -> None:
        """Verify exhausting all retries records failure in telemetry."""
        attempts = 0

        def mock_transport(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal attempts
            attempts += 1
            return 503, b"service_unavailable"

        hook = SlackNotificationHook(
            max_retries=2,
            retry_delay_sec=0.01,
            http_transport=mock_transport,
        )
        success = hook.send_notification(
            severity=AlertSeverity.CRITICAL,
            title="Service Down",
            dag_id="dag",
            task_id="task",
            execution_date="2026-10-23",
        )

        assert success is False
        assert attempts == 3  # initial + 2 retries
        assert hook.telemetry.notifications_failed == 1


# =============================================================================
# 6. PAGERDUTY NOTIFICATION HOOK TESTS
# =============================================================================
class TestPagerDutyHook:
    """Validate PagerDuty Events API v2 payload formatting and escalation."""

    def test_pagerduty_payload_structure(self) -> None:
        """Verify PagerDuty event payload attributes and deduplication key."""
        hook = PagerDutyHook(routing_key="pd-test-key")
        payload = hook.build_event_payload(
            action=PagerDutyAction.TRIGGER,
            summary="[CRITICAL] Task failed: execute_gold_var_calculation",
            severity=PagerDutySeverity.CRITICAL,
            dag_id="trading_risk_daily_batch_pipeline",
            task_id="databricks_medallion.execute_gold_var_calculation",
            execution_date="2026-10-23T01:00:00Z",
            environment="prod",
            error_message="VaRCalculationException: Covariance matrix non-positive semidefinite",
            log_url="https://composer.cloud.google.com/airflow/log?task_id=gold_var",
        )

        assert payload["routing_key"] == "pd-test-key"
        assert payload["event_action"] == "trigger"
        assert payload["dedup_key"] == "trading-risk/prod/trading_risk_daily_batch_pipeline/databricks_medallion.execute_gold_var_calculation"

        p_body = payload["payload"]
        assert p_body["severity"] == "critical"
        assert p_body["source"] == "cloud-composer-prod"
        assert p_body["custom_details"]["error_message"].startswith("VaRCalculationException")
        assert payload["links"][0]["href"] == "https://composer.cloud.google.com/airflow/log?task_id=gold_var"

    def test_successful_event_dispatch(self) -> None:
        """Verify HTTP 202 Accepted dispatches cleanly."""
        call_count = 0

        def mock_transport(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal call_count
            call_count += 1
            return 202, b'{"status": "success", "message": "Event processed"}'

        hook = PagerDutyHook(http_transport=mock_transport)
        success = hook.send_event(
            action=PagerDutyAction.TRIGGER,
            summary="Test Trigger",
            severity=PagerDutySeverity.ERROR,
            dag_id="dag",
            task_id="task",
            execution_date="2026-10-23",
        )

        assert success is True
        assert call_count == 1
        assert hook.telemetry.events_succeeded == 1

    def test_pagerduty_transient_failure_backoff_retry(self) -> None:
        """Verify PagerDuty transient 503 triggers retry and succeeds."""
        attempts = 0

        def mock_transport(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                return 503, b"Service Unavailable"
            return 200, b'{"status": "success"}'

        hook = PagerDutyHook(
            retry_delay_sec=0.01,
            http_transport=mock_transport,
        )
        success = hook.send_event(
            action=PagerDutyAction.TRIGGER,
            summary="Transient Retry Test",
            severity=PagerDutySeverity.CRITICAL,
            dag_id="dag",
            task_id="task",
            execution_date="2026-10-23",
        )

        assert success is True
        assert attempts == 2
        assert hook.telemetry.retries_performed == 1


# =============================================================================
# 7. NOTIFICATION DISPATCHER & CALLBACK TESTS
# =============================================================================
class TestNotificationDispatcherAndCallbacks:
    """Validate unified notification dispatcher routing and Airflow callback functions."""

    def test_dispatcher_severity_routing_warning(self) -> None:
        """Verify WARNING severity routes to Slack and does NOT trigger PagerDuty."""
        slack_calls = 0
        pd_calls = 0

        def mock_slack(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal slack_calls
            slack_calls += 1
            return 200, b"ok"

        def mock_pd(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal pd_calls
            pd_calls += 1
            return 200, b"ok"

        dispatcher = NotificationDispatcher(
            slack_hook=SlackNotificationHook(http_transport=mock_slack),
            pagerduty_hook=PagerDutyHook(http_transport=mock_pd),
        )

        res = dispatcher.dispatch(
            severity=AlertSeverity.WARNING,
            title="Transient Warning",
            dag_id="test_dag",
            task_id="test_task",
            execution_date="2026-10-23T01:00:00Z",
        )

        assert res["slack"] is True
        assert res["pagerduty"] is False
        assert slack_calls == 1
        assert pd_calls == 0

    def test_dispatcher_severity_routing_critical(self) -> None:
        """Verify CRITICAL severity routes to Slack AND triggers PagerDuty."""
        slack_calls = 0
        pd_calls = 0

        def mock_slack(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal slack_calls
            slack_calls += 1
            return 200, b"ok"

        def mock_pd(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
            nonlocal pd_calls
            pd_calls += 1
            return 202, b'{"status": "success"}'

        dispatcher = NotificationDispatcher(
            slack_hook=SlackNotificationHook(http_transport=mock_slack),
            pagerduty_hook=PagerDutyHook(http_transport=mock_pd),
        )

        res = dispatcher.dispatch(
            severity=AlertSeverity.CRITICAL,
            title="Fatal Error",
            dag_id="test_dag",
            task_id="test_task",
            execution_date="2026-10-23T01:00:00Z",
            error_message="OutOfMemoryError: Java heap space",
        )

        assert res["slack"] is True
        assert res["pagerduty"] is True
        assert slack_calls == 1
        assert pd_calls == 1

    def test_on_failure_callback_with_retries_remaining(self) -> None:
        """Verify on_failure_callback emits WARNING when retries are still remaining."""
        dispatched_severities: List[AlertSeverity] = []

        class MockDispatcher(NotificationDispatcher):
            def dispatch(self, severity: AlertSeverity, *args: Any, **kwargs: Any) -> Dict[str, bool]:
                dispatched_severities.append(severity)
                return {"slack": True, "pagerduty": False}

        dispatcher = MockDispatcher()
        callback = create_failure_callback(dispatcher)

        # Context where try_number = 1, max_tries = 4
        dag = DAG(dag_id="test_dag")
        task = EmptyOperator(task_id="test_task", dag=dag, retries=3)
        context = create_mock_task_context(
            dag=dag,
            task=task,
            try_number=1,
            exception=RuntimeError("Transient network blip"),
        )

        callback(context)
        assert dispatched_severities == [AlertSeverity.WARNING]

    def test_on_failure_callback_retries_exhausted(self) -> None:
        """Verify on_failure_callback emits CRITICAL when retries are exhausted."""
        dispatched_severities: List[AlertSeverity] = []

        class MockDispatcher(NotificationDispatcher):
            def dispatch(self, severity: AlertSeverity, *args: Any, **kwargs: Any) -> Dict[str, bool]:
                dispatched_severities.append(severity)
                return {"slack": True, "pagerduty": True}

        dispatcher = MockDispatcher()
        callback = create_failure_callback(dispatcher)

        # Context where try_number = 4, max_tries = 4 (exhausted!)
        dag = DAG(dag_id="test_dag")
        task = EmptyOperator(task_id="test_task", dag=dag, retries=3)
        context = create_mock_task_context(
            dag=dag,
            task=task,
            try_number=4,
            exception=RuntimeError("Persistent Dataproc error"),
        )

        callback(context)
        assert dispatched_severities == [AlertSeverity.CRITICAL]

    def test_on_retry_callback(self) -> None:
        """Verify on_retry_callback emits WARNING alert."""
        dispatched_severities: List[AlertSeverity] = []

        class MockDispatcher(NotificationDispatcher):
            def dispatch(self, severity: AlertSeverity, *args: Any, **kwargs: Any) -> Dict[str, bool]:
                dispatched_severities.append(severity)
                return {"slack": True, "pagerduty": False}

        dispatcher = MockDispatcher()
        callback = create_retry_callback(dispatcher)

        dag = DAG(dag_id="test_dag")
        task = EmptyOperator(task_id="test_task", dag=dag, retries=3)
        context = create_mock_task_context(
            dag=dag,
            task=task,
            try_number=2,
            exception=ConnectionResetError("Connection reset by peer"),
        )

        callback(context)
        assert dispatched_severities == [AlertSeverity.WARNING]

    def test_on_success_callback(self) -> None:
        """Verify on_success_callback emits SUCCESS notification."""
        dispatched_severities: List[AlertSeverity] = []

        class MockDispatcher(NotificationDispatcher):
            def dispatch(self, severity: AlertSeverity, *args: Any, **kwargs: Any) -> Dict[str, bool]:
                dispatched_severities.append(severity)
                return {"slack": True, "pagerduty": False}

        dispatcher = MockDispatcher()
        callback = create_success_callback(dispatcher)

        dag = DAG(dag_id="test_dag")
        task = EmptyOperator(task_id="publish_risk_report", dag=dag)
        context = create_mock_task_context(dag=dag, task=task, state="success")

        callback(context)
        assert dispatched_severities == [AlertSeverity.SUCCESS]

    def test_sla_miss_callback(self) -> None:
        """Verify sla_miss_callback emits CRITICAL alert to Slack and PagerDuty."""
        dispatched_severities: List[AlertSeverity] = []

        class MockDispatcher(NotificationDispatcher):
            def dispatch(self, severity: AlertSeverity, *args: Any, **kwargs: Any) -> Dict[str, bool]:
                dispatched_severities.append(severity)
                return {"slack": True, "pagerduty": True}

        dispatcher = MockDispatcher()
        callback = create_sla_miss_callback(dispatcher)

        dag = DAG(dag_id="trading_risk_daily_batch_pipeline")
        callback(
            dag=dag,
            task_list="submit_pyspark_job",
            blocking_task_list="submit_pyspark_job",
            slas=["SLA(60 min)"],
            blocking_tis=["<TI: submit_pyspark_job>"],
        )

        assert dispatched_severities == [AlertSeverity.CRITICAL]
