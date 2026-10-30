"""Unit and Integration Tests for Databricks Gold Pipeline Airflow Orchestration (Day 20).

Validates:
1. Databricks Gold Lakehouse & Task Retry Configuration:
   - Invariant validation, default resolution, environment variable overrides.
   - Autoscaling cluster specifications with Spot executor cost optimization and Delta Lake configs.
   - Task submit payloads for Silver Cleansing, Gold VaR, and Delta Table Maintenance.
   - Run-now parameter formatting and existing cluster ID routing.
2. Databricks Airflow Compatibility Operators & In-Memory State Registry:
   - Thread-safe lifecycle transitions in `DatabricksJobStateRegistry`.
   - `DatabricksSubmitRunOperator` execution, template rendering (`{{ ds }}`), and return codes.
   - `DatabricksRunNowOperator` execution, validation, and parameter rendering.
   - Automated retry logic on transient cloud errors (HTTP 429 rate limit, Spot worker preemption).
   - Execution attempt tracking and escalation via retry and failure callbacks.
3. Standalone Databricks Gold Orchestration DAG (`databricks_gold_pipeline`):
   - DAG topology, schedule, SLA, tags, and acyclic topological ordering.
   - TaskGroup hierarchies (`silver_lakehouse_cleansing`, `gold_lakehouse_var_and_features`).
   - Default args enforcement (retries, exponential backoff, callbacks).
4. Master Lakehouse DAG Integration (`trading_risk_daily_batch_pipeline`):
   - Verification that `databricks_medallion_processing` TaskGroup uses production `DatabricksSubmitRunOperator` instances.
   - Upstream dependency on Dataproc batch aggregation and downstream dependency on Data Quality gates.
   - Master DAG topological sort and cycle-free validation.
5. End-to-End Simulation & Spot Preemption Recovery:
   - End-to-end multi-task execution through Silver Cleansing, Gold VaR, Feature Store, and Maintenance.
   - Simulation of Spot instance preemption during Gold VaR calculation with automated recovery.
"""

from datetime import datetime, timedelta
import json
import os
from typing import Any, Dict, List
import pytest

from orchestration.airflow.airflow_compat import (
    AirflowException,
    DatabricksJobStateRegistry,
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
    TriggerRule,
    create_mock_task_context,
)
from orchestration.airflow.callbacks.failure_callbacks import (
    create_failure_callback,
    create_retry_callback,
)
from orchestration.airflow.config.airflow_config import AirflowEnvironmentConfig
from orchestration.airflow.config.databricks_job_config import (
    DatabricksGoldJobConfig,
    DatabricksTaskRetryConfig,
)
from orchestration.airflow.dags.databricks_gold_dag import (
    build_databricks_gold_dag,
    databricks_gold_dag,
)
from orchestration.airflow.dags.trading_risk_daily_batch_dag import (
    build_trading_risk_dag,
    trading_risk_dag,
)
from orchestration.airflow.hooks.notification_dispatcher import NotificationDispatcher
from orchestration.airflow.hooks.slack_webhook_hook import AlertSeverity, SlackNotificationHook


@pytest.fixture(autouse=True)
def reset_databricks_registry() -> None:
    """Reset the Databricks mock registry before each test."""
    DatabricksJobStateRegistry.reset()
    yield
    DatabricksJobStateRegistry.reset()


# =============================================================================
# 1. DATABRICKS GOLD CONFIGURATION & RETRY POLICY TESTS
# =============================================================================
class TestDatabricksGoldJobConfig:
    """Validate Databricks Gold pipeline and retry configuration builders."""

    def test_default_config_resolution(self) -> None:
        """Verify default configuration parameters."""
        cfg = DatabricksGoldJobConfig()
        assert cfg.environment == "dev"
        assert cfg.gcp_project_id == "trading-risk-dev-001"
        assert cfg.gcp_region == "us-central1"
        assert cfg.workspace_url == "https://trading-risk-dev.cloud.databricks.com"
        assert cfg.cluster_name == "tre-databricks-bronze-dev"
        assert cfg.spark_version == "15.4.x-scala2.12"
        assert cfg.min_workers == 2
        assert cfg.max_workers == 8
        assert cfg.use_spot_executors is True
        assert cfg.monte_carlo_simulations == 10000
        assert cfg.var_limit_usd == 250000.0
        assert cfg.horizon_days == 1
        assert cfg.confidence_levels == "0.95,0.99"
        assert cfg.retry_config.retries == 3
        assert cfg.retry_config.retry_delay_seconds == 60
        assert cfg.retry_config.retry_exponential_backoff is True
        assert cfg.retry_config.max_retry_delay_seconds == 300

    def test_config_validation_rules(self) -> None:
        """Verify configuration validation invariants."""
        # Invalid workers
        with pytest.raises(ValueError, match="min_workers must be at least 1"):
            DatabricksGoldJobConfig(min_workers=0)

        with pytest.raises(ValueError, match="max_workers must be >= min_workers"):
            DatabricksGoldJobConfig(min_workers=5, max_workers=3)

        # Invalid simulations
        with pytest.raises(ValueError, match="monte_carlo_simulations must be positive"):
            DatabricksGoldJobConfig(monte_carlo_simulations=0)

        # Invalid VaR limit
        with pytest.raises(ValueError, match="var_limit_usd must be positive"):
            DatabricksGoldJobConfig(var_limit_usd=-100.0)

        # Invalid horizon
        with pytest.raises(ValueError, match="horizon_days must be positive"):
            DatabricksGoldJobConfig(horizon_days=0)

        # Invalid confidence levels
        with pytest.raises(ValueError, match="Confidence level"):
            DatabricksGoldJobConfig(confidence_levels="0.95,1.5")

        with pytest.raises(ValueError, match="Invalid confidence level specification"):
            DatabricksGoldJobConfig(confidence_levels="0.95,invalid")

        # Invalid retry config
        with pytest.raises(ValueError, match="retries cannot be negative"):
            DatabricksTaskRetryConfig(retries=-1).validate()

        with pytest.raises(ValueError, match="retry_delay_seconds must be positive"):
            DatabricksTaskRetryConfig(retry_delay_seconds=0).validate()

        with pytest.raises(ValueError, match="max_retry_delay_seconds must be >= retry_delay_seconds"):
            DatabricksTaskRetryConfig(retry_delay_seconds=120, max_retry_delay_seconds=60).validate()

    def test_config_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify construction from environment / Airflow variables."""
        monkeypatch.setenv("AIRFLOW_VAR_ENVIRONMENT", "prod")
        monkeypatch.setenv("AIRFLOW_VAR_GCP_PROJECT", "trading-risk-prod-999")
        monkeypatch.setenv("AIRFLOW_VAR_VAR_MONTE_CARLO_SIMS", "25000")
        monkeypatch.setenv("AIRFLOW_VAR_VAR_LIMIT_USD", "500000.0")
        monkeypatch.setenv("AIRFLOW_VAR_DATABRICKS_RETRIES", "5")
        monkeypatch.setenv("AIRFLOW_VAR_DATABRICKS_CLUSTER_ID", "1031-cluster-prod")

        cfg = DatabricksGoldJobConfig.from_env()
        assert cfg.environment == "prod"
        assert cfg.gcp_project_id == "trading-risk-prod-999"
        assert cfg.monte_carlo_simulations == 25000
        assert cfg.var_limit_usd == 500000.0
        assert cfg.retry_config.retries == 5
        assert cfg.existing_cluster_id == "1031-cluster-prod"

    def test_cluster_spec_dict_structure(self) -> None:
        """Verify new_cluster specification matches Terraform and Delta Lake requirements."""
        cfg = DatabricksGoldJobConfig(environment="dev")
        spec = cfg.to_cluster_spec_dict()

        assert spec["spark_version"] == "15.4.x-scala2.12"
        assert spec["node_type_id"] == "n2-standard-4"
        assert spec["autoscale"]["min_workers"] == 2
        assert spec["autoscale"]["max_workers"] == 8
        assert spec["autotermination_minutes"] == 20
        assert spec["gcp_attributes"]["use_preemptible_executors"] is True
        assert spec["gcp_attributes"]["availability"] == "SPOT_WITH_FALLBACK_GCP"

        spark_conf = spec["spark_conf"]
        assert spark_conf["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
        assert spark_conf["spark.sql.catalog.spark_catalog"] == "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        assert spark_conf["spark.databricks.delta.optimizeWrite.enabled"] == "true"
        assert spark_conf["spark.databricks.delta.autoCompact.enabled"] == "true"
        assert "GoogleHadoopFileSystem" in spark_conf["spark.hadoop.fs.gs.impl"]

    def test_silver_submit_run_dict(self) -> None:
        """Verify Silver Cleansing submit payload structure."""
        cfg = DatabricksGoldJobConfig(environment="dev")
        payload = cfg.to_silver_submit_run_dict(target_date="2026-10-31")

        assert "tre-silver-cleansing-dev-2026-10-31" in payload["run_name"]
        assert len(payload["tasks"]) == 1
        task = payload["tasks"][0]
        assert task["task_key"] == "cleanse_enforce_and_deduplicate_to_silver"
        assert task["max_retries"] == 3
        assert task["min_retry_interval_millis"] == 60000
        assert task["retry_on_timeout"] is True
        assert "new_cluster" in task

        base_params = task["notebook_task"]["base_parameters"]
        assert base_params["environment"] == "dev"
        assert base_params["source_type"] == "all"
        assert base_params["ingestion_date"] == "2026-10-31"
        assert base_params["catalog_name"] == "trading_risk"
        assert base_params["silver_schema_name"] == "silver"

    def test_gold_var_submit_dict(self) -> None:
        """Verify Gold VaR submit payload structure."""
        cfg = DatabricksGoldJobConfig(environment="dev", monte_carlo_simulations=5000)
        payload = cfg.to_gold_var_submit_dict(target_date="2026-10-31")

        assert "tre-gold-var-feature-store-dev-2026-10-31" in payload["run_name"]
        task = payload["tasks"][0]
        assert task["task_key"] == "compute_portfolio_var_and_feature_store"
        assert task["max_retries"] == 3

        base_params = task["notebook_task"]["base_parameters"]
        assert base_params["valuation_date"] == "2026-10-31"
        assert base_params["monte_carlo_simulations"] == "5000"
        assert base_params["confidence_levels"] == "0.95,0.99"
        assert base_params["var_limit_usd"] == "250000.0"
        assert base_params["orders_source_path"] == "trading_risk.silver.silver_trade_orders"
        assert base_params["var_target_path"] == "trading_risk.gold.gold_portfolio_var"

    def test_existing_cluster_routing(self) -> None:
        """Verify that existing cluster ID takes precedence over new_cluster."""
        cfg = DatabricksGoldJobConfig(existing_cluster_id="cluster-existing-123")
        payload = cfg.to_gold_var_submit_dict(target_date="2026-10-31")

        task = payload["tasks"][0]
        assert task["existing_cluster_id"] == "cluster-existing-123"
        assert "new_cluster" not in task

    def test_run_now_parameters_formatting(self) -> None:
        """Verify run-now parameter dictionaries for Silver and Gold jobs."""
        cfg = DatabricksGoldJobConfig(environment="dev")
        silver_params = cfg.to_silver_run_now_params(target_date="2026-10-31")
        assert silver_params["ingestion_date"] == "2026-10-31"
        assert silver_params["source_type"] == "all"

        gold_params = cfg.to_gold_var_run_now_params(target_date="2026-10-31")
        assert gold_params["valuation_date"] == "2026-10-31"
        assert gold_params["monte_carlo_simulations"] == "10000"


# =============================================================================
# 2. DATABRICKS AIRFLOW OPERATORS & IN-MEMORY REGISTRY TESTS
# =============================================================================
class TestDatabricksAirflowOperators:
    """Validate DatabricksSubmitRunOperator, DatabricksRunNowOperator, and registry."""

    def test_registry_lifecycle_and_reset(self) -> None:
        """Verify submit_run records in DatabricksJobStateRegistry."""
        spec = {"run_name": "test_run", "tasks": [{"task_key": "t1"}]}
        res = DatabricksJobStateRegistry.submit_run(spec, caller_task_id="task_1")

        assert res["run_id"] == 7001
        assert res["state"]["life_cycle_state"] == "TERMINATED"
        assert res["state"]["result_state"] == "SUCCESS"
        assert DatabricksJobStateRegistry.get_attempt_count("task_1") == 1

        run = DatabricksJobStateRegistry.get_run(7001)
        assert run is not None
        assert run["run_name"] == "test_run"

        DatabricksJobStateRegistry.reset()
        assert DatabricksJobStateRegistry.get_run(7001) is None
        assert DatabricksJobStateRegistry.get_attempt_count("task_1") == 0

    def test_submit_run_operator_successful_execution(self) -> None:
        """Verify DatabricksSubmitRunOperator execution."""
        op = DatabricksSubmitRunOperator(
            task_id="test_submit_op",
            json={"run_name": "daily_gold_var", "tasks": [{"task_key": "gold_task"}]},
            databricks_conn_id="databricks_default",
        )
        context = create_mock_task_context(dag=None, task=op)
        run_id = op.execute(context)

        assert isinstance(run_id, int)
        assert run_id >= 7001
        assert DatabricksJobStateRegistry.get_attempt_count("test_submit_op") == 1

    def test_submit_run_operator_jinja_rendering(self) -> None:
        """Verify Jinja template parameter expansion in notebook parameters and run name."""
        op = DatabricksSubmitRunOperator(
            task_id="jinja_test_op",
            json={
                "run_name": "gold_var_{{ ds_nodash }}_attempt_{{ task_instance.try_number }}",
                "tasks": [
                    {
                        "task_key": "var_task",
                        "notebook_task": {
                            "notebook_path": "/Workspace/var_runner",
                            "base_parameters": {
                                "valuation_date": "{{ ds }}",
                            },
                        },
                    }
                ],
            },
        )
        test_date = datetime(2026, 10, 31)
        context = create_mock_task_context(dag=None, task=op, execution_date=test_date, try_number=2)
        run_id = op.execute(context)

        run = DatabricksJobStateRegistry.get_run(run_id)
        assert run is not None
        assert run["run_name"] == "gold_var_20261031_attempt_2"
        task = run["spec"]["tasks"][0]
        assert task["notebook_task"]["base_parameters"]["valuation_date"] == "2026-10-31"

    def test_run_now_operator_execution(self) -> None:
        """Verify DatabricksRunNowOperator execution."""
        op = DatabricksRunNowOperator(
            task_id="test_run_now_op",
            job_id=1055,
            notebook_params={"valuation_date": "2026-10-31"},
        )
        context = create_mock_task_context(dag=None, task=op)
        run_id = op.execute(context)

        assert isinstance(run_id, int)
        run = DatabricksJobStateRegistry.get_run(run_id)
        assert run is not None
        assert run["job_id"] == 1055
        assert run["parameters"]["valuation_date"] == "2026-10-31"

    def test_run_now_operator_requires_job_id_or_name(self) -> None:
        """Verify error raised when neither job_id nor job_name is provided."""
        op = DatabricksRunNowOperator(task_id="invalid_op")
        with pytest.raises(AirflowException, match="Either job_id or job_name must be provided"):
            op.execute({})

    def test_automated_retry_logic_on_transient_failure(self) -> None:
        """Test automated retry logic recovering from transient failures (e.g. rate limit, spot preemption)."""
        retries_recorded: List[Dict[str, Any]] = []

        def mock_retry_cb(context: Dict[str, Any]) -> None:
            retries_recorded.append(context)

        op = DatabricksSubmitRunOperator(
            task_id="transient_retry_task",
            json={"run_name": "transient_test"},
            retries=3,
            retry_delay=timedelta(seconds=1),
            retry_exponential_backoff=True,
            on_retry_callback=mock_retry_cb,
        )

        # Inject 2 consecutive transient failures (e.g. HTTP 429 Too Many Requests)
        transient_error = AirflowException("Databricks API HTTP 429: Rate limit exceeded, retrying...")
        DatabricksJobStateRegistry.inject_failure("transient_retry_task", transient_error, fail_count=2)

        # Attempt 1: Should fail with transient_error
        ctx1 = create_mock_task_context(dag=None, task=op, try_number=1, exception=transient_error)
        with pytest.raises(AirflowException, match="Rate limit exceeded"):
            op.execute(ctx1)
        # Airflow worker calls on_retry_callback
        mock_retry_cb(ctx1)

        # Attempt 2: Should fail with transient_error again
        ctx2 = create_mock_task_context(dag=None, task=op, try_number=2, exception=transient_error)
        with pytest.raises(AirflowException, match="Rate limit exceeded"):
            op.execute(ctx2)
        mock_retry_cb(ctx2)

        # Attempt 3: Failures exhausted -> should now SUCCEED cleanly!
        ctx3 = create_mock_task_context(dag=None, task=op, try_number=3)
        run_id = op.execute(ctx3)
        assert run_id >= 7001

        # Total attempts must be 3
        assert DatabricksJobStateRegistry.get_attempt_count("transient_retry_task") == 3
        assert len(retries_recorded) == 2

    def test_permanent_failure_exhausts_retries_and_triggers_failure_callback(self) -> None:
        """Test that permanent failure exhausts max retries and triggers failure callback."""
        failures_recorded: List[Dict[str, Any]] = []

        def mock_failure_cb(context: Dict[str, Any]) -> None:
            failures_recorded.append(context)

        op = DatabricksSubmitRunOperator(
            task_id="permanent_failure_task",
            json={"run_name": "permanent_fail_test"},
            retries=2,
            on_failure_callback=mock_failure_cb,
        )

        perm_error = AirflowException("Databricks Notebook Execution Error: ZeroDivisionError in Portfolio VaR")
        # Injected failure set to 10 fails (exceeds retries=2)
        DatabricksJobStateRegistry.inject_failure("permanent_failure_task", perm_error, fail_count=10)

        max_tries = (op.retries or 0) + 1  # 3 total tries
        for attempt in range(1, max_tries + 1):
            ctx = create_mock_task_context(
                dag=None,
                task=op,
                try_number=attempt,
                exception=perm_error,
            )
            with pytest.raises(AirflowException, match="ZeroDivisionError"):
                op.execute(ctx)

            if attempt == max_tries:
                mock_failure_cb(ctx)

        assert DatabricksJobStateRegistry.get_attempt_count("permanent_failure_task") == 3
        assert len(failures_recorded) == 1
        assert "ZeroDivisionError" in str(failures_recorded[0]["exception"])


# =============================================================================
# 3. STANDALONE DATABRICKS GOLD DAG TESTS
# =============================================================================
class TestDatabricksGoldDAG:
    """Validate standalone Databricks Gold orchestration DAG."""

    def test_dag_metadata_and_properties(self) -> None:
        """Verify DAG ID, description, schedule, SLA, and tags."""
        dag = build_databricks_gold_dag()

        assert dag.dag_id == "databricks_gold_pipeline"
        assert dag.schedule_interval == "30 1 * * *"
        assert dag.max_active_runs == 1
        assert dag.catchup is False
        assert "market-risk" in dag.tags
        assert "databricks" in dag.tags
        assert "gold" in dag.tags
        assert dag.sla == timedelta(minutes=60)

    def test_dag_default_args_retry_policy(self) -> None:
        """Verify that DAG default_args enforce automated retries and callbacks."""
        dag = build_databricks_gold_dag()

        assert dag.default_args["retries"] == 3
        assert dag.default_args["retry_delay"] == timedelta(seconds=60)
        assert dag.default_args["retry_exponential_backoff"] is True
        assert dag.default_args["max_retry_delay"] == timedelta(seconds=300)
        assert dag.default_args["on_failure_callback"] is not None
        assert dag.default_args["on_retry_callback"] is not None

    def test_dag_task_existence_and_types(self) -> None:
        """Verify all expected operators and task groups exist in the DAG."""
        dag = build_databricks_gold_dag()
        task_ids = set(dag.task_ids)

        expected_tasks = {
            "start_gold_pipeline",
            "verify_silver_readiness",
            "cleanse_silver_medallion",
            "compute_gold_portfolio_var",
            "publish_gold_risk_features",
            "optimize_and_vacuum_gold_tables",
            "notify_gold_completion",
        }
        for task_id in expected_tasks:
            assert task_id in task_ids, f"Task '{task_id}' missing from databricks_gold_dag"

    def test_dag_linear_and_group_dependencies(self) -> None:
        """Verify topological flow from verification -> Silver -> Gold VaR -> Features -> Optimize -> Notify."""
        dag = build_databricks_gold_dag()

        start = dag.get_task("start_gold_pipeline")
        verify = dag.get_task("verify_silver_readiness")
        cleanse_silver = dag.get_task("cleanse_silver_medallion")
        gold_var = dag.get_task("compute_gold_portfolio_var")
        gold_features = dag.get_task("publish_gold_risk_features")
        maintenance = dag.get_task("optimize_and_vacuum_gold_tables")
        notify = dag.get_task("notify_gold_completion")

        assert "verify_silver_readiness" in start.downstream_task_ids
        assert "cleanse_silver_medallion" in verify.downstream_task_ids
        assert "compute_gold_portfolio_var" in cleanse_silver.downstream_task_ids
        assert "publish_gold_risk_features" in gold_var.downstream_task_ids
        assert "optimize_and_vacuum_gold_tables" in gold_features.downstream_task_ids
        assert "notify_gold_completion" in maintenance.downstream_task_ids

    def test_dag_topological_sort_and_acyclic(self) -> None:
        """Ensure DAG has zero cycles and can be sorted topologically."""
        dag = build_databricks_gold_dag()
        ordered = dag.topological_sort()

        assert len(ordered) == len(dag.tasks)
        task_order = [t.task_id for t in ordered]

        # Verify logical execution precedence
        assert task_order.index("start_gold_pipeline") < task_order.index("verify_silver_readiness")
        assert task_order.index("verify_silver_readiness") < task_order.index("cleanse_silver_medallion")
        assert task_order.index("cleanse_silver_medallion") < task_order.index("compute_gold_portfolio_var")
        assert task_order.index("compute_gold_portfolio_var") < task_order.index("publish_gold_risk_features")
        assert task_order.index("publish_gold_risk_features") < task_order.index("optimize_and_vacuum_gold_tables")
        assert task_order.index("optimize_and_vacuum_gold_tables") < task_order.index("notify_gold_completion")


# =============================================================================
# 4. MASTER LAKEHOUSE DAG INTEGRATION TESTS
# =============================================================================
class TestTradingRiskDailyBatchDagGoldIntegration:
    """Validate master orchestration DAG integration with Databricks Medallion TaskGroup."""

    def test_medallion_group_uses_real_databricks_operators(self) -> None:
        """Verify that databricks_medallion_processing uses production operators with retries."""
        dag = build_trading_risk_dag()

        silver_task = dag.get_task("execute_silver_cleansing")
        gold_var_task = dag.get_task("execute_gold_var_calculation")
        maintenance_task = dag.get_task("execute_gold_table_maintenance")

        assert isinstance(silver_task, DatabricksSubmitRunOperator)
        assert isinstance(gold_var_task, DatabricksSubmitRunOperator)
        assert isinstance(maintenance_task, DatabricksSubmitRunOperator)

        # Verify automated retry settings
        assert silver_task.retries == 3
        assert gold_var_task.retries == 3
        assert maintenance_task.retries == 3
        assert silver_task.retry_exponential_backoff is True
        assert gold_var_task.retry_exponential_backoff is True
        assert silver_task.on_retry_callback is not None
        assert gold_var_task.on_retry_callback is not None

    def test_master_dag_dependencies_across_tiers(self) -> None:
        """Verify Dataproc -> Databricks -> Data Quality -> BigQuery dependency chain."""
        dag = build_trading_risk_dag()

        # Dataproc teardown must feed into Databricks Silver cleansing
        dp_teardown = dag.get_task("teardown_cluster")
        db_silver = dag.get_task("execute_silver_cleansing")
        db_gold = dag.get_task("execute_gold_var_calculation")
        db_maint = dag.get_task("execute_gold_table_maintenance")
        dq_silver = dag.get_task("validate_silver_suite")

        assert db_silver.task_id in dp_teardown.downstream_task_ids
        assert db_gold.task_id in db_silver.downstream_task_ids
        assert db_maint.task_id in db_gold.downstream_task_ids
        assert dq_silver.task_id in db_maint.downstream_task_ids

    def test_master_dag_validates_without_cycles(self) -> None:
        """Verify master DAG completes topological sort without cycles."""
        dag = build_trading_risk_dag()
        ordered = dag.topological_sort()
        assert len(ordered) == len(dag.tasks)
        assert len(dag.tasks) >= 12


# =============================================================================
# 5. END-TO-END SIMULATION & SPOT PREEMPTION RECOVERY TESTS
# =============================================================================
class TestEndToEndDatabricksGoldSimulation:
    """Verify end-to-end execution of Databricks Gold pipelines with spot recovery."""

    def test_full_pipeline_happy_path_execution(self) -> None:
        """Simulate end-to-end execution of all tasks in the Databricks Gold DAG."""
        dag = build_databricks_gold_dag()
        ordered_tasks = dag.topological_sort()
        test_date = datetime(2026, 10, 31)

        executed_run_ids = []
        for task in ordered_tasks:
            ctx = create_mock_task_context(dag=dag, task=task, execution_date=test_date)
            result = task.execute(ctx)
            if isinstance(result, int):
                executed_run_ids.append(result)

        # Silver Cleansing, Gold VaR, Gold Features, Gold Maintenance = 4 Databricks runs
        assert len(executed_run_ids) == 4
        all_runs = DatabricksJobStateRegistry.get_all_runs()
        assert len(all_runs) == 4

        for run in all_runs:
            assert run["state"]["result_state"] == "SUCCESS"

    def test_spot_worker_preemption_and_automated_recovery(self) -> None:
        """Simulate Spot executor preemption during Gold VaR calculation with automated retry recovery."""
        dag = build_databricks_gold_dag()
        gold_var_task = dag.get_task("compute_gold_portfolio_var")
        assert isinstance(gold_var_task, DatabricksSubmitRunOperator)

        # Inject Spot Preemption Exception on first attempt
        preemption_exc = AirflowException(
            "Databricks Driver Lost / GCP Spot Worker Preempted: Executor lost on node vm-worker-spot-02"
        )
        DatabricksJobStateRegistry.inject_failure(
            "compute_gold_portfolio_var",
            preemption_exc,
            fail_count=1,
        )

        retry_events = []
        gold_var_task.on_retry_callback = lambda ctx: retry_events.append(ctx)

        # Attempt 1: Spot worker preempted
        ctx1 = create_mock_task_context(
            dag=dag,
            task=gold_var_task,
            try_number=1,
            exception=preemption_exc,
        )
        with pytest.raises(AirflowException, match="GCP Spot Worker Preempted"):
            gold_var_task.execute(ctx1)

        # Airflow worker triggers retry callback
        gold_var_task.on_retry_callback(ctx1)
        assert len(retry_events) == 1

        # Attempt 2: Automated retry spins up replacement Spot/On-Demand executor and succeeds!
        ctx2 = create_mock_task_context(dag=dag, task=gold_var_task, try_number=2)
        run_id = gold_var_task.execute(ctx2)

        assert isinstance(run_id, int)
        run = DatabricksJobStateRegistry.get_run(run_id)
        assert run is not None
        assert run["state"]["result_state"] == "SUCCESS"
        assert DatabricksJobStateRegistry.get_attempt_count("compute_gold_portfolio_var") == 2
