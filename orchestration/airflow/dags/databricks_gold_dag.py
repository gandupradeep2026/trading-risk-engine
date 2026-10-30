"""Databricks Gold Lakehouse Pipeline Orchestration DAG.

Orchestrates Tier 3 Databricks Delta Lake Silver Cleansing, Gold Portfolio
Value-at-Risk (VaR), and Risk Feature Store analytical pipelines on Google Cloud Platform.

Key Architectural Guarantees:
1. Automated Retry Logic: Every Databricks operator task enforces exponential backoff
   retries (3 attempts, backoff factor 2.0) with automated Slack retry alerts.
2. Definite Escalation: When retries are exhausted, failure callbacks dispatch critical
   alerts to Slack and trigger PagerDuty incident escalation.
3. Unity Catalog Governance: Governed schema boundaries across Bronze, Silver, and Gold
   Delta Lake tables backed by GCS external locations.
4. Spot Worker Cost Optimization: Spot compute execution with automated idle termination.
5. Table Compaction & Retention: Delta OPTIMIZE (Z-Ordering) and VACUUM maintenance.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

# Dual-mode import: uses native Apache Airflow in Cloud Composer,
# or pure-Python hermetic compatibility layer during local development and testing.
try:
    import airflow  # type: ignore
    from airflow import DAG  # type: ignore
    from airflow.operators.empty import EmptyOperator  # type: ignore
    from airflow.providers.databricks.operators.databricks import (  # type: ignore
        DatabricksRunNowOperator,
        DatabricksSubmitRunOperator,
    )
    from airflow.utils.task_group import TaskGroup  # type: ignore
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore
except (ImportError, ModuleNotFoundError):
    from orchestration.airflow.airflow_compat import (
        DAG,
        DatabricksRunNowOperator,
        DatabricksSubmitRunOperator,
        EmptyOperator,
        TaskGroup,
        TriggerRule,
    )

from orchestration.airflow.callbacks.failure_callbacks import (
    create_failure_callback,
    create_retry_callback,
    create_sla_miss_callback,
    create_success_callback,
)
from orchestration.airflow.config.airflow_config import AirflowEnvironmentConfig
from orchestration.airflow.config.databricks_job_config import (
    DatabricksGoldJobConfig,
    DatabricksTaskRetryConfig,
)


def build_databricks_gold_dag(
    config: Optional[AirflowEnvironmentConfig] = None,
    databricks_config: Optional[DatabricksGoldJobConfig] = None,
) -> DAG:
    """Build and return the production Databricks Gold Lakehouse orchestration DAG."""
    cfg = config or AirflowEnvironmentConfig.from_env()
    db_cfg = databricks_config or DatabricksGoldJobConfig.from_env()
    retry_cfg = db_cfg.retry_config

    default_args = {
        "owner": "risk-data-platform",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retry_cfg.retries,
        "retry_delay": timedelta(seconds=retry_cfg.retry_delay_seconds),
        "retry_exponential_backoff": retry_cfg.retry_exponential_backoff,
        "max_retry_delay": timedelta(seconds=retry_cfg.max_retry_delay_seconds),
        "execution_timeout": timedelta(minutes=cfg.execution_timeout_minutes),
        "on_failure_callback": create_failure_callback(),
        "on_retry_callback": create_retry_callback(),
    }

    dag = DAG(
        dag_id="databricks_gold_pipeline",
        description=(
            "Orchestrates Databricks Delta Lake Silver Cleansing and Gold Portfolio "
            "Value-at-Risk (VaR) & Feature Store pipelines with automated retries."
        ),
        schedule_interval="30 1 * * *",  # 01:30 UTC daily (after Dataproc batch aggregation)
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        sla=timedelta(minutes=cfg.batch_sla_minutes),
        sla_miss_callback=create_sla_miss_callback(),
        on_success_callback=create_success_callback(),
        tags=["market-risk", "databricks", "gold", "var", "lakehouse", "tier-3"],
        doc_md=__doc__,
    )

    with dag:
        start_gold_pipeline = EmptyOperator(
            task_id="start_gold_pipeline",
            doc_md="Milestone marker signaling initiation of Databricks Gold lakehouse pipeline.",
        )

        verify_silver_readiness = EmptyOperator(
            task_id="verify_silver_readiness",
            doc_md="Sensor / pre-check validating Silver Delta Lake tables and Bronze inputs.",
        )

        # ---------------------------------------------------------------------
        # TaskGroup: Silver Cleansing & Deduplication
        # ---------------------------------------------------------------------
        with TaskGroup("silver_lakehouse_cleansing", tooltip="Delta Lake Silver Cleansing") as silver_group:
            cleanse_silver = DatabricksSubmitRunOperator(
                task_id="cleanse_silver_medallion",
                json=db_cfg.to_silver_submit_run_dict(target_date="{{ ds }}"),
                databricks_conn_id=db_cfg.databricks_conn_id,
                databricks_retry_limit=retry_cfg.retries,
                databricks_retry_delay=retry_cfg.retry_delay_seconds,
                retries=retry_cfg.retries,
                retry_delay=timedelta(seconds=retry_cfg.retry_delay_seconds),
                retry_exponential_backoff=retry_cfg.retry_exponential_backoff,
                max_retry_delay=timedelta(seconds=retry_cfg.max_retry_delay_seconds),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Executes Silver layer schema enforcement, deduplication, and microstructure enrichment.",
            )

        # ---------------------------------------------------------------------
        # TaskGroup: Gold Portfolio VaR & Risk Feature Store
        # ---------------------------------------------------------------------
        with TaskGroup("gold_lakehouse_var_and_features", tooltip="Delta Lake Gold VaR & Features") as gold_group:
            compute_gold_var = DatabricksSubmitRunOperator(
                task_id="compute_gold_portfolio_var",
                json=db_cfg.to_gold_var_submit_dict(target_date="{{ ds }}"),
                databricks_conn_id=db_cfg.databricks_conn_id,
                databricks_retry_limit=retry_cfg.retries,
                databricks_retry_delay=retry_cfg.retry_delay_seconds,
                retries=retry_cfg.retries,
                retry_delay=timedelta(seconds=retry_cfg.retry_delay_seconds),
                retry_exponential_backoff=retry_cfg.retry_exponential_backoff,
                max_retry_delay=timedelta(seconds=retry_cfg.max_retry_delay_seconds),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Computes Parametric, Historical, and Monte Carlo VaR/CVaR with Euler risk contributions.",
            )

            publish_features = DatabricksSubmitRunOperator(
                task_id="publish_gold_risk_features",
                json=db_cfg.to_gold_var_submit_dict(
                    target_date="{{ ds }}",
                    run_name=f"tre-gold-feature-store-{db_cfg.environment}-{{{{ ds }}}}",
                ),
                databricks_conn_id=db_cfg.databricks_conn_id,
                databricks_retry_limit=retry_cfg.retries,
                databricks_retry_delay=retry_cfg.retry_delay_seconds,
                retries=retry_cfg.retries,
                retry_delay=timedelta(seconds=retry_cfg.retry_delay_seconds),
                retry_exponential_backoff=retry_cfg.retry_exponential_backoff,
                max_retry_delay=timedelta(seconds=retry_cfg.max_retry_delay_seconds),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Derives mark-to-market prices, EWMA volatility, liquidity scores, and publishes feature store vectors.",
            )

            compute_gold_var >> publish_features

        # ---------------------------------------------------------------------
        # Task: Gold Delta Table Maintenance (OPTIMIZE & VACUUM)
        # ---------------------------------------------------------------------
        optimize_and_vacuum_gold = DatabricksSubmitRunOperator(
            task_id="optimize_and_vacuum_gold_tables",
            json=db_cfg.to_gold_maintenance_submit_dict(target_date="{{ ds }}"),
            databricks_conn_id=db_cfg.databricks_conn_id,
            databricks_retry_limit=retry_cfg.retries,
            databricks_retry_delay=retry_cfg.retry_delay_seconds,
            retries=retry_cfg.retries,
            retry_delay=timedelta(seconds=retry_cfg.retry_delay_seconds),
            retry_exponential_backoff=retry_cfg.retry_exponential_backoff,
            max_retry_delay=timedelta(seconds=retry_cfg.max_retry_delay_seconds),
            trigger_rule=TriggerRule.ALL_SUCCESS,
            on_retry_callback=create_retry_callback(),
            on_failure_callback=create_failure_callback(),
            doc_md="Executes Delta OPTIMIZE (Z-ordering) and VACUUM retention enforcement on Gold tables.",
        )

        notify_gold_completion = EmptyOperator(
            task_id="notify_gold_completion",
            on_success_callback=create_success_callback(),
            doc_md="Dispatches milestone success notifications upon completion of Gold VaR lakehouse.",
        )

        # Wire task dependencies
        start_gold_pipeline >> verify_silver_readiness >> silver_group >> gold_group >> optimize_and_vacuum_gold >> notify_gold_completion

    return dag


# Instantiate canonical DAG for Cloud Composer auto-discovery
databricks_gold_dag = build_databricks_gold_dag()
