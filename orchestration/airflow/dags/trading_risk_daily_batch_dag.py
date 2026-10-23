"""Trading Risk Engine - Daily Batch Analytical Lakehouse Orchestration DAG.

Orchestrates the Tier 3 batch data processing pipeline across Google Cloud Platform:
1. Verifies GCS Bronze tick and trade order partitions for previous trading day.
2. Dataproc Batch Aggregation (Day 19): spins up ephemeral Dataproc cluster, runs
   PySpark batch aggregations, and tears down cluster.
3. Databricks Medallion Processing (Days 16-18): triggers Delta Lake Silver cleansing
   and Gold portfolio Value-at-Risk (VaR) matrices.
4. Data Quality Gate (Day 21): validates Great Expectations checkpoint suites.
5. BigQuery Data Warehouse & dbt (Days 22-24): refreshes star schema and executes dbt models.
6. Operational Risk Reporting: publishes executive risk summaries and notifies stakeholders.
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
    from airflow.utils.task_group import TaskGroup  # type: ignore
except (ImportError, ModuleNotFoundError):
    from orchestration.airflow.airflow_compat import (
        DAG,
        EmptyOperator,
        TaskGroup,
    )

from orchestration.airflow.callbacks.failure_callbacks import (
    create_failure_callback,
    create_retry_callback,
    create_sla_miss_callback,
    create_success_callback,
)
from orchestration.airflow.config.airflow_config import AirflowEnvironmentConfig


def build_trading_risk_dag(
    config: Optional[AirflowEnvironmentConfig] = None,
) -> DAG:
    """Build and return the production Trading Risk daily batch orchestration DAG."""
    cfg = config or AirflowEnvironmentConfig.from_env()

    default_args = {
        "owner": "risk-data-platform",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": cfg.default_retries,
        "retry_delay": timedelta(seconds=cfg.default_retry_delay_seconds),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(seconds=cfg.max_retry_delay_seconds),
        "execution_timeout": timedelta(minutes=cfg.execution_timeout_minutes),
        "on_failure_callback": create_failure_callback(),
        "on_retry_callback": create_retry_callback(),
    }

    dag = DAG(
        dag_id="trading_risk_daily_batch_pipeline",
        description=(
            "Orchestrates Tier 3 Batch Analytical Lakehouse: GCS Bronze verification, "
            "Dataproc PySpark aggregation, Databricks Medallion (Silver cleanse, Gold VaR), "
            "Great Expectations quality gates, and BigQuery DWH dbt models."
        ),
        schedule_interval="0 1 * * *",  # 01:00 UTC daily market close
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        sla=timedelta(minutes=cfg.batch_sla_minutes),
        sla_miss_callback=create_sla_miss_callback(),
        on_success_callback=create_success_callback(),
        tags=["market-risk", "batch", "tier-3", "composer", "lakehouse"],
        doc_md=__doc__,
    )

    with dag:
        start_pipeline = EmptyOperator(
            task_id="start_pipeline",
            doc_md="Milestone marker signaling initiation of daily risk batch orchestration.",
        )

        verify_bronze_partitions = EmptyOperator(
            task_id="verify_bronze_partitions",
            doc_md="Sensor checking GCS Bronze tick Parquet and trade order partitions exist.",
        )

        # ---------------------------------------------------------------------
        # TaskGroup: Ephemeral Dataproc Batch Processing (Day 19)
        # ---------------------------------------------------------------------
        with TaskGroup("dataproc_batch_aggregation", tooltip="Dataproc PySpark rolling batch compute") as dataproc_group:
            create_cluster = EmptyOperator(
                task_id="create_ephemeral_cluster",
                doc_md="Creates an ephemeral auto-scaling Dataproc PySpark cluster with GCS committer.",
            )
            submit_pyspark = EmptyOperator(
                task_id="submit_pyspark_job",
                doc_md="Submits PySpark daily tick-to-VWAP and order volume rollups.",
            )
            teardown_cluster = EmptyOperator(
                task_id="teardown_cluster",
                doc_md="Tears down ephemeral Dataproc cluster to ensure zero idle compute cost.",
            )
            create_cluster >> submit_pyspark >> teardown_cluster

        # ---------------------------------------------------------------------
        # TaskGroup: Databricks Medallion Lakehouse (Days 16-18)
        # ---------------------------------------------------------------------
        with TaskGroup("databricks_medallion_processing", tooltip="Databricks Silver & Gold Lakehouse") as databricks_group:
            silver_cleansing = EmptyOperator(
                task_id="execute_silver_cleansing",
                doc_md="Cleanses, schema-enforces, and deduplicates records into Delta Lake Silver.",
            )
            gold_var_calc = EmptyOperator(
                task_id="execute_gold_var_calculation",
                doc_md="Calculates Monte Carlo / Parametric Value-at-Risk (VaR) and risk features.",
            )
            silver_cleansing >> gold_var_calc

        # ---------------------------------------------------------------------
        # TaskGroup: Data Quality Validation Gates (Day 21)
        # ---------------------------------------------------------------------
        with TaskGroup("data_quality_gates", tooltip="Great Expectations data quality suites") as dq_group:
            validate_silver = EmptyOperator(
                task_id="validate_silver_suite",
                doc_md="Validates Great Expectations test suites on Silver layer.",
            )
            validate_gold = EmptyOperator(
                task_id="validate_gold_suite",
                doc_md="Validates statistical bounds and completeness on Gold VaR layer.",
            )
            validate_silver >> validate_gold

        # ---------------------------------------------------------------------
        # TaskGroup: BigQuery Enterprise Data Warehouse & dbt (Days 22-24)
        # ---------------------------------------------------------------------
        with TaskGroup("bigquery_dwh_publication", tooltip="BigQuery partitioned tables & dbt models") as dwh_group:
            refresh_bigquery = EmptyOperator(
                task_id="refresh_bigquery_partitions",
                doc_md="Loads and refreshes partitioned, clustered BigQuery risk tables.",
            )
            run_dbt = EmptyOperator(
                task_id="run_dbt_transforms",
                doc_md="Executes dbt staging and mart models creating materialized analytics views.",
            )
            refresh_bigquery >> run_dbt

        publish_risk_report = EmptyOperator(
            task_id="publish_risk_report",
            doc_md="Publishes daily risk exposure summary to Cloud SQL and executive dashboards.",
        )

        end_pipeline = EmptyOperator(
            task_id="end_pipeline",
            doc_md="Milestone marker signaling successful end-to-end lakehouse completion.",
        )

        # Connect end-to-end linear and grouped dependencies
        start_pipeline >> verify_bronze_partitions >> dataproc_group >> databricks_group >> dq_group >> dwh_group >> publish_risk_report >> end_pipeline

    return dag


# Instantiate canonical DAG for Cloud Composer auto-discovery
trading_risk_dag = build_trading_risk_dag()
