"""Trading Risk Engine - Daily Batch Analytical Lakehouse Orchestration DAG.

Orchestrates the Tier 3 batch data processing pipeline across Google Cloud Platform:
1. Verifies GCS Bronze tick and trade order partitions for previous trading day.
2. Dataproc Batch Aggregation (Day 19): spins up ephemeral Dataproc cluster, runs
   PySpark batch aggregations, and tears down cluster with TriggerRule.ALL_DONE.
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
    from airflow.providers.databricks.operators.databricks import (  # type: ignore
        DatabricksRunNowOperator,
        DatabricksSubmitRunOperator,
    )
    from airflow.providers.google.cloud.operators.dataproc import (  # type: ignore
        DataprocCreateClusterOperator,
        DataprocDeleteClusterOperator,
        DataprocSubmitJobOperator,
    )
    from airflow.providers.google.cloud.operators.bigquery import (  # type: ignore
        BigQueryInsertJobOperator,
    )
    from airflow.utils.task_group import TaskGroup  # type: ignore
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore
    from great_expectations_provider.operators.great_expectations import (  # type: ignore
        GreatExpectationsOperator,
    )
    from orchestration.airflow.airflow_compat import (
        CloudSqlToBigQueryFederationOperator,
        DbtRunOperator,
        DbtTestOperator,
    )
except (ImportError, ModuleNotFoundError):
    from orchestration.airflow.airflow_compat import (
        DAG,
        BigQueryInsertJobOperator,
        CloudSqlToBigQueryFederationOperator,
        DatabricksRunNowOperator,
        DatabricksSubmitRunOperator,
        DataprocCreateClusterOperator,
        DataprocDeleteClusterOperator,
        DataprocSubmitJobOperator,
        DbtRunOperator,
        DbtTestOperator,
        EmptyOperator,
        GreatExpectationsOperator,
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
from orchestration.airflow.config.databricks_job_config import DatabricksGoldJobConfig
from orchestration.airflow.config.dataproc_cluster_config import (
    DataprocBatchJobConfig,
    DataprocEphemeralClusterConfig,
)
from orchestration.airflow.config.great_expectations_config import GreatExpectationsConfig


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
        cluster_name = f"tre-dp-batch-{{{{ ds_nodash }}}}-{{{{ task_instance.try_number }}}}"
        dp_cluster_cfg = DataprocEphemeralClusterConfig(
            project_id=cfg.gcp_project_id,
            region=cfg.gcp_region,
            zone=cfg.dataproc_zone,
            subnetwork="sb-data-services",
            cluster_name_prefix=f"tre-dp-batch-{cfg.environment}",
            service_account=f"sa-composer-{cfg.environment}@{cfg.gcp_project_id}.iam.gserviceaccount.com",
            internal_ip_only=True,
        )
        dp_job_cfg = DataprocBatchJobConfig(
            project_id=cfg.gcp_project_id,
            region=cfg.gcp_region,
            main_python_file_uri=f"gs://{cfg.bronze_bucket}/scripts/dataproc/daily_tick_aggregation.py",
            args=[
                "--target-date",
                "{{ ds }}",
                "--environment",
                cfg.environment,
                "--bronze-bucket",
                cfg.bronze_bucket,
                "--silver-bucket",
                cfg.silver_bucket,
                "--source-ticks-prefix",
                "ticks",
                "--source-orders-prefix",
                "orders",
                "--output-prefix",
                "dataproc/daily_aggregates",
            ],
        )

        with TaskGroup("dataproc_batch_aggregation", tooltip="Dataproc PySpark rolling batch compute") as dataproc_group:
            create_cluster = DataprocCreateClusterOperator(
                task_id="create_ephemeral_cluster",
                project_id=cfg.gcp_project_id,
                region=cfg.gcp_region,
                cluster_name=cluster_name,
                cluster_config=dp_cluster_cfg.to_cluster_config_dict(),
                gcp_conn_id=cfg.dataproc_conn_id,
                delete_on_error=True,
                doc_md="Creates an ephemeral auto-scaling Dataproc PySpark cluster with GCS committer.",
            )
            submit_pyspark = DataprocSubmitJobOperator(
                task_id="submit_pyspark_job",
                project_id=cfg.gcp_project_id,
                region=cfg.gcp_region,
                job=dp_job_cfg.to_pyspark_job_dict(cluster_name=cluster_name),
                gcp_conn_id=cfg.dataproc_conn_id,
                doc_md="Submits PySpark daily tick-to-VWAP and order volume rollups.",
            )
            teardown_cluster = DataprocDeleteClusterOperator(
                task_id="teardown_cluster",
                project_id=cfg.gcp_project_id,
                region=cfg.gcp_region,
                cluster_name=cluster_name,
                gcp_conn_id=cfg.dataproc_conn_id,
                trigger_rule=TriggerRule.ALL_DONE,
                doc_md="Tears down ephemeral Dataproc cluster to ensure zero idle compute cost.",
            )
            create_cluster >> submit_pyspark >> teardown_cluster

        # ---------------------------------------------------------------------
        # TaskGroup: Databricks Medallion Lakehouse (Days 16-18, 20)
        # ---------------------------------------------------------------------
        db_cfg = DatabricksGoldJobConfig.from_env()
        db_retry_cfg = db_cfg.retry_config

        with TaskGroup("databricks_medallion_processing", tooltip="Databricks Silver & Gold Lakehouse") as databricks_group:
            silver_cleansing = DatabricksSubmitRunOperator(
                task_id="execute_silver_cleansing",
                json=db_cfg.to_silver_submit_run_dict(target_date="{{ ds }}"),
                databricks_conn_id=db_cfg.databricks_conn_id,
                databricks_retry_limit=db_retry_cfg.retries,
                databricks_retry_delay=db_retry_cfg.retry_delay_seconds,
                retries=db_retry_cfg.retries,
                retry_delay=timedelta(seconds=db_retry_cfg.retry_delay_seconds),
                retry_exponential_backoff=db_retry_cfg.retry_exponential_backoff,
                max_retry_delay=timedelta(seconds=db_retry_cfg.max_retry_delay_seconds),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Cleanses, schema-enforces, and deduplicates records into Delta Lake Silver with automated retries.",
            )
            gold_var_calc = DatabricksSubmitRunOperator(
                task_id="execute_gold_var_calculation",
                json=db_cfg.to_gold_var_submit_dict(target_date="{{ ds }}"),
                databricks_conn_id=db_cfg.databricks_conn_id,
                databricks_retry_limit=db_retry_cfg.retries,
                databricks_retry_delay=db_retry_cfg.retry_delay_seconds,
                retries=db_retry_cfg.retries,
                retry_delay=timedelta(seconds=db_retry_cfg.retry_delay_seconds),
                retry_exponential_backoff=db_retry_cfg.retry_exponential_backoff,
                max_retry_delay=timedelta(seconds=db_retry_cfg.max_retry_delay_seconds),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Calculates Monte Carlo / Parametric Value-at-Risk (VaR) and risk features with automated retries.",
            )
            gold_maintenance = DatabricksSubmitRunOperator(
                task_id="execute_gold_table_maintenance",
                json=db_cfg.to_gold_maintenance_submit_dict(target_date="{{ ds }}"),
                databricks_conn_id=db_cfg.databricks_conn_id,
                databricks_retry_limit=db_retry_cfg.retries,
                databricks_retry_delay=db_retry_cfg.retry_delay_seconds,
                retries=db_retry_cfg.retries,
                retry_delay=timedelta(seconds=db_retry_cfg.retry_delay_seconds),
                retry_exponential_backoff=db_retry_cfg.retry_exponential_backoff,
                max_retry_delay=timedelta(seconds=db_retry_cfg.max_retry_delay_seconds),
                trigger_rule=TriggerRule.ALL_SUCCESS,
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Applies Delta OPTIMIZE (Z-ordering) and VACUUM maintenance across Gold tables.",
            )
            silver_cleansing >> gold_var_calc >> gold_maintenance

        # ---------------------------------------------------------------------
        # TaskGroup: Data Quality Validation Gates (Day 21)
        # ---------------------------------------------------------------------
        ge_cfg = GreatExpectationsConfig.from_env()

        with TaskGroup("data_quality_gates", tooltip="Great Expectations data quality suites") as dq_group:
            validate_silver_suite = GreatExpectationsOperator(
                task_id="validate_silver_suite",
                checkpoint_name=ge_cfg.silver_checkpoint_name,
                expectation_suite_name=ge_cfg.silver_ticks_suite,
                data_asset_name="silver_market_ticks",
                data_asset_path=ge_cfg.get_silver_ticks_path("{{ ds }}"),
                data_context_root_dir=ge_cfg.data_context_root_dir,
                fail_task_on_validation_failure=ge_cfg.fail_task_on_validation_failure,
                retries=ge_cfg.checkpoint_retry_limit,
                retry_delay=timedelta(seconds=ge_cfg.checkpoint_retry_delay_seconds),
                retry_exponential_backoff=ge_cfg.retry_exponential_backoff,
                max_retry_delay=timedelta(seconds=ge_cfg.max_retry_delay_seconds),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Validates Great Expectations test suites on Silver layer.",
            )
            validate_gold_suite = GreatExpectationsOperator(
                task_id="validate_gold_suite",
                checkpoint_name=ge_cfg.gold_checkpoint_name,
                expectation_suite_name=ge_cfg.gold_var_suite,
                data_asset_name="gold_portfolio_var",
                data_asset_path=ge_cfg.get_gold_var_path("{{ ds }}"),
                data_context_root_dir=ge_cfg.data_context_root_dir,
                fail_task_on_validation_failure=ge_cfg.fail_task_on_validation_failure,
                retries=ge_cfg.checkpoint_retry_limit,
                retry_delay=timedelta(seconds=ge_cfg.checkpoint_retry_delay_seconds),
                retry_exponential_backoff=ge_cfg.retry_exponential_backoff,
                max_retry_delay=timedelta(seconds=ge_cfg.max_retry_delay_seconds),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Validates statistical bounds and completeness on Gold VaR layer.",
            )
            publish_data_docs = GreatExpectationsOperator(
                task_id="publish_data_docs",
                checkpoint_name=ge_cfg.master_checkpoint_name,
                expectation_suite_name=ge_cfg.gold_var_suite,
                data_context_root_dir=ge_cfg.data_context_root_dir,
                data_docs_dir=f"uncommitted/data_docs/{cfg.environment}",
                fail_task_on_validation_failure=False,
                doc_md="Compiles and publishes consolidated Great Expectations HTML Data Docs.",
            )
            validate_silver_suite >> validate_gold_suite >> publish_data_docs

        # ---------------------------------------------------------------------
        # TaskGroup: BigQuery Enterprise Data Warehouse & dbt (Days 22-25)
        # ---------------------------------------------------------------------
        with TaskGroup("bigquery_dwh_publication", tooltip="BigQuery partitioned tables & dbt models") as dwh_group:
            replicate_cloudsql = CloudSqlToBigQueryFederationOperator(
                task_id="replicate_cloudsql_operational_data",
                tables="all",
                target_date="{{ ds }}",
                connection_id=f"conn_cloudsql_risk_db_{cfg.environment}",
                retries=3,
                retry_delay=timedelta(seconds=60),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(seconds=300),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Replicates operational risk alerts and mandate limits from Cloud SQL to BigQuery via federated queries.",
            )
            refresh_bigquery = BigQueryInsertJobOperator(
                task_id="refresh_bigquery_partitions",
                configuration={
                    "query": {
                        "query": (
                            f"MERGE `{cfg.gcp_project_id}.trading_risk_dwh_{cfg.environment}.fact_risk_stats` T "
                            f"USING `{cfg.gcp_project_id}.trading_risk_dwh_{cfg.environment}.stg_daily_risk_stats` S "
                            "ON T.risk_stat_id = S.risk_stat_id AND T.valuation_date = '{{ ds }}' "
                            "WHEN MATCHED THEN UPDATE SET limit_utilization_pct = S.limit_utilization_pct "
                            "WHEN NOT MATCHED THEN INSERT ROW;"
                        ),
                        "useLegacySql": False,
                    }
                },
                location=cfg.gcp_region,
                retries=3,
                retry_delay=timedelta(seconds=60),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(seconds=300),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Refreshes partitioned, clustered BigQuery risk tables with latest valuation snapshots.",
            )
            run_dbt = DbtRunOperator(
                task_id="run_dbt_transforms",
                models="staging intermediate",
                target=cfg.environment,
                retries=3,
                retry_delay=timedelta(seconds=60),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(seconds=300),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Executes dbt staging and intermediate models transforming star schema into analytical layers.",
            )
            test_dbt = DbtTestOperator(
                task_id="test_dbt_models",
                models="staging intermediate",
                target=cfg.environment,
                retries=2,
                retry_delay=timedelta(seconds=30),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(seconds=120),
                on_retry_callback=create_retry_callback(),
                on_failure_callback=create_failure_callback(),
                doc_md="Evaluates dbt schema tests (unique, not_null, accepted_values, relationships).",
            )
            replicate_cloudsql >> refresh_bigquery
            refresh_bigquery >> run_dbt
            replicate_cloudsql >> run_dbt
            run_dbt >> test_dbt


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
        publish_data_docs >> refresh_bigquery

    return dag


# Instantiate canonical DAG for Cloud Composer auto-discovery
trading_risk_dag = build_trading_risk_dag()
