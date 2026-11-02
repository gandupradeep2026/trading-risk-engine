"""Airflow orchestration package for Trading Risk Engine."""

from orchestration.airflow.airflow_compat import (
    DatabricksJobStateRegistry,
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
    DataprocClusterStateRegistry,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    GreatExpectationsOperator,
    GreatExpectationsValidationRegistry,
    BigQueryInsertJobOperator,
    BigQueryJobRegistry,
    TriggerRule,
)
from orchestration.airflow.config.airflow_config import AirflowEnvironmentConfig
from orchestration.airflow.config.databricks_job_config import (
    DatabricksGoldJobConfig,
    DatabricksTaskRetryConfig,
)
from orchestration.airflow.config.dataproc_cluster_config import (
    DataprocBatchJobConfig,
    DataprocEphemeralClusterConfig,
)
from orchestration.airflow.config.great_expectations_config import GreatExpectationsConfig
from orchestration.airflow.connections.connection_manager import (
    AirflowConnection,
    GcpConnectionManager,
)
from orchestration.airflow.dags.databricks_gold_dag import (
    build_databricks_gold_dag,
    databricks_gold_dag,
)
from orchestration.airflow.dags.great_expectations_dag import (
    build_great_expectations_dag,
    great_expectations_dag,
)
from orchestration.airflow.hooks.notification_dispatcher import NotificationDispatcher
from orchestration.airflow.hooks.pagerduty_hook import PagerDutyHook
from orchestration.airflow.hooks.slack_webhook_hook import (
    AlertSeverity,
    SlackNotificationHook,
)

__all__ = [
    "AirflowEnvironmentConfig",
    "AirflowConnection",
    "GcpConnectionManager",
    "SlackNotificationHook",
    "PagerDutyHook",
    "NotificationDispatcher",
    "AlertSeverity",
    # Dataproc Ephemeral Orchestration
    "DataprocEphemeralClusterConfig",
    "DataprocBatchJobConfig",
    "DataprocCreateClusterOperator",
    "DataprocSubmitJobOperator",
    "DataprocDeleteClusterOperator",
    "TriggerRule",
    "DataprocClusterStateRegistry",
    # Databricks Gold Orchestration
    "DatabricksGoldJobConfig",
    "DatabricksTaskRetryConfig",
    "DatabricksSubmitRunOperator",
    "DatabricksRunNowOperator",
    "DatabricksJobStateRegistry",
    "build_databricks_gold_dag",
    "databricks_gold_dag",
    # Great Expectations Data Quality Orchestration
    "GreatExpectationsConfig",
    "GreatExpectationsOperator",
    "GreatExpectationsValidationRegistry",
    "build_great_expectations_dag",
    "great_expectations_dag",
    # BigQuery DWH Orchestration
    "BigQueryInsertJobOperator",
    "BigQueryJobRegistry",
]
