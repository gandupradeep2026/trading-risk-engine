"""Cloud Composer & Airflow Environment Configuration.

Provides strongly typed configuration for GCP resource bindings, bucket paths,
Airflow connection IDs, and SLA parameters across dev, staging, and prod environments.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import os
from typing import Any, Dict, Optional


@dataclass
class AirflowEnvironmentConfig:
    """Strongly typed configuration for Cloud Composer Airflow pipelines."""

    environment: str = "dev"
    gcp_project_id: str = "trading-risk-dev-001"
    gcp_region: str = "us-central1"

    # GCS Data Lake Buckets
    bronze_bucket: str = "trading-risk-bronze-dev"
    silver_bucket: str = "trading-risk-silver-dev"
    gold_bucket: str = "trading-risk-gold-dev"

    # Cloud SQL PostgreSQL
    cloudsql_instance: str = "trading-risk-dev-001:us-central1:trading-risk-db-dev"
    cloudsql_database: str = "trading_risk_db"
    cloudsql_user: str = "risk_app_user"

    # Ephemeral Dataproc Batch Cluster
    dataproc_cluster_name: str = "dp-risk-batch-dev"
    dataproc_zone: str = "us-central1-a"

    # Airflow Connections
    gcp_conn_id: str = "google_cloud_default"
    dataproc_conn_id: str = "gcp_dataproc_default"
    cloudsql_conn_id: str = "gcp_cloudsql_postgres"
    databricks_conn_id: str = "databricks_default"
    slack_conn_id: str = "slack_alerts_connection"
    pagerduty_conn_id: str = "pagerduty_alerts_connection"

    # Notification settings
    slack_channel: str = "#risk-alerts-dev"
    slack_webhook_url: Optional[str] = None
    pagerduty_routing_key: Optional[str] = None
    alert_severity_threshold: str = "WARNING"  # INFO, WARNING, CRITICAL

    # Pipeline SLA & Retry Policies
    batch_sla_minutes: int = 60
    default_retries: int = 3
    default_retry_delay_seconds: int = 300
    max_retry_delay_seconds: int = 1800
    execution_timeout_minutes: int = 120

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate configuration invariants."""
        if not self.environment:
            raise ValueError("environment cannot be empty")
        if not self.gcp_project_id:
            raise ValueError("gcp_project_id cannot be empty")
        if not self.bronze_bucket or not self.silver_bucket or not self.gold_bucket:
            raise ValueError("GCS bronze, silver, and gold buckets must be configured")
        if self.batch_sla_minutes <= 0:
            raise ValueError("batch_sla_minutes must be positive")
        if self.default_retries < 0:
            raise ValueError("default_retries cannot be negative")
        if self.default_retry_delay_seconds <= 0:
            raise ValueError("default_retry_delay_seconds must be positive")
        if self.execution_timeout_minutes <= 0:
            raise ValueError("execution_timeout_minutes must be positive")

    @classmethod
    def from_env(cls) -> AirflowEnvironmentConfig:
        """Construct configuration by reading Airflow Variables or environment variables."""
        env = os.getenv("AIRFLOW_VAR_ENVIRONMENT", os.getenv("ENVIRONMENT", "dev")).lower()
        project_id = os.getenv("AIRFLOW_VAR_GCP_PROJECT", os.getenv("GCP_PROJECT_ID", f"trading-risk-{env}-001"))
        region = os.getenv("AIRFLOW_VAR_GCP_REGION", os.getenv("GCP_REGION", "us-central1"))

        return cls(
            environment=env,
            gcp_project_id=project_id,
            gcp_region=region,
            bronze_bucket=os.getenv("AIRFLOW_VAR_GCS_BRONZE_BUCKET", f"trading-risk-bronze-{env}"),
            silver_bucket=os.getenv("AIRFLOW_VAR_GCS_SILVER_BUCKET", f"trading-risk-silver-{env}"),
            gold_bucket=os.getenv("AIRFLOW_VAR_GCS_GOLD_BUCKET", f"trading-risk-gold-{env}"),
            cloudsql_instance=os.getenv(
                "AIRFLOW_VAR_CLOUDSQL_INSTANCE",
                f"{project_id}:{region}:trading-risk-db-{env}",
            ),
            cloudsql_database=os.getenv("AIRFLOW_VAR_CLOUDSQL_DATABASE", "trading_risk_db"),
            cloudsql_user=os.getenv("AIRFLOW_VAR_CLOUDSQL_USER", "risk_app_user"),
            dataproc_cluster_name=os.getenv("AIRFLOW_VAR_DATAPROC_CLUSTER", f"dp-risk-batch-{env}"),
            dataproc_zone=os.getenv("AIRFLOW_VAR_DATAPROC_ZONE", f"{region}-a"),
            slack_channel=os.getenv("AIRFLOW_VAR_SLACK_CHANNEL", f"#risk-alerts-{env}"),
            slack_webhook_url=os.getenv("AIRFLOW_VAR_SLACK_WEBHOOK_URL"),
            pagerduty_routing_key=os.getenv("AIRFLOW_VAR_PAGERDUTY_ROUTING_KEY"),
            batch_sla_minutes=int(os.getenv("AIRFLOW_VAR_BATCH_SLA_MINUTES", "60")),
            default_retries=int(os.getenv("AIRFLOW_VAR_DEFAULT_RETRIES", "3")),
            default_retry_delay_seconds=int(os.getenv("AIRFLOW_VAR_DEFAULT_RETRY_DELAY_SEC", "300")),
            max_retry_delay_seconds=int(os.getenv("AIRFLOW_VAR_MAX_RETRY_DELAY_SEC", "1800")),
            execution_timeout_minutes=int(os.getenv("AIRFLOW_VAR_EXEC_TIMEOUT_MIN", "120")),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return asdict(self)
