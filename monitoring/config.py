"""Configuration and Alert Policy Catalog for Cloud Monitoring & Alerting.

Defines strongly-typed representations of Google Cloud Monitoring alert policies,
notification channels, severity tiers, Dead-Letter Queue (DLQ) poison pill categories,
and runtime monitoring engine configurations.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class AlertSeverity(str, Enum):
    """Alert severity tiers."""

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"
    RESOLVED = "RESOLVED"


class AlertChannel(str, Enum):
    """Notification delivery channel targets."""

    SLACK = "slack"
    PAGERDUTY = "pagerduty"
    EMAIL = "email"
    WEBHOOK = "webhook"


class IncidentState(str, Enum):
    """GCP Cloud Monitoring incident states."""

    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    CLOSED = "closed"


class PoisonPillCategory(str, Enum):
    """Dead-Letter Queue message failure categorizations."""

    JSON_DECODE_ERROR = "JSON_DECODE_ERROR"
    SCHEMA_VALIDATION_ERROR = "SCHEMA_VALIDATION_ERROR"
    CROSSED_QUOTE_ERROR = "CROSSED_QUOTE_ERROR"
    NON_POSITIVE_PRICE_ERROR = "NON_POSITIVE_PRICE_ERROR"
    NON_POSITIVE_QUANTITY_ERROR = "NON_POSITIVE_QUANTITY_ERROR"
    MAX_RETRIES_EXHAUSTED = "MAX_RETRIES_EXHAUSTED"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


@dataclass(frozen=True)
class AlertPolicyDefinition:
    """Specification of a Google Cloud Monitoring alert policy."""

    policy_id: str
    display_name: str
    description: str
    metric_type: str
    resource_type: str
    comparison: str  # e.g. "COMPARISON_GT", "COMPARISON_LT"
    threshold_value: float
    duration_seconds: int
    aligner: str  # e.g. "ALIGN_MEAN", "ALIGN_MAX", "ALIGN_RATE"
    reducer: str  # e.g. "REDUCE_SUM", "REDUCE_MAX", "REDUCE_MEAN"
    alignment_period_seconds: int
    filter_query: str
    severity: AlertSeverity
    channels: List[AlertChannel]
    documentation_url: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "display_name": self.display_name,
            "description": self.description,
            "metric_type": self.metric_type,
            "resource_type": self.resource_type,
            "comparison": self.comparison,
            "threshold_value": self.threshold_value,
            "duration_seconds": self.duration_seconds,
            "aligner": self.aligner,
            "reducer": self.reducer,
            "alignment_period_seconds": self.alignment_period_seconds,
            "filter_query": self.filter_query,
            "severity": self.severity.value,
            "channels": [c.value for c in self.channels],
            "documentation_url": self.documentation_url,
        }


# Canonical Enterprise GCP Cloud Monitoring Alert Policy Catalog
DEFAULT_ALERT_POLICIES: Dict[str, AlertPolicyDefinition] = {
    "pubsub_subscription_lag": AlertPolicyDefinition(
        policy_id="pubsub_subscription_lag",
        display_name="Pub/Sub Trade Orders Subscription Lag",
        description="Subscription backlog has exceeded threshold indicating consumer stall or Dataflow lag.",
        metric_type="pubsub.googleapis.com/subscription/num_undelivered_messages",
        resource_type="pubsub_subscription",
        comparison="COMPARISON_GT",
        threshold_value=10000.0,
        duration_seconds=300,
        aligner="ALIGN_MEAN",
        reducer="REDUCE_SUM",
        alignment_period_seconds=60,
        filter_query='resource.type = "pubsub_subscription" AND metric.type = "pubsub.googleapis.com/subscription/num_undelivered_messages"',
        severity=AlertSeverity.CRITICAL,
        channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
        documentation_url="docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#pubsub-lag-remediation",
    ),
    "pubsub_oldest_unacked_age": AlertPolicyDefinition(
        policy_id="pubsub_oldest_unacked_age",
        display_name="Pub/Sub Oldest Unacknowledged Message Age",
        description="Oldest unacknowledged message age in subscription exceeds latency SLO (300s).",
        metric_type="pubsub.googleapis.com/subscription/oldest_unacked_message_age",
        resource_type="pubsub_subscription",
        comparison="COMPARISON_GT",
        threshold_value=300.0,
        duration_seconds=300,
        aligner="ALIGN_MAX",
        reducer="REDUCE_MAX",
        alignment_period_seconds=60,
        filter_query='resource.type = "pubsub_subscription" AND metric.type = "pubsub.googleapis.com/subscription/oldest_unacked_message_age"',
        severity=AlertSeverity.CRITICAL,
        channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
        documentation_url="docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#pubsub-lag-remediation",
    ),
    "pubsub_dlq_message_spike": AlertPolicyDefinition(
        policy_id="pubsub_dlq_message_spike",
        display_name="Trade Orders DLQ Poison Pill Received",
        description="Dead-Letter Topic received unparseable or rejected financial orders requiring forensic triage.",
        metric_type="pubsub.googleapis.com/topic/send_request_count",
        resource_type="pubsub_topic",
        comparison="COMPARISON_GT",
        threshold_value=0.0,
        duration_seconds=60,
        aligner="ALIGN_RATE",
        reducer="REDUCE_SUM",
        alignment_period_seconds=60,
        filter_query='resource.type = "pubsub_topic" AND metric.type = "pubsub.googleapis.com/topic/send_request_count"',
        severity=AlertSeverity.CRITICAL,
        channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
        documentation_url="docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#dlq-triage-sop",
    ),
    "dataflow_system_lag": AlertPolicyDefinition(
        policy_id="dataflow_system_lag",
        display_name="Cloud Dataflow Streaming System Lag",
        description="Dataflow streaming pipeline watermark lag has exceeded latency boundaries.",
        metric_type="dataflow.googleapis.com/job/system_lag",
        resource_type="dataflow_job",
        comparison="COMPARISON_GT",
        threshold_value=120.0,
        duration_seconds=300,
        aligner="ALIGN_MAX",
        reducer="REDUCE_MAX",
        alignment_period_seconds=60,
        filter_query='resource.type = "dataflow_job" AND metric.type = "dataflow.googleapis.com/job/system_lag"',
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
        documentation_url="docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#dataflow-lag-remediation",
    ),
    "cloudsql_cpu_high": AlertPolicyDefinition(
        policy_id="cloudsql_cpu_high",
        display_name="Cloud SQL PostgreSQL High CPU Utilization",
        description="Cloud SQL database CPU usage sustained above 80% indicating connection saturation or missing indexes.",
        metric_type="cloudsql.googleapis.com/database/cpu/utilization",
        resource_type="cloudsql_database",
        comparison="COMPARISON_GT",
        threshold_value=0.80,
        duration_seconds=600,
        aligner="ALIGN_MEAN",
        reducer="REDUCE_MEAN",
        alignment_period_seconds=60,
        filter_query='resource.type = "cloudsql_database" AND metric.type = "cloudsql.googleapis.com/database/cpu/utilization"',
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
        documentation_url="docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#cloudsql-cpu-remediation",
    ),
    "ingestion_worker_down": AlertPolicyDefinition(
        policy_id="ingestion_worker_down",
        display_name="Ingestion Worker VM Downtime",
        description="Market ingestion compute worker heartbeat is absent or VM is stopped.",
        metric_type="compute.googleapis.com/instance/uptime",
        resource_type="gce_instance",
        comparison="COMPARISON_LT",
        threshold_value=1.0,
        duration_seconds=180,
        aligner="ALIGN_RATE",
        reducer="REDUCE_SUM",
        alignment_period_seconds=60,
        filter_query='resource.type = "gce_instance" AND metric.type = "compute.googleapis.com/instance/uptime"',
        severity=AlertSeverity.CRITICAL,
        channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
        documentation_url="docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#ingestion-worker-remediation",
    ),
}


@dataclass
class MonitoringConfig:
    """Runtime configuration for Cloud Monitoring and Alerting engine."""

    project_id: str = "trading-risk-dev-001"
    environment: str = "dev"
    slack_webhook_url: str = "https://hooks.slack.com/services/MOCK/T000/B000"
    slack_alerts_channel: str = "#risk-alerts"
    slack_dlq_channel: str = "#risk-dlq"
    pubsub_lag_threshold_messages: int = 10000
    pubsub_oldest_unacked_age_seconds: int = 300
    pubsub_dlq_threshold_messages: int = 1
    dataflow_system_lag_threshold_seconds: int = 120
    cloudsql_cpu_threshold: float = 0.80
    alert_cooldown_seconds: float = 300.0  # Alert deduplication cooldown
    max_retries: int = 3
    retry_backoff_sec: float = 0.5

    @classmethod
    def from_env(cls) -> MonitoringConfig:
        """Instantiate configuration from environment variables with safe defaults."""
        return cls(
            project_id=os.getenv("GCP_PROJECT_ID", "trading-risk-dev-001"),
            environment=os.getenv("DEPLOYMENT_ENV", "dev"),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/MOCK/T000/B000"),
            slack_alerts_channel=os.getenv("SLACK_ALERTS_CHANNEL", "#risk-alerts"),
            slack_dlq_channel=os.getenv("SLACK_DLQ_CHANNEL", "#risk-dlq"),
            pubsub_lag_threshold_messages=int(os.getenv("PUBSUB_LAG_THRESHOLD_MESSAGES", "10000")),
            pubsub_oldest_unacked_age_seconds=int(os.getenv("PUBSUB_OLDEST_UNACKED_AGE_SECONDS", "300")),
            pubsub_dlq_threshold_messages=int(os.getenv("PUBSUB_DLQ_THRESHOLD_MESSAGES", "1")),
            dataflow_system_lag_threshold_seconds=int(os.getenv("DATAFLOW_SYSTEM_LAG_THRESHOLD_SECONDS", "120")),
            cloudsql_cpu_threshold=float(os.getenv("CLOUDSQL_CPU_THRESHOLD", "0.80")),
            alert_cooldown_seconds=float(os.getenv("ALERT_COOLDOWN_SECONDS", "300.0")),
            max_retries=int(os.getenv("MONITORING_MAX_RETRIES", "3")),
            retry_backoff_sec=float(os.getenv("MONITORING_RETRY_BACKOFF_SEC", "0.5")),
        )

    def validate(self) -> None:
        """Validate configuration constraints."""
        if not self.project_id:
            raise ValueError("project_id cannot be empty")
        if self.environment not in ("dev", "staging", "prod"):
            raise ValueError(f"Invalid environment: '{self.environment}'. Must be dev, staging, or prod.")
        if self.pubsub_lag_threshold_messages <= 0:
            raise ValueError("pubsub_lag_threshold_messages must be strictly positive")
        if self.pubsub_oldest_unacked_age_seconds <= 0:
            raise ValueError("pubsub_oldest_unacked_age_seconds must be strictly positive")
        if not (0.0 < self.cloudsql_cpu_threshold <= 1.0):
            raise ValueError("cloudsql_cpu_threshold must be between 0.0 and 1.0")

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "project_id": self.project_id,
            "environment": self.environment,
            "slack_webhook_url": self.slack_webhook_url[:15] + "...",
            "slack_alerts_channel": self.slack_alerts_channel,
            "slack_dlq_channel": self.slack_dlq_channel,
            "pubsub_lag_threshold_messages": self.pubsub_lag_threshold_messages,
            "pubsub_oldest_unacked_age_seconds": self.pubsub_oldest_unacked_age_seconds,
            "pubsub_dlq_threshold_messages": self.pubsub_dlq_threshold_messages,
            "dataflow_system_lag_threshold_seconds": self.dataflow_system_lag_threshold_seconds,
            "cloudsql_cpu_threshold": self.cloudsql_cpu_threshold,
            "alert_cooldown_seconds": self.alert_cooldown_seconds,
            "max_retries": self.max_retries,
            "retry_backoff_sec": self.retry_backoff_sec,
        }
