"""Operational Telemetry & Prometheus Metric Exposition for Monitoring Engine."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class MonitoringTelemetry:
    """Thread-safe operational telemetry for Cloud Monitoring and Slack alerting."""

    evaluations_total: int = 0
    incidents_opened_total: int = 0
    incidents_closed_total: int = 0
    slack_alerts_dispatched_total: int = 0
    slack_alerts_failed_total: int = 0
    slack_alerts_suppressed_total: int = 0
    dlq_records_scanned_total: int = 0

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    def record_evaluation(self) -> None:
        with self._lock:
            self.evaluations_total += 1

    def record_incident_opened(self) -> None:
        with self._lock:
            self.incidents_opened_total += 1

    def record_incident_closed(self) -> None:
        with self._lock:
            self.incidents_closed_total += 1

    def record_slack_alert(self, success: bool, suppressed: bool = False) -> None:
        with self._lock:
            if suppressed:
                self.slack_alerts_suppressed_total += 1
            elif success:
                self.slack_alerts_dispatched_total += 1
            else:
                self.slack_alerts_failed_total += 1

    def record_dlq_scan(self, count: int) -> None:
        with self._lock:
            self.dlq_records_scanned_total += count

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "evaluations_total": self.evaluations_total,
                "incidents_opened_total": self.incidents_opened_total,
                "incidents_closed_total": self.incidents_closed_total,
                "slack_alerts_dispatched_total": self.slack_alerts_dispatched_total,
                "slack_alerts_failed_total": self.slack_alerts_failed_total,
                "slack_alerts_suppressed_total": self.slack_alerts_suppressed_total,
                "dlq_records_scanned_total": self.dlq_records_scanned_total,
            }

    def format_prometheus_text(self) -> str:
        """Render operational metrics in standard Prometheus exposition format."""
        with self._lock:
            lines = [
                "# HELP cloud_monitoring_evaluations_total Total number of metric evaluations performed.",
                "# TYPE cloud_monitoring_evaluations_total counter",
                f"cloud_monitoring_evaluations_total {self.evaluations_total}",
                "# HELP cloud_monitoring_incidents_opened_total Total number of alert incidents opened.",
                "# TYPE cloud_monitoring_incidents_opened_total counter",
                f"cloud_monitoring_incidents_opened_total {self.incidents_opened_total}",
                "# HELP cloud_monitoring_incidents_closed_total Total number of alert incidents resolved.",
                "# TYPE cloud_monitoring_incidents_closed_total counter",
                f"cloud_monitoring_incidents_closed_total {self.incidents_closed_total}",
                "# HELP cloud_monitoring_slack_alerts_dispatched_total Total number of Slack alerts successfully sent.",
                "# TYPE cloud_monitoring_slack_alerts_dispatched_total counter",
                f"cloud_monitoring_slack_alerts_dispatched_total {self.slack_alerts_dispatched_total}",
                "# HELP cloud_monitoring_slack_alerts_failed_total Total number of failed Slack alert attempts.",
                "# TYPE cloud_monitoring_slack_alerts_failed_total counter",
                f"cloud_monitoring_slack_alerts_failed_total {self.slack_alerts_failed_total}",
                "# HELP cloud_monitoring_slack_alerts_suppressed_total Total number of alerts suppressed by cooldown deduplication.",
                "# TYPE cloud_monitoring_slack_alerts_suppressed_total counter",
                f"cloud_monitoring_slack_alerts_suppressed_total {self.slack_alerts_suppressed_total}",
                "# HELP cloud_monitoring_dlq_records_scanned_total Total number of DLQ messages inspected.",
                "# TYPE cloud_monitoring_dlq_records_scanned_total counter",
                f"cloud_monitoring_dlq_records_scanned_total {self.dlq_records_scanned_total}",
            ]
            return "\n".join(lines)
