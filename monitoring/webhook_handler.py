"""GCP Cloud Monitoring Webhook Handler & Slack Converter.

Parses Google Cloud Monitoring incident webhook payloads (schema v1.2), extracts
metric diagnostics, and transforms them into rich Slack Block Kit notifications.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from monitoring.config import AlertSeverity, IncidentState
from monitoring.slack_alerts import SlackDlqAlertDispatcher


@dataclass
class GcpMonitoringIncident:
    """Parsed incident record from Google Cloud Monitoring webhook."""

    incident_id: str
    scoping_project_id: str
    policy_name: str
    condition_name: str
    state: IncidentState
    started_at: float
    ended_at: Optional[float]
    summary: str
    url: str
    resource_id: str
    resource_type: str
    metric_type: str
    observed_value: str
    threshold_value: str
    severity: AlertSeverity

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "scoping_project_id": self.scoping_project_id,
            "policy_name": self.policy_name,
            "condition_name": self.condition_name,
            "state": self.state.value,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "summary": self.summary,
            "url": self.url,
            "resource_id": self.resource_id,
            "resource_type": self.resource_type,
            "metric_type": self.metric_type,
            "observed_value": self.observed_value,
            "threshold_value": self.threshold_value,
            "severity": self.severity.value,
        }


class CloudMonitoringWebhookHandler:
    """Parses GCP Cloud Monitoring webhook notifications and dispatches to Slack."""

    @staticmethod
    def parse_webhook_payload(raw_payload: Any) -> GcpMonitoringIncident:
        """Parse raw webhook data (JSON string or dict) into strongly typed incident."""
        data: Dict[str, Any]
        if isinstance(raw_payload, str):
            data = json.loads(raw_payload)
        elif isinstance(raw_payload, dict):
            data = raw_payload
        else:
            raise ValueError(f"Unsupported webhook payload type: {type(raw_payload)}")

        incident_raw = data.get("incident", data)
        state_str = str(incident_raw.get("state", "open")).lower()
        state = IncidentState.CLOSED if state_str == "closed" else IncidentState.OPEN

        # Infer severity
        policy_name = incident_raw.get("policy_name", "Unknown Alert Policy")
        policy_lower = policy_name.lower()
        if state == IncidentState.CLOSED:
            severity = AlertSeverity.RESOLVED
        elif "critical" in policy_lower or "dlq" in policy_lower or "down" in policy_lower:
            severity = AlertSeverity.CRITICAL
        elif "warn" in policy_lower or "lag" in policy_lower:
            severity = AlertSeverity.WARNING
        else:
            severity = AlertSeverity.INFO

        metric_dict = incident_raw.get("metric", {})
        metric_type = metric_dict.get("type", "unknown.metric") if isinstance(metric_dict, dict) else str(metric_dict)

        cond = incident_raw.get("condition", {})
        cond_thresh = cond.get("conditionThreshold", {}) if isinstance(cond, dict) else {}
        thresh_val = str(cond_thresh.get("thresholdValue", incident_raw.get("threshold_value", "N/A")))
        obs_val = str(incident_raw.get("observed_value", "Exceeded threshold"))

        return GcpMonitoringIncident(
            incident_id=str(incident_raw.get("incident_id", "inc-unknown")),
            scoping_project_id=str(incident_raw.get("scoping_project_id", "trading-risk")),
            policy_name=policy_name,
            condition_name=str(incident_raw.get("condition_name", "Condition breached")),
            state=state,
            started_at=float(incident_raw.get("started_at", 0)),
            ended_at=float(incident_raw.get("ended_at")) if incident_raw.get("ended_at") else None,
            summary=str(incident_raw.get("summary", "Cloud Monitoring alert condition met")),
            url=str(incident_raw.get("url", "")),
            resource_id=str(incident_raw.get("resource_id", incident_raw.get("resource_name", "unknown_resource"))),
            resource_type=str(incident_raw.get("resource_type", "unknown")),
            metric_type=metric_type,
            observed_value=obs_val,
            threshold_value=thresh_val,
            severity=severity,
        )

    def handle_webhook(
        self,
        raw_payload: Any,
        dispatcher: SlackDlqAlertDispatcher,
        bypass_cooldown: bool = False,
    ) -> bool:
        """Process incoming webhook and dispatch formatted Slack notification."""
        incident = self.parse_webhook_payload(raw_payload)
        return dispatcher.dispatch_incident_alert(
            policy_name=incident.policy_name,
            severity=incident.severity,
            summary=incident.summary,
            metric_name=incident.metric_type,
            observed_value=incident.observed_value,
            threshold_value=incident.threshold_value,
            incident_id=incident.incident_id,
            url=incident.url if incident.url else None,
            bypass_cooldown=bypass_cooldown,
        )
