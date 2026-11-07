"""Alert Policy Evaluator & Incident Lifecycle Manager.

Provides zero-cost local simulation of Google Cloud Monitoring alert evaluation,
maintaining incident states across sliding evaluation windows and triggering alerts
on breach and resolution.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from monitoring.config import AlertPolicyDefinition, AlertSeverity, IncidentState, DEFAULT_ALERT_POLICIES
from monitoring.slack_alerts import SlackDlqAlertDispatcher


@dataclass
class AlertIncident:
    """Active or closed incident tracking."""

    incident_id: str
    policy_id: str
    policy_name: str
    severity: AlertSeverity
    state: IncidentState
    observed_value: float
    threshold_value: float
    summary: str
    opened_at: float
    closed_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "policy_id": self.policy_id,
            "policy_name": self.policy_name,
            "severity": self.severity.value,
            "state": self.state.value,
            "observed_value": round(self.observed_value, 2),
            "threshold_value": round(self.threshold_value, 2),
            "summary": self.summary,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
        }


class AlertPolicyEvaluator:
    """Evaluates metrics against alert policy thresholds and manages incident lifecycle."""

    def __init__(
        self,
        policies: Optional[Dict[str, AlertPolicyDefinition]] = None,
        dispatcher: Optional[SlackDlqAlertDispatcher] = None,
    ) -> None:
        self.policies = policies or dict(DEFAULT_ALERT_POLICIES)
        self.dispatcher = dispatcher
        self.active_incidents: Dict[str, AlertIncident] = {}
        self.incident_history: List[AlertIncident] = []

    def evaluate_metric(
        self,
        policy_id: str,
        observed_value: float,
        timestamp: Optional[float] = None,
        resource_id: Optional[str] = None,
    ) -> Optional[AlertIncident]:
        """Evaluate a single metric value against policy and transition incident lifecycle."""
        now = timestamp if timestamp is not None else time.time()
        policy = self.policies.get(policy_id)
        if not policy:
            raise ValueError(f"Unknown policy ID: '{policy_id}'")

        # Compare observed against threshold
        is_breached = False
        if policy.comparison == "COMPARISON_GT":
            is_breached = observed_value > policy.threshold_value
        elif policy.comparison == "COMPARISON_LT":
            is_breached = observed_value < policy.threshold_value
        elif policy.comparison == "COMPARISON_GE":
            is_breached = observed_value >= policy.threshold_value
        elif policy.comparison == "COMPARISON_LE":
            is_breached = observed_value <= policy.threshold_value

        existing_incident = self.active_incidents.get(policy_id)

        if is_breached:
            if not existing_incident:
                # Open new incident
                inc_id = f"inc-{uuid.uuid4().hex[:8]}"
                summary = (
                    f"Metric '{policy.metric_type}' breached threshold: {observed_value} "
                    f"{policy.comparison} {policy.threshold_value} on {resource_id or 'resource'}."
                )
                incident = AlertIncident(
                    incident_id=inc_id,
                    policy_id=policy_id,
                    policy_name=policy.display_name,
                    severity=policy.severity,
                    state=IncidentState.OPEN,
                    observed_value=observed_value,
                    threshold_value=policy.threshold_value,
                    summary=summary,
                    opened_at=now,
                )
                self.active_incidents[policy_id] = incident
                self.incident_history.append(incident)

                # Dispatch notification if dispatcher is wired
                if self.dispatcher:
                    self.dispatcher.dispatch_incident_alert(
                        policy_name=policy.display_name,
                        severity=policy.severity,
                        summary=summary,
                        metric_name=policy.metric_type,
                        observed_value=f"{observed_value:.2f}",
                        threshold_value=f"{policy.threshold_value:.2f}",
                        incident_id=inc_id,
                    )
                return incident
            else:
                # Update observed value
                existing_incident.observed_value = observed_value
                return existing_incident
        else:
            if existing_incident:
                # Resolve active incident
                existing_incident.state = IncidentState.CLOSED
                existing_incident.closed_at = now
                summary = f"Metric '{policy.metric_type}' normalized to {observed_value:.2f}. Incident resolved."

                if self.dispatcher:
                    self.dispatcher.dispatch_incident_alert(
                        policy_name=policy.display_name,
                        severity=AlertSeverity.RESOLVED,
                        summary=summary,
                        metric_name=policy.metric_type,
                        observed_value=f"{observed_value:.2f}",
                        threshold_value=f"{policy.threshold_value:.2f}",
                        incident_id=existing_incident.incident_id,
                    )

                del self.active_incidents[policy_id]
                return existing_incident

        return None

    def evaluate_batch(
        self,
        metrics_dict: Dict[str, float],
        timestamp: Optional[float] = None,
    ) -> List[AlertIncident]:
        """Evaluate multiple policy metrics in batch."""
        incidents: List[AlertIncident] = []
        for policy_id, val in metrics_dict.items():
            if policy_id in self.policies:
                inc = self.evaluate_metric(policy_id, val, timestamp=timestamp)
                if inc is not None:
                    incidents.append(inc)
        return incidents
