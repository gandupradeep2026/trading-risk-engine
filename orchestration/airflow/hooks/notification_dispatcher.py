"""Notification Dispatcher for Trading Risk Airflow Orchestration.

Dispatches operational alerts and incident events across Slack and PagerDuty
based on severity thresholds, retry counters, and SLA constraints.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Dict, Optional

from orchestration.airflow.config.airflow_config import AirflowEnvironmentConfig
from orchestration.airflow.hooks.pagerduty_hook import (
    PagerDutyAction,
    PagerDutyHook,
    PagerDutySeverity,
)
from orchestration.airflow.hooks.slack_webhook_hook import (
    AlertSeverity,
    SlackNotificationHook,
)

logger = logging.getLogger("trading_risk.airflow.dispatcher")


@dataclass
class DispatcherTelemetry:
    """Consolidated telemetry for alert dispatches."""
    total_dispatched: int = 0
    slack_dispatched: int = 0
    pagerduty_dispatched: int = 0
    failures: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_dispatched": self.total_dispatched,
            "slack_dispatched": self.slack_dispatched,
            "pagerduty_dispatched": self.pagerduty_dispatched,
            "failures": self.failures,
        }


class NotificationDispatcher:
    """Unified dispatcher routing alerts to Slack channels and PagerDuty services."""

    def __init__(
        self,
        config: Optional[AirflowEnvironmentConfig] = None,
        slack_hook: Optional[SlackNotificationHook] = None,
        pagerduty_hook: Optional[PagerDutyHook] = None,
    ) -> None:
        self.config = config or AirflowEnvironmentConfig()
        self.slack_hook = slack_hook or SlackNotificationHook(
            webhook_url=self.config.slack_webhook_url,
            default_channel=self.config.slack_channel,
        )
        self.pagerduty_hook = pagerduty_hook or PagerDutyHook(
            routing_key=self.config.pagerduty_routing_key,
        )
        self.telemetry = DispatcherTelemetry()

    def dispatch(
        self,
        severity: AlertSeverity,
        title: str,
        dag_id: str,
        task_id: str,
        execution_date: str,
        try_number: int = 1,
        max_tries: int = 4,
        error_message: Optional[str] = None,
        log_url: Optional[str] = None,
        duration_sec: Optional[float] = None,
        dedup_key: Optional[str] = None,
    ) -> Dict[str, bool]:
        """Dispatch notification to Slack and conditionally page PagerDuty on critical errors."""
        self.telemetry.total_dispatched += 1
        results: Dict[str, bool] = {"slack": False, "pagerduty": False}

        # 1. Always attempt Slack notification
        slack_ok = self.slack_hook.send_notification(
            severity=severity,
            title=title,
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            environment=self.config.environment,
            try_number=try_number,
            max_tries=max_tries,
            error_message=error_message,
            log_url=log_url,
            duration_sec=duration_sec,
        )
        results["slack"] = slack_ok
        if slack_ok:
            self.telemetry.slack_dispatched += 1
        else:
            self.telemetry.failures += 1

        # 2. Trigger PagerDuty only for CRITICAL severity (e.g. retries exhausted, SLA breach)
        if severity == AlertSeverity.CRITICAL:
            pd_ok = self.pagerduty_hook.send_event(
                action=PagerDutyAction.TRIGGER,
                summary=f"[{severity.value}] {dag_id}.{task_id}: {title}",
                severity=PagerDutySeverity.CRITICAL,
                dag_id=dag_id,
                task_id=task_id,
                execution_date=execution_date,
                environment=self.config.environment,
                dedup_key=dedup_key,
                error_message=error_message,
                log_url=log_url,
            )
            results["pagerduty"] = pd_ok
            if pd_ok:
                self.telemetry.pagerduty_dispatched += 1
            else:
                self.telemetry.failures += 1
        elif severity == AlertSeverity.SUCCESS and dedup_key:
            # Auto-resolve incident if resolved
            pd_ok = self.pagerduty_hook.send_event(
                action=PagerDutyAction.RESOLVE,
                summary=f"[RESOLVED] {dag_id}.{task_id}: {title}",
                severity=PagerDutySeverity.INFO,
                dag_id=dag_id,
                task_id=task_id,
                execution_date=execution_date,
                environment=self.config.environment,
                dedup_key=dedup_key,
            )
            results["pagerduty"] = pd_ok

        return results
