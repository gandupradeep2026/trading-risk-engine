"""PagerDuty Events API v2 Notification Hook for Cloud Composer.

Dispatches high-urgency incidents for critical SLA misses and task execution failures
with deduplication keys, structured diagnostics, and deep links into Cloud Logging.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.error
import urllib.request

logger = logging.getLogger("trading_risk.airflow.pagerduty_hook")


class PagerDutySeverity(str, Enum):
    """PagerDuty event severity mapping."""
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class PagerDutyAction(str, Enum):
    """PagerDuty event actions."""
    TRIGGER = "trigger"
    ACKNOWLEDGE = "acknowledge"
    RESOLVE = "resolve"


@dataclass
class PagerDutyHookTelemetry:
    """Operational telemetry for PagerDuty event emissions."""
    events_attempted: int = 0
    events_succeeded: int = 0
    events_failed: int = 0
    retries_performed: int = 0
    last_latency_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "events_attempted": self.events_attempted,
            "events_succeeded": self.events_succeeded,
            "events_failed": self.events_failed,
            "retries_performed": self.retries_performed,
            "last_latency_ms": round(self.last_latency_ms, 2),
        }


class PagerDutyHook:
    """PagerDuty Events API v2 client for enterprise incident management."""

    def __init__(
        self,
        routing_key: Optional[str] = None,
        api_endpoint: str = "https://events.pagerduty.com/v2/enqueue",
        max_retries: int = 3,
        retry_delay_sec: float = 0.5,
        backoff_factor: float = 2.0,
        http_transport: Optional[Callable[[str, bytes, Dict[str, str]], Tuple[int, bytes]]] = None,
    ) -> None:
        self.routing_key = routing_key or "MOCK_PAGERDUTY_INTEGRATION_KEY"
        self.api_endpoint = api_endpoint
        self.max_retries = max_retries
        self.retry_delay_sec = retry_delay_sec
        self.backoff_factor = backoff_factor
        self.http_transport = http_transport or self._default_http_transport
        self.telemetry = PagerDutyHookTelemetry()

    @staticmethod
    def _default_http_transport(
        url: str, data: bytes, headers: Dict[str, str]
    ) -> Tuple[int, bytes]:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def build_event_payload(
        self,
        action: PagerDutyAction,
        summary: str,
        severity: PagerDutySeverity,
        dag_id: str,
        task_id: str,
        execution_date: str,
        environment: str = "dev",
        dedup_key: Optional[str] = None,
        error_message: Optional[str] = None,
        log_url: Optional[str] = None,
        custom_details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Format event payload conforming strictly to PagerDuty Events API v2."""
        key = dedup_key or f"trading-risk/{environment}/{dag_id}/{task_id}"

        details: Dict[str, Any] = {
            "environment": environment,
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": execution_date,
        }
        if error_message:
            details["error_message"] = error_message
        if custom_details:
            details.update(custom_details)

        payload: Dict[str, Any] = {
            "routing_key": self.routing_key,
            "event_action": action.value,
            "dedup_key": key,
            "payload": {
                "summary": summary,
                "source": f"cloud-composer-{environment}",
                "severity": severity.value,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "component": task_id,
                "group": "trading-risk-engine",
                "class": "batch-lakehouse-pipeline",
                "custom_details": details,
            },
        }

        if log_url:
            payload["links"] = [
                {
                    "href": log_url,
                    "text": "Airflow Task Execution Log",
                }
            ]

        return payload

    def send_event(
        self,
        action: PagerDutyAction,
        summary: str,
        severity: PagerDutySeverity,
        dag_id: str,
        task_id: str,
        execution_date: str,
        environment: str = "dev",
        dedup_key: Optional[str] = None,
        error_message: Optional[str] = None,
        log_url: Optional[str] = None,
        custom_details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Dispatch event to PagerDuty Events API v2 with exponential backoff on retryable codes."""
        payload = self.build_event_payload(
            action=action,
            summary=summary,
            severity=severity,
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            environment=environment,
            dedup_key=dedup_key,
            error_message=error_message,
            log_url=log_url,
            custom_details=custom_details,
        )

        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json; charset=utf-8"}

        self.telemetry.events_attempted += 1
        start_time = time.time()
        current_delay = self.retry_delay_sec

        for attempt in range(self.max_retries + 1):
            try:
                status, body = self.http_transport(self.api_endpoint, data, headers)
                if status in (200, 202):
                    self.telemetry.events_succeeded += 1
                    self.telemetry.last_latency_ms = (time.time() - start_time) * 1000.0
                    return True

                if status == 429 or 500 <= status < 600:
                    if attempt < self.max_retries:
                        self.telemetry.retries_performed += 1
                        time.sleep(current_delay)
                        current_delay *= self.backoff_factor
                        continue

                logger.error(f"PagerDuty event failed with HTTP {status}: {body.decode('utf-8', errors='ignore')}")
                break

            except Exception as e:
                if attempt < self.max_retries:
                    self.telemetry.retries_performed += 1
                    time.sleep(current_delay)
                    current_delay *= self.backoff_factor
                else:
                    logger.error(f"PagerDuty transport error: {e}")

        self.telemetry.events_failed += 1
        self.telemetry.last_latency_ms = (time.time() - start_time) * 1000.0
        return False
