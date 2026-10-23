"""Slack Webhook Notification Hook for Cloud Composer / Airflow.

Constructs rich Slack Block Kit alerts with severity indicators, execution metadata,
log links, and error diagnostics. Features exponential backoff retry handling for
rate limits (HTTP 429) and transient 5xx errors, with zero external service dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional
import urllib.error
import urllib.request

logger = logging.getLogger("trading_risk.airflow.slack_hook")


class AlertSeverity(str, Enum):
    """Notification severity tiers."""
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"
    SUCCESS = "SUCCESS"


@dataclass
class SlackHookTelemetry:
    """Operational metrics for Slack notifications."""
    notifications_attempted: int = 0
    notifications_succeeded: int = 0
    notifications_failed: int = 0
    retries_performed: int = 0
    last_latency_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "notifications_attempted": self.notifications_attempted,
            "notifications_succeeded": self.notifications_succeeded,
            "notifications_failed": self.notifications_failed,
            "retries_performed": self.retries_performed,
            "last_latency_ms": round(self.last_latency_ms, 2),
        }


class SlackNotificationHook:
    """Slack Webhook client supporting Block Kit formatting and resilient delivery."""

    SEVERITY_COLORS = {
        AlertSeverity.CRITICAL: "#E01E5A",  # Red
        AlertSeverity.WARNING: "#ECB22E",   # Yellow / Amber
        AlertSeverity.INFO: "#439FE0",      # Blue
        AlertSeverity.SUCCESS: "#2EB886",   # Green
    }

    SEVERITY_EMOJIS = {
        AlertSeverity.CRITICAL: ":rotating_light:",
        AlertSeverity.WARNING: ":warning:",
        AlertSeverity.INFO: ":information_source:",
        AlertSeverity.SUCCESS: ":white_check_mark:",
    }

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        default_channel: str = "#risk-alerts",
        max_retries: int = 3,
        retry_delay_sec: float = 0.5,
        backoff_factor: float = 2.0,
        http_transport: Optional[Callable[[str, bytes, Dict[str, str]], Tuple[int, bytes]]] = None,
    ) -> None:
        self.webhook_url = webhook_url or "https://hooks.slack.com/services/MOCK/T000/B000"
        self.default_channel = default_channel
        self.max_retries = max_retries
        self.retry_delay_sec = retry_delay_sec
        self.backoff_factor = backoff_factor
        self.http_transport = http_transport or self._default_http_transport
        self.telemetry = SlackHookTelemetry()

    @staticmethod
    def _default_http_transport(
        url: str, data: bytes, headers: Dict[str, str]
    ) -> Tuple[int, bytes]:
        """Production HTTP POST using standard library urllib."""
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def build_block_kit_payload(
        self,
        severity: AlertSeverity,
        title: str,
        dag_id: str,
        task_id: str,
        execution_date: str,
        environment: str = "dev",
        try_number: int = 1,
        max_tries: int = 4,
        error_message: Optional[str] = None,
        log_url: Optional[str] = None,
        duration_sec: Optional[float] = None,
        channel: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Construct Slack Block Kit message payload with interactive metadata."""
        emoji = self.SEVERITY_EMOJIS.get(severity, ":bell:")
        color = self.SEVERITY_COLORS.get(severity, "#439FE0")
        target_channel = channel or self.default_channel

        blocks: List[Dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} [{severity.value}] {title}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Environment:*\n`{environment.upper()}`"},
                    {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                    {"type": "mrkdwn", "text": f"*Attempt:*\n`{try_number} of {max_tries}`"},
                    {"type": "mrkdwn", "text": f"*Execution Date:*\n`{execution_date}`"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n`{duration_sec:.1f}s`" if duration_sec else "*Duration:*\n`N/A`"},
                ],
            },
        ]

        if error_message:
            # Slack section text blocks have a 3000 character limit
            truncated = (error_message[:2900] + "\n...[truncated]") if len(error_message) > 2900 else error_message
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error Diagnostics:*\n```{truncated}```",
                    },
                }
            )

        # Action buttons linking to logs and monitoring
        actions: List[Dict[str, Any]] = []
        if log_url:
            actions.append(
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Task Log", "emoji": True},
                    "url": log_url,
                    "style": "primary" if severity == AlertSeverity.CRITICAL else "default",
                }
            )
        actions.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "GCP Cloud Logging", "emoji": True},
                "url": f"https://console.cloud.google.com/logs/query?query=resource.labels.task_id%3D%22{task_id}%22",
            }
        )

        blocks.append({"type": "actions", "elements": actions})

        # Wrap in attachment to provide left-border severity color
        return {
            "channel": target_channel,
            "text": f"[{severity.value}] {dag_id}.{task_id}: {title}",
            "attachments": [
                {
                    "color": color,
                    "blocks": blocks,
                }
            ],
        }

    def send_notification(
        self,
        severity: AlertSeverity,
        title: str,
        dag_id: str,
        task_id: str,
        execution_date: str,
        environment: str = "dev",
        try_number: int = 1,
        max_tries: int = 4,
        error_message: Optional[str] = None,
        log_url: Optional[str] = None,
        duration_sec: Optional[float] = None,
        channel: Optional[str] = None,
    ) -> bool:
        """Send formatted alert to Slack webhook with exponential backoff on transient errors."""
        payload = self.build_block_kit_payload(
            severity=severity,
            title=title,
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            environment=environment,
            try_number=try_number,
            max_tries=max_tries,
            error_message=error_message,
            log_url=log_url,
            duration_sec=duration_sec,
            channel=channel,
        )

        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json; charset=utf-8"}

        self.telemetry.notifications_attempted += 1
        start_time = time.time()
        current_delay = self.retry_delay_sec

        for attempt in range(self.max_retries + 1):
            try:
                status, body = self.http_transport(self.webhook_url, data, headers)
                if status == 200:
                    self.telemetry.notifications_succeeded += 1
                    self.telemetry.last_latency_ms = (time.time() - start_time) * 1000.0
                    return True

                if status == 429 or 500 <= status < 600:
                    # Retryable rate limit or server error
                    if attempt < self.max_retries:
                        self.telemetry.retries_performed += 1
                        time.sleep(current_delay)
                        current_delay *= self.backoff_factor
                        continue

                # Permanent client error (4xx except 429)
                logger.error(f"Slack webhook failed permanently with status {status}: {body.decode('utf-8', errors='ignore')}")
                break

            except Exception as e:
                if attempt < self.max_retries:
                    self.telemetry.retries_performed += 1
                    time.sleep(current_delay)
                    current_delay *= self.backoff_factor
                else:
                    logger.error(f"Slack webhook transport exception: {e}")

        self.telemetry.notifications_failed += 1
        self.telemetry.last_latency_ms = (time.time() - start_time) * 1000.0
        return False
