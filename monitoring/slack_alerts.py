"""Slack Block Kit Alert Dispatcher for Cloud Monitoring & DLQ Poison Pills.

Formats rich, actionable Slack Block Kit incident notifications and provides resilient
delivery over webhooks with alert deduplication/cooldown suppression and exponential backoff.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from monitoring.config import AlertSeverity, PoisonPillCategory
from monitoring.metrics import PubSubLagMetrics

logger = logging.getLogger("trading_risk.monitoring.slack_alerts")


class SlackBlockKitBuilder:
    """Builds Slack Block Kit JSON structures."""

    SEVERITY_EMOJIS = {
        AlertSeverity.CRITICAL: ":rotating_light:",
        AlertSeverity.WARNING: ":warning:",
        AlertSeverity.INFO: ":information_source:",
        AlertSeverity.RESOLVED: ":white_check_mark:",
    }

    @classmethod
    def build_dlq_poison_pill_blocks(
        cls,
        environment: str,
        topic_name: str,
        category: PoisonPillCategory,
        error_message: str,
        ticker: Optional[str] = None,
        portfolio_id: Optional[str] = None,
        delivery_attempts: int = 5,
        payload_preview: Optional[str] = None,
        runbook_url: str = "docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#dlq-triage-sop",
    ) -> List[Dict[str, Any]]:
        """Construct Slack Block Kit notification for a Dead-Letter Queue event."""
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        inst = ticker or "MULTIPLE/UNKNOWN"
        port = portfolio_id or "UNKNOWN"

        blocks: List[Dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{cls.SEVERITY_EMOJIS[AlertSeverity.CRITICAL]} [CRITICAL] Trade Order DLQ Poison Pill Received",
                    "emoji": True,
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Environment:* `{environment.upper()}` | *DLQ Topic:* `{topic_name}` | *Timestamp:* `{now_utc}`",
                    }
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Failure Category:*\n`{category.value}`"},
                    {"type": "mrkdwn", "text": f"*Instrument / Ticker:*\n`{inst}`"},
                    {"type": "mrkdwn", "text": f"*Portfolio:*\n`{port}`"},
                    {"type": "mrkdwn", "text": f"*Delivery Attempts:*\n`{delivery_attempts}` (Max Retries Reached)"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Forensic Error Diagnostic:*\n```{error_message[:400]}```",
                },
            },
        ]

        if payload_preview:
            # Mask sensitive values and truncate to 500 chars
            sanitized = payload_preview[:500]
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Sanitized Payload Preview:*\n```{sanitized}```",
                    },
                }
            )

        blocks.extend(
            [
                {"type": "divider"},
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "View DLQ Runbook", "emoji": True},
                            "url": f"https://github.com/trading-risk/trading-risk-engine/blob/main/{runbook_url}",
                            "style": "danger",
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "GCP Cloud Monitoring", "emoji": True},
                            "url": f"https://console.cloud.google.com/monitoring/alerting?project=trading-risk-{environment}",
                        },
                    ],
                },
            ]
        )
        return blocks

    @classmethod
    def build_pubsub_lag_blocks(
        cls,
        environment: str,
        metrics: PubSubLagMetrics,
        threshold_messages: int,
        threshold_age_sec: float,
        runbook_url: str = "docs/CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md#pubsub-lag-remediation",
    ) -> List[Dict[str, Any]]:
        """Construct Slack Block Kit notification for a Pub/Sub subscription lag event."""
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        is_crit = metrics.severity.value in ("CRITICAL_LAG", "STALLED_CONSUMER")
        emoji = cls.SEVERITY_EMOJIS[AlertSeverity.CRITICAL if is_crit else AlertSeverity.WARNING]
        etd_str = f"{metrics.estimated_time_to_drain_sec:.1f}s" if metrics.estimated_time_to_drain_sec else "INDEFINITE (Accumulating)"

        blocks: List[Dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} [{metrics.severity.value}] Pub/Sub Trade Orders Lag Alert",
                    "emoji": True,
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Environment:* `{environment.upper()}` | *Subscription:* `{metrics.subscription_id}` | *Timestamp:* `{now_utc}`",
                    }
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Backlog Messages:*\n*{metrics.num_undelivered_messages:,}* (SLO: < {threshold_messages:,})",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Oldest Message Age:*\n*{metrics.oldest_unacked_message_age_sec:.1f}s* (SLO: < {threshold_age_sec:.1f}s)",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Ingestion vs Drain Rate:*\nIn: `{metrics.publish_rate_msgs_per_sec:.1f}/s` | Out: `{metrics.consumption_rate_msgs_per_sec:.1f}/s`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Estimated Drain Time:*\n`{etd_str}`",
                    },
                ],
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Lag Remediation SOP", "emoji": True},
                        "url": f"https://github.com/trading-risk/trading-risk-engine/blob/main/{runbook_url}",
                        "style": "primary",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Dataflow Metrics", "emoji": True},
                        "url": f"https://console.cloud.google.com/dataflow/jobs?project=trading-risk-{environment}",
                    },
                ],
            },
        ]
        return blocks

    @classmethod
    def build_generic_incident_blocks(
        cls,
        environment: str,
        policy_name: str,
        severity: AlertSeverity,
        summary: str,
        metric_name: str,
        observed_value: str,
        threshold_value: str,
        incident_id: str,
        url: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Construct Slack Block Kit notification for a GCP Cloud Monitoring Incident."""
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        emoji = cls.SEVERITY_EMOJIS.get(severity, ":bell:")

        blocks: List[Dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} [{severity.value}] GCP Cloud Monitoring: {policy_name}",
                    "emoji": True,
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Environment:* `{environment.upper()}` | *Incident ID:* `{incident_id}` | *Timestamp:* `{now_utc}`",
                    }
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Metric:* `{metric_name}`"},
                    {"type": "mrkdwn", "text": f"*Observed Value:*\n`{observed_value}`"},
                    {"type": "mrkdwn", "text": f"*Threshold:*\n`{threshold_value}`"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n`{severity.value}`"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Incident Summary:*\n{summary}"},
            },
        ]

        if url:
            blocks.append(
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "View Incident in GCP", "emoji": True},
                            "url": url,
                        }
                    ],
                }
            )

        return blocks


class SlackDlqAlertDispatcher:
    """Dispatches Slack alerts with deduplication, cooldowns, and retries."""

    def __init__(
        self,
        webhook_url: str = "https://hooks.slack.com/services/MOCK/T000/B000",
        environment: str = "dev",
        cooldown_seconds: float = 300.0,
        max_retries: int = 3,
        retry_backoff_sec: float = 0.5,
        http_transport: Optional[Callable[[str, bytes, Dict[str, str]], Tuple[int, bytes]]] = None,
    ) -> None:
        self.webhook_url = webhook_url
        self.environment = environment
        self.cooldown_seconds = cooldown_seconds
        self.max_retries = max_retries
        self.retry_backoff_sec = retry_backoff_sec
        self.http_transport = http_transport or self._default_http_transport
        self._alert_cache: Dict[str, float] = {}
        self._suppressed_counts: Dict[str, int] = {}

        # Telemetry
        self.alerts_dispatched: int = 0
        self.alerts_suppressed: int = 0
        self.alerts_failed: int = 0
        self.retries_performed: int = 0

    @staticmethod
    def _default_http_transport(url: str, data: bytes, headers: Dict[str, str]) -> Tuple[int, bytes]:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def _should_suppress_alert(self, fingerprint: str, now: float) -> bool:
        """Check if alert is within cooldown window to prevent alert storms."""
        last_sent = self._alert_cache.get(fingerprint)
        if last_sent is not None and (now - last_sent) < self.cooldown_seconds:
            self._suppressed_counts[fingerprint] = self._suppressed_counts.get(fingerprint, 0) + 1
            self.alerts_suppressed += 1
            return True
        self._alert_cache[fingerprint] = now
        return False

    def send_slack_payload(self, payload: Dict[str, Any]) -> bool:
        """Transmit payload to webhook with exponential backoff and rate limit recovery."""
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        delay = self.retry_backoff_sec

        for attempt in range(1, self.max_retries + 1):
            try:
                status, response_bytes = self.http_transport(self.webhook_url, data, headers)
                if status in (200, 201):
                    self.alerts_dispatched += 1
                    return True
                elif status == 429:
                    # Rate limited: backoff exponentially
                    self.retries_performed += 1
                    time.sleep(delay)
                    delay *= 2.0
                elif 500 <= status < 600:
                    # Transient server error
                    self.retries_performed += 1
                    time.sleep(delay)
                    delay *= 2.0
                else:
                    logger.error("Slack webhook returned non-retryable status %d: %s", status, response_bytes)
                    self.alerts_failed += 1
                    return False
            except Exception as e:
                self.retries_performed += 1
                time.sleep(delay)
                delay *= 2.0

        self.alerts_failed += 1
        return False

    def dispatch_dlq_alert(
        self,
        topic_name: str,
        category: PoisonPillCategory,
        error_message: str,
        ticker: Optional[str] = None,
        portfolio_id: Optional[str] = None,
        delivery_attempts: int = 5,
        payload_preview: Optional[str] = None,
        bypass_cooldown: bool = False,
    ) -> bool:
        """Dispatch Slack alert for Dead-Letter Queue event with deduplication."""
        fingerprint = f"dlq:{category.value}:{ticker or '*'}:{portfolio_id or '*'}"
        now = time.time()
        if not bypass_cooldown and self._should_suppress_alert(fingerprint, now):
            logger.info("Suppressed duplicate DLQ alert for fingerprint: %s", fingerprint)
            return False

        blocks = SlackBlockKitBuilder.build_dlq_poison_pill_blocks(
            environment=self.environment,
            topic_name=topic_name,
            category=category,
            error_message=error_message,
            ticker=ticker,
            portfolio_id=portfolio_id,
            delivery_attempts=delivery_attempts,
            payload_preview=payload_preview,
        )
        payload = {
            "text": f":rotating_light: [CRITICAL] DLQ Poison Pill received on {topic_name}: {category.value}",
            "blocks": blocks,
        }
        return self.send_slack_payload(payload)

    def dispatch_lag_alert(
        self,
        metrics: PubSubLagMetrics,
        threshold_messages: int,
        threshold_age_sec: float,
        bypass_cooldown: bool = False,
    ) -> bool:
        """Dispatch Slack alert for Pub/Sub subscription lag event."""
        fingerprint = f"lag:{metrics.subscription_id}:{metrics.severity.value}"
        now = time.time()
        if not bypass_cooldown and self._should_suppress_alert(fingerprint, now):
            return False

        blocks = SlackBlockKitBuilder.build_pubsub_lag_blocks(
            environment=self.environment,
            metrics=metrics,
            threshold_messages=threshold_messages,
            threshold_age_sec=threshold_age_sec,
        )
        payload = {
            "text": f":warning: [{metrics.severity.value}] Pub/Sub lag on {metrics.subscription_id}: {metrics.num_undelivered_messages} unacked messages",
            "blocks": blocks,
        }
        return self.send_slack_payload(payload)

    def dispatch_incident_alert(
        self,
        policy_name: str,
        severity: AlertSeverity,
        summary: str,
        metric_name: str,
        observed_value: str,
        threshold_value: str,
        incident_id: str,
        url: Optional[str] = None,
        bypass_cooldown: bool = False,
    ) -> bool:
        """Dispatch generic GCP Cloud Monitoring incident alert."""
        fingerprint = f"incident:{policy_name}:{incident_id}:{severity.value}"
        now = time.time()
        # Resolved alerts always bypass cooldown so closures are never missed
        if severity != AlertSeverity.RESOLVED and not bypass_cooldown and self._should_suppress_alert(fingerprint, now):
            return False

        blocks = SlackBlockKitBuilder.build_generic_incident_blocks(
            environment=self.environment,
            policy_name=policy_name,
            severity=severity,
            summary=summary,
            metric_name=metric_name,
            observed_value=observed_value,
            threshold_value=threshold_value,
            incident_id=incident_id,
            url=url,
        )
        payload = {
            "text": f"[{severity.value}] GCP Cloud Monitoring: {policy_name}",
            "blocks": blocks,
        }
        return self.send_slack_payload(payload)
