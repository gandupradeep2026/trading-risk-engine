"""Pub/Sub Lag Metrics Calculator & Prometheus Formatter.

Calculates undelivered message backlog, oldest unacknowledged message age,
net drain rate, estimated time to drain (ETD), consumer stall detection,
and formats metrics according to OpenMetrics / Prometheus exposition standard.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class LagSeverityTier(str, Enum):
    """Consumer subscription lag severity classification."""

    HEALTHY = "HEALTHY"
    MODERATE_LAG = "MODERATE_LAG"
    CRITICAL_LAG = "CRITICAL_LAG"
    STALLED_CONSUMER = "STALLED_CONSUMER"


@dataclass
class PubSubLagMetrics:
    """Snapshot of computed Pub/Sub subscription lag metrics."""

    subscription_id: str
    timestamp: float
    num_undelivered_messages: int
    oldest_unacked_message_age_sec: float
    publish_rate_msgs_per_sec: float
    consumption_rate_msgs_per_sec: float
    net_drain_rate_msgs_per_sec: float
    estimated_time_to_drain_sec: Optional[float]
    is_stalled: bool
    severity: LagSeverityTier

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics snapshot to dictionary."""
        return {
            "subscription_id": self.subscription_id,
            "timestamp": self.timestamp,
            "num_undelivered_messages": self.num_undelivered_messages,
            "oldest_unacked_message_age_sec": round(self.oldest_unacked_message_age_sec, 2),
            "publish_rate_msgs_per_sec": round(self.publish_rate_msgs_per_sec, 2),
            "consumption_rate_msgs_per_sec": round(self.consumption_rate_msgs_per_sec, 2),
            "net_drain_rate_msgs_per_sec": round(self.net_drain_rate_msgs_per_sec, 2),
            "estimated_time_to_drain_sec": (
                round(self.estimated_time_to_drain_sec, 2)
                if self.estimated_time_to_drain_sec is not None
                else None
            ),
            "is_stalled": self.is_stalled,
            "severity": self.severity.value,
        }

    def to_prometheus_text(self) -> str:
        """Render metrics into standard Prometheus exposition format."""
        sub = self.subscription_id
        etd_val = self.estimated_time_to_drain_sec if self.estimated_time_to_drain_sec is not None else -1.0
        lines = [
            "# HELP cloud_monitoring_pubsub_backlog_messages Undelivered message backlog in subscription.",
            "# TYPE cloud_monitoring_pubsub_backlog_messages gauge",
            f'cloud_monitoring_pubsub_backlog_messages{{subscription_id="{sub}"}} {self.num_undelivered_messages}',
            "# HELP cloud_monitoring_pubsub_oldest_unacked_seconds Age of oldest unacknowledged message in seconds.",
            "# TYPE cloud_monitoring_pubsub_oldest_unacked_seconds gauge",
            f'cloud_monitoring_pubsub_oldest_unacked_seconds{{subscription_id="{sub}"}} {self.oldest_unacked_message_age_sec:.2f}',
            "# HELP cloud_monitoring_pubsub_publish_rate_msgs_per_sec Message publishing rate into subscription topic.",
            "# TYPE cloud_monitoring_pubsub_publish_rate_msgs_per_sec gauge",
            f'cloud_monitoring_pubsub_publish_rate_msgs_per_sec{{subscription_id="{sub}"}} {self.publish_rate_msgs_per_sec:.2f}',
            "# HELP cloud_monitoring_pubsub_consumption_rate_msgs_per_sec Message acknowledgment rate by consumers.",
            "# TYPE cloud_monitoring_pubsub_consumption_rate_msgs_per_sec gauge",
            f'cloud_monitoring_pubsub_consumption_rate_msgs_per_sec{{subscription_id="{sub}"}} {self.consumption_rate_msgs_per_sec:.2f}',
            "# HELP cloud_monitoring_pubsub_drain_time_seconds Estimated time to drain backlog in seconds (-1 if growing).",
            "# TYPE cloud_monitoring_pubsub_drain_time_seconds gauge",
            f'cloud_monitoring_pubsub_drain_time_seconds{{subscription_id="{sub}"}} {etd_val:.2f}',
            "# HELP cloud_monitoring_pubsub_consumer_stalled 1 if consumer is completely stalled, 0 otherwise.",
            "# TYPE cloud_monitoring_pubsub_consumer_stalled gauge",
            f'cloud_monitoring_pubsub_consumer_stalled{{subscription_id="{sub}"}} {1 if self.is_stalled else 0}',
        ]
        return "\n".join(lines)


class PubSubLagMetricsCalculator:
    """Calculates streaming subscription lag and drain dynamics."""

    def __init__(
        self,
        lag_threshold_messages: int = 10000,
        oldest_unacked_age_threshold_sec: float = 300.0,
    ) -> None:
        self.lag_threshold_messages = lag_threshold_messages
        self.oldest_unacked_age_threshold_sec = oldest_unacked_age_threshold_sec

    def compute_lag_metrics(
        self,
        subscription_id: str,
        num_undelivered_messages: int,
        oldest_unacked_message_age_sec: float,
        publish_rate_msgs_per_sec: float,
        consumption_rate_msgs_per_sec: float,
        current_timestamp: Optional[float] = None,
    ) -> PubSubLagMetrics:
        """Compute holistic lag dynamics and severity tier."""
        ts = current_timestamp if current_timestamp is not None else time.time()
        net_drain_rate = consumption_rate_msgs_per_sec - publish_rate_msgs_per_sec

        # Calculate Estimated Time to Drain (ETD)
        etd_sec: Optional[float]
        if num_undelivered_messages <= 0:
            etd_sec = 0.0
        elif net_drain_rate > 0:
            etd_sec = num_undelivered_messages / net_drain_rate
        else:
            # Backlog is accumulating or steady state with zero drain
            etd_sec = None

        # Check stalled condition: messages waiting, zero consumption
        is_stalled = (
            num_undelivered_messages > 0
            and consumption_rate_msgs_per_sec <= 0.0
            and (publish_rate_msgs_per_sec > 0.0 or oldest_unacked_message_age_sec > 60.0)
        )

        # Classify severity
        if is_stalled:
            severity = LagSeverityTier.STALLED_CONSUMER
        elif (
            num_undelivered_messages >= self.lag_threshold_messages
            or oldest_unacked_message_age_sec >= self.oldest_unacked_age_threshold_sec
        ):
            severity = LagSeverityTier.CRITICAL_LAG
        elif (
            num_undelivered_messages >= (self.lag_threshold_messages * 0.5)
            or oldest_unacked_message_age_sec >= (self.oldest_unacked_age_threshold_sec * 0.5)
        ):
            severity = LagSeverityTier.MODERATE_LAG
        else:
            severity = LagSeverityTier.HEALTHY

        return PubSubLagMetrics(
            subscription_id=subscription_id,
            timestamp=ts,
            num_undelivered_messages=max(0, num_undelivered_messages),
            oldest_unacked_message_age_sec=max(0.0, oldest_unacked_message_age_sec),
            publish_rate_msgs_per_sec=max(0.0, publish_rate_msgs_per_sec),
            consumption_rate_msgs_per_sec=max(0.0, consumption_rate_msgs_per_sec),
            net_drain_rate_msgs_per_sec=net_drain_rate,
            estimated_time_to_drain_sec=etd_sec,
            is_stalled=is_stalled,
            severity=severity,
        )

    def simulate_lag_from_mock(
        self,
        subscription_id: str,
        backlog_count: int,
        oldest_msg_timestamp: float,
        publish_rate: float,
        consumption_rate: float,
        now_timestamp: Optional[float] = None,
    ) -> PubSubLagMetrics:
        """Helper to simulate lag calculation from mock queues or testing pipelines."""
        now = now_timestamp if now_timestamp is not None else time.time()
        age_sec = max(0.0, now - oldest_msg_timestamp) if backlog_count > 0 else 0.0
        return self.compute_lag_metrics(
            subscription_id=subscription_id,
            num_undelivered_messages=backlog_count,
            oldest_unacked_message_age_sec=age_sec,
            publish_rate_msgs_per_sec=publish_rate,
            consumption_rate_msgs_per_sec=consumption_rate,
            current_timestamp=now,
        )
