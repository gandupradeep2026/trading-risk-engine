"""Comprehensive Zero-Cost Unit and Integration Tests for Cloud Monitoring & Slack Alerts (Day 28).

Verifies:
1. Terraform Cloud Monitoring module (HCL syntax, balanced braces, root wiring, policies, channels).
2. Monitoring configuration and canonical GCP Alert Policy Catalog.
3. Pub/Sub lag metrics calculator (backlog, oldest age, net drain rate, ETD, stall detection, Prometheus).
4. Slack Block Kit builders (DLQ poison pills, Pub/Sub lag, Cloud Monitoring incidents).
5. SlackDlqAlertDispatcher (deduplication cooldowns, retry exponential backoff on HTTP 429/5xx).
6. DeadLetterQueueMonitor (poison pill classification, forensic reporting, batch scan and alert).
7. CloudMonitoringWebhookHandler (GCP incident webhook v1.2 parsing and Slack dispatch).
8. AlertPolicyEvaluator (incident lifecycle: open on breach, close on recovery).
9. Operational telemetry and OpenMetrics / Prometheus exposition.
10. Monitoring CLI runner commands and structured reports.
11. Runbook and documentation integrity.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
import pytest

from monitoring.config import (
    AlertChannel,
    AlertPolicyDefinition,
    AlertSeverity,
    DEFAULT_ALERT_POLICIES,
    IncidentState,
    MonitoringConfig,
    PoisonPillCategory,
)
from monitoring.metrics import (
    LagSeverityTier,
    PubSubLagMetrics,
    PubSubLagMetricsCalculator,
)
from monitoring.slack_alerts import (
    SlackBlockKitBuilder,
    SlackDlqAlertDispatcher,
)
from monitoring.dlq_monitor import (
    DeadLetterQueueMonitor,
    DlqForensicRecord,
    DlqForensicReport,
)
from monitoring.webhook_handler import (
    CloudMonitoringWebhookHandler,
    GcpMonitoringIncident,
)
from monitoring.evaluator import (
    AlertIncident,
    AlertPolicyEvaluator,
)
from monitoring.telemetry import MonitoringTelemetry
from monitoring.cli import main as monitoring_cli_main


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


# ==============================================================================
# 1. Terraform Cloud Monitoring Module Tests
# ==============================================================================


class TestTerraformMonitoringModule:
    """Validates Terraform HCL manifests for Cloud Monitoring."""

    def test_monitoring_module_files_exist(self, repo_root: Path) -> None:
        module_dir = repo_root / "terraform" / "modules" / "monitoring"
        assert module_dir.is_dir(), f"Missing monitoring module directory: {module_dir}"
        assert (module_dir / "main.tf").is_file(), "Missing main.tf in monitoring module"
        assert (module_dir / "variables.tf").is_file(), "Missing variables.tf in monitoring module"
        assert (module_dir / "outputs.tf").is_file(), "Missing outputs.tf in monitoring module"
        assert (module_dir / "versions.tf").is_file(), "Missing versions.tf in monitoring module"

    def test_balanced_braces_in_monitoring_module(self, repo_root: Path) -> None:
        module_dir = repo_root / "terraform" / "modules" / "monitoring"
        for tf_file in module_dir.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            open_count = content.count("{")
            close_count = content.count("}")
            assert open_count == close_count, f"Unbalanced braces in {tf_file.name}: {open_count} != {close_count}"

    def test_root_terraform_wires_monitoring_module(self, repo_root: Path) -> None:
        root_main = (repo_root / "terraform" / "main.tf").read_text(encoding="utf-8")
        assert 'module "monitoring"' in root_main
        assert 'source = "./modules/monitoring"' in root_main
        assert "notification_slack_channel" in root_main
        assert "trade_orders_subscription_id" in root_main

        root_vars = (repo_root / "terraform" / "variables.tf").read_text(encoding="utf-8")
        assert 'variable "notification_slack_channel"' in root_vars
        assert 'variable "pubsub_lag_threshold_messages"' in root_vars
        assert 'variable "pubsub_dlq_threshold_messages"' in root_vars

        root_outputs = (repo_root / "terraform" / "outputs.tf").read_text(encoding="utf-8")
        assert "monitoring_slack_channel_id" in root_outputs
        assert "monitoring_alert_policy_ids" in root_outputs

        dev_vars = (repo_root / "terraform" / "environments" / "dev.tfvars").read_text(encoding="utf-8")
        assert "notification_slack_channel" in dev_vars
        assert "pubsub_lag_threshold_messages" in dev_vars

        prod_vars = (repo_root / "terraform" / "environments" / "prod.tfvars").read_text(encoding="utf-8")
        assert "notification_slack_channel" in prod_vars
        assert "pubsub_lag_threshold_messages" in prod_vars

    def test_notification_channels_and_alert_policies_in_module(self, repo_root: Path) -> None:
        main_tf = (repo_root / "terraform" / "modules" / "monitoring" / "main.tf").read_text(encoding="utf-8")
        # Notification channels
        assert 'resource "google_monitoring_notification_channel" "slack_alerts"' in main_tf
        assert 'resource "google_monitoring_notification_channel" "slack_dlq"' in main_tf
        assert 'resource "google_monitoring_notification_channel" "email_alerts"' in main_tf
        assert 'resource "google_monitoring_notification_channel" "pagerduty_alerts"' in main_tf

        # Alert policies
        assert 'resource "google_monitoring_alert_policy" "pubsub_subscription_lag"' in main_tf
        assert 'resource "google_monitoring_alert_policy" "pubsub_dlq_activity"' in main_tf
        assert 'resource "google_monitoring_alert_policy" "dataflow_system_lag"' in main_tf
        assert 'resource "google_monitoring_alert_policy" "cloudsql_cpu_utilization"' in main_tf

        # Aggregations & Metrics
        assert "ALIGN_MEAN" in main_tf
        assert "ALIGN_RATE" in main_tf
        assert "ALIGN_MAX" in main_tf
        assert "num_undelivered_messages" in main_tf
        assert "send_request_count" in main_tf


# ==============================================================================
# 2. Monitoring Configuration & Catalog Tests
# ==============================================================================


class TestMonitoringConfigAndPolicyCatalog:
    """Validates configuration models and default policy catalog."""

    def test_default_alert_policies_catalog(self) -> None:
        assert len(DEFAULT_ALERT_POLICIES) >= 6
        assert "pubsub_subscription_lag" in DEFAULT_ALERT_POLICIES
        assert "pubsub_dlq_message_spike" in DEFAULT_ALERT_POLICIES
        assert "dataflow_system_lag" in DEFAULT_ALERT_POLICIES
        assert "cloudsql_cpu_high" in DEFAULT_ALERT_POLICIES

        dlq_policy = DEFAULT_ALERT_POLICIES["pubsub_dlq_message_spike"]
        assert dlq_policy.severity == AlertSeverity.CRITICAL
        assert AlertChannel.SLACK in dlq_policy.channels
        assert dlq_policy.threshold_value == 0.0

        lag_policy = DEFAULT_ALERT_POLICIES["pubsub_subscription_lag"]
        assert lag_policy.threshold_value == 10000.0
        assert lag_policy.severity == AlertSeverity.CRITICAL

    def test_policy_definition_serialization(self) -> None:
        policy = DEFAULT_ALERT_POLICIES["pubsub_dlq_message_spike"]
        d = policy.to_dict()
        assert d["policy_id"] == "pubsub_dlq_message_spike"
        assert d["severity"] == "CRITICAL"
        assert "slack" in d["channels"]
        assert d["duration_seconds"] == 60

    def test_monitoring_config_defaults_and_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = MonitoringConfig()
        cfg.validate()
        assert cfg.environment == "dev"
        assert cfg.pubsub_lag_threshold_messages == 10000

        # Test env variable loading
        monkeypatch.setenv("DEPLOYMENT_ENV", "prod")
        monkeypatch.setenv("PUBSUB_LAG_THRESHOLD_MESSAGES", "20000")
        monkeypatch.setenv("CLOUDSQL_CPU_THRESHOLD", "0.85")
        cfg_env = MonitoringConfig.from_env()
        cfg_env.validate()
        assert cfg_env.environment == "prod"
        assert cfg_env.pubsub_lag_threshold_messages == 20000
        assert cfg_env.cloudsql_cpu_threshold == 0.85

    def test_monitoring_config_validation_rejections(self) -> None:
        cfg_bad_env = MonitoringConfig(environment="invalid-env")
        with pytest.raises(ValueError, match="Invalid environment"):
            cfg_bad_env.validate()

        cfg_bad_lag = MonitoringConfig(pubsub_lag_threshold_messages=-5)
        with pytest.raises(ValueError, match="strictly positive"):
            cfg_bad_lag.validate()

        cfg_bad_cpu = MonitoringConfig(cloudsql_cpu_threshold=1.5)
        with pytest.raises(ValueError, match="between 0.0 and 1.0"):
            cfg_bad_cpu.validate()


# ==============================================================================
# 3. Pub/Sub Lag Metrics Calculator Tests
# ==============================================================================


class TestPubSubLagMetricsCalculator:
    """Validates real-time calculation of lag dynamics, ETD, and Prometheus format."""

    def test_healthy_lag_metrics(self) -> None:
        calc = PubSubLagMetricsCalculator(lag_threshold_messages=10000, oldest_unacked_age_threshold_sec=300.0)
        metrics = calc.compute_lag_metrics(
            subscription_id="sub-orders-dev",
            num_undelivered_messages=500,
            oldest_unacked_message_age_sec=15.0,
            publish_rate_msgs_per_sec=200.0,
            consumption_rate_msgs_per_sec=250.0,
            current_timestamp=1730000000.0,
        )
        assert metrics.severity == LagSeverityTier.HEALTHY
        assert not metrics.is_stalled
        assert metrics.net_drain_rate_msgs_per_sec == 50.0
        assert metrics.estimated_time_to_drain_sec == 10.0  # 500 / 50 = 10s

    def test_moderate_lag_tier(self) -> None:
        calc = PubSubLagMetricsCalculator(lag_threshold_messages=10000, oldest_unacked_age_threshold_sec=300.0)
        metrics = calc.compute_lag_metrics(
            subscription_id="sub-orders-dev",
            num_undelivered_messages=5500,  # > 50% of 10,000
            oldest_unacked_message_age_sec=100.0,
            publish_rate_msgs_per_sec=200.0,
            consumption_rate_msgs_per_sec=210.0,
        )
        assert metrics.severity == LagSeverityTier.MODERATE_LAG

    def test_critical_lag_on_backlog_or_age_breach(self) -> None:
        calc = PubSubLagMetricsCalculator(lag_threshold_messages=10000, oldest_unacked_age_threshold_sec=300.0)
        # Backlog breach
        m1 = calc.compute_lag_metrics(
            subscription_id="sub-orders-dev",
            num_undelivered_messages=12000,
            oldest_unacked_message_age_sec=50.0,
            publish_rate_msgs_per_sec=200.0,
            consumption_rate_msgs_per_sec=250.0,
        )
        assert m1.severity == LagSeverityTier.CRITICAL_LAG

        # Oldest age breach
        m2 = calc.compute_lag_metrics(
            subscription_id="sub-orders-dev",
            num_undelivered_messages=2000,
            oldest_unacked_message_age_sec=350.0,
            publish_rate_msgs_per_sec=200.0,
            consumption_rate_msgs_per_sec=250.0,
        )
        assert m2.severity == LagSeverityTier.CRITICAL_LAG

    def test_stalled_consumer_detection(self) -> None:
        calc = PubSubLagMetricsCalculator(lag_threshold_messages=10000, oldest_unacked_age_threshold_sec=300.0)
        metrics = calc.compute_lag_metrics(
            subscription_id="sub-orders-dev",
            num_undelivered_messages=2500,
            oldest_unacked_message_age_sec=45.0,
            publish_rate_msgs_per_sec=150.0,
            consumption_rate_msgs_per_sec=0.0,  # Consumer stopped
        )
        assert metrics.is_stalled
        assert metrics.severity == LagSeverityTier.STALLED_CONSUMER
        assert metrics.estimated_time_to_drain_sec is None

    def test_negative_drain_rate_yields_none_etd(self) -> None:
        calc = PubSubLagMetricsCalculator()
        metrics = calc.compute_lag_metrics(
            subscription_id="sub-orders-dev",
            num_undelivered_messages=1000,
            oldest_unacked_message_age_sec=20.0,
            publish_rate_msgs_per_sec=300.0,
            consumption_rate_msgs_per_sec=100.0,  # Accumulating
        )
        assert metrics.net_drain_rate_msgs_per_sec == -200.0
        assert metrics.estimated_time_to_drain_sec is None

    def test_simulate_lag_from_mock(self) -> None:
        calc = PubSubLagMetricsCalculator()
        now = 1730001000.0
        oldest = 1730000800.0  # 200s ago
        metrics = calc.simulate_lag_from_mock(
            subscription_id="sub-mock",
            backlog_count=3000,
            oldest_msg_timestamp=oldest,
            publish_rate=100.0,
            consumption_rate=120.0,
            now_timestamp=now,
        )
        assert metrics.num_undelivered_messages == 3000
        assert metrics.oldest_unacked_message_age_sec == 200.0
        assert metrics.net_drain_rate_msgs_per_sec == 20.0
        assert metrics.estimated_time_to_drain_sec == 150.0  # 3000 / 20

    def test_prometheus_exposition_format(self) -> None:
        calc = PubSubLagMetricsCalculator()
        metrics = calc.compute_lag_metrics(
            subscription_id="sub-trade-orders-dev",
            num_undelivered_messages=7500,
            oldest_unacked_message_age_sec=120.5,
            publish_rate_msgs_per_sec=200.0,
            consumption_rate_msgs_per_sec=250.0,
        )
        prom = metrics.to_prometheus_text()
        assert "cloud_monitoring_pubsub_backlog_messages" in prom
        assert 'subscription_id="sub-trade-orders-dev"' in prom
        assert "7500" in prom
        assert "cloud_monitoring_pubsub_drain_time_seconds" in prom
        assert "cloud_monitoring_pubsub_consumer_stalled" in prom


# ==============================================================================
# 4. Slack Block Kit Alert Formatter Tests
# ==============================================================================


class TestSlackBlockKitBuilder:
    """Validates structural correctness of Slack Block Kit JSON objects."""

    def test_build_dlq_poison_pill_blocks(self) -> None:
        blocks = SlackBlockKitBuilder.build_dlq_poison_pill_blocks(
            environment="prod",
            topic_name="top-trade-orders-dlq-prod",
            category=PoisonPillCategory.CROSSED_QUOTE_ERROR,
            error_message="Crossed quote detected: bid 152.00 > ask 151.80",
            ticker="AAPL",
            portfolio_id="PORT-EQ-01",
            delivery_attempts=5,
            payload_preview='{"ticker": "AAPL", "bid": 152.00, "ask": 151.80}',
        )
        assert len(blocks) >= 6
        header = blocks[0]
        assert header["type"] == "header"
        assert "Trade Order DLQ Poison Pill Received" in header["text"]["text"]

        context = blocks[1]
        assert context["type"] == "context"
        assert "PROD" in context["elements"][0]["text"]

        fields_sec = blocks[3]
        fields_text = " ".join(f["text"] for f in fields_sec["fields"])
        assert "CROSSED_QUOTE_ERROR" in fields_text
        assert "AAPL" in fields_text
        assert "PORT-EQ-01" in fields_text

        # Verify buttons in actions block
        actions = blocks[-1]
        assert actions["type"] == "actions"
        assert any("DLQ Runbook" in btn["text"]["text"] for btn in actions["elements"])

    def test_build_pubsub_lag_blocks(self) -> None:
        calc = PubSubLagMetricsCalculator()
        metrics = calc.compute_lag_metrics(
            subscription_id="sub-orders-prod",
            num_undelivered_messages=15000,
            oldest_unacked_message_age_sec=420.0,
            publish_rate_msgs_per_sec=300.0,
            consumption_rate_msgs_per_sec=200.0,
        )
        blocks = SlackBlockKitBuilder.build_pubsub_lag_blocks(
            environment="prod",
            metrics=metrics,
            threshold_messages=10000,
            threshold_age_sec=300.0,
        )
        assert len(blocks) >= 4
        assert "CRITICAL_LAG" in blocks[0]["text"]["text"]
        fields_sec = blocks[3]
        fields_text = " ".join(f["text"] for f in fields_sec["fields"])
        assert "15,000" in fields_text
        assert "420.0s" in fields_text

    def test_build_generic_incident_blocks(self) -> None:
        blocks = SlackBlockKitBuilder.build_generic_incident_blocks(
            environment="dev",
            policy_name="Cloud SQL PostgreSQL High CPU",
            severity=AlertSeverity.WARNING,
            summary="CPU utilization reached 88.5%",
            metric_name="cloudsql.googleapis.com/database/cpu/utilization",
            observed_value="88.5%",
            threshold_value="80.0%",
            incident_id="inc-sql-001",
            url="https://console.cloud.google.com/monitoring",
        )
        assert len(blocks) >= 5
        assert "[WARNING]" in blocks[0]["text"]["text"]
        assert "inc-sql-001" in blocks[1]["elements"][0]["text"]


# ==============================================================================
# 5. Slack DLQ Alert Dispatcher & Retry Tests
# ==============================================================================


class TestSlackDlqAlertDispatcher:
    """Validates webhook transport, alert deduplication cooldown, and retry backoff."""

    def test_dispatch_dlq_alert_success(self) -> None:
        sent_requests: List[Tuple[str, bytes, dict]] = []

        def mock_transport(url: str, data: bytes, headers: dict) -> Tuple[int, bytes]:
            sent_requests.append((url, data, headers))
            return 200, b'{"ok": true}'

        dispatcher = SlackDlqAlertDispatcher(
            webhook_url="https://hooks.slack.com/services/TEST/001",
            environment="dev",
            http_transport=mock_transport,
        )
        success = dispatcher.dispatch_dlq_alert(
            topic_name="top-trade-orders-dlq-dev",
            category=PoisonPillCategory.JSON_DECODE_ERROR,
            error_message="Invalid JSON payload",
            ticker="NVDA",
            portfolio_id="PORT-MAIN",
        )
        assert success
        assert len(sent_requests) == 1
        payload = json.loads(sent_requests[0][1].decode("utf-8"))
        assert "blocks" in payload
        assert dispatcher.alerts_dispatched == 1
        assert dispatcher.alerts_suppressed == 0

    def test_alert_deduplication_cooldown_suppression(self) -> None:
        sent_count = 0

        def mock_transport(url: str, data: bytes, headers: dict) -> Tuple[int, bytes]:
            nonlocal sent_count
            sent_count += 1
            return 200, b'{"ok": true}'

        dispatcher = SlackDlqAlertDispatcher(
            cooldown_seconds=300.0,
            http_transport=mock_transport,
        )
        # First alert goes through
        res1 = dispatcher.dispatch_dlq_alert(
            topic_name="top-dlq",
            category=PoisonPillCategory.NON_POSITIVE_PRICE_ERROR,
            error_message="Price -10.0 <= 0",
            ticker="TSLA",
        )
        assert res1 is True
        assert sent_count == 1

        # Second identical alert within cooldown is suppressed
        res2 = dispatcher.dispatch_dlq_alert(
            topic_name="top-dlq",
            category=PoisonPillCategory.NON_POSITIVE_PRICE_ERROR,
            error_message="Price -15.0 <= 0",
            ticker="TSLA",
        )
        assert res2 is False
        assert sent_count == 1
        assert dispatcher.alerts_suppressed == 1

        # Bypassing cooldown forces send
        res3 = dispatcher.dispatch_dlq_alert(
            topic_name="top-dlq",
            category=PoisonPillCategory.NON_POSITIVE_PRICE_ERROR,
            error_message="Price -15.0 <= 0",
            ticker="TSLA",
            bypass_cooldown=True,
        )
        assert res3 is True
        assert sent_count == 2

    def test_http_429_rate_limit_backoff_and_retry(self) -> None:
        attempts = 0

        def mock_transport(url: str, data: bytes, headers: dict) -> Tuple[int, bytes]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return 429, b"Rate limited"
            return 200, b'{"ok": true}'

        dispatcher = SlackDlqAlertDispatcher(
            max_retries=3,
            retry_backoff_sec=0.01,
            http_transport=mock_transport,
        )
        success = dispatcher.send_slack_payload({"text": "Test alert"})
        assert success is True
        assert attempts == 2
        assert dispatcher.retries_performed == 1
        assert dispatcher.alerts_dispatched == 1

    def test_http_500_transient_error_retry(self) -> None:
        attempts = 0

        def mock_transport(url: str, data: bytes, headers: dict) -> Tuple[int, bytes]:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                return 503, b"Service Unavailable"
            return 200, b'{"ok": true}'

        dispatcher = SlackDlqAlertDispatcher(
            max_retries=3,
            retry_backoff_sec=0.01,
            http_transport=mock_transport,
        )
        success = dispatcher.send_slack_payload({"text": "Test alert"})
        assert success is True
        assert attempts == 3
        assert dispatcher.retries_performed == 2

    def test_non_retryable_http_error_fails_immediately(self) -> None:
        attempts = 0

        def mock_transport(url: str, data: bytes, headers: dict) -> Tuple[int, bytes]:
            nonlocal attempts
            attempts += 1
            return 400, b"Bad Request"

        dispatcher = SlackDlqAlertDispatcher(
            max_retries=3,
            retry_backoff_sec=0.01,
            http_transport=mock_transport,
        )
        success = dispatcher.send_slack_payload({"text": "Test alert"})
        assert success is False
        assert attempts == 1
        assert dispatcher.alerts_failed == 1


# ==============================================================================
# 6. Dead-Letter Queue Forensic Monitor Tests
# ==============================================================================


class TestDeadLetterQueueMonitor:
    """Validates poison pill classification, report generation, and batch alerting."""

    def test_payload_classification_rules(self) -> None:
        monitor = DeadLetterQueueMonitor()

        # JSON Decode
        cat1, _ = monitor.classify_payload("{bad json: true}", "JSONDecodeError")
        assert cat1 == PoisonPillCategory.JSON_DECODE_ERROR

        # Crossed Quote
        cat2, _ = monitor.classify_payload({"bid": 105.0, "ask": 100.0}, "Crossed quote")
        assert cat2 == PoisonPillCategory.CROSSED_QUOTE_ERROR

        # Non-positive Price
        cat3, _ = monitor.classify_payload({"ticker": "AAPL", "price": -50.0}, "price must be positive")
        assert cat3 == PoisonPillCategory.NON_POSITIVE_PRICE_ERROR

        # Non-positive Quantity
        cat4, _ = monitor.classify_payload({"ticker": "AAPL", "quantity": 0}, "quantity must be positive")
        assert cat4 == PoisonPillCategory.NON_POSITIVE_QUANTITY_ERROR

        # Schema Validation
        cat5, _ = monitor.classify_payload({"random_field": 1}, "Missing required field order_id")
        assert cat5 == PoisonPillCategory.SCHEMA_VALIDATION_ERROR

    def test_inspect_records_forensic_report(self) -> None:
        monitor = DeadLetterQueueMonitor()
        records = [
            {
                "order_id": "ord-001",
                "ticker": "AAPL",
                "portfolio_id": "PORT-A",
                "raw_payload": {"price": -10.0},
                "error_message": "Negative price",
            },
            {
                "order_id": "ord-002",
                "ticker": "AAPL",
                "portfolio_id": "PORT-A",
                "raw_payload": "{malformed",
                "error_message": "JSON syntax error",
            },
            {
                "order_id": "ord-003",
                "ticker": "GOOGL",
                "portfolio_id": "PORT-B",
                "raw_payload": {"bid": 200.0, "ask": 190.0},
                "error_message": "Crossed quote",
            },
        ]
        report = monitor.inspect_records(records, source_topic="top-dlq-test")
        assert report.total_dead_letters == 3
        assert "AAPL" in report.affected_tickers
        assert "GOOGL" in report.affected_tickers
        assert "PORT-A" in report.affected_portfolios
        assert report.categories_breakdown.get(PoisonPillCategory.NON_POSITIVE_PRICE_ERROR.value) == 1
        assert report.categories_breakdown.get(PoisonPillCategory.JSON_DECODE_ERROR.value) == 1
        assert report.categories_breakdown.get(PoisonPillCategory.CROSSED_QUOTE_ERROR.value) == 1

        md = report.to_markdown()
        assert "Dead-Letter Queue Forensic Incident Report" in md
        assert "AAPL, GOOGL" in md or "GOOGL, AAPL" in md

    def test_scan_and_alert_dispatches_grouped_notifications(self) -> None:
        monitor = DeadLetterQueueMonitor()
        sent_messages: List[str] = []

        def mock_transport(url: str, data: bytes, headers: dict) -> Tuple[int, bytes]:
            sent_messages.append(data.decode("utf-8"))
            return 200, b'{"ok": true}'

        dispatcher = SlackDlqAlertDispatcher(http_transport=mock_transport)
        records = [
            {
                "order_id": "ord-01",
                "ticker": "NVDA",
                "portfolio_id": "PORT-TECH",
                "raw_payload": {"price": -5.0},
                "error_message": "Price error",
            },
            {
                "order_id": "ord-02",
                "ticker": "NVDA",
                "portfolio_id": "PORT-TECH",
                "raw_payload": {"price": -6.0},
                "error_message": "Price error",
            },
            {
                "order_id": "ord-03",
                "ticker": "MSFT",
                "portfolio_id": "PORT-MAIN",
                "raw_payload": {"bid": 50.0, "ask": 40.0},
                "error_message": "Crossed",
            },
        ]
        # Should group the 2 NVDA records together, sending 2 alerts total (1 for NVDA, 1 for MSFT)
        alerts_sent = monitor.scan_and_alert(records, dispatcher, bypass_cooldown=True)
        assert alerts_sent == 2
        assert len(sent_messages) == 2


# ==============================================================================
# 7. GCP Cloud Monitoring Webhook Handler Tests
# ==============================================================================


class TestCloudMonitoringWebhookHandler:
    """Validates GCP webhook payload parsing and Slack notification dispatch."""

    def test_parse_gcp_webhook_payload(self) -> None:
        handler = CloudMonitoringWebhookHandler()
        raw_webhook = {
            "version": "1.2",
            "incident": {
                "incident_id": "0.inc-999",
                "scoping_project_id": "trading-risk-prod",
                "resource_id": "sub-trade-orders-dataflow-prod",
                "resource_name": "sub-trade-orders-dataflow-prod",
                "resource_type": "pubsub_subscription",
                "metric": {
                    "type": "pubsub.googleapis.com/subscription/num_undelivered_messages",
                    "displayName": "Undelivered messages",
                },
                "started_at": 1730005000,
                "state": "open",
                "summary": "Pub/Sub subscription backlog exceeded 10,000",
                "url": "https://console.cloud.google.com/monitoring/alerting/incidents/0.inc-999",
                "condition_name": "Backlog threshold",
                "policy_name": "[PROD] Pub/Sub Trade Orders Subscription Lag",
                "observed_value": "13500",
                "condition": {
                    "conditionThreshold": {
                        "thresholdValue": 10000.0,
                    }
                },
            },
        }
        incident = handler.parse_webhook_payload(raw_webhook)
        assert incident.incident_id == "0.inc-999"
        assert incident.state == IncidentState.OPEN
        assert incident.severity == AlertSeverity.WARNING or incident.severity == AlertSeverity.CRITICAL
        assert incident.observed_value == "13500"
        assert incident.threshold_value == "10000.0"

    def test_parse_resolved_incident(self) -> None:
        handler = CloudMonitoringWebhookHandler()
        webhook_closed = {
            "incident": {
                "incident_id": "0.inc-100",
                "state": "closed",
                "policy_name": "Pub/Sub Lag",
                "ended_at": 1730006000,
            }
        }
        incident = handler.parse_webhook_payload(webhook_closed)
        assert incident.state == IncidentState.CLOSED
        assert incident.severity == AlertSeverity.RESOLVED

    def test_handle_webhook_dispatches_to_slack(self) -> None:
        handler = CloudMonitoringWebhookHandler()
        dispatched: List[str] = []

        def mock_transport(url: str, data: bytes, headers: dict) -> Tuple[int, bytes]:
            dispatched.append(data.decode("utf-8"))
            return 200, b'{"ok": true}'

        dispatcher = SlackDlqAlertDispatcher(http_transport=mock_transport)
        raw_webhook = {
            "incident": {
                "incident_id": "inc-01",
                "policy_name": "Dataflow Lag Alert",
                "state": "open",
                "summary": "Dataflow lag > 120s",
                "metric": {"type": "dataflow.googleapis.com/job/system_lag"},
                "observed_value": "150s",
                "url": "https://console.cloud.google.com",
            }
        }
        success = handler.handle_webhook(raw_webhook, dispatcher, bypass_cooldown=True)
        assert success is True
        assert len(dispatched) == 1
        assert "Dataflow Lag Alert" in dispatched[0]


# ==============================================================================
# 8. Alert Policy Evaluator & Incident Lifecycle Tests
# ==============================================================================


class TestAlertPolicyEvaluator:
    """Validates local simulation of Google Cloud Monitoring alert evaluation."""

    def test_metric_below_threshold_no_incident(self) -> None:
        evaluator = AlertPolicyEvaluator()
        inc = evaluator.evaluate_metric("pubsub_subscription_lag", observed_value=5000.0)
        assert inc is None
        assert len(evaluator.active_incidents) == 0

    def test_metric_breach_opens_and_resolves_incident(self) -> None:
        alerts_sent: List[str] = []

        def mock_transport(url: str, data: bytes, headers: dict) -> Tuple[int, bytes]:
            alerts_sent.append(data.decode("utf-8"))
            return 200, b'{"ok": true}'

        dispatcher = SlackDlqAlertDispatcher(http_transport=mock_transport)
        evaluator = AlertPolicyEvaluator(dispatcher=dispatcher)

        # 1. Breach opens incident
        inc_open = evaluator.evaluate_metric(
            "pubsub_subscription_lag",
            observed_value=12500.0,
            timestamp=1730000000.0,
        )
        assert inc_open is not None
        assert inc_open.state == IncidentState.OPEN
        assert "pubsub_subscription_lag" in evaluator.active_incidents
        assert len(alerts_sent) == 1

        # 2. Metric normalizes: resolves incident
        inc_closed = evaluator.evaluate_metric(
            "pubsub_subscription_lag",
            observed_value=3000.0,
            timestamp=1730000300.0,
        )
        assert inc_closed is not None
        assert inc_closed.state == IncidentState.CLOSED
        assert inc_closed.closed_at == 1730000300.0
        assert "pubsub_subscription_lag" not in evaluator.active_incidents
        assert len(alerts_sent) == 2

    def test_evaluate_batch(self) -> None:
        evaluator = AlertPolicyEvaluator()
        metrics = {
            "pubsub_subscription_lag": 15000.0,  # Breach
            "dataflow_system_lag": 180.0,  # Breach
            "cloudsql_cpu_high": 0.40,  # Normal
        }
        incidents = evaluator.evaluate_batch(metrics)
        assert len(incidents) == 2
        assert len(evaluator.active_incidents) == 2


# ==============================================================================
# 9. Operational Telemetry & Prometheus Tests
# ==============================================================================


class TestMonitoringTelemetry:
    """Validates operational metrics tracking and Prometheus exposition."""

    def test_telemetry_counters(self) -> None:
        tel = MonitoringTelemetry()
        tel.record_evaluation()
        tel.record_incident_opened()
        tel.record_incident_closed()
        tel.record_slack_alert(success=True)
        tel.record_slack_alert(success=False)
        tel.record_slack_alert(success=True, suppressed=True)
        tel.record_dlq_scan(count=5)

        d = tel.to_dict()
        assert d["evaluations_total"] == 1
        assert d["incidents_opened_total"] == 1
        assert d["incidents_closed_total"] == 1
        assert d["slack_alerts_dispatched_total"] == 1
        assert d["slack_alerts_failed_total"] == 1
        assert d["slack_alerts_suppressed_total"] == 1
        assert d["dlq_records_scanned_total"] == 5

        prom = tel.format_prometheus_text()
        assert "cloud_monitoring_evaluations_total 1" in prom
        assert "cloud_monitoring_incidents_opened_total 1" in prom
        assert "cloud_monitoring_slack_alerts_dispatched_total 1" in prom
        assert "cloud_monitoring_dlq_records_scanned_total 5" in prom


# ==============================================================================
# 10. Monitoring CLI Runner Tests
# ==============================================================================


class TestMonitoringCLI:
    """Validates CLI flags and operations."""

    def test_cli_validate_policies(self, capsys: pytest.CaptureFixture) -> None:
        rc = monitoring_cli_main(["--validate-policies"])
        assert rc == 0
        captured = capsys.readouterr().out
        assert "Validated" in captured
        assert "Pub/Sub Trade Orders Subscription Lag" in captured

    def test_cli_check_lag(self, capsys: pytest.CaptureFixture) -> None:
        rc = monitoring_cli_main(["--check-lag", "--backlog-count", "15000", "--oldest-age-sec", "400"])
        assert rc == 0
        captured = capsys.readouterr().out
        assert "Pub/Sub Subscription Lag Metrics" in captured
        assert "15,000" in captured
        assert "CRITICAL_LAG" in captured

    def test_cli_inspect_dlq(self, capsys: pytest.CaptureFixture) -> None:
        rc = monitoring_cli_main(["--inspect-dlq"])
        assert rc == 0
        captured = capsys.readouterr().out
        assert "Dead-Letter Queue Forensic Incident Report" in captured
        assert "AAPL" in captured

    def test_cli_simulate_alert(self, capsys: pytest.CaptureFixture) -> None:
        rc = monitoring_cli_main(["--simulate-alert"])
        assert rc == 0
        captured = capsys.readouterr().out
        assert "Simulated Slack DLQ Alert Dispatched" in captured

    def test_cli_json_report(self, capsys: pytest.CaptureFixture) -> None:
        rc = monitoring_cli_main(["--validate-policies", "--check-lag", "--json-report"])
        assert rc == 0
        captured = capsys.readouterr().out
        data = json.loads(captured)
        assert data["status"] == "SUCCESS"
        assert "alert_policies" in data
        assert "lag_metrics" in data

    def test_cli_export_prometheus(self, capsys: pytest.CaptureFixture) -> None:
        rc = monitoring_cli_main(["--check-lag", "--export-prometheus"])
        assert rc == 0
        captured = capsys.readouterr().out
        assert "cloud_monitoring_pubsub_backlog_messages" in captured


# ==============================================================================
# 11. Runbook & Documentation Integrity Tests
# ==============================================================================


class TestRunbookAndDocumentation:
    """Validates that documentation and runbooks are maintained."""

    def test_monitoring_runbook_exists_and_valid(self, repo_root: Path) -> None:
        runbook = repo_root / "docs" / "CLOUD_MONITORING_AND_ALERTING_RUNBOOK.md"
        assert runbook.is_file(), f"Missing runbook at {runbook}"
        content = runbook.read_text(encoding="utf-8")
        assert len(content) > 500
        assert "#pubsub-lag-remediation" in content
        assert "#dlq-triage-sop" in content
        assert "#dataflow-lag-remediation" in content
        assert "#cloudsql-cpu-remediation" in content
