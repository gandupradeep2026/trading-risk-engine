"""Command-Line Interface (CLI) for Cloud Monitoring, Pub/Sub Lag & Slack Alerts."""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from monitoring.config import MonitoringConfig, DEFAULT_ALERT_POLICIES, PoisonPillCategory
from monitoring.metrics import PubSubLagMetricsCalculator
from monitoring.dlq_monitor import DeadLetterQueueMonitor
from monitoring.slack_alerts import SlackDlqAlertDispatcher
from monitoring.evaluator import AlertPolicyEvaluator
from monitoring.telemetry import MonitoringTelemetry


def create_parser() -> argparse.ArgumentParser:
    """Construct command-line argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m monitoring.cli",
        description="Trading Risk Engine: Cloud Monitoring, Pub/Sub Lag & Slack DLQ Alert CLI",
    )
    parser.add_argument(
        "--validate-policies",
        action="store_true",
        help="Validate all Cloud Monitoring alert policy specifications and display summary.",
    )
    parser.add_argument(
        "--check-lag",
        action="store_true",
        help="Compute current Pub/Sub subscription lag metrics and drain estimates.",
    )
    parser.add_argument(
        "--backlog-count",
        type=int,
        default=12500,
        help="Simulated backlog count for --check-lag.",
    )
    parser.add_argument(
        "--oldest-age-sec",
        type=float,
        default=350.0,
        help="Simulated oldest unacked message age in seconds.",
    )
    parser.add_argument(
        "--inspect-dlq",
        action="store_true",
        help="Inspect sample Dead-Letter Queue records and generate forensic report.",
    )
    parser.add_argument(
        "--simulate-alert",
        action="store_true",
        help="Simulate a critical DLQ poison-pill alert and test Slack dispatch.",
    )
    parser.add_argument(
        "--json-report",
        action="store_true",
        help="Output structured JSON summary report to stdout.",
    )
    parser.add_argument(
        "--export-prometheus",
        action="store_true",
        help="Output operational monitoring metrics in Prometheus format.",
    )
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        help="Target environment (dev, staging, prod).",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Execute CLI runner commands."""
    parser = create_parser()
    args = parser.parse_args(argv)

    config = MonitoringConfig.from_env()
    config.environment = args.environment
    config.validate()

    telemetry = MonitoringTelemetry()
    calculator = PubSubLagMetricsCalculator(
        lag_threshold_messages=config.pubsub_lag_threshold_messages,
        oldest_unacked_age_threshold_sec=config.pubsub_oldest_unacked_age_seconds,
    )
    dlq_monitor = DeadLetterQueueMonitor()

    # Capture mock outputs if testing
    mock_sent_payloads: List[str] = []

    def mock_http_transport(url: str, data: bytes, headers: dict) -> tuple:
        mock_sent_payloads.append(data.decode("utf-8"))
        return 200, b'{"ok": true}'

    dispatcher = SlackDlqAlertDispatcher(
        webhook_url=config.slack_webhook_url,
        environment=config.environment,
        cooldown_seconds=config.alert_cooldown_seconds,
        http_transport=mock_http_transport,
    )

    report_data: dict = {
        "status": "SUCCESS",
        "environment": config.environment,
        "project_id": config.project_id,
    }

    if args.validate_policies:
        policies_list = [p.to_dict() for p in DEFAULT_ALERT_POLICIES.values()]
        report_data["alert_policies_count"] = len(policies_list)
        report_data["alert_policies"] = policies_list
        if not args.json_report:
            print(f"Validated {len(policies_list)} Cloud Monitoring Alert Policies:")
            for p in policies_list:
                print(f"  - [{p['severity']}] {p['display_name']} ({p['policy_id']}) -> {p['comparison']} {p['threshold_value']}")

    if args.check_lag:
        lag_metrics = calculator.compute_lag_metrics(
            subscription_id=f"sub-trade-orders-dataflow-{config.environment}",
            num_undelivered_messages=args.backlog_count,
            oldest_unacked_message_age_sec=args.oldest_age_sec,
            publish_rate_msgs_per_sec=250.0,
            consumption_rate_msgs_per_sec=200.0,
        )
        report_data["lag_metrics"] = lag_metrics.to_dict()
        if not args.json_report:
            print("\nPub/Sub Subscription Lag Metrics:")
            print(f"  - Backlog: {lag_metrics.num_undelivered_messages:,} messages")
            print(f"  - Oldest Unacked Age: {lag_metrics.oldest_unacked_message_age_sec:.1f}s")
            print(f"  - Severity: {lag_metrics.severity.value}")
            print(f"  - Drain Rate: {lag_metrics.net_drain_rate_msgs_per_sec:.1f} msgs/sec")
            print(f"  - Estimated Drain Time: {lag_metrics.estimated_time_to_drain_sec}s")

    if args.inspect_dlq:
        sample_records = [
            {
                "order_id": "ord-bad-001",
                "ticker": "AAPL",
                "portfolio_id": "PORT-TECH",
                "price": -150.25,
                "error_message": "Negative price validation failed: -150.25 <= 0",
                "delivery_attempts": 5,
            },
            {
                "order_id": "ord-bad-002",
                "ticker": "MSFT",
                "portfolio_id": "PORT-MAIN",
                "raw_payload": "{'invalid_json': }",
                "error_message": "JSONDecodeError: Expecting property name enclosed in double quotes",
                "delivery_attempts": 5,
            },
        ]
        forensic_report = dlq_monitor.inspect_records(sample_records)
        report_data["dlq_forensic_report"] = forensic_report.to_dict()
        if not args.json_report:
            print("\n" + forensic_report.to_markdown())

    if args.simulate_alert:
        sent = dispatcher.dispatch_dlq_alert(
            topic_name=f"top-trade-orders-dlq-{config.environment}",
            category=PoisonPillCategory.NON_POSITIVE_PRICE_ERROR,
            error_message="Unit test simulated poison pill: negative price detected on AAPL",
            ticker="AAPL",
            portfolio_id="PORT-DEV",
            delivery_attempts=5,
            payload_preview='{"order_id": "sim-001", "ticker": "AAPL", "price": -50.0}',
            bypass_cooldown=True,
        )
        report_data["simulate_alert_dispatched"] = sent
        telemetry.record_slack_alert(success=sent)
        if not args.json_report:
            print(f"\nSimulated Slack DLQ Alert Dispatched: {sent}")

    if args.export_prometheus:
        prom_text = telemetry.format_prometheus_text()
        if args.check_lag:
            prom_text += "\n" + lag_metrics.to_prometheus_text()
        print(prom_text)

    if args.json_report:
        print(json.dumps(report_data, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
