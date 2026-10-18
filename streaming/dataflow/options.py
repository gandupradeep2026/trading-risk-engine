"""Apache Beam Pipeline Options for Trading Risk Engine.

Configures enterprise execution parameters, GCP resource locations, Pub/Sub IO paths,
and runtime settings for both Google Cloud Dataflow and local DirectRunner execution.
"""

import os
from typing import Any, Dict, Optional, Tuple

from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, StandardOptions


class RiskEnginePipelineOptions(PipelineOptions):
    """Custom pipeline options for the Trading Risk Engine streaming analytics pipeline.

    Supports configuration across dev, staging, and prod environments, with automatic
    derivation of canonical Pub/Sub topics, subscriptions, and GCS storage tiers matching
    the Terraform enterprise infrastructure catalog.
    """

    @classmethod
    def _add_argparse_args(cls, parser) -> None:
        """Register command-line arguments for the risk streaming pipeline."""
        parser.add_argument(
            "--environment",
            default=os.getenv("ENVIRONMENT", "dev"),
            choices=["dev", "staging", "prod"],
            help="Deployment environment tier: dev, staging, or prod.",
        )
        parser.add_argument(
            "--project_id",
            default=os.getenv("GCP_PROJECT", "trading-risk-engine-dev"),
            help="Google Cloud Project ID.",
        )
        parser.add_argument(
            "--input_subscription",
            default=None,
            help="Full Cloud Pub/Sub subscription path to consume trade orders from.",
        )
        parser.add_argument(
            "--input_topic",
            default=None,
            help="Full Cloud Pub/Sub topic path to consume trade orders from (alternative to subscription).",
        )
        parser.add_argument(
            "--deadletter_topic",
            default=None,
            help="Full Cloud Pub/Sub topic path to publish deadlettered / malformed orders to.",
        )
        parser.add_argument(
            "--output_alerts_topic",
            default=None,
            help="Full Cloud Pub/Sub topic path to publish real-time risk alerts to.",
        )
        parser.add_argument(
            "--gcs_output_path",
            default=None,
            help="Google Cloud Storage URI for persisting raw audit records (Bronze cold tier).",
        )
        parser.add_argument(
            "--enable_attributes",
            action="store_true",
            default=True,
            help="Whether to read Pub/Sub messages with attributes as PubsubMessage objects.",
        )
        parser.add_argument(
            "--enable_deadletter_sink",
            action="store_true",
            default=True,
            help="Whether to enable the automated Pub/Sub Dead-Letter Queue (DLQ) sink.",
        )
        parser.add_argument(
            "--window_duration_seconds",
            type=int,
            default=300,
            help="Sliding window duration in seconds for rolling drawdown risk evaluation (default: 300s / 5m).",
        )
        parser.add_argument(
            "--window_slide_seconds",
            type=int,
            default=60,
            help="Sliding window slide period in seconds (default: 60s / 1m).",
        )
        parser.add_argument(
            "--max_anomaly_drop_threshold",
            type=float,
            default=0.05,
            help="Maximum acceptable portfolio price drawdown threshold before generating a risk alert (default: 5%%).",
        )
        parser.add_argument(
            "--max_window_notional_usd",
            type=float,
            default=2500000.0,
            help="Maximum cumulative 5-minute window notional exposure before triggering a breach alert (default: $2.5M).",
        )
        parser.add_argument(
            "--enable_window_anomaly_detection",
            action="store_true",
            default=True,
            help="Whether to apply 5-minute sliding window transforms and anomaly detection DoFns.",
        )
        parser.add_argument(
            "--enable_alerts_pubsub_sink",
            action="store_true",
            default=False,
            help="Whether to publish detected risk alerts directly to the Pub/Sub alerts topic.",
        )
        parser.add_argument(
            "--wait_until_finish",
            action="store_true",
            default=False,
            help="Block and wait for the pipeline to finish before exiting (useful for testing or batch runs).",
        )

    @staticmethod
    def get_canonical_resource_names(project_id: str, environment: str) -> Dict[str, str]:
        """Generate canonical GCP resource paths matching the Terraform infrastructure catalog."""
        env = environment.lower()
        return {
            "trade_orders_topic": f"projects/{project_id}/topics/top-trade-orders-{env}",
            "trade_orders_sub": f"projects/{project_id}/subscriptions/sub-trade-orders-dataflow-{env}",
            "trade_orders_dlq": f"projects/{project_id}/topics/top-trade-orders-dlq-{env}",
            "trade_orders_dlq_sub": f"projects/{project_id}/subscriptions/sub-trade-orders-dlq-{env}",
            "risk_alerts_topic": f"projects/{project_id}/topics/top-risk-alerts-{env}",
            "risk_alerts_sub": f"projects/{project_id}/subscriptions/sub-risk-alerts-consumer-{env}",
            "bronze_bucket": f"gs://trading-risk-bronze-{env}",
            "staging_bucket": f"gs://trading-risk-dataflow-staging-{env}",
        }

    def resolve_defaults(self) -> None:
        """Resolve unspecified Pub/Sub topics and subscriptions to their canonical defaults."""
        canon = self.get_canonical_resource_names(self.project_id, self.environment)
        if not self.input_subscription and not self.input_topic:
            self.input_subscription = canon["trade_orders_sub"]
        if not self.deadletter_topic:
            self.deadletter_topic = canon["trade_orders_dlq"]
        if not self.output_alerts_topic:
            self.output_alerts_topic = canon["risk_alerts_topic"]
        if not self.gcs_output_path:
            self.gcs_output_path = f"{canon['bronze_bucket']}/dataflow/orders/raw/"

    def validate_options(self, is_test: bool = False) -> None:
        """Validate pipeline options for correctness and consistency."""
        if self.environment not in ("dev", "staging", "prod"):
            raise ValueError(f"Invalid environment: '{self.environment}'. Must be dev, staging, or prod.")

        if self.window_duration_seconds <= 0:
            raise ValueError(f"window_duration_seconds must be positive, got {self.window_duration_seconds}")

        if self.window_slide_seconds <= 0:
            raise ValueError(f"window_slide_seconds must be positive, got {self.window_slide_seconds}")

        if self.window_slide_seconds > self.window_duration_seconds:
            raise ValueError("window_slide_seconds cannot be greater than window_duration_seconds")

        if self.max_anomaly_drop_threshold <= 0 or self.max_anomaly_drop_threshold >= 1.0:
            raise ValueError("max_anomaly_drop_threshold must be strictly between 0 and 1 (exclusive)")

        if not is_test:
            if not self.input_subscription and not self.input_topic:
                raise ValueError("Must specify either --input_subscription or --input_topic for streaming input.")

        # If running on DataflowRunner, enforce required cloud parameters
        standard_opts = self.view_as(StandardOptions)
        if standard_opts.runner == "DataflowRunner":
            cloud_opts = self.view_as(GoogleCloudOptions)
            if not cloud_opts.project:
                cloud_opts.project = self.project_id
            if not cloud_opts.temp_location and not cloud_opts.staging_location:
                raise ValueError("DataflowRunner requires --temp_location or --staging_location (e.g. gs://bucket/temp).")
            if not standard_opts.streaming:
                standard_opts.streaming = True
