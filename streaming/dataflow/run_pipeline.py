"""CLI Entry Point to Launch the Trading Risk Engine Dataflow Pipeline.

Supports local DirectRunner execution and Google Cloud DataflowRunner submission.

Usage:
    # Run locally with DirectRunner:
    python -m streaming.dataflow.run_pipeline \
        --runner=DirectRunner \
        --environment=dev \
        --input_subscription=projects/dev-project/subscriptions/sub-trade-orders-dataflow-dev

    # Submit to Cloud Dataflow:
    python -m streaming.dataflow.run_pipeline \
        --runner=DataflowRunner \
        --project=my-gcp-project \
        --region=us-central1 \
        --temp_location=gs://my-bucket/temp \
        --staging_location=gs://my-bucket/staging \
        --environment=prod
"""

import logging
import sys
from typing import List, Optional

from apache_beam.options.pipeline_options import StandardOptions

from streaming.dataflow.options import RiskEnginePipelineOptions
from streaming.dataflow.pipeline import build_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
)
logger = logging.getLogger("trading_risk_pipeline")


def run(argv: Optional[List[str]] = None) -> None:
    """Parse arguments, construct the Apache Beam pipeline, and trigger execution."""
    pipeline_options = RiskEnginePipelineOptions(argv)
    pipeline_options.resolve_defaults()
    pipeline_options.validate_options()

    standard_opts = pipeline_options.view_as(StandardOptions)
    runner = standard_opts.runner or "DirectRunner"

    logger.info("Initializing Trading Risk Pipeline on runner: %s", runner)
    logger.info("Environment: %s | Project: %s", pipeline_options.environment, pipeline_options.project_id)
    if pipeline_options.input_subscription:
        logger.info("Consuming from Pub/Sub Subscription: %s", pipeline_options.input_subscription)
    elif pipeline_options.input_topic:
        logger.info("Consuming from Pub/Sub Topic: %s", pipeline_options.input_topic)
    logger.info("Dead-Letter Topic: %s", pipeline_options.deadletter_topic)
    if getattr(pipeline_options, "enable_gcs_raw_sink", False):
        logger.info("GCS Raw Orders Sink enabled: %s", pipeline_options.gcs_output_path)
    if getattr(pipeline_options, "enable_cloudsql_sink", False):
        logger.info(
            "Cloud SQL Alerts Sink enabled: %s (table: %s)",
            pipeline_options.cloudsql_instance,
            pipeline_options.cloudsql_table,
        )

    bundle = build_pipeline(pipeline_options)

    logger.info("Pipeline DAG successfully constructed. Launching job...")
    result = bundle.pipeline.run()

    if pipeline_options.wait_until_finish:
        logger.info("Waiting for pipeline execution to complete...")
        result.wait_until_finish()
        logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    run(sys.argv[1:])
