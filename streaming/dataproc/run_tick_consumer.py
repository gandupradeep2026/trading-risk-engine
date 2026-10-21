"""CLI Entrypoint for the Dataproc PySpark Structured Streaming Market Tick Consumer.

Supports both:
- `--dry_run` / `--local_simulation`: Runs a zero-cost hermetic local simulation using
  `MarketFeedGenerator` and `LocalKafkaProducerMock` to validate end-to-end Spark streaming
  configuration, schema parsing, watermark progression, and telemetry without a live cluster.
- Cluster execution mode: Initializes `SparkSession`, connects to the Kafka bootstrap servers,
  and starts the continuous Structured Streaming query into GCS Bronze Parquet & Quarantine sinks.
"""

import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional

from ingestion.kafka.producer import KafkaTickProducer, LocalKafkaProducerMock, ProducerConfig
from ingestion.market_feed import MarketFeedGenerator
from streaming.dataproc.config import DataprocStreamingConfig
from streaming.dataproc.tick_consumer import PySparkKafkaTickConsumer

logger = logging.getLogger(__name__)


def run_local_simulation(
    config: DataprocStreamingConfig,
    num_ticks: int = 100,
    seed: int = 42,
) -> Dict[str, Any]:
    """Execute a deterministic local streaming simulation with synthetic market ticks."""
    sim_config = DataprocStreamingConfig(
        project_id=config.project_id,
        environment=config.environment,
        region=config.region,
        cluster_name=config.cluster_name,
        app_name=config.app_name,
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        kafka_subscribe_topics=config.kafka_subscribe_topics,
        kafka_dlq_topic=config.kafka_dlq_topic,
        starting_offsets="earliest",
        max_offsets_per_trigger=config.max_offsets_per_trigger,
        trigger_processing_time=config.trigger_processing_time,
        watermark_delay_threshold=config.watermark_delay_threshold,
        checkpoint_location=config.checkpoint_location,
        bronze_output_path=config.bronze_output_path,
        quarantine_output_path=config.quarantine_output_path,
    )

    topic = sim_config.kafka_subscribe_topics.split(",")[0].strip()
    mock_broker = LocalKafkaProducerMock(num_partitions_map={topic: 4})
    producer_cfg = ProducerConfig(
        bootstrap_servers=sim_config.kafka_bootstrap_servers,
        topic=topic,
        deadletter_topic=sim_config.kafka_dlq_topic,
        simulation_mode=True,
    )
    producer_service = KafkaTickProducer(config=producer_cfg, mock_producer=mock_broker)

    generator = MarketFeedGenerator(seed=seed)
    for _ in range(num_ticks):
        producer_service.send_tick(generator.simulate_tick())

    consumer = PySparkKafkaTickConsumer(config=sim_config)
    batches = consumer.drain_producer(mock_broker)

    return {
        "mode": "local_simulation",
        "config": sim_config.to_dict(),
        "plan_descriptor": consumer.build_stream_plan_descriptor().__dict__,
        "batches_executed": len(batches),
        "telemetry": consumer.telemetry.to_dict(),
    }


def main(argv: Optional[List[str]] = None) -> int:
    """Main CLI entrypoint for Dataproc PySpark tick streaming job."""
    parser = argparse.ArgumentParser(
        description="Dataproc PySpark Structured Streaming Market Tick Consumer Runner"
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Validate configuration and print Spark Structured Streaming plan descriptor as JSON.",
    )
    parser.add_argument(
        "--local_simulation",
        action="store_true",
        help="Run a hermetic local micro-batch simulation using MarketFeedGenerator and LocalKafkaProducerMock.",
    )
    parser.add_argument(
        "--simulation_ticks",
        type=int,
        default=100,
        help="Number of synthetic market ticks to generate in --local_simulation mode.",
    )
    parsed_flags, remaining_args = parser.parse_known_args(argv)

    try:
        config = DataprocStreamingConfig.from_cli_args(remaining_args)
    except ValueError as exc:
        logger.error("Invalid DataprocStreamingConfig: %s", exc)
        return 2

    if parsed_flags.dry_run:
        consumer = PySparkKafkaTickConsumer(config=config)
        output = {
            "mode": "dry_run",
            "config": config.to_dict(),
            "spark_conf": config.to_spark_conf_dict(),
            "kafka_read_options": config.to_kafka_read_options(),
            "plan": consumer.build_stream_plan_descriptor().__dict__,
        }
        print(json.dumps(output, indent=2))
        return 0

    if parsed_flags.local_simulation:
        summary = run_local_simulation(
            config=config, num_ticks=parsed_flags.simulation_ticks
        )
        print(json.dumps(summary, indent=2))
        return 0

    # Production cluster execution path
    spark = PySparkKafkaTickConsumer.build_spark_session(config)
    raw_df = PySparkKafkaTickConsumer.build_kafka_read_stream(spark, config)
    valid_df, corrupt_df = PySparkKafkaTickConsumer.build_parsed_tick_streams(raw_df, config)
    bronze_query, quarantine_query = PySparkKafkaTickConsumer.start_Bronze_and_quarantine_writers(
        valid_df, corrupt_df, config
    )
    logger.info(
        "Started Dataproc Structured Streaming queries: bronze=%s, quarantine=%s",
        bronze_query,
        quarantine_query,
    )
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
