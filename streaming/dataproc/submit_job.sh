#!/usr/bin/env bash
# ==============================================================================
# Submit Dataproc PySpark Structured Streaming Market Tick Consumer Job
# ==============================================================================
# Submits the continuous Kafka market tick streaming application to an ephemeral
# or long-running GCP Dataproc cluster with the Spark SQL Kafka 0-10 connector.
#
# Usage:
#   ./streaming/dataproc/submit_job.sh [ENVIRONMENT] [PROJECT_ID] [REGION]
# ==============================================================================

set -euo pipefail

ENVIRONMENT="${1:-dev}"
PROJECT_ID="${2:-trading-risk-engine-${ENVIRONMENT}}"
REGION="${3:-us-central1}"
CLUSTER_NAME="${DATAPROC_CLUSTER_NAME:-dp-tick-streaming-${ENVIRONMENT}}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-10.10.10.50:9092}"
KAFKA_TOPIC="${KAFKA_SUBSCRIBE_TOPICS:-market.ticks.v1}"
SPARK_KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

echo "Submitting PySpark Structured Streaming job to Dataproc cluster '${CLUSTER_NAME}' in ${PROJECT_ID} (${REGION})..."

gcloud dataproc jobs submit pyspark \
  streaming/dataproc/run_tick_consumer.py \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --cluster="${CLUSTER_NAME}" \
  --py-files="streaming.zip,ingestion.zip" \
  --properties="spark.jars.packages=${SPARK_KAFKA_PACKAGE},spark.sql.adaptive.enabled=true,spark.streaming.backpressure.enabled=true,spark.dynamicAllocation.enabled=false" \
  -- \
  --project_id="${PROJECT_ID}" \
  --environment="${ENVIRONMENT}" \
  --region="${REGION}" \
  --cluster_name="${CLUSTER_NAME}" \
  --kafka_bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --kafka_subscribe_topics="${KAFKA_TOPIC}" \
  --starting_offsets="latest" \
  --max_offsets_per_trigger=50000 \
  --trigger_processing_time="5 seconds" \
  --watermark_delay_threshold="30 seconds"
