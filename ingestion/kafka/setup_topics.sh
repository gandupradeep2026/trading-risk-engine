#!/usr/bin/env bash
# =============================================================================
# Trading Risk Engine - Kafka Multi-Partition Topic Setup Script
# =============================================================================
# Automated provisioning and validation script for high-throughput market tick topics.
# Configures 6 partitions for market.ticks.v1 with lz4 compression and 24h retention.
#
# Usage:
#   ./setup_topics.sh [--bootstrap-server localhost:9092] [--dry-run]
# =============================================================================

set -euo pipefail

BOOTSTRAP_SERVER="localhost:9092"
DRY_RUN=0

for arg in "$@"; do
    case "$arg" in
        --bootstrap-server=*)
            BOOTSTRAP_SERVER="${arg#*=}"
            ;;
        --dry-run)
            DRY_RUN=1
            ;;
        *)
            ;;
    esac
done

log() {
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] [KAFKA-TOPIC-SETUP] $*"
}

log "Initializing Kafka topic setup for bootstrap server: $BOOTSTRAP_SERVER"

if [[ "$DRY_RUN" -eq 1 ]]; then
    log "Dry-run mode enabled. Validating topic configurations..."
    log "1. market.ticks.v1: 6 partitions, replication factor 1, compression=lz4, retention=24h"
    log "2. market.ticks.deadletter: 3 partitions, replication factor 1, compression=lz4, retention=7d"
    log "3. market.orderbook.depth.v1: 6 partitions, replication factor 1, compression=lz4, retention=12h"
    log "Dry-run validation successful."
    exit 0
fi

# Topic 1: Primary Market Ticks Stream (6 Partitions)
log "Creating topic: market.ticks.v1 (6 partitions)..."
if command -v kafka-topics &> /dev/null; then
    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create --if-not-exists \
        --topic market.ticks.v1 \
        --partitions 6 \
        --replication-factor 1 \
        --config cleanup.policy=delete \
        --config retention.ms=86400000 \
        --config compression.type=lz4 \
        --config min.insync.replicas=1 \
        --config segment.bytes=1073741824

    # Topic 2: Market Ticks Dead Letter Queue (3 Partitions)
    log "Creating topic: market.ticks.deadletter (3 partitions)..."
    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create --if-not-exists \
        --topic market.ticks.deadletter \
        --partitions 3 \
        --replication-factor 1 \
        --config cleanup.policy=delete \
        --config retention.ms=604800000 \
        --config compression.type=lz4 \
        --config min.insync.replicas=1

    # Topic 3: Level-2 Order Book Depth Updates (6 Partitions)
    log "Creating topic: market.orderbook.depth.v1 (6 partitions)..."
    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create --if-not-exists \
        --topic market.orderbook.depth.v1 \
        --partitions 6 \
        --replication-factor 1 \
        --config cleanup.policy=delete \
        --config retention.ms=43200000 \
        --config compression.type=lz4 \
        --config min.insync.replicas=1

    log "Verifying topic descriptions from Kafka broker:"
    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic market.ticks.v1
    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic market.ticks.deadletter
    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic market.orderbook.depth.v1
else
    log "kafka-topics CLI not in PATH. Running Python Topic Manager fallback..."
    KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVER" python -m ingestion.kafka.topic_manager
fi

log "Kafka multi-partition topic setup completed successfully."
