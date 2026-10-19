# trading-risk-engine: State & Milestone Runbook

## Execution Summary
- **Current Milestone Target**: Day 11
- **Platform**: Multi-Asset Real-Time Streaming & Enterprise Risk Management Engine on Google Cloud Platform (GCP)
- **Repository Root**: `trading-risk-engine`

---

## 30-Day Milestone Tracker

- [x] **Day 01**: Repo bootstrap, architecture documentation, Terraform network/VPC/subnets module.
- [x] **Day 02**: Terraform modules for GCS storage buckets (lifecycle policies) and Cloud SQL PostgreSQL instance.
- [x] **Day 03**: Terraform modules for Pub/Sub topics, dead-letter subscriptions, and IAM service account roles.
- [x] **Day 04**: Compute Engine VM provisioning script with systemd and Docker for ingestion worker.
- [x] **Day 05**: Kafka broker configuration and multi-partition tick-data topic setup.
- [x] **Day 06**: Python multi-threaded ingestion engine streaming real-time market ticks into Kafka.
- [x] **Day 07**: Asynchronous trade order producer publishing order executions to Cloud Pub/Sub.
- [x] **Day 08**: Cloud Dataflow (Apache Beam) pipeline boilerplate with Pub/Sub IO and JSON decoders.
- [x] **Day 09**: Dataflow sliding window transforms (5-min drop detection) and custom DoFn anomaly detectors.
- [x] **Day 10**: Multi-output Dataflow pipeline routing clean alerts to Cloud SQL and raw sinks to GCS.
- [ ] **Day 11**: Comprehensive Apache Beam unit tests using TestStream and mock events.
- [ ] **Day 12**: Dataproc PySpark streaming application consuming continuous tick records from Kafka.
- [ ] **Day 13**: PySpark rolling VWAP (Volume-Weighted Average Price) and volatility band aggregations.
- [ ] **Day 14**: PySpark streaming sink writing partitioned Snappy-compressed Parquet directly to GCS.
- [ ] **Day 15**: Cloud Composer (Apache Airflow) DAG scaffolding, GCP connections, and notification hooks.
- [ ] **Day 16**: Databricks workspace configs and Delta Lake Bronze layer ingestion script.
- [ ] **Day 17**: Databricks Silver layer cleansing, schema enforcement, and deduplication notebooks.
- [ ] **Day 18**: Databricks Gold layer: portfolio Value-at-Risk (VaR) calculation and risk feature store.
- [ ] **Day 19**: Airflow DAG orchestrating ephemeral Dataproc cluster creation, job execution, and teardown.
- [ ] **Day 20**: Airflow DAG orchestrating Databricks Gold pipelines with automated retry logic.
- [ ] **Day 21**: Great Expectations data quality test suites integrated into the Airflow DAG pipeline.
- [ ] **Day 22**: BigQuery DDL: partitioned and clustered star-schema tables for trades, tickers, and risk stats.
- [ ] **Day 23**: dbt project initialization with staging and intermediate SQL models for BigQuery.
- [ ] **Day 24**: dbt mart models and BigQuery materialized views for real-time market performance analytics.
- [ ] **Day 25**: Cloud SQL to BigQuery replication setup via BigQuery external/federated queries.
- [ ] **Day 26**: Performance benchmarking documentation and BigQuery cost optimization runbook.
- [ ] **Day 27**: GitHub Actions CI workflow (linting, black, flake8, mypy, pytest on push).
- [ ] **Day 28**: Cloud Monitoring alerting policies, Pub/Sub lag metrics, and Slack DLQ alerts.
- [ ] **Day 29**: End-to-end integration test runner orchestrating synthetic data from ingestion to BigQuery.
- [ ] **Day 30**: Executive architecture README with Mermaid diagrams, benchmarks, and portfolio case study.

---

## Daily Milestone Log

### Day 01 - 2026-10-12
- **Milestone Completed**: Repo bootstrap, architecture documentation, Terraform network/VPC/subnets module.
- **Key Changes Implemented**:
  - Bootstrapped production repository structure across `terraform/`, `ingestion/`, `streaming/`, `batch/`, `dwh/`, `tests/`, and `.github/workflows/`.
  - Authored comprehensive platform overview in `README.md` and detailed multi-tier architectural specification in `ARCHITECTURE.md`.
  - Built production Terraform network module (`terraform/modules/network/`): custom VPC, 3 segmented subnets (`sb-ingestion`, `sb-streaming`, `sb-data-services`), Private Google Access (PGA), Cloud Router & Cloud NAT for egress-only external connectivity, Private Service Access (PSA) IP reservation for Cloud SQL peering, and strict firewall rules (internal mesh, Dataflow inter-worker communication, Google IAP SSH bastion access).
  - Configured root Terraform manifests (`main.tf`, `variables.tf`, `outputs.tf`) and environment tfvars for `dev` and `prod`.
  - Created Pytest suite in `tests/test_network_architecture.py` verifying Terraform syntax, HCL brace matching, CIDR non-overlapping constraints, PGA enforcement, and IAP SSH constraints.
- **Verification Status**: 8/8 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 02 - 2026-10-12
- **Milestone Completed**: Terraform modules for GCS storage buckets (lifecycle policies) and Cloud SQL PostgreSQL instance.
- **Key Changes Implemented**:
  - Built production Terraform storage module (`terraform/modules/storage/`): 4 tier-isolated buckets (`bronze-raw`, `silver-processed`, `gold-curated`, `dataflow-staging`), enforcing Uniform Bucket-Level Access, regulatory object versioning, multipart upload abortion after 7 days, and automated lifecycle migration (Standard -> Nearline -> Coldline -> Expiration).
  - Built production Terraform Cloud SQL PostgreSQL module (`terraform/modules/cloudsql/`): PostgreSQL 15, private IP binding over Private Service Access (RFC 1918) with zero public IPv4 exposure, Point-In-Time Recovery (PITR) with 7-day transaction log retention, 14 retained backups, Query Insights, and low-latency database flags (`max_connections`, `log_min_duration_statement`).
  - Authored PostgreSQL operational schema in `terraform/modules/cloudsql/init_schema.sql` defining `risk_alerts` (UUID, JSONB payload, operational composite indices) and `portfolio_limits`.
  - Wired storage and cloudsql modules into root `terraform/main.tf`, `variables.tf`, `outputs.tf`, `dev.tfvars`, and `prod.tfvars`.
  - Created comprehensive unit test suite in `tests/test_storage_and_database.py` verifying uniform access enforcement, versioning, lifecycle rules, private IP constraints, and SQL schema syntax.
- **Verification Status**: 19/19 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 03 - 2026-10-12
- **Milestone Completed**: Terraform modules for Pub/Sub topics, dead-letter subscriptions, and IAM service account roles.
- **Key Changes Implemented**:
  - Built production Terraform Pub/Sub and IAM module (`terraform/modules/pubsub/`):
    - `top-trade-orders` primary order execution stream with dedicated Dead-Letter Queue (DLQ) `top-trade-orders-dlq`.
    - `sub-trade-orders-dataflow` subscription with strict `DeadLetterPolicy` (`max_delivery_attempts = 5`) and exponential backoff retry policy (10s to 600s).
    - `top-risk-alerts` operational risk anomaly notification topic with downstream subscription.
    - Least-privilege service accounts: `sa-ingestion` (`roles/pubsub.publisher`) and `sa-dataflow` (`roles/pubsub.subscriber`, `roles/pubsub.publisher`).
    - GCP Pub/Sub system agent role bindings to forward messages to DLQ on repeated consumption failures.
  - Zero-cost local emulator infrastructure:
    - `ingestion/docker-compose.emulator.yml`: Standalone local emulator definitions for Google Cloud Pub/Sub and PostgreSQL.
    - `ingestion/pubsub_client.py`: High-throughput `TradeOrder` dataclass with automatic USD notional calculation, validation, JSON serialization, and in-memory `LocalPubSubMock` with complete DLQ delivery simulation.
  - Integrated pubsub module into root `terraform/main.tf`, `variables.tf`, `outputs.tf`, and environment tfvars.
  - Test suite in `tests/test_pubsub_and_iam.py`: 9 new unit tests verifying topic/DLQ definitions, IAM least-privilege roles, TradeOrder validation, JSON roundtripping, and DLQ diversion upon exceeding max delivery attempts.
- **Verification Status**: 28/28 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 04 - 2026-10-13
- **Milestone Completed**: Compute Engine VM provisioning script with systemd and Docker for ingestion worker.
- **Key Changes Implemented**:
  - Built production Terraform Compute Engine module (`terraform/modules/compute/`):
    - Dedicated market ingestion worker VM (`vm-ingestion-worker-${var.environment}`) provisioned in `sb-ingestion`.
    - Zero external IPv4 allocation (no `access_config`), routing all egress traffic via Cloud NAT and administrative access via IAP SSH.
    - Attached to least-privilege IAM service account `sa-ingestion` with `cloud-platform` scopes.
    - Shielded VM configuration enabled: Secure Boot, vTPM, and Integrity Monitoring.
    - Cloud Ops metadata configuration with OS Login enabled and automated metadata startup script.
  - Compute Engine automated startup script (`terraform/modules/compute/scripts/startup.sh`):
    - High-throughput low-latency Linux kernel network and socket buffer tuning (`net.core.rmem_max=16MB`, TCP buffer allocations, file descriptor limits).
    - Automated Docker Engine installation with JSON-file log rotation and daemon limits (`max-size: 50m`, `max-file: 5`).
    - Dedicated unprivileged service user `riskworker` (UID 10001).
    - Automatic deployment and activation of systemd service unit.
  - Production Systemd unit (`ingestion/systemd/trading-ingestion.service`):
    - `Restart=always`, `RestartSec=5s`, `KillMode=mixed`, `TimeoutStopSec=30`.
    - Resource boundaries: `MemoryMax=4G`, `CPUQuota=200%`.
    - Unprivileged execution under `riskworker` in docker group.
  - Standalone host provisioning script (`ingestion/scripts/provision_worker.sh`):
    - Full idempotency supporting `--validate-only`, `--dry-run`, and full provisioning execution.
  - Container runtime & deployment definitions:
    - Multi-stage hardened `ingestion/Dockerfile` with non-root user, healthcheck, and Python 3.12-slim base.
    - `ingestion/docker-compose.yml` for containerized worker execution.
    - Runtime dependency manifest `ingestion/requirements.txt`.
  - Python Ingestion Worker daemon (`ingestion/worker.py`):
    - `IngestionWorker` orchestrating multi-asset synthetic/live order generation and resilient publishing.
    - Exponential backoff retry logic on publish errors.
    - Embedded lightweight HTTP health (`/health`) and Prometheus metrics (`/metrics`) daemon.
    - POSIX signal handling (SIGTERM/SIGINT) for clean graceful termination.
  - Wired compute module into root `terraform/main.tf`, `variables.tf`, `outputs.tf`, `dev.tfvars`, and `prod.tfvars`.
  - Zero-cost test suite in `tests/test_compute_ingestion_worker.py`: 18 new unit tests validating Terraform compute security settings, systemd configuration, shell scripts, Dockerfile hardening, and worker daemon runtime.
- **Verification Status**: 46/46 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 05 - 2026-10-14
- **Milestone Completed**: Kafka broker configuration and multi-partition tick-data topic setup.
- **Key Changes Implemented**:
  - Authored production Apache Kafka KRaft broker configuration in `ingestion/kafka/server.properties`:
    - Configured KRaft controller quorum removing ZooKeeper dependency.
    - VPC listeners on ports 9092, 9093 (controller), and 9094.
    - Low-latency financial throughput tuning: `lz4` compression, 8 network/IO threads, 1MB socket buffers.
    - Zero data loss guarantees: `unclean.leader.election.enable=false`, `auto.create.topics.enable=false`, `min.insync.replicas=1`.
    - Retention policy: 24-hour sliding operational buffer (`log.retention.hours=24`, 10GB per partition limit).
  - Built local Kafka container orchestration in `ingestion/kafka/docker-compose.kafka.yml` with healthchecks.
  - Implemented multi-asset financial tick data model in `ingestion/market_tick.py`:
    - `MarketTick` dataclass with quantitative validation: bid-ask spread non-inversion, positive prices/volumes, automatic spread and mid-price calculations.
    - `DeterministicPartitioner` hash partitioner ensuring strict per-ticker chronological order affinity across partitions.
  - Authored declarative Kafka Topic Manager in `ingestion/kafka/topic_manager.py`:
    - `CORE_TOPIC_CATALOG` specifying `market.ticks.v1` (6 partitions, lz4, 24h retention), `market.ticks.deadletter` (3 partitions, 7d retention), and `market.orderbook.depth.v1` (6 partitions).
    - `LocalKafkaAdminMock` providing zero-cost, hermetic in-memory topic provisioning, partition queue routing, and inspection for local development and CI testing.
    - Topic health check and readiness verification logic.
  - Automated topic setup shell script in `ingestion/kafka/setup_topics.sh` with `--dry-run` and CLI fallback.
  - Added test suite in `tests/test_kafka_broker_and_topics.py`: 14 unit tests validating server properties, compose definitions, tick invariants, deterministic hash routing, topic catalog, and in-memory multi-partition admin mock.
- **Verification Status**: 60/60 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 06 - 2026-10-15
- **Milestone Completed**: Python multi-threaded ingestion engine streaming real-time market ticks into Kafka.
- **Key Changes Implemented**:
  - Implemented multi-asset market data feed simulator in `ingestion/market_feed.py`:
    - Full asset class coverage across Equities (AAPL, MSFT, GOOGL, NVDA, TSLA, JPM, GS), ETFs (SPY, QQQ), FX (EURUSD, GBPUSD, USDJPY), Crypto (BTCUSD, ETHUSD), and Commodities (XAUUSD, BRENT).
    - Stochastic Geometric Brownian Motion (GBM) with Ornstein-Uhlenbeck mean-reversion drift, dynamic bid-ask spread modelling, and thread-safe strictly monotonic sequence numbers.
    - Controlled anomaly and corruption generator for dead-letter testing (crossed quotes, negative prices, negative volumes).
  - Built production Kafka tick producer client in `ingestion/kafka/producer.py`:
    - Encapsulates `MarketTick` JSON serialization, deterministic partition routing key (`ASSET_CLASS#TICKER`), and tracking headers (`source`, `asset_class`, `ticker`, `sequence_number`, `timestamp`).
    - Exponential backoff retry loop with automated Dead-Letter Queue (DLQ) routing to `market.ticks.deadletter` when retries are exhausted.
    - Zero-cost in-memory `LocalKafkaProducerMock` providing deterministic multi-partition routing (6 partitions for ticks, 3 for DLQ), delivery callback simulation, and fault injection for unit testing.
  - Implemented multi-threaded real-time streaming ingestion engine in `ingestion/streaming_engine.py` (and entry point `ingestion/kafka_engine.py`):
    - Worker pool architecture with decoupled feed worker fleet generating ticks into a bounded, thread-safe queue (`queue.Queue`) and producer worker fleet publishing to Kafka.
    - Backpressure management protecting system memory against broker lag or queue saturation.
    - Real-time dead-letter queue routing for crossed or malformed quotes.
    - Embedded telemetry store (`EngineTelemetry`) tracking throughput (ticks/sec, KB/sec), buffer high-watermarks, published counts, errors, and latencies.
    - Embedded lightweight HTTP health (`/health`) and Prometheus metrics exposition (`/metrics`) endpoints.
    - Graceful shutdown with poison-pill thread joins, queue draining, producer flushing, and POSIX signal handling.
  - Infrastructure updates:
    - Added `tick-streaming-engine` service definition to `ingestion/kafka/docker-compose.kafka.yml` with health checks.
    - Authored systemd service unit `ingestion/systemd/trading-kafka-streaming.service` enforcing `MemoryMax=4G`, `CPUQuota=400%`, and unprivileged execution under `riskworker`.
  - Authored comprehensive test suite in `tests/test_kafka_tick_ingestion_engine.py`:
    - 24 unit and integration tests covering multi-asset profiles, GBM dynamics, sequence monotonicity, mock producer partitioning, delivery callbacks, transient & permanent fault injection, producer retries, DLQ routing, multi-threaded streaming concurrency, backpressure, HTTP endpoints, and systemd definitions.
- **Verification Status**: 84/84 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 07 - 2026-10-16
- **Milestone Completed**: Asynchronous trade order producer publishing order executions to Cloud Pub/Sub.
- **Key Changes Implemented**:
  - Enhanced financial `TradeOrder` data model in `ingestion/pubsub_client.py`:
    - Added execution metadata fields: `trader_id`, `execution_status`, and monotonic `sequence_number`.
    - Added partition ordering key generation helpers (`compound` `portfolio#ticker`, `portfolio`, `ticker`), Pub/Sub message attributes dictionary mapping, strict validation rules, and backward-compatible JSON deserialization.
  - Implemented zero-cost in-memory asynchronous Pub/Sub mock in `ingestion/pubsub/mock.py`:
    - `PubSubPublishFuture`: async-compatible future inheriting from `concurrent.futures.Future` with callback hooks and native `__await__` support.
    - `AsyncLocalPubSubMock`: in-memory simulation of `google.cloud.pubsub_v1.PublisherClient` with message ordering preservation per ordering key, paused ordering key simulation on failure, configurable simulated latency, and transient/permanent fault injection.
  - Built production-grade `AsyncTradeOrderProducer` in `ingestion/pubsub/async_producer.py`:
    - Asynchronous publishing with `asyncio.gather` batching, message ordering (`enable_message_ordering=True`), and attributes mapping.
    - Concurrency flow control & backpressure management via `asyncio.Semaphore(max_inflight_messages)`.
    - Exponential backoff retry loop with automatic ordering key resume on transient network errors.
    - Dead-Letter Queue (DLQ) automated routing to `top-trade-orders-dlq` for structural validation failures and exhausted retries.
    - Non-blocking deferred publishing (`publish_defer`) with callback dispatch.
    - Real-time coroutine-safe telemetry store (`AsyncProducerTelemetry`) tracking throughput (orders/sec), p50/p95/p99 latency percentiles, errors, and Prometheus exposition formatting.
  - Implemented realistic multi-asset trade order flow generator in `ingestion/order_feed.py`:
    - Full asset coverage across Equities, ETFs, FX, Crypto, and Commodities with lot sizing, price boundaries, and order types (MARKET, LIMIT, STOP_LOSS).
    - Thread-safe strictly monotonic sequence numbering per `(portfolio_id, ticker)`.
    - Synthetic corruption injection for Dead-Letter Queue testing.
  - Built high-frequency asynchronous streaming order engine daemon in `ingestion/async_order_engine.py`:
    - Decoupled generator coroutine pushing to bounded `asyncio.Queue` and pool of async worker coroutines publishing to Pub/Sub.
    - Embedded HTTP server serving `/health` (JSON) and `/metrics` (Prometheus) endpoints.
    - Graceful shutdown lifecycle: drains buffered queue items, flushes producer inflight batches, stops HTTP daemon, and handles SIGINT/SIGTERM cleanly.
  - Infrastructure & deployment manifests:
    - Authored systemd service unit in `ingestion/systemd/trading-async-order-producer.service` enforcing `MemoryMax=4G`, `CPUQuota=300%`, and unprivileged execution under `riskworker`.
    - Added `async-order-producer` service definition to `ingestion/docker-compose.yml` with health check and port 8081.
  - Comprehensive zero-cost test suite in `tests/test_async_order_producer.py`:
    - 27 unit and integration tests validating schema rules, ordering keys, mock future callbacks, flow control, backpressure, retries, DLQ routing, order feed generation, async order engine lifecycle, HTTP health/metrics endpoints, and systemd/compose configurations.
- **Verification Status**: 111/111 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 08 - 2026-10-17
- **Milestone Completed**: Cloud Dataflow (Apache Beam) pipeline boilerplate with Pub/Sub IO and JSON decoders.
- **Key Changes Implemented**:
  - Streaming compute architecture package initialized under `streaming/` with `streaming/requirements.txt` defining runtime dependencies (`apache-beam[gcp]`, `google-cloud-pubsub`, `pydantic`).
  - Implemented `RiskEnginePipelineOptions` in `streaming/dataflow/options.py`:
    - Inherits from Apache Beam's `PipelineOptions` with custom argument parsing for `--environment`, `--project_id`, `--input_subscription`, `--input_topic`, `--deadletter_topic`, `--output_alerts_topic`, `--gcs_output_path`, `--window_duration_seconds`, `--window_slide_seconds`, `--max_anomaly_drop_threshold`, `--enable_attributes`, and `--enable_deadletter_sink`.
    - Auto-generates canonical GCP resource paths matching the Terraform infrastructure catalog (`top-trade-orders-{env}`, `sub-trade-orders-dataflow-{env}`, `top-trade-orders-dlq-{env}`, `top-risk-alerts-{env}`, `trading-risk-bronze-{env}`).
    - Comprehensive option validation ensuring consistency, positive window durations, and DataflowRunner required parameters.
  - Implemented financial trade order decoders and Dead-Letter Queue (DLQ) routing in `streaming/dataflow/decoders.py`:
    - `DeadLetterRecord` envelope dataclass capturing unparseable/invalid payloads, error diagnostics, timestamps, original message IDs, ordering keys, and source Pub/Sub attributes.
    - `DecodeAndValidateTradeOrderFn` Beam DoFn supporting `PubsubMessage`, raw `bytes`, `str`, and `dict` inputs.
    - Defensive UTF-8 handling and JSON syntax validation with error categorization (`UnicodeDecodeError`, `JSONDecodeError`).
    - Domain invariant validation enforcing positive prices, positive quantities, non-empty order/portfolio/ticker identifiers, valid sides (`BUY`, `SELL`), and valid order types (`MARKET`, `LIMIT`, `STOP_LOSS`, `STOP_LIMIT`).
    - Event-time timestamp extraction (ISO 8601 strings and numeric epoch seconds/millis) assigning `beam.window.TimestampedValue` for sliding window aggregations.
    - Cloud Monitoring / Prometheus Beam metrics counters and distributions (`messages_received`, `valid_orders_decoded`, `json_decode_errors`, `schema_validation_errors`, `unicode_decode_errors`, `missing_field_errors`, `deadletter_emitted`, `order_notional_usd`, `payload_size_bytes`).
    - `FormatDeadLetterForPubSubFn` formatting failed records into `PubsubMessage` with diagnostic headers.
    - `DecodeAndValidateOrders` composite PTransform branching clean orders to `TAG_VALID_ORDERS` and corrupt records to `TAG_DEADLETTER`.
  - Built production Dataflow pipeline DAG builder in `streaming/dataflow/pipeline.py`:
    - `build_pipeline` configuring Pub/Sub IO reads (`ReadFromPubSub` by subscription or topic with attributes), applying order decoding/validation, branching outputs, and routing dead-letter records to `WriteToPubSub(deadletter_topic)`.
    - Encapsulates DAG in `DataflowPipelineBundle` for composability in subsequent windowing and sink milestones.
  - Added CLI execution entry point in `streaming/dataflow/run_pipeline.py` supporting both local DirectRunner and GCP DataflowRunner.
  - Built interactive local visual dashboard server in `streaming/dashboard_server.py` (serving on `http://localhost:8050`) driving `MarketFeedGenerator`, `OrderFeedGenerator`, and `DecodeAndValidateTradeOrderFn` in real time with fault injection and custom JSON payload testing.
  - Comprehensive zero-cost hermetic unit and integration test suite in `tests/test_dataflow_order_pipeline.py`:
    - 41 unit and integration tests covering pipeline options, canonical naming, validation rules, DeadLetterRecord serialization/roundtrips, DoFn processing across all valid and corrupted input formats, missing fields, domain rules, Beam DirectRunner execution with clean streams, mixed streams with deterministic DLQ routing, attribute retention, DAG construction, and the interactive visual dashboard server.
- **Verification Status**: 152/152 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 09 - 2026-10-18
- **Milestone Completed**: Dataflow sliding window transforms (5-min drop detection) and custom DoFn anomaly detectors.
- **Key Changes Implemented**:
  - Implemented quantitative sliding-window anomaly detection and risk alerting module in `streaming/dataflow/anomaly_detector.py`:
    - `RiskAlert` dataclass matching the Cloud SQL PostgreSQL `risk_alerts` operational schema (`alert_id`, `portfolio_id`, `ticker`, `asset_class`, `alert_type`, `severity`, `metric_name`, `threshold_value`, `observed_value`, `event_timestamp`, `detected_at`, `payload_json`) with JSON roundtripping, Cloud SQL row dictionary serialization, and `PubsubMessage` conversion with ordering keys.
    - `WindowDrawdownStats` dataclass capturing 5-minute rolling window microstructure aggregates (`open_price`, `close_price`, `peak_price`, `trough_price`, `vwap`, `max_drawdown_pct`, `window_drop_pct`, `total_notional_usd`, `buy_notional_usd`, `sell_notional_usd`, `net_delta_usd`).
    - `KeyByPortfolioAndTickerFn` extracting composite `portfolio_id#ticker` keys for group-by-key windowing.
    - `DetectSlidingWindowDrawdownFn` custom Beam `DoFn` computing chronological peak-to-trough drawdown (`max_drawdown_pct`), open-to-close drop (`window_drop_pct`), Volume-Weighted Average Price (`vwap`), and gross/directional notional exposure across 5-minute sliding windows, emitting tagged `RiskAlert` records for:
      1. `FLASH_CRASH_DRAWDOWN` (rolling 5-minute drawdown >= threshold, default 5.0%),
      2. `NOTIONAL_EXPOSURE_BREACH` (rolling 5-minute cumulative notional >= limit, default $2.5M),
      3. `CONCENTRATED_SELL_PRESSURE` (institutional sell-side notional concentration >= 85% across >= 4 orders).
    - `EvaluateSingleOrderSpikeFn` zero-latency custom `DoFn` detecting fat-finger single-order notional limit breaches (`SINGLE_ORDER_FAT_FINGER`).
    - `FormatRiskAlertForPubSubFn` formatting `RiskAlert` records into `PubsubMessage` payloads for `top-risk-alerts-{env}`.
    - `SlidingWindowAnomalyTransform` composite `PTransform` applying `beam.WindowInto(window.SlidingWindows(size=300, period=60))`, keying by `portfolio_id#ticker`, grouping by key, and branching outputs into `window_stats` and `risk_alerts`.
  - Extended `RiskEnginePipelineOptions` in `streaming/dataflow/options.py` with `--max_window_notional_usd`, `--enable_window_anomaly_detection`, and `--enable_alerts_pubsub_sink`.
  - Integrated `SlidingWindowAnomalyTransform` into `build_pipeline` and `DataflowPipelineBundle` in `streaming/dataflow/pipeline.py` and exported new symbols in `streaming/dataflow/__init__.py`.
  - Authored zero-cost unit and Apache Beam `TestPipeline` integration test suite in `tests/test_dataflow_window_anomalies.py` (15 tests covering severity classification, `RiskAlert` Cloud SQL/PubSub serialization, chronological peak-to-trough vs. monotonic rally behavior, notional breach, concentrated sell pressure, fat-finger single-order spikes, and full 5-minute sliding window Beam execution).
- **Verification Status**: 167/167 tests passed cleanly (`python -m pytest tests/ -v`).

### Day 10 - 2026-10-19
- **Milestone Completed**: Multi-output Dataflow pipeline routing clean alerts to Cloud SQL and raw sinks to GCS.
- **Key Changes Implemented**:
  - Built production Cloud SQL PostgreSQL operational risk alerts sink in `streaming/dataflow/cloudsql_sink.py`:
    - `CloudSqlConfig`: Enterprise configuration dataclass with connection pooling, table definitions, batch sizing (`batch_size=50`), retry limits (`max_retries=3`), exponential backoff (`retry_backoff_sec=0.5`), and driver selection (`auto`, `pg8000`, `sqlite3`, `mock`).
    - `CloudSqlDatabaseClient`: Database client managing connection lifecycles, idempotent table initialization (`risk_alerts` table DDL matching `terraform/modules/cloudsql/init_schema.sql`), and bulk row inserts with deduplication (`ON CONFLICT (alert_id) DO NOTHING` for PostgreSQL / `INSERT OR IGNORE` for SQLite). Supports pure-Python `pg8000` on GCP and zero-cost hermetic in-memory/file SQLite for local testing and CI.
    - `WriteAlertsToCloudSqlFn`: Custom Beam `DoFn` implementing bundle-level batching (`start_bundle`, `process`, `finish_bundle`), exponential backoff on transient errors, dead-letter routing to `TAG_CLOUDSQL_FAILED` on persistent failures, and Cloud Monitoring / Prometheus metrics (`alerts_written_cloudsql`, `cloudsql_batches_flushed`, `cloudsql_write_errors`, `cloudsql_deadletter_emitted`).
    - `WriteAlertsToCloudSql`: Composite `PTransform` branching clean alerts into `TAG_CLOUDSQL_SUCCESS` and `TAG_CLOUDSQL_FAILED`.
  - Built production Google Cloud Storage (GCS) raw audit & data lake cold-tier sink in `streaming/dataflow/gcs_sink.py`:
    - `FormatOrderForGcsFn`: Custom Beam `DoFn` serializing `TradeOrder` objects into structured newline-delimited JSON records enriched with cold-tier audit metadata (`_tier: BRONZE_RAW`, `_gcs_ingested_at`).
    - `FormatDeadLetterForGcsFn`: Custom Beam `DoFn` formatting `DeadLetterRecord` instances with forensic error diagnostics for regulatory compliance (`_tier: BRONZE_DEADLETTER`, `_gcs_ingested_at`).
    - `WriteRawOrdersToGcs`: Composite `PTransform` applying fixed windowing (default: 60s) and writing partitioned JSON lines files to GCS Bronze bucket (`trading-risk-bronze-{env}`) or local directories.
    - `WriteRawDeadLetterToGcs`: Composite `PTransform` windowing and archiving unparseable payloads to cold storage.
    - `_normalize_output_prefix`: Normalizes output paths ensuring directory prefixes and slash conventions.
  - Extended `RiskEnginePipelineOptions` in `streaming/dataflow/options.py`:
    - Added CLI parameters for Cloud SQL: `--enable_cloudsql_sink`, `--cloudsql_instance`, `--cloudsql_database`, `--cloudsql_user`, `--cloudsql_password`, `--cloudsql_host`, `--cloudsql_port`, `--cloudsql_table`, `--cloudsql_batch_size`, `--cloudsql_max_retries`, `--cloudsql_retry_backoff_sec`, `--cloudsql_driver`, `--cloudsql_sqlite_path`.
    - Added CLI parameters for GCS sinks: `--enable_gcs_raw_sink`, `--gcs_window_seconds`, `--gcs_num_shards`, `--gcs_deadletter_output_path`, `--enable_gcs_deadletter_sink`.
    - Canonical resolution for `cloudsql_instance` (`{project_id}:us-central1:trading-risk-db-{env}`) and GCS Bronze paths.
    - Validation constraints for `gcs_window_seconds > 0` and `cloudsql_batch_size > 0`.
  - Multi-output Dataflow DAG integration in `streaming/dataflow/pipeline.py`:
    - Updated `build_pipeline` and `DataflowPipelineBundle` to wire and expose `cloudsql_alerts_written`, `cloudsql_failures`, `gcs_orders_written`, and `gcs_deadletter_written`.
    - Updated package exports in `streaming/dataflow/__init__.py` and logging in `streaming/dataflow/run_pipeline.py`.
    - Added `google-cloud-storage>=2.14.0` and `pg8000>=1.30.0` to `streaming/requirements.txt`.
    - Added `to_dict()` helper to `TradeOrder` in `ingestion/pubsub_client.py`.
  - Authored zero-cost unit and integration test suite in `tests/test_dataflow_multi_output_sinks.py`:
    - 17 unit and integration tests covering Cloud SQL config/options resolution, SQLite DDL structure, idempotent deduplication, transient retry with backoff, permanent error containment, DoFn batching and metrics, GCS JSON lines formatting, prefix normalization, Beam DirectRunner file writing, and end-to-end multi-output pipeline execution (orders to GCS raw, malformed to DLQ, flash-crash alerts to Cloud SQL SQLite).
- **Verification Status**: 184/184 tests passed cleanly (`python -m pytest tests/ -v`).


