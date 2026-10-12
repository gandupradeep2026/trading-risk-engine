# trading-risk-engine: State & Milestone Runbook

## Execution Summary
- **Current Milestone Target**: Day 03
- **Platform**: Multi-Asset Real-Time Streaming & Enterprise Risk Management Engine on Google Cloud Platform (GCP)
- **Repository Root**: `trading-risk-engine`

---

## 30-Day Milestone Tracker

- [x] **Day 01**: Repo bootstrap, architecture documentation, Terraform network/VPC/subnets module.
- [x] **Day 02**: Terraform modules for GCS storage buckets (lifecycle policies) and Cloud SQL PostgreSQL instance.
- [ ] **Day 03**: Terraform modules for Pub/Sub topics, dead-letter subscriptions, and IAM service account roles.
- [ ] **Day 04**: Compute Engine VM provisioning script with systemd and Docker for ingestion worker.
- [ ] **Day 05**: Kafka broker configuration and multi-partition tick-data topic setup.
- [ ] **Day 06**: Python multi-threaded ingestion engine streaming real-time market ticks into Kafka.
- [ ] **Day 07**: Asynchronous trade order producer publishing order executions to Cloud Pub/Sub.
- [ ] **Day 08**: Cloud Dataflow (Apache Beam) pipeline boilerplate with Pub/Sub IO and JSON decoders.
- [ ] **Day 09**: Dataflow sliding window transforms (5-min drop detection) and custom DoFn anomaly detectors.
- [ ] **Day 10**: Multi-output Dataflow pipeline routing clean alerts to Cloud SQL and raw sinks to GCS.
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
