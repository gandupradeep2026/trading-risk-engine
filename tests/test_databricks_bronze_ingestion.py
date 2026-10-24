"""Databricks Workspace Configs and Delta Lake Bronze Ingestion Tests (Day 16).

Validates:
1. Terraform Databricks Module:
   - Module files exist, HCL braces balanced, root main/vars/outputs wired.
   - Dev/Prod tfvars definitions for Databricks workspace and cluster.
   - Cluster spot attributes, Delta Spark configurations, Unity Catalog GCS external locations.
2. Databricks Workspace & Cluster Configurations:
   - Dataclass invariants, validations, autoscale limits, Spark confs, and API payload generation.
3. Schemas & Quality Contracts:
   - Spark SQL DDL schemas and PyArrow schemas for ticks, orders, and rolling VWAP.
   - Domain invariant validation (non-positive prices, crossed quotes, invalid sides/order types).
   - Forensic quarantine record formatting.
4. Lineage Hashing:
   - Deterministic SHA-256 payload hashing and audit column isolation.
5. Delta Log Manager & ACID Commits:
   - Commit version sequencing, action structures (commitInfo, protocol, metaData, add), and active file tracking.
6. Hermetic Delta Lake Bronze Ingestion:
   - Ingestion of market ticks from PySpark Parquet files (Day 14 format).
   - Ingestion of trade orders from Dataflow JSON lines files (Day 10 format).
   - Ingestion of rolling VWAP records.
   - Forensic isolation of corrupted records to quarantine storage without ingestion failure.
   - Delta Lake table maintenance (OPTIMIZE Z-Ordering and VACUUM retention commits).
7. Databricks REST API Client & In-Memory Mock:
   - Job creation, execution triggers, status polling, run cancellation, and cluster lifecycle.
8. Databricks Production Notebook Runner:
   - Dry-run validation, CLI argument parsing, multi-source ingestion execution, and structured JSON output.
9. Operational Telemetry:
   - Prometheus metrics exposition and counter monotonicity.
"""

from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Dict, List
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from batch.databricks import (
    ARROW_DELTA_BRONZE_ORDER_SCHEMA,
    ARROW_DELTA_BRONZE_TICK_SCHEMA,
    ARROW_DELTA_BRONZE_VWAP_SCHEMA,
    BRONZE_MARKET_TICKS_CONFIG,
    BRONZE_ROLLING_VWAP_CONFIG,
    BRONZE_TRADE_ORDERS_CONFIG,
    BronzeIngestionResult,
    BronzeIngestionTelemetry,
    DatabricksClient,
    DatabricksClientException,
    DatabricksClusterConfig,
    DatabricksWorkspaceConfig,
    DeltaBronzeIngestionEngine,
    DeltaLogManager,
    DeltaTableConfig,
    MockDatabricksClient,
    QuarantinedBronzeRecord,
    RunLifeCycleState,
    RunResultState,
    compute_record_hash,
    validate_bronze_order_record,
    validate_bronze_tick_record,
    validate_bronze_vwap_record,
)
from batch.databricks.notebooks.bronze_ingestion_runner import (
    parse_args,
    run_bronze_ingestion,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_DATABRICKS_DIR = REPO_ROOT / "terraform" / "modules" / "databricks"
TF_ROOT_DIR = REPO_ROOT / "terraform"
TF_ENV_DIR = REPO_ROOT / "terraform" / "environments"


# =============================================================================
# 1. TERRAFORM DATABRICKS MODULE VALIDATION
# =============================================================================

class TestTerraformDatabricksModule:
    """Validate Databricks Terraform module files, security, and root wiring."""

    def test_databricks_module_files_exist(self) -> None:
        """Verify all essential Databricks Terraform module files exist."""
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf"]
        for filename in required:
            file_path = TF_DATABRICKS_DIR / filename
            assert file_path.is_file(), f"Missing required file: {file_path}"
            assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

    def test_balanced_braces_in_databricks_module(self) -> None:
        """Verify balanced HCL block braces in databricks module."""
        for tf_file in TF_DATABRICKS_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_file.name}"

    def test_root_terraform_wires_databricks_module(self) -> None:
        """Verify root terraform/main.tf, variables.tf, and outputs.tf wire the databricks module."""
        root_main = (TF_ROOT_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'module "databricks"' in root_main
        assert 'source = "./modules/databricks"' in root_main
        assert "gcs_bronze_bucket_name   = module.storage.bronze_bucket_name" in root_main
        assert "gcs_silver_bucket_name   = module.storage.silver_bucket_name" in root_main
        assert "gcs_gold_bucket_name     = module.storage.gold_bucket_name" in root_main

        root_vars = (TF_ROOT_DIR / "variables.tf").read_text(encoding="utf-8")
        assert 'variable "databricks_workspace_url"' in root_vars
        assert 'variable "databricks_cluster_node_type"' in root_vars
        assert 'variable "databricks_spark_version"' in root_vars
        assert 'variable "databricks_min_workers"' in root_vars
        assert 'variable "databricks_max_workers"' in root_vars
        assert 'variable "databricks_catalog_name"' in root_vars

        root_outputs = (TF_ROOT_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert 'output "databricks_cluster_id"' in root_outputs
        assert 'output "databricks_bronze_job_id"' in root_outputs
        assert 'output "databricks_catalog_name"' in root_outputs
        assert 'output "databricks_bronze_schema"' in root_outputs
        assert 'output "databricks_bronze_external_location_id"' in root_outputs

    def test_cluster_and_unity_catalog_definitions(self) -> None:
        """Verify databricks main.tf specifies spot clusters, Delta Spark confs, and Unity Catalog."""
        main_tf = (TF_DATABRICKS_DIR / "main.tf").read_text(encoding="utf-8")
        assert "resource \"databricks_cluster\" \"bronze_lakehouse_cluster\"" in main_tf
        assert "resource \"databricks_catalog\" \"lakehouse_catalog\"" in main_tf
        assert "resource \"databricks_schema\" \"bronze_schema\"" in main_tf
        assert "resource \"databricks_storage_credential\" \"gcs_credential\"" in main_tf
        assert "resource \"databricks_external_location\" \"bronze_external_location\"" in main_tf
        assert "resource \"databricks_job\" \"bronze_ingestion_job\"" in main_tf
        assert "SPOT_WITH_FALLBACK_GCP" in main_tf
        assert "spark.sql.extensions" in main_tf
        assert "io.delta.sql.DeltaSparkSessionExtension" in main_tf
        assert "spark.databricks.delta.optimizeWrite.enabled" in main_tf
        assert "spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled" in main_tf

    def test_environment_tfvars_definitions(self) -> None:
        """Verify dev.tfvars and prod.tfvars define Databricks environment properties."""
        dev_vars = (TF_ENV_DIR / "dev.tfvars").read_text(encoding="utf-8")
        assert "databricks_workspace_url" in dev_vars
        assert "databricks_cluster_node_type        = \"n2-standard-4\"" in dev_vars
        assert "databricks_max_workers              = 4" in dev_vars

        prod_vars = (TF_ENV_DIR / "prod.tfvars").read_text(encoding="utf-8")
        assert "databricks_workspace_url" in prod_vars
        assert "databricks_cluster_node_type        = \"n2-standard-8\"" in prod_vars
        assert "databricks_max_workers              = 16" in prod_vars


# =============================================================================
# 2. DATABRICKS WORKSPACE & CLUSTER CONFIGURATION TESTS
# =============================================================================

class TestDatabricksConfigs:
    """Validate DatabricksClusterConfig and DatabricksWorkspaceConfig models."""

    def test_cluster_config_defaults_and_validation(self) -> None:
        """Verify default cluster parameters and Delta Spark confs."""
        cfg = DatabricksClusterConfig()
        assert cfg.cluster_name == "tre-databricks-bronze-dev"
        assert cfg.spark_version == "15.4.x-scala2.12"
        assert cfg.node_type_id == "n2-standard-4"
        assert cfg.min_workers == 1
        assert cfg.max_workers == 8
        assert cfg.autotermination_minutes == 20
        assert cfg.gcp_availability == "SPOT_WITH_FALLBACK_GCP"
        assert "spark.sql.extensions" in cfg.spark_conf
        assert cfg.spark_conf["spark.databricks.delta.optimizeWrite.enabled"] == "true"

    def test_cluster_config_invalid_invariants(self) -> None:
        """Verify validation errors on contradictory cluster settings."""
        with pytest.raises(ValueError, match="min_workers .* cannot exceed max_workers"):
            DatabricksClusterConfig(min_workers=10, max_workers=5)

        with pytest.raises(ValueError, match="min_workers must be non-negative"):
            DatabricksClusterConfig(min_workers=-1)

        with pytest.raises(ValueError, match="max_workers must be at least 1"):
            DatabricksClusterConfig(max_workers=0)

        with pytest.raises(ValueError, match="autotermination_minutes must be >= 0"):
            DatabricksClusterConfig(autotermination_minutes=-5)

        with pytest.raises(ValueError, match="Invalid gcp_availability"):
            DatabricksClusterConfig(gcp_availability="INVALID_OPTION")

    def test_cluster_config_to_api_payload(self) -> None:
        """Verify Clusters API payload structure."""
        cfg = DatabricksClusterConfig(
            cluster_name="test-cluster",
            node_type_id="n2-standard-8",
            min_workers=2,
            max_workers=12,
        )
        payload = cfg.to_api_payload()
        assert payload["cluster_name"] == "test-cluster"
        assert payload["node_type_id"] == "n2-standard-8"
        assert payload["autoscale"] == {"min_workers": 2, "max_workers": 12}
        assert payload["gcp_attributes"]["availability"] == "SPOT_WITH_FALLBACK_GCP"
        assert "spark.sql.extensions" in payload["spark_conf"]

    def test_cluster_config_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify cluster configuration parsing from environment variables."""
        monkeypatch.setenv("DATABRICKS_CLUSTER_NAME", "cluster-from-env")
        monkeypatch.setenv("DATABRICKS_CLUSTER_MIN_WORKERS", "3")
        monkeypatch.setenv("DATABRICKS_CLUSTER_MAX_WORKERS", "9")
        monkeypatch.setenv("DATABRICKS_CLUSTER_GCP_AVAILABILITY", "ON_DEMAND_GCP")

        cfg = DatabricksClusterConfig.from_env()
        assert cfg.cluster_name == "cluster-from-env"
        assert cfg.min_workers == 3
        assert cfg.max_workers == 9
        assert cfg.gcp_availability == "ON_DEMAND_GCP"

    def test_workspace_config_defaults_and_urls(self) -> None:
        """Verify default workspace URLs and GCS Medallion bucket paths."""
        ws = DatabricksWorkspaceConfig(environment="prod", project_id="trading-risk-prod-001")
        assert ws.workspace_url == "https://trading-risk-dev.cloud.databricks.com"
        assert ws.bronze_bucket_name == "tre-trading-risk-prod-001-prod-bronze-raw"
        assert ws.bronze_delta_path == "gs://tre-trading-risk-prod-001-prod-bronze-raw/delta/bronze"
        assert ws.get_table_uri("bronze_market_ticks", "bronze") == (
            "gs://tre-trading-risk-prod-001-prod-bronze-raw/delta/bronze/bronze_market_ticks"
        )
        assert ws.get_table_uri("silver_cleansed_trades", "silver") == (
            "gs://tre-trading-risk-prod-001-prod-silver-processed/delta/silver/silver_cleansed_trades"
        )

    def test_workspace_config_invalid_url(self) -> None:
        """Verify workspace_url validation."""
        with pytest.raises(ValueError, match="workspace_url must start with http"):
            DatabricksWorkspaceConfig(workspace_url="ftp://invalid.databricks.com")

    def test_delta_table_config_invariants(self) -> None:
        """Verify DeltaTableConfig properties and full table identifiers."""
        assert BRONZE_MARKET_TICKS_CONFIG.full_table_identifier == "trading_risk.bronze.bronze_market_ticks"
        assert BRONZE_MARKET_TICKS_CONFIG.partition_columns == ["year", "month", "day", "asset_class"]
        assert BRONZE_MARKET_TICKS_CONFIG.z_order_columns == ["ticker", "event_timestamp"]

        props = BRONZE_MARKET_TICKS_CONFIG.to_table_properties()
        assert props["delta.enableChangeDataFeed"] == "true"
        assert props["delta.autoOptimize.optimizeWrite"] == "true"
        assert props["delta.autoOptimize.autoCompact"] == "true"

        with pytest.raises(ValueError, match="Invalid table_name"):
            DeltaTableConfig(table_name="invalid-name-with-dashes!")


# =============================================================================
# 3. DATA CONTRACTS & VALIDATION TESTS
# =============================================================================

class TestSchemasAndContracts:
    """Validate Spark SQL and PyArrow schemas and domain record validation."""

    def test_arrow_schemas_contain_audit_columns(self) -> None:
        """Verify all Delta Bronze PyArrow schemas include standard audit columns."""
        for schema in [
            ARROW_DELTA_BRONZE_TICK_SCHEMA,
            ARROW_DELTA_BRONZE_ORDER_SCHEMA,
            ARROW_DELTA_BRONZE_VWAP_SCHEMA,
        ]:
            field_names = set(schema.names)
            assert "_ingested_at" in field_names
            assert "_source_file" in field_names
            assert "_raw_record_hash" in field_names
            assert "_ingest_batch_id" in field_names
            assert "_ingest_date" in field_names
            assert "year" in field_names
            assert "month" in field_names
            assert "day" in field_names

    def test_validate_bronze_tick_record_success_and_errors(self) -> None:
        """Verify domain validation for market ticks."""
        valid_tick = {
            "ticker": "AAPL",
            "bid": 180.25,
            "ask": 180.30,
            "last_price": 180.27,
            "volume": 500.0,
            "timestamp": "2026-10-24T10:00:00Z",
            "asset_class": "EQUITY",
        }
        is_val, cat, msg = validate_bronze_tick_record(valid_tick)
        assert is_val is True
        assert cat is None

        # Crossed quotes: bid > ask
        crossed_tick = dict(valid_tick, bid=181.0, ask=180.0)
        is_val, cat, msg = validate_bronze_tick_record(crossed_tick)
        assert is_val is False
        assert cat == "CROSSED_QUOTES"

        # Negative price
        neg_price_tick = dict(valid_tick, bid=-1.0)
        is_val, cat, msg = validate_bronze_tick_record(neg_price_tick)
        assert is_val is False
        assert cat == "NON_POSITIVE_PRICE"

        # Negative volume
        neg_vol_tick = dict(valid_tick, volume=-10.0)
        is_val, cat, msg = validate_bronze_tick_record(neg_vol_tick)
        assert is_val is False
        assert cat == "NEGATIVE_VOLUME"

        # Missing field
        missing_tick = dict(valid_tick)
        del missing_tick["ticker"]
        is_val, cat, msg = validate_bronze_tick_record(missing_tick)
        assert is_val is False
        assert cat == "MISSING_REQUIRED_FIELD"

    def test_validate_bronze_order_record_success_and_errors(self) -> None:
        """Verify domain validation for trade orders."""
        valid_order = {
            "order_id": "ORD-12345",
            "portfolio_id": "PORT-GLOBAL",
            "ticker": "MSFT",
            "side": "BUY",
            "quantity": 100.0,
            "price": 420.50,
            "order_type": "LIMIT",
            "timestamp": "2026-10-24T10:05:00Z",
        }
        is_val, cat, msg = validate_bronze_order_record(valid_order)
        assert is_val is True

        # Invalid side
        bad_side = dict(valid_order, side="HOLD")
        is_val, cat, msg = validate_bronze_order_record(bad_side)
        assert is_val is False
        assert cat == "INVALID_SIDE"

        # Invalid order type
        bad_type = dict(valid_order, order_type="FILL_OR_KILL")
        is_val, cat, msg = validate_bronze_order_record(bad_type)
        assert is_val is False
        assert cat == "INVALID_ORDER_TYPE"

        # Non-positive quantity
        bad_qty = dict(valid_order, quantity=0.0)
        is_val, cat, msg = validate_bronze_order_record(bad_qty)
        assert is_val is False
        assert cat == "NON_POSITIVE_QUANTITY"

    def test_validate_bronze_vwap_record_success_and_errors(self) -> None:
        """Verify domain validation for rolling VWAP records."""
        valid_vwap = {
            "ticker": "NVDA",
            "asset_class": "EQUITY",
            "window_start": "2026-10-24T10:00:00Z",
            "window_end": "2026-10-24T10:05:00Z",
            "vwap": 125.40,
            "total_volume": 15000.0,
        }
        is_val, cat, msg = validate_bronze_vwap_record(valid_vwap)
        assert is_val is True

        # Non-positive vwap
        bad_vwap = dict(valid_vwap, vwap=0.0)
        is_val, cat, msg = validate_bronze_vwap_record(bad_vwap)
        assert is_val is False
        assert cat == "NON_POSITIVE_VWAP"

    def test_quarantined_record_serialization(self) -> None:
        """Verify QuarantinedBronzeRecord serializes to JSON dictionary."""
        q = QuarantinedBronzeRecord(
            record_index=42,
            raw_payload='{"ticker": "NVDA", "bid": -10}',
            error_category="NON_POSITIVE_PRICE",
            error_message="Bid must be > 0",
            source_file="gs://bucket/raw.json",
            quarantined_at="2026-10-24T10:00:00Z",
            batch_id="batch-uuid-123",
        )
        d = q.to_dict()
        assert d["record_index"] == 42
        assert d["error_category"] == "NON_POSITIVE_PRICE"
        assert d["batch_id"] == "batch-uuid-123"


# =============================================================================
# 4. DETERMINISTIC RECORD HASHING TESTS
# =============================================================================

class TestRecordHashing:
    """Validate SHA-256 record hashing for audit lineage and idempotency."""

    def test_deterministic_hash_generation(self) -> None:
        """Verify identical payloads generate identical 64-char SHA-256 hashes."""
        payload1 = {"ticker": "AAPL", "price": 180.50, "volume": 100}
        payload2 = {"volume": 100, "ticker": "AAPL", "price": 180.50}
        hash1 = compute_record_hash(payload1)
        hash2 = compute_record_hash(payload2)
        assert len(hash1) == 64
        assert hash1 == hash2

        # Value change generates different hash
        payload3 = {"ticker": "AAPL", "price": 180.51, "volume": 100}
        assert compute_record_hash(payload3) != hash1

    def test_hash_ignores_audit_columns(self) -> None:
        """Verify variable audit columns (_ingested_at, etc.) do not affect payload hash."""
        base_payload = {"ticker": "GOOGL", "bid": 175.0, "ask": 175.1}
        enriched1 = dict(base_payload, _ingested_at="2026-10-24T10:00:00", _source_file="file1.parquet")
        enriched2 = dict(base_payload, _ingested_at="2026-10-24T10:05:00", _source_file="file2.parquet")
        assert compute_record_hash(enriched1) == compute_record_hash(enriched2)


# =============================================================================
# 5. DELTA TRANSACTION LOG TESTS
# =============================================================================

class TestDeltaLogManager:
    """Validate Delta Lake transaction log (_delta_log/) commit protocol."""

    def test_delta_log_lifecycle_and_versions(self, tmp_path: Path) -> None:
        """Verify version sequencing from -1 (empty) to v0, v1, etc."""
        table_dir = tmp_path / "delta_table"
        table_dir.mkdir()
        mgr = DeltaLogManager(table_dir)
        assert mgr.get_latest_version() == -1

        # Write v0 commit
        commit_0 = mgr.write_commit(0, [
            {"commitInfo": {"operation": "WRITE", "timestamp": 1000}},
            {"add": {"path": "year=2026/part-1.parquet", "size": 1024}},
        ])
        assert commit_0.exists()
        assert mgr.get_latest_version() == 0

        # Write v1 commit
        mgr.write_commit(1, [
            {"commitInfo": {"operation": "WRITE", "timestamp": 2000}},
            {"add": {"path": "year=2026/part-2.parquet", "size": 2048}},
        ])
        assert mgr.get_latest_version() == 1

        history = mgr.read_commit_history()
        assert len(history) == 2
        assert history[0]["version"] == 0
        assert history[1]["version"] == 1

    def test_active_files_tracking(self, tmp_path: Path) -> None:
        """Verify active file list adds and removes across transactions."""
        table_dir = tmp_path / "active_files_table"
        table_dir.mkdir()
        mgr = DeltaLogManager(table_dir)

        # v0 adds f1 and f2
        mgr.write_commit(0, [
            {"add": {"path": "f1.parquet", "size": 100}},
            {"add": {"path": "f2.parquet", "size": 200}},
        ])
        active_0 = {f["path"] for f in mgr.get_active_files()}
        assert active_0 == {"f1.parquet", "f2.parquet"}

        # v1 removes f1 and adds f3 (e.g. compaction)
        mgr.write_commit(1, [
            {"remove": {"path": "f1.parquet"}},
            {"add": {"path": "f3.parquet", "size": 300}},
        ])
        active_1 = {f["path"] for f in mgr.get_active_files()}
        assert active_1 == {"f2.parquet", "f3.parquet"}


# =============================================================================
# 6. HERMETIC DELTA LAKE BRONZE INGESTION TESTS
# =============================================================================

class TestHermeticDeltaBronzeIngestion:
    """Validate Delta Bronze ingestion engine across ticks, orders, and VWAP."""

    @pytest.fixture
    def sample_ticks(self) -> List[Dict[str, Any]]:
        """Fixture producing clean market tick records."""
        return [
            {
                "ticker": "AAPL",
                "bid": 182.10,
                "ask": 182.15,
                "last_price": 182.12,
                "volume": 120.0,
                "timestamp": "2026-10-24T10:00:01Z",
                "asset_class": "EQUITY",
                "sequence_number": 1,
                "source": "SIM_FEED",
            },
            {
                "ticker": "MSFT",
                "bid": 425.50,
                "ask": 425.60,
                "last_price": 425.55,
                "volume": 200.0,
                "timestamp": "2026-10-24T10:00:02Z",
                "asset_class": "EQUITY",
                "sequence_number": 2,
                "source": "SIM_FEED",
            },
            {
                "ticker": "BTCUSD",
                "bid": 68500.0,
                "ask": 68510.0,
                "last_price": 68505.0,
                "volume": 1.5,
                "timestamp": "2026-10-24T10:00:03Z",
                "asset_class": "CRYPTO",
                "sequence_number": 3,
                "source": "SIM_FEED",
            },
        ]

    def test_ingest_market_ticks_from_memory(self, tmp_path: Path, sample_ticks: List[Dict[str, Any]]) -> None:
        """Verify in-memory ticks ingestion into Delta Bronze table."""
        engine = DeltaBronzeIngestionEngine()
        target_table = tmp_path / "bronze_market_ticks"

        res = engine.ingest_market_ticks(
            source_path=sample_ticks,
            target_path=target_table,
            ingest_date=date(2026, 10, 24),
        )

        assert isinstance(res, BronzeIngestionResult)
        assert res.records_read == 3
        assert res.records_ingested == 3
        assert res.records_quarantined == 0
        assert res.delta_commit_version == 0
        assert len(res.partitions_affected) >= 2  # EQUITY and CRYPTO

        # Verify Delta Log exists
        log_dir = target_table / "_delta_log"
        assert (log_dir / "00000000000000000000.json").exists()

        # Read back written Parquet files and check audit columns
        parquet_files = list(target_table.rglob("*.snappy.parquet"))
        assert len(parquet_files) >= 2
        for pf in parquet_files:
            table = pq.read_table(pf)
            assert "_ingested_at" in table.column_names
            assert "_raw_record_hash" in table.column_names
            assert "_ingest_batch_id" in table.column_names
            assert "_ingest_date" in table.column_names
            assert len(table) > 0

    def test_ingest_market_ticks_from_parquet_files(self, tmp_path: Path, sample_ticks: List[Dict[str, Any]]) -> None:
        """Verify ingesting PySpark-generated partitioned Snappy Parquet files into Delta Bronze."""
        raw_source_dir = tmp_path / "raw_parquet_ticks"
        part_dir = raw_source_dir / "year=2026" / "month=10" / "day=24" / "asset_class=EQUITY"
        part_dir.mkdir(parents=True)

        raw_arrow = pa.Table.from_pylist([
            sample_ticks[0],
            sample_ticks[1],
        ])
        pq.write_table(raw_arrow, part_dir / "raw_ticks.snappy.parquet", compression="snappy")

        engine = DeltaBronzeIngestionEngine()
        target_table = tmp_path / "bronze_ticks_from_parquet"

        res = engine.ingest_market_ticks(
            source_path=raw_source_dir,
            target_path=target_table,
            ingest_date=date(2026, 10, 24),
        )

        assert res.records_read == 2
        assert res.records_ingested == 2
        assert res.delta_commit_version == 0

    def test_ingest_trade_orders_from_json_files(self, tmp_path: Path) -> None:
        """Verify ingesting Dataflow raw JSON lines trade orders into Delta Bronze."""
        raw_source_dir = tmp_path / "raw_json_orders"
        raw_source_dir.mkdir()
        order_file = raw_source_dir / "orders_part_001.jsonl"

        orders = [
            {
                "order_id": "ORD-001",
                "portfolio_id": "PORT-GLOBAL",
                "ticker": "AAPL",
                "side": "BUY",
                "quantity": 500.0,
                "price": 182.50,
                "order_type": "LIMIT",
                "timestamp": "2026-10-24T10:15:00Z",
                "trader_id": "TRADER-7",
                "execution_status": "FILLED",
                "sequence_number": 101,
                "notional_usd": 91250.0,
            },
            {
                "order_id": "ORD-002",
                "portfolio_id": "PORT-GLOBAL",
                "ticker": "MSFT",
                "side": "SELL",
                "quantity": 200.0,
                "price": 425.00,
                "order_type": "MARKET",
                "timestamp": "2026-10-24T10:15:30Z",
                "trader_id": "TRADER-9",
                "execution_status": "FILLED",
                "sequence_number": 102,
                "notional_usd": 85000.0,
            },
        ]
        order_file.write_text("\n".join(json.dumps(o) for o in orders) + "\n", encoding="utf-8")

        engine = DeltaBronzeIngestionEngine()
        target_table = tmp_path / "bronze_trade_orders"

        res = engine.ingest_trade_orders(
            source_path=raw_source_dir,
            target_path=target_table,
            ingest_date=date(2026, 10, 24),
        )

        assert res.records_read == 2
        assert res.records_ingested == 2
        assert res.table_name == "bronze_trade_orders"
        assert res.delta_commit_version == 0

    def test_ingest_rolling_vwap_records(self, tmp_path: Path) -> None:
        """Verify rolling VWAP aggregations ingestion into Delta Bronze."""
        vwap_records = [
            {
                "ticker": "AAPL",
                "asset_class": "EQUITY",
                "window_start": "2026-10-24T10:00:00Z",
                "window_end": "2026-10-24T10:05:00Z",
                "tick_count": 150,
                "open_price": 182.0,
                "high_price": 183.0,
                "low_price": 181.5,
                "close_price": 182.8,
                "total_volume": 25000.0,
                "total_notional": 4560000.0,
                "vwap": 182.40,
                "price_stddev": 0.35,
                "price_variance": 0.1225,
                "volatility_multiplier": 2.0,
                "upper_band": 183.10,
                "lower_band": 181.70,
                "bandwidth_bps": 76.75,
                "percent_b": 0.78,
                "is_upper_breach": False,
                "is_lower_breach": False,
                "volatility_spike": False,
            }
        ]
        engine = DeltaBronzeIngestionEngine()
        target_table = tmp_path / "bronze_rolling_vwap"

        res = engine.ingest_rolling_vwap(
            source_path=vwap_records,
            target_path=target_table,
            ingest_date=date(2026, 10, 24),
        )

        assert res.records_ingested == 1
        assert res.delta_commit_version == 0
        assert res.table_name == "bronze_rolling_vwap"

    def test_quarantine_corrupted_records(self, tmp_path: Path) -> None:
        """Verify invalid records are routed to quarantine storage while valid records are ingested."""
        records = [
            # 1. Valid record
            {
                "ticker": "AAPL",
                "bid": 180.0,
                "ask": 180.1,
                "last_price": 180.05,
                "volume": 100.0,
                "timestamp": "2026-10-24T10:00:00Z",
                "asset_class": "EQUITY",
                "sequence_number": 1,
            },
            # 2. Corrupt: crossed quotes (bid > ask)
            {
                "ticker": "AAPL",
                "bid": 185.0,
                "ask": 180.0,
                "last_price": 182.0,
                "volume": 100.0,
                "timestamp": "2026-10-24T10:00:01Z",
                "asset_class": "EQUITY",
                "sequence_number": 2,
            },
            # 3. Corrupt: negative price
            {
                "ticker": "GOOGL",
                "bid": -10.0,
                "ask": 170.0,
                "last_price": 170.0,
                "volume": 50.0,
                "timestamp": "2026-10-24T10:00:02Z",
                "asset_class": "EQUITY",
                "sequence_number": 3,
            },
        ]

        engine = DeltaBronzeIngestionEngine()
        target_table = tmp_path / "quarantine_test_table"
        quarantine_dir = tmp_path / "custom_quarantine"

        res = engine.ingest_market_ticks(
            source_path=records,
            target_path=target_table,
            quarantine_dir=quarantine_dir,
        )

        assert res.records_read == 3
        assert res.records_ingested == 1
        assert res.records_quarantined == 2
        assert res.quarantine_path is not None
        assert Path(res.quarantine_path).exists()

        # Read quarantined log
        lines = [json.loads(line) for line in Path(res.quarantine_path).read_text(encoding="utf-8").splitlines() if line.strip()]
        assert len(lines) == 2
        categories = {item["error_category"] for item in lines}
        assert "CROSSED_QUOTES" in categories
        assert "NON_POSITIVE_PRICE" in categories

    def test_delta_table_maintenance_optimize_and_vacuum(self, tmp_path: Path, sample_ticks: List[Dict[str, Any]]) -> None:
        """Verify Delta Lake OPTIMIZE and VACUUM commits append transactions."""
        engine = DeltaBronzeIngestionEngine()
        target_table = tmp_path / "maintenance_table"

        # Ingestion = commit v0
        res0 = engine.ingest_market_ticks(source_path=sample_ticks, target_path=target_table)
        assert res0.delta_commit_version == 0

        # OPTIMIZE = commit v1
        opt_res = engine.optimize_table(target_table, z_order_columns=["ticker", "timestamp"])
        assert opt_res["version"] == 1
        assert opt_res["operation"] == "OPTIMIZE"

        # VACUUM = commit v2
        vac_res = engine.vacuum_table(target_table, retention_hours=168)
        assert vac_res["version"] == 2
        assert vac_res["operation"] == "VACUUM"

        # History should have 3 commits
        log_mgr = DeltaLogManager(target_table)
        history = log_mgr.read_commit_history()
        assert len(history) == 3
        assert history[0]["commit_info"]["operation"] == "WRITE"
        assert history[1]["commit_info"]["operation"] == "OPTIMIZE"
        assert history[2]["commit_info"]["operation"] == "VACUUM"

    def test_incremental_appends_and_version_advancement(self, tmp_path: Path, sample_ticks: List[Dict[str, Any]]) -> None:
        """Verify subsequent batch runs append with monotonically incrementing commit versions."""
        engine = DeltaBronzeIngestionEngine()
        target_table = tmp_path / "incremental_table"

        res1 = engine.ingest_market_ticks(source_path=sample_ticks[:2], target_path=target_table)
        assert res1.delta_commit_version == 0
        assert res1.records_ingested == 2

        res2 = engine.ingest_market_ticks(source_path=sample_ticks[2:], target_path=target_table)
        assert res2.delta_commit_version == 1
        assert res2.records_ingested == 1

        log_mgr = DeltaLogManager(target_table)
        active_files = log_mgr.get_active_files()
        assert len(active_files) >= 2


# =============================================================================
# 7. DATABRICKS REST API CLIENT & MOCK TESTS
# =============================================================================

class TestDatabricksClientAndMock:
    """Validate Databricks REST API Client and in-memory mock."""

    def test_mock_client_create_and_run_job(self) -> None:
        """Verify creating and triggering a Databricks Job in mock."""
        client = MockDatabricksClient(auto_succeed_runs=True)

        job_spec = {
            "name": "test-bronze-job",
            "notebook_task": {"notebook_path": "/Workspace/test"},
        }
        job_id = client.create_job(job_spec)
        assert job_id >= 1000

        run_id = client.run_job_now(job_id, parameters={"env": "dev"})
        assert run_id >= 5000

        run_details = client.get_run(run_id)
        assert run_details["run_id"] == run_id
        assert run_details["state"]["life_cycle_state"] == RunLifeCycleState.TERMINATED.value
        assert run_details["state"]["result_state"] == RunResultState.SUCCESS.value

        run_output = client.get_run_output(run_id)
        assert "notebook_output" in run_output

    def test_mock_client_cancel_run(self) -> None:
        """Verify run cancellation updates state to CANCELED."""
        client = MockDatabricksClient(auto_succeed_runs=False)
        job_id = client.create_job({"name": "long-running-job"})
        run_id = client.run_job_now(job_id)

        # Before cancel: RUNNING
        run_before = client.get_run(run_id)
        assert run_before["state"]["life_cycle_state"] == RunLifeCycleState.RUNNING.value

        client.cancel_run(run_id)

        # After cancel: TERMINATED / CANCELED
        run_after = client.get_run(run_id)
        assert run_after["state"]["life_cycle_state"] == RunLifeCycleState.TERMINATED.value
        assert run_after["state"]["result_state"] == RunResultState.CANCELED.value

    def test_mock_client_cluster_lifecycle(self) -> None:
        """Verify creating, inspecting, and terminating a cluster in mock."""
        client = MockDatabricksClient()
        cfg = DatabricksClusterConfig(cluster_name="test-lifecycle-cluster")

        cluster_id = client.create_cluster(cfg)
        assert cluster_id.startswith("mock-cluster-")

        info = client.get_cluster(cluster_id)
        assert info["state"] == "RUNNING"
        assert info["cluster_name"] == "test-lifecycle-cluster"

        client.terminate_cluster(cluster_id)
        info_terminated = client.get_cluster(cluster_id)
        assert info_terminated["state"] == "TERMINATED"

    def test_mock_client_job_not_found(self) -> None:
        """Verify error when running a non-existent job."""
        client = MockDatabricksClient()
        with pytest.raises(DatabricksClientException, match="Job ID 99999 not found"):
            client.run_job_now(99999)


# =============================================================================
# 8. PRODUCTION NOTEBOOK RUNNER CLI TESTS
# =============================================================================

class TestNotebookRunnerCLI:
    """Validate 01_bronze_ingestion_runner CLI script and parameter parsing."""

    def test_cli_argument_parsing_defaults(self) -> None:
        """Verify argument parser defaults."""
        args = parse_args([])
        assert args.environment == "dev"
        assert args.source_type == "market_ticks"
        assert args.run_optimize is True
        assert args.run_vacuum is True
        assert args.dry_run is False

    def test_cli_dry_run_mode(self) -> None:
        """Verify dry-run returns execution plan without writing files."""
        args = parse_args(["--dry-run", "--source-type", "trade_orders"])
        res = run_bronze_ingestion(args)
        assert res["status"] == "DRY_RUN"
        assert res["source_type"] == "trade_orders"

    def test_cli_full_execution_market_ticks(self, tmp_path: Path) -> None:
        """Verify CLI runner executing real ingestion for market ticks."""
        source_dir = tmp_path / "source_ticks"
        source_dir.mkdir()
        tick_file = source_dir / "ticks.jsonl"
        tick_file.write_text(
            json.dumps({
                "ticker": "NVDA",
                "bid": 128.50,
                "ask": 128.55,
                "last_price": 128.52,
                "volume": 1000.0,
                "timestamp": "2026-10-24T10:00:00Z",
                "asset_class": "EQUITY",
                "sequence_number": 1,
            }) + "\n",
            encoding="utf-8",
        )

        target_dir = tmp_path / "delta_ticks"

        args = parse_args([
            "--source-type", "market_ticks",
            "--source-path", str(source_dir),
            "--target-path", str(target_dir),
            "--ingestion-date", "2026-10-24",
            "--run-optimize", "true",
            "--run-vacuum", "true",
        ])

        summary = run_bronze_ingestion(args)
        assert summary["status"] == "SUCCESS"
        assert len(summary["results"]) == 1
        res = summary["results"][0]
        assert res["records_ingested"] == 1
        assert "optimize" in res
        assert "vacuum" in res
        assert Path(target_dir / "_delta_log").exists()


# =============================================================================
# 9. TELEMETRY & PROMETHEUS METRICS TESTS
# =============================================================================

class TestTelemetryAndMetrics:
    """Validate Bronze ingestion metrics tracking and Prometheus format."""

    def test_telemetry_snapshot_and_prometheus(self) -> None:
        """Verify metrics record success and format valid Prometheus lines."""
        telemetry = BronzeIngestionTelemetry()
        telemetry.record_success(
            records_ingested=5000,
            quarantined=12,
            bytes_ingested=1048576,
            files=4,
            commit_version=3,
            duration_sec=1.25,
        )

        assert telemetry.batches_completed == 1
        assert telemetry.records_ingested_total == 5000
        assert telemetry.quarantined_records_total == 12
        assert telemetry.latest_commit_version == 3

        prom_text = telemetry.format_prometheus()
        assert "databricks_bronze_batches_total 1" in prom_text
        assert "databricks_bronze_records_ingested_total 5000" in prom_text
        assert "databricks_bronze_quarantined_records_total 12" in prom_text
        assert "databricks_bronze_commit_version 3" in prom_text
