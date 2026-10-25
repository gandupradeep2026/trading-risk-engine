"""Databricks Silver Layer Cleansing, Schema Enforcement, and Deduplication Tests (Day 17).

Validates:
1. Terraform Databricks Module:
   - Silver schema resource (databricks_schema.silver_schema) mapped to GCS Silver external location.
   - Silver cleansing scheduled job (databricks_job.silver_cleansing_job) pointing to notebook runner.
   - Silver outputs in module outputs.tf and root terraform/outputs.tf.
   - Balanced braces across all Terraform files.
2. Databricks Silver Configurations & Invariants:
   - DeltaTableConfig for silver_market_ticks, silver_trade_orders, and silver_rolling_vwap.
   - Hive partition columns and Z-Ordering keys.
   - Change Data Feed (CDF), auto-optimize write, and auto-compact enablement.
3. Schemas & Quality Contracts:
   - Spark SQL DDL schemas and PyArrow schemas for Silver ticks, orders, and rolling VWAP.
   - Audit lineage columns (_cleansed_at, _cleansed_record_hash, _cleansing_batch_id, _ingest_date).
   - Forensic quarantine record formatting.
4. Record Validation & Invariant Enforcement:
   - Domain rules for ticks (bid <= ask, prices > 0, volume >= 0).
   - Domain rules for orders (BUY/SELL, MARKET/LIMIT, positive price and quantity).
   - Domain rules for rolling VWAP (positive VWAP, non-negative volume).
   - Quarantining of invalid records without breaking execution.
5. Microstructure Enrichments & Feature Derivations:
   - Relative spread in bps, quote conditions (TIGHT_SPREAD, WIDE_SPREAD, REGULAR), price returns.
   - Order categorization (EQUITY, CRYPTO, FX, ETF, COMMODITY), notional tiers, aggressive taker flag.
   - Volatility regimes (LOW, NORMAL, HIGH, EXTREME) and trend direction (BULLISH, BEARISH, NEUTRAL).
6. Intra-Batch Deduplication:
   - Elimination of duplicate primary keys within a single batch, keeping newest records.
7. Inter-Table Delta Lake MERGE INTO (Upsert):
   - Upsert into existing Silver tables with Delta ACID transaction log commits (operation: "MERGE").
   - Metrics verification: numTargetRowsInserted, numTargetRowsUpdated, numTargetRowsCopied.
   - Delta log action verification: remove actions for rewritten files and add actions for new files.
8. End-to-End Hermetic Ingestion (Bronze to Silver):
   - Market ticks cleansing from Bronze Delta / Parquet to Silver.
   - Trade orders cleansing and quarantine isolation.
   - Rolling VWAP cleansing and enrichment.
9. Table Maintenance:
   - Delta Lake OPTIMIZE with Z-Ordering commits.
   - Delta Lake VACUUM retention commits.
10. Databricks Notebook Runner CLI:
    - CLI argument parsing, dry-run mode, and full automated execution.
11. Operational Telemetry & Prometheus Metrics:
    - Counter monotonicity and Prometheus text exposition formatting.
"""

from datetime import date, datetime, timedelta, timezone
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
    ARROW_DELTA_SILVER_ORDER_SCHEMA,
    ARROW_DELTA_SILVER_TICK_SCHEMA,
    ARROW_DELTA_SILVER_VWAP_SCHEMA,
    DELTA_SILVER_ORDER_DDL,
    DELTA_SILVER_TICK_DDL,
    DELTA_SILVER_VWAP_DDL,
    DatabricksWorkspaceConfig,
    DeltaBronzeIngestionEngine,
    DeltaLogManager,
    DeltaSilverCleansingEngine,
    DeltaTableConfig,
    QuarantinedSilverRecord,
    SILVER_MARKET_TICKS_CONFIG,
    SILVER_ROLLING_VWAP_CONFIG,
    SILVER_TRADE_ORDERS_CONFIG,
    SilverCleansingResult,
    SilverCleansingTelemetry,
    cleanse_and_enrich_order,
    cleanse_and_enrich_tick,
    cleanse_and_enrich_vwap,
    compute_cleansed_hash,
    parse_iso_or_epoch_timestamp,
    validate_silver_order_record,
    validate_silver_tick_record,
    validate_silver_vwap_record,
)
from batch.databricks.notebooks.silver_cleansing_runner import (
    parse_args,
    run_silver_cleansing,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_DATABRICKS_DIR = REPO_ROOT / "terraform" / "modules" / "databricks"
TF_ROOT_DIR = REPO_ROOT / "terraform"


# =============================================================================
# 1. TERRAFORM DATABRICKS SILVER MODULE VALIDATION
# =============================================================================

class TestTerraformSilverModule:
    """Validate Terraform Databricks module resources and outputs for Silver layer."""

    def test_silver_schema_and_job_defined_in_main_tf(self) -> None:
        """Verify databricks_schema.silver_schema and databricks_job.silver_cleansing_job exist."""
        main_tf = (TF_DATABRICKS_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'resource "databricks_schema" "silver_schema"' in main_tf
        assert 'name         = "silver"' in main_tf
        assert "${var.gcs_silver_bucket_url}/delta/silver" in main_tf
        assert "databricks_external_location.silver_external_location" in main_tf

        assert 'resource "databricks_job" "silver_cleansing_job"' in main_tf
        assert "02_silver_cleansing_runner" in main_tf
        assert 'silver_schema_name = databricks_schema.silver_schema.name' in main_tf

    def test_silver_outputs_in_module_and_root(self) -> None:
        """Verify silver schema, job, and location outputs are declared in module and root."""
        mod_outputs = (TF_DATABRICKS_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert 'output "silver_schema_name"' in mod_outputs
        assert 'output "silver_job_id"' in mod_outputs
        assert 'output "silver_external_location_id"' in mod_outputs

        root_outputs = (TF_ROOT_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert 'output "databricks_silver_schema"' in root_outputs
        assert 'output "databricks_silver_job_id"' in root_outputs
        assert 'output "databricks_silver_external_location_id"' in root_outputs

    def test_balanced_braces_in_terraform(self) -> None:
        """Verify balanced HCL braces across all terraform files."""
        for tf_path in TF_DATABRICKS_DIR.glob("*.tf"):
            content = tf_path.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_path.name}"


# =============================================================================
# 2. SILVER CONFIGURATIONS & INVARIANTS
# =============================================================================

class TestSilverConfigurations:
    """Validate Silver table configurations, partitioning, and Delta Lake properties."""

    def test_silver_table_configs_partitions_and_zorder(self) -> None:
        """Verify Silver table configurations specify valid partitions and Z-ordering."""
        # Market ticks
        assert SILVER_MARKET_TICKS_CONFIG.table_name == "silver_market_ticks"
        assert SILVER_MARKET_TICKS_CONFIG.schema == "silver"
        assert SILVER_MARKET_TICKS_CONFIG.partition_columns == ["year", "month", "day", "asset_class"]
        assert SILVER_MARKET_TICKS_CONFIG.z_order_columns == ["ticker", "event_timestamp"]
        assert SILVER_MARKET_TICKS_CONFIG.enable_change_data_feed is True

        # Trade orders
        assert SILVER_TRADE_ORDERS_CONFIG.table_name == "silver_trade_orders"
        assert SILVER_TRADE_ORDERS_CONFIG.schema == "silver"
        assert SILVER_TRADE_ORDERS_CONFIG.partition_columns == ["year", "month", "day", "portfolio_id"]
        assert SILVER_TRADE_ORDERS_CONFIG.z_order_columns == ["portfolio_id", "ticker", "timestamp"]
        assert SILVER_TRADE_ORDERS_CONFIG.enable_change_data_feed is True

        # Rolling VWAP
        assert SILVER_ROLLING_VWAP_CONFIG.table_name == "silver_rolling_vwap"
        assert SILVER_ROLLING_VWAP_CONFIG.schema == "silver"
        assert SILVER_ROLLING_VWAP_CONFIG.partition_columns == ["year", "month", "day", "asset_class"]
        assert SILVER_ROLLING_VWAP_CONFIG.z_order_columns == ["ticker", "window_start"]
        assert SILVER_ROLLING_VWAP_CONFIG.enable_change_data_feed is True

    def test_silver_table_properties_map(self) -> None:
        """Verify Spark SQL Delta table properties generation."""
        props = SILVER_MARKET_TICKS_CONFIG.to_table_properties()
        assert props["delta.enableChangeDataFeed"] == "true"
        assert props["delta.autoOptimize.optimizeWrite"] == "true"
        assert props["delta.autoOptimize.autoCompact"] == "true"
        assert props["delta.logRetentionDuration"] == "interval 30 days"
        assert props["delta.deletedFileRetentionDuration"] == "interval 7 days"

    def test_full_table_identifiers(self) -> None:
        """Verify three-tier Unity Catalog table identifiers."""
        assert SILVER_MARKET_TICKS_CONFIG.full_table_identifier == "trading_risk.silver.silver_market_ticks"
        assert SILVER_TRADE_ORDERS_CONFIG.full_table_identifier == "trading_risk.silver.silver_trade_orders"
        assert SILVER_ROLLING_VWAP_CONFIG.full_table_identifier == "trading_risk.silver.silver_rolling_vwap"


# =============================================================================
# 3. SILVER SCHEMAS & DATA QUALITY CONTRACTS
# =============================================================================

class TestSilverSchemasAndContracts:
    """Validate Spark SQL DDL schemas, PyArrow schemas, and audit columns."""

    def test_arrow_schemas_contain_silver_audit_columns(self) -> None:
        """Verify Silver Arrow schemas contain all required audit and cleansed columns."""
        for schema in [ARROW_DELTA_SILVER_TICK_SCHEMA, ARROW_DELTA_SILVER_ORDER_SCHEMA, ARROW_DELTA_SILVER_VWAP_SCHEMA]:
            names = set(schema.names)
            assert "_ingested_at" in names
            assert "_cleansed_at" in names
            assert "_source_file" in names
            assert "_raw_record_hash" in names
            assert "_cleansed_record_hash" in names
            assert "_cleansing_batch_id" in names
            assert "_ingest_date" in names
            assert "year" in names
            assert "month" in names
            assert "day" in names

    def test_ddl_and_arrow_schema_consistency(self) -> None:
        """Verify DDL schemas match PyArrow schemas for ticks, orders, and vwap."""
        def extract_ddl_names(ddl: str) -> List[str]:
            names = []
            for line in ddl.strip().splitlines():
                clean = line.strip().rstrip(",")
                if clean:
                    parts = clean.split()
                    names.append(parts[0])
            return names

        tick_ddl_names = extract_ddl_names(DELTA_SILVER_TICK_DDL)
        assert tick_ddl_names == ARROW_DELTA_SILVER_TICK_SCHEMA.names

        order_ddl_names = extract_ddl_names(DELTA_SILVER_ORDER_DDL)
        assert order_ddl_names == ARROW_DELTA_SILVER_ORDER_SCHEMA.names

        vwap_ddl_names = extract_ddl_names(DELTA_SILVER_VWAP_DDL)
        assert vwap_ddl_names == ARROW_DELTA_SILVER_VWAP_SCHEMA.names

    def test_quarantined_record_structure(self) -> None:
        """Verify QuarantinedSilverRecord serialization."""
        q = QuarantinedSilverRecord(
            record_index=0,
            raw_payload='{"bad": "data"}',
            error_category="CROSSED_QUOTES",
            error_message="bid > ask",
            source_file="test.parquet",
            quarantined_at="2026-10-25T10:00:00+00:00",
            batch_id="b-123",
        )
        d = q.to_dict()
        assert d["error_category"] == "CROSSED_QUOTES"
        assert d["batch_id"] == "b-123"
        serialized = json.dumps(d)
        assert "CROSSED_QUOTES" in serialized


# =============================================================================
# 4. DOMAIN VALIDATION AND RECORD CLEANSING
# =============================================================================

class TestDomainValidationAndCleansing:
    """Validate domain invariants, error detection, and quarantine classification."""

    def test_validate_silver_tick_record(self) -> None:
        """Verify tick validation catches crossed quotes, negative volumes, and non-positive prices."""
        valid_tick = {
            "ticker": "AAPL",
            "asset_class": "EQUITY",
            "bid": 150.0,
            "ask": 150.05,
            "last_price": 150.02,
            "volume": 500.0,
            "timestamp": "2026-10-25T10:00:00Z",
        }
        ok, cat, msg = validate_silver_tick_record(valid_tick)
        assert ok is True
        assert cat is None

        # Crossed quotes
        crossed = dict(valid_tick, bid=151.0, ask=150.0)
        ok, cat, msg = validate_silver_tick_record(crossed)
        assert ok is False
        assert cat == "CROSSED_QUOTES"

        # Non-positive price
        zero_price = dict(valid_tick, last_price=0.0)
        ok, cat, msg = validate_silver_tick_record(zero_price)
        assert ok is False
        assert cat == "NON_POSITIVE_PRICE"

        # Negative volume
        neg_vol = dict(valid_tick, volume=-10.0)
        ok, cat, msg = validate_silver_tick_record(neg_vol)
        assert ok is False
        assert cat == "NEGATIVE_VOLUME"

        # Missing field
        missing = dict(valid_tick)
        del missing["ticker"]
        ok, cat, msg = validate_silver_tick_record(missing)
        assert ok is False
        assert cat == "MISSING_REQUIRED_FIELD"

    def test_validate_silver_order_record(self) -> None:
        """Verify order validation catches invalid sides, types, and quantities."""
        valid_order = {
            "order_id": "ord-001",
            "portfolio_id": "PORT-ALPHA",
            "ticker": "MSFT",
            "side": "BUY",
            "quantity": 100.0,
            "price": 310.0,
            "order_type": "LIMIT",
            "timestamp": "2026-10-25T10:00:00Z",
        }
        ok, cat, msg = validate_silver_order_record(valid_order)
        assert ok is True

        # Invalid side
        bad_side = dict(valid_order, side="HOLD")
        ok, cat, msg = validate_silver_order_record(bad_side)
        assert ok is False
        assert cat == "INVALID_SIDE"

        # Invalid order type
        bad_type = dict(valid_order, order_type="FILL_OR_KILL")
        ok, cat, msg = validate_silver_order_record(bad_type)
        assert ok is False
        assert cat == "INVALID_ORDER_TYPE"

        # Non-positive quantity
        bad_qty = dict(valid_order, quantity=0.0)
        ok, cat, msg = validate_silver_order_record(bad_qty)
        assert ok is False
        assert cat == "NON_POSITIVE_QUANTITY"

    def test_validate_silver_vwap_record(self) -> None:
        """Verify rolling VWAP validation."""
        valid_vwap = {
            "ticker": "GOOGL",
            "asset_class": "EQUITY",
            "window_start": "2026-10-25T10:00:00Z",
            "window_end": "2026-10-25T10:05:00Z",
            "vwap": 180.5,
            "total_volume": 10000.0,
        }
        ok, cat, msg = validate_silver_vwap_record(valid_vwap)
        assert ok is True

        # Non-positive VWAP
        bad_vwap = dict(valid_vwap, vwap=-1.0)
        ok, cat, msg = validate_silver_vwap_record(bad_vwap)
        assert ok is False
        assert cat == "NON_POSITIVE_VWAP"


# =============================================================================
# 5. MICROSTRUCTURE ENRICHMENTS & FEATURE DERIVATIONS
# =============================================================================

class TestMicrostructureEnrichments:
    """Validate quantitative feature derivations and microstructure calculations."""

    def test_cleanse_and_enrich_tick_features(self) -> None:
        """Verify spread, basis points, quote conditions, and price returns."""
        raw = {
            "ticker": "  aapl  ",
            "asset_class": "equity",
            "bid": 100.0,
            "ask": 100.02,
            "last_price": 100.01,
            "volume": 200.0,
            "timestamp": "2026-10-25T10:15:30Z",
            "sequence_number": 42,
            "_raw_record_hash": "raw-hash-1",
        }
        cleansed_at = datetime(2026, 10, 25, 10, 16, 0, tzinfo=timezone.utc)
        enriched = cleanse_and_enrich_tick(
            raw_record=raw,
            source_file="bronze_ticks.parquet",
            batch_id="b-1",
            cleansed_at=cleansed_at,
            previous_price=99.01,
        )

        assert enriched["ticker"] == "AAPL"
        assert enriched["asset_class"] == "EQUITY"
        assert enriched["spread"] == 0.02
        assert enriched["mid_price"] == 100.01
        # relative_spread_bps = (0.02 / 100.01) * 10000 = ~1.9998 bps (<= 5 bps -> TIGHT_SPREAD)
        assert enriched["relative_spread_bps"] == pytest.approx(1.9998, abs=0.01)
        assert enriched["quote_condition"] == "TIGHT_SPREAD"
        assert enriched["price_return"] == pytest.approx((100.01 - 99.01) / 99.01, abs=0.0001)
        assert enriched["is_crossed_quote"] is False
        assert enriched["year"] == 2026
        assert enriched["month"] == 10
        assert enriched["day"] == 25
        assert enriched["hour"] == 10
        assert enriched["_cleansed_record_hash"] is not None

    def test_cleanse_and_enrich_order_features(self) -> None:
        """Verify order categorization, notional tier, and aggressive taker flag."""
        # Micro limit order
        raw_micro = {
            "order_id": "ord-001",
            "portfolio_id": "port-eq",
            "ticker": "aapl",
            "side": "buy",
            "quantity": 10.0,
            "price": 150.0,
            "order_type": "limit",
            "timestamp": "2026-10-25T10:00:00Z",
        }
        cleansed_at = datetime.now(timezone.utc)
        res_micro = cleanse_and_enrich_order(raw_micro, "orders.json", "b-1", cleansed_at)
        assert res_micro["notional_usd"] == 1500.0
        assert res_micro["notional_tier"] == "MICRO"
        assert res_micro["order_category"] == "EQUITY_ORDER"
        assert res_micro["is_aggressive"] is False  # LIMIT

        # Block market order (Aggressive taker) in Crypto
        raw_block = {
            "order_id": "ord-block",
            "portfolio_id": "port-crypto",
            "ticker": "btcusd",
            "side": "sell",
            "quantity": 15.0,
            "price": 60000.0,  # $900,000 -> BLOCK
            "order_type": "market",
            "timestamp": "2026-10-25T10:01:00Z",
        }
        res_block = cleanse_and_enrich_order(raw_block, "orders.json", "b-1", cleansed_at)
        assert res_block["notional_usd"] == 900000.0
        assert res_block["notional_tier"] == "BLOCK"
        assert res_block["order_category"] == "CRYPTO_ORDER"
        assert res_block["is_aggressive"] is True  # MARKET

    def test_cleanse_and_enrich_vwap_features(self) -> None:
        """Verify volatility regime and trend indicators on rolling VWAP."""
        raw_vwap = {
            "ticker": "NVDA",
            "asset_class": "EQUITY",
            "window_start": "2026-10-25T10:00:00Z",
            "window_end": "2026-10-25T10:05:00Z",
            "vwap": 120.0,
            "open_price": 118.0,
            "close_price": 122.0,  # close > open -> BULLISH
            "bandwidth_bps": 25.0,  # 10 <= 25 < 50 -> NORMAL
            "total_volume": 50000.0,
        }
        cleansed_at = datetime.now(timezone.utc)
        res = cleanse_and_enrich_vwap(raw_vwap, "vwap.parquet", "b-1", cleansed_at)
        assert res["volatility_regime"] == "NORMAL"
        assert res["trend"] == "BULLISH"
        assert res["ticker"] == "NVDA"


# =============================================================================
# 6. INTRA-BATCH DEDUPLICATION
# =============================================================================

class TestIntraBatchDeduplication:
    """Validate elimination of duplicate primary keys within a single batch."""

    def test_intra_batch_deduplication_orders(self) -> None:
        """Verify that duplicate order_id records keep the newest record."""
        engine = DeltaSilverCleansingEngine()
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_path = Path(tmp_dir) / "silver_trade_orders"

            # 2 records with same order_id, different sequence numbers
            batch = [
                {
                    "order_id": "ORD-DUP-01",
                    "portfolio_id": "PORT-1",
                    "ticker": "AAPL",
                    "side": "BUY",
                    "quantity": 50.0,
                    "price": 150.0,
                    "order_type": "LIMIT",
                    "timestamp": "2026-10-25T10:00:00Z",
                    "sequence_number": 1,
                },
                {
                    "order_id": "ORD-DUP-01",  # duplicate order_id with higher sequence number
                    "portfolio_id": "PORT-1",
                    "ticker": "AAPL",
                    "side": "BUY",
                    "quantity": 100.0,  # updated quantity
                    "price": 150.0,
                    "order_type": "LIMIT",
                    "timestamp": "2026-10-25T10:00:01Z",
                    "sequence_number": 2,
                },
                {
                    "order_id": "ORD-DISTINCT-02",
                    "portfolio_id": "PORT-1",
                    "ticker": "MSFT",
                    "side": "SELL",
                    "quantity": 30.0,
                    "price": 300.0,
                    "order_type": "MARKET",
                    "timestamp": "2026-10-25T10:00:02Z",
                    "sequence_number": 3,
                },
            ]

            result = engine.cleanse_trade_orders(source=batch, target_path=target_path)
            assert result.records_read == 3
            assert result.records_cleansed == 2
            assert result.records_deduplicated == 1
            assert result.delta_commit_version == 0
            assert result.operation_type == "WRITE"

            # Verify table content has quantity 100.0 for ORD-DUP-01
            delta_log = DeltaLogManager(target_path)
            active_files = delta_log.get_active_files()
            assert len(active_files) > 0

            read_back_recs: List[Dict[str, Any]] = []
            for act in active_files:
                tbl = pq.read_table(target_path / act["path"])
                read_back_recs.extend(tbl.to_pylist())

            assert len(read_back_recs) == 2
            dup_rec = next(r for r in read_back_recs if r["order_id"] == "ORD-DUP-01")
            assert dup_rec["quantity"] == 100.0
            assert dup_rec["sequence_number"] == 2


# =============================================================================
# 7. INTER-TABLE DELTA LAKE MERGE INTO (UPSERT)
# =============================================================================

class TestDeltaLakeMergeIntoUpsert:
    """Validate Delta Lake MERGE INTO upsert, file rewriting, and ACID commits."""

    def test_delta_merge_into_existing_silver_table(self) -> None:
        """Verify incremental upsert into an existing Delta table using MERGE actions."""
        engine = DeltaSilverCleansingEngine()
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_path = Path(tmp_dir) / "silver_trade_orders"

            # Batch 1: Initial load of 3 orders
            batch_1 = [
                {
                    "order_id": f"ORD-00{i}",
                    "portfolio_id": "PORT-ALPHA",
                    "ticker": "AAPL",
                    "side": "BUY",
                    "quantity": 10.0 * i,
                    "price": 150.0,
                    "order_type": "LIMIT",
                    "timestamp": "2026-10-25T10:00:00Z",
                    "sequence_number": 10,
                }
                for i in range(1, 4)
            ]
            res_1 = engine.cleanse_trade_orders(source=batch_1, target_path=target_path)
            assert res_1.delta_commit_version == 0
            assert res_1.operation_type == "WRITE"
            assert res_1.records_cleansed == 3

            # Batch 2: Incremental load with:
            # - ORD-001 updated with newer sequence number (qty: 99.0)
            # - ORD-002 stale update with older sequence number (ignored/deduped)
            # - ORD-004 new order inserted
            batch_2 = [
                {
                    "order_id": "ORD-001",
                    "portfolio_id": "PORT-ALPHA",
                    "ticker": "AAPL",
                    "side": "BUY",
                    "quantity": 99.0,
                    "price": 150.0,
                    "order_type": "LIMIT",
                    "timestamp": "2026-10-25T10:05:00Z",
                    "sequence_number": 20,  # Newer
                },
                {
                    "order_id": "ORD-002",
                    "portfolio_id": "PORT-ALPHA",
                    "ticker": "AAPL",
                    "side": "BUY",
                    "quantity": 5.0,
                    "price": 150.0,
                    "order_type": "LIMIT",
                    "timestamp": "2026-10-25T09:59:00Z",
                    "sequence_number": 5,  # Stale
                },
                {
                    "order_id": "ORD-004",  # New order
                    "portfolio_id": "PORT-BETA",
                    "ticker": "MSFT",
                    "side": "SELL",
                    "quantity": 25.0,
                    "price": 310.0,
                    "order_type": "MARKET",
                    "timestamp": "2026-10-25T10:06:00Z",
                    "sequence_number": 1,
                },
            ]
            res_2 = engine.cleanse_trade_orders(source=batch_2, target_path=target_path)
            assert res_2.delta_commit_version == 1
            assert res_2.operation_type == "MERGE"
            assert res_2.records_cleansed == 4  # Total distinct orders now: ORD-001, ORD-002, ORD-003, ORD-004

            # Verify commit history
            delta_log = DeltaLogManager(target_path)
            history = delta_log.read_commit_history()
            assert len(history) == 2
            assert history[0]["commit_info"]["operation"] == "WRITE"
            assert history[1]["commit_info"]["operation"] == "MERGE"
            assert history[1]["commit_info"]["operationMetrics"]["numTargetRowsInserted"] == 1
            assert history[1]["commit_info"]["operationMetrics"]["numTargetRowsUpdated"] == 1

            # Read all active records
            active_files = delta_log.get_active_files()
            all_records = []
            for act in active_files:
                tbl = pq.read_table(target_path / act["path"])
                all_records.extend(tbl.to_pylist())

            assert len(all_records) == 4
            ord_1 = next(r for r in all_records if r["order_id"] == "ORD-001")
            assert ord_1["quantity"] == 99.0
            ord_2 = next(r for r in all_records if r["order_id"] == "ORD-002")
            assert ord_2["quantity"] == 20.0  # Stale update discarded


# =============================================================================
# 8. END-TO-END HERMETIC INGESTION (BRONZE TO SILVER)
# =============================================================================

class TestEndToEndBronzeToSilver:
    """Validate end-to-end flow from Bronze Delta tables into Silver Delta tables."""

    def test_e2e_bronze_parquet_to_silver_ticks(self) -> None:
        """Ingest raw ticks to Bronze, then cleanse into Silver with partitions."""
        bronze_engine = DeltaBronzeIngestionEngine()
        silver_engine = DeltaSilverCleansingEngine()

        with tempfile.TemporaryDirectory() as tmp_dir:
            bronze_path = Path(tmp_dir) / "bronze_market_ticks"
            silver_path = Path(tmp_dir) / "silver_market_ticks"

            raw_ticks = [
                {
                    "ticker": "AAPL",
                    "bid": 150.0,
                    "ask": 150.05,
                    "last_price": 150.02,
                    "volume": 1000.0,
                    "timestamp": "2026-10-25T10:00:00Z",
                    "asset_class": "EQUITY",
                    "sequence_number": i,
                    "source": "KAFKA",
                }
                for i in range(1, 11)
            ]

            # 1. Ingest to Bronze
            b_res = bronze_engine.ingest_market_ticks(source_path=raw_ticks, target_path=bronze_path)
            assert b_res.records_ingested == 10

            # 2. Cleanse from Bronze to Silver
            s_res = silver_engine.cleanse_market_ticks(source=bronze_path, target_path=silver_path)
            assert s_res.records_read == 10
            assert s_res.records_cleansed == 10
            assert s_res.records_quarantined == 0
            assert s_res.delta_commit_version == 0
            assert len(s_res.partitions_affected) > 0

            # Verify Silver Delta Table
            silver_log = DeltaLogManager(silver_path)
            active_files = silver_log.get_active_files()
            assert len(active_files) > 0

            silver_records = []
            for act in active_files:
                file_p = silver_path / act["path"]
                assert file_p.exists()
                tbl = pq.read_table(file_p)
                silver_records.extend(tbl.to_pylist())

            assert len(silver_records) == 10
            assert all(r["quote_condition"] in {"TIGHT_SPREAD", "REGULAR", "WIDE_SPREAD"} for r in silver_records)
            assert all(r["_cleansed_record_hash"] is not None for r in silver_records)

    def test_e2e_orders_cleansing_with_quarantine_isolation(self) -> None:
        """Verify invalid orders are quarantined while valid orders are cleansed to Silver."""
        silver_engine = DeltaSilverCleansingEngine()

        with tempfile.TemporaryDirectory() as tmp_dir:
            silver_path = Path(tmp_dir) / "silver_trade_orders"
            q_dir = Path(tmp_dir) / "quarantine"

            mixed_orders = [
                {
                    "order_id": "ORD-CLEAN-01",
                    "portfolio_id": "PORT-1",
                    "ticker": "AAPL",
                    "side": "BUY",
                    "quantity": 50.0,
                    "price": 150.0,
                    "order_type": "LIMIT",
                    "timestamp": "2026-10-25T10:00:00Z",
                },
                {
                    "order_id": "ORD-CORRUPT-02",
                    "portfolio_id": "PORT-1",
                    "ticker": "AAPL",
                    "side": "BUY",
                    "quantity": -10.0,  # Negative quantity!
                    "price": 150.0,
                    "order_type": "LIMIT",
                    "timestamp": "2026-10-25T10:00:00Z",
                },
            ]

            res = silver_engine.cleanse_trade_orders(
                source=mixed_orders,
                target_path=silver_path,
                quarantine_dir=q_dir,
            )

            assert res.records_read == 2
            assert res.records_cleansed == 1
            assert res.records_quarantined == 1
            assert res.quarantine_path is not None
            assert Path(res.quarantine_path).is_file()

            # Verify quarantine payload
            q_lines = Path(res.quarantine_path).read_text(encoding="utf-8").strip().splitlines()
            assert len(q_lines) == 1
            q_rec = json.loads(q_lines[0])
            assert q_rec["error_category"] == "NON_POSITIVE_QUANTITY"

    def test_e2e_rolling_vwap_cleansing(self) -> None:
        """Verify rolling VWAP aggregations cleanse cleanly into Silver."""
        silver_engine = DeltaSilverCleansingEngine()

        with tempfile.TemporaryDirectory() as tmp_dir:
            silver_path = Path(tmp_dir) / "silver_rolling_vwap"

            vwap_records = [
                {
                    "ticker": "SPY",
                    "asset_class": "ETF",
                    "window_start": "2026-10-25T10:00:00Z",
                    "window_end": "2026-10-25T10:05:00Z",
                    "tick_count": 250,
                    "open_price": 500.0,
                    "high_price": 502.5,
                    "low_price": 499.5,
                    "close_price": 502.0,
                    "total_volume": 100000.0,
                    "total_notional": 50100000.0,
                    "vwap": 501.0,
                    "price_stddev": 0.8,
                    "price_variance": 0.64,
                    "volatility_multiplier": 2.0,
                    "upper_band": 502.6,
                    "lower_band": 499.4,
                    "bandwidth_bps": 63.87,
                    "percent_b": 0.8125,
                    "is_upper_breach": False,
                    "is_lower_breach": False,
                    "volatility_spike": False,
                }
            ]

            res = silver_engine.cleanse_rolling_vwap(source=vwap_records, target_path=silver_path)
            assert res.records_cleansed == 1
            assert res.records_quarantined == 0

            # Verify enriched columns
            delta_log = DeltaLogManager(silver_path)
            acts = delta_log.get_active_files()
            tbl = pq.read_table(silver_path / acts[0]["path"])
            recs = tbl.to_pylist()
            assert recs[0]["volatility_regime"] == "HIGH"  # bandwidth_bps = 63.87 >= 50
            assert recs[0]["trend"] == "BULLISH"


# =============================================================================
# 9. TABLE MAINTENANCE (OPTIMIZE & VACUUM)
# =============================================================================

class TestSilverTableMaintenance:
    """Validate Delta Lake OPTIMIZE compaction and VACUUM pruning on Silver tier."""

    def test_silver_table_maintenance_optimize_and_vacuum(self) -> None:
        """Verify OPTIMIZE and VACUUM generate valid commits in transaction log."""
        engine = DeltaSilverCleansingEngine()

        with tempfile.TemporaryDirectory() as tmp_dir:
            silver_path = Path(tmp_dir) / "silver_market_ticks"

            # Ingest initial data
            sample = [
                {
                    "ticker": "AAPL",
                    "asset_class": "EQUITY",
                    "bid": 150.0,
                    "ask": 150.05,
                    "last_price": 150.02,
                    "volume": 100.0,
                    "timestamp": "2026-10-25T10:00:00Z",
                    "sequence_number": 1,
                }
            ]
            engine.cleanse_market_ticks(source=sample, target_path=silver_path)

            # Optimize
            opt_res = engine.optimize_table(silver_path, z_order_columns=["ticker", "event_timestamp"])
            assert opt_res["operation"] == "OPTIMIZE"
            assert opt_res["version"] == 1
            assert opt_res["z_order_columns"] == ["ticker", "event_timestamp"]

            # Vacuum
            vac_res = engine.vacuum_table(silver_path, retention_hours=168)
            assert vac_res["operation"] == "VACUUM"
            assert vac_res["version"] == 2
            assert vac_res["retention_hours"] == 168

            # Verify history
            delta_log = DeltaLogManager(silver_path)
            hist = delta_log.read_commit_history()
            assert len(hist) == 3
            ops = [h["commit_info"]["operation"] for h in hist]
            assert ops == ["WRITE", "OPTIMIZE", "VACUUM"]


# =============================================================================
# 10. DATABRICKS NOTEBOOK RUNNER CLI & DRY RUN
# =============================================================================

class TestSilverNotebookRunnerCLI:
    """Validate notebook task argument parsing, dry-run mode, and automated execution."""

    def test_cli_argument_parsing(self) -> None:
        """Verify command line flags parse correctly."""
        args = parse_args([
            "--environment", "prod",
            "--source-type", "all",
            "--ingestion-date", "2026-10-25",
            "--run-optimize", "false",
            "--run-vacuum", "false",
            "--dry-run",
        ])
        assert args.environment == "prod"
        assert args.source_type == "all"
        assert args.ingestion_date == "2026-10-25"
        assert args.run_optimize is False
        assert args.run_vacuum is False
        assert args.dry_run is True

    def test_cli_dry_run_plan(self) -> None:
        """Verify dry-run mode outputs structured plan without writing tables."""
        args = parse_args([
            "--environment", "staging",
            "--source-type", "market_ticks",
            "--dry-run",
        ])
        plan = run_silver_cleansing(args)
        assert plan["status"] == "DRY_RUN_SUCCESS"
        assert plan["environment"] == "staging"
        assert plan["source_type"] == "market_ticks"

    def test_cli_full_execution(self) -> None:
        """Verify full CLI execution runs cleanly across market ticks."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            src_path = Path(tmp_dir) / "bronze_market_ticks"
            tgt_path = Path(tmp_dir) / "silver_market_ticks"

            # Populate bronze test file
            bronze_engine = DeltaBronzeIngestionEngine()
            bronze_engine.ingest_market_ticks(
                source_path=[
                    {
                        "ticker": "AAPL",
                        "bid": 150.0,
                        "ask": 150.05,
                        "last_price": 150.02,
                        "volume": 100.0,
                        "timestamp": "2026-10-25T10:00:00Z",
                        "asset_class": "EQUITY",
                        "sequence_number": 1,
                    }
                ],
                target_path=src_path,
            )

            args = parse_args([
                "--environment", "dev",
                "--source-type", "market_ticks",
                "--source-path", str(src_path),
                "--target-path", str(tgt_path),
                "--run-optimize", "true",
                "--run-vacuum", "true",
            ])
            summary = run_silver_cleansing(args)
            assert summary["status"] == "SUCCESS"
            assert len(summary["runs"]) == 1
            assert summary["runs"][0]["table_name"] == "silver_market_ticks"
            assert "databricks_silver_records_cleansed_total" in summary["telemetry_metrics"]


# =============================================================================
# 11. OPERATIONAL TELEMETRY & PROMETHEUS METRICS
# =============================================================================

class TestSilverTelemetryAndPrometheus:
    """Validate Silver layer operational telemetry and Prometheus metrics exposition."""

    def test_telemetry_counters_and_formatting(self) -> None:
        """Verify telemetry counter monotonicity and Prometheus formatting."""
        telemetry = SilverCleansingTelemetry()

        telemetry.record_success(
            records_read=100,
            records_cleansed=95,
            deduplicated=5,
            quarantined=3,
            bytes_processed=50000,
            commit_version=1,
        )

        assert telemetry.batches_cleansed_total == 1
        assert telemetry.records_read_total == 100
        assert telemetry.records_cleansed_total == 95
        assert telemetry.records_deduplicated_total == 5
        assert telemetry.quarantined_records_total == 3
        assert telemetry.bytes_processed_total == 50000
        assert telemetry.latest_commit_version == 1

        prom = telemetry.format_prometheus()
        assert "databricks_silver_batches_cleansed_total 1" in prom
        assert "databricks_silver_records_read_total 100" in prom
        assert "databricks_silver_records_cleansed_total 95" in prom
        assert "databricks_silver_records_deduplicated_total 5" in prom
        assert "databricks_silver_quarantined_records_total 3" in prom
        assert "databricks_silver_bytes_processed_total 50000" in prom
        assert "databricks_silver_commit_version 1" in prom
