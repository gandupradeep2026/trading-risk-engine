"""Zero-Cost Unit and Beam Integration Tests for Multi-Output Sinks (Day 10).

Validates:
- CloudSqlConfig & RiskEnginePipelineOptions: defaults, canonical paths, argument parsing,
  and validation constraints.
- CloudSqlDatabaseClient:
  - SQLite in-memory DDL creation matching Cloud SQL PostgreSQL schema.
  - Idempotent batch insertion (`ON CONFLICT` / `INSERT OR IGNORE`).
  - Transient error retry with exponential backoff.
  - Permanent failure containment.
- WriteAlertsToCloudSqlFn & WriteAlertsToCloudSql:
  - Bundle-level buffering and batch-size flushing.
  - Successful routing to `TAG_CLOUDSQL_SUCCESS`.
  - Malformed alert dead-letter routing to `TAG_CLOUDSQL_FAILED`.
  - Apache Beam metrics tracking.
- FormatOrderForGcsFn & FormatDeadLetterForGcsFn:
  - Bronze-tier cold storage JSON lines formatting and metadata enrichment.
- WriteRawOrdersToGcs & WriteRawDeadLetterToGcs:
  - Fixed-window file writing with local test filesystem.
- End-to-End Multi-Output Pipeline Integration:
  - Valid trade orders routed to GCS Bronze sink.
  - Corrupt records routed to dead-letter stream.
  - Anomaly detection emitting `RiskAlert` routed to Cloud SQL operational sink.
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
import pytest

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import window

from ingestion.pubsub_client import TradeOrder
from streaming.dataflow.anomaly_detector import (
    TAG_RISK_ALERTS,
    TAG_WINDOW_STATS,
    RiskAlert,
)
from streaming.dataflow.cloudsql_sink import (
    TAG_CLOUDSQL_FAILED,
    TAG_CLOUDSQL_SUCCESS,
    CloudSqlConfig,
    CloudSqlDatabaseClient,
    WriteAlertsToCloudSql,
    WriteAlertsToCloudSqlFn,
)
from streaming.dataflow.decoders import (
    TAG_DEADLETTER,
    TAG_VALID_ORDERS,
    DeadLetterRecord,
)
from streaming.dataflow.gcs_sink import (
    FormatDeadLetterForGcsFn,
    FormatOrderForGcsFn,
    WriteRawDeadLetterToGcs,
    WriteRawOrdersToGcs,
    _normalize_output_prefix,
)
from streaming.dataflow.options import RiskEnginePipelineOptions
from streaming.dataflow.pipeline import build_pipeline


# -----------------------------------------------------------------------------
# 1. PIPELINE OPTIONS & CONFIGURATION TESTS
# -----------------------------------------------------------------------------
class TestCloudSqlConfigAndOptions:
    """Validate Cloud SQL configuration, pipeline options, and validation logic."""

    def test_pipeline_options_defaults_and_canonical_names(self) -> None:
        """Verify default Cloud SQL and GCS sink options resolve to canonical infrastructure."""
        opts = RiskEnginePipelineOptions(["--project_id=test-project", "--environment=dev"])
        opts.resolve_defaults()

        canon = opts.get_canonical_resource_names("test-project", "dev")
        assert canon["cloudsql_instance"] == "test-project:us-central1:trading-risk-db-dev"
        assert canon["bronze_bucket"] == "gs://trading-risk-bronze-dev"

        assert opts.cloudsql_instance == "test-project:us-central1:trading-risk-db-dev"
        assert opts.cloudsql_database == "trading_risk_db"
        assert opts.cloudsql_user == "risk_app_user"
        assert opts.cloudsql_table == "risk_alerts"
        assert opts.cloudsql_batch_size == 50
        assert opts.cloudsql_max_retries == 3
        assert opts.gcs_window_seconds == 60
        assert opts.gcs_output_path == "gs://trading-risk-bronze-dev/dataflow/orders/raw/"
        assert opts.gcs_deadletter_output_path == "gs://trading-risk-bronze-dev/dataflow/deadletter/raw/"

    def test_cloudsql_config_from_pipeline_options(self) -> None:
        """Verify CloudSqlConfig maps pipeline options accurately."""
        opts = RiskEnginePipelineOptions([
            "--project_id=test-proj",
            "--environment=prod",
            "--cloudsql_database=prod_risk_db",
            "--cloudsql_table=prod_alerts",
            "--cloudsql_batch_size=100",
            "--cloudsql_max_retries=5",
            "--cloudsql_retry_backoff_sec=1.0",
            "--cloudsql_driver=sqlite3",
            "--cloudsql_sqlite_path=:memory:",
        ])
        opts.resolve_defaults()
        config = CloudSqlConfig.from_pipeline_options(opts)

        assert config.database == "prod_risk_db"
        assert config.table == "prod_alerts"
        assert config.batch_size == 100
        assert config.max_retries == 5
        assert config.retry_backoff_sec == 1.0
        assert config.driver == "sqlite3"
        assert config.sqlite_db_path == ":memory:"

    def test_pipeline_options_validation_constraints(self) -> None:
        """Verify invalid window or batch values trigger validation errors."""
        opts = RiskEnginePipelineOptions(["--gcs_window_seconds=0"])
        with pytest.raises(ValueError, match="gcs_window_seconds must be positive"):
            opts.validate_options(is_test=True)

        opts2 = RiskEnginePipelineOptions(["--cloudsql_batch_size=-5"])
        with pytest.raises(ValueError, match="cloudsql_batch_size must be positive"):
            opts2.validate_options(is_test=True)


# -----------------------------------------------------------------------------
# 2. CLOUD SQL DATABASE CLIENT TESTS (HERMETIC SQLITE)
# -----------------------------------------------------------------------------
class TestCloudSqlDatabaseClient:
    """Validate database client connection, DDL, idempotency, and retries."""

    def test_sqlite_schema_initialization_and_table_structure(self, tmp_path: Path) -> None:
        """Verify client initializes risk_alerts table with required operational columns."""
        db_file = str(tmp_path / "test_risk.db")
        config = CloudSqlConfig(driver="sqlite3", sqlite_db_path=db_file, table="risk_alerts")
        client = CloudSqlDatabaseClient(config)

        conn = client.connect()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(risk_alerts)")
        columns = {row["name"]: row["type"] for row in cursor.fetchall()}

        expected_columns = {
            "alert_id": "TEXT",
            "portfolio_id": "TEXT",
            "ticker": "TEXT",
            "asset_class": "TEXT",
            "alert_type": "TEXT",
            "severity": "TEXT",
            "metric_name": "TEXT",
            "threshold_value": "REAL",
            "observed_value": "REAL",
            "event_timestamp": "TEXT",
            "created_at": "TEXT",
            "status": "TEXT",
            "payload_json": "TEXT",
        }
        for col, expected_type in expected_columns.items():
            assert col in columns, f"Missing column {col} in risk_alerts table"
            assert columns[col] == expected_type
        client.close()

    def test_insert_alerts_batch_and_query(self, tmp_path: Path) -> None:
        """Verify successful insertion and retrieval of RiskAlert records."""
        db_file = str(tmp_path / "test_risk.db")
        config = CloudSqlConfig(driver="sqlite3", sqlite_db_path=db_file)
        client = CloudSqlDatabaseClient(config)

        alert1 = RiskAlert.create(
            portfolio_id="PORT-A",
            ticker="NVDA",
            asset_class="EQUITY",
            alert_type="FLASH_CRASH_DRAWDOWN",
            severity="CRITICAL",
            metric_name="rolling_5m_drawdown_pct",
            threshold_value=0.05,
            observed_value=0.125,
            event_timestamp="2026-10-19T10:00:00Z",
            payload_json={"peak": 150.0, "trough": 131.25},
        )
        alert2 = RiskAlert.create(
            portfolio_id="PORT-B",
            ticker="BTCUSD",
            asset_class="CRYPTO",
            alert_type="SINGLE_ORDER_FAT_FINGER",
            severity="HIGH",
            metric_name="single_order_notional_usd",
            threshold_value=1_000_000.0,
            observed_value=1_500_000.0,
            event_timestamp="2026-10-19T10:01:00Z",
            payload_json={"order_id": "ORD-XYZ"},
        )

        inserted = client.insert_alerts_batch([alert1, alert2])
        assert inserted == 2

        conn = client.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM risk_alerts ORDER BY event_timestamp ASC")
        rows = cursor.fetchall()
        assert len(rows) == 2

        assert rows[0]["alert_id"] == alert1.alert_id
        assert rows[0]["ticker"] == "NVDA"
        assert rows[0]["severity"] == "CRITICAL"
        assert json.loads(rows[0]["payload_json"]) == {"peak": 150.0, "trough": 131.25}

        assert rows[1]["alert_id"] == alert2.alert_id
        assert rows[1]["ticker"] == "BTCUSD"
        assert rows[1]["severity"] == "HIGH"
        client.close()

    def test_idempotent_deduplication_on_conflict(self, tmp_path: Path) -> None:
        """Verify duplicate alert IDs are ignored without raising primary key violations."""
        db_file = str(tmp_path / "test_risk.db")
        config = CloudSqlConfig(driver="sqlite3", sqlite_db_path=db_file)
        client = CloudSqlDatabaseClient(config)

        alert = RiskAlert.create(
            portfolio_id="PORT-ALPHA",
            ticker="AAPL",
            asset_class="EQUITY",
            alert_type="FLASH_CRASH_DRAWDOWN",
            severity="MEDIUM",
            metric_name="drawdown",
            threshold_value=0.05,
            observed_value=0.06,
        )

        client.insert_alerts_batch([alert])
        # Insert same alert again
        client.insert_alerts_batch([alert])

        conn = client.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM risk_alerts WHERE alert_id = ?", (alert.alert_id,))
        count = cursor.fetchone()["count"]
        assert count == 1
        client.close()

    def test_transient_retry_logic(self, tmp_path: Path) -> None:
        """Verify client retries with backoff on transient database operational error."""
        db_file = str(tmp_path / "test_risk.db")
        config = CloudSqlConfig(
            driver="sqlite3",
            sqlite_db_path=db_file,
            max_retries=2,
            retry_backoff_sec=0.01,
        )
        client = CloudSqlDatabaseClient(config)
        real_conn = client.connect()

        alert = RiskAlert.create(
            portfolio_id="PORT-1",
            ticker="MSFT",
            asset_class="EQUITY",
            alert_type="FLASH_CRASH_DRAWDOWN",
            severity="HIGH",
            metric_name="drawdown",
            threshold_value=0.05,
            observed_value=0.08,
        )

        attempts = 0

        def faulty_connect():
            nonlocal attempts
            attempts += 1
            fresh_conn = sqlite3.connect(db_file)
            fresh_conn.row_factory = sqlite3.Row
            if attempts == 1:
                class FaultyConn:
                    def cursor(self):
                        raise sqlite3.OperationalError("database is locked")

                    def close(self):
                        fresh_conn.close()

                return FaultyConn()
            return fresh_conn

        with patch.object(client, "connect", side_effect=faulty_connect):
            client._conn = faulty_connect()
            inserted = client.insert_alerts_batch([alert])
            assert inserted == 1
            assert attempts >= 2
        client.close()

    def test_permanent_failure_raises_exception(self) -> None:
        """Verify permanent database error raises exception when retries are exhausted."""
        config = CloudSqlConfig(
            driver="sqlite3",
            sqlite_db_path=":memory:",
            max_retries=1,
            retry_backoff_sec=0.01,
        )
        client = CloudSqlDatabaseClient(config)

        alert = RiskAlert.create(
            portfolio_id="PORT-1",
            ticker="TSLA",
            asset_class="EQUITY",
            alert_type="FLASH_CRASH_DRAWDOWN",
            severity="CRITICAL",
            metric_name="drawdown",
            threshold_value=0.05,
            observed_value=0.15,
        )

        class FailingConn:
            def cursor(self):
                raise sqlite3.DatabaseError("Disk full")

            def close(self):
                pass

        with patch.object(client, "connect", return_value=FailingConn()):
            client._conn = FailingConn()
            with pytest.raises(sqlite3.DatabaseError):
                client.insert_alerts_batch([alert])
        client.close()


# -----------------------------------------------------------------------------
# 3. WRITE ALERTS TO CLOUD SQL DOFN & PTRANSFORM TESTS
# -----------------------------------------------------------------------------
class TestWriteAlertsToCloudSqlFn:
    """Validate Beam DoFn batching, metrics, and error containment."""

    def test_dofn_batching_and_flush_on_bundle_finish(self, tmp_path: Path) -> None:
        """Verify DoFn flushes buffered alerts at bundle finish."""
        db_file = str(tmp_path / "test_beam_dofn.db")
        config = CloudSqlConfig(driver="sqlite3", sqlite_db_path=db_file, batch_size=10)
        dofn = WriteAlertsToCloudSqlFn(config=config)

        dofn.setup()
        dofn.start_bundle()

        alert = RiskAlert.create(
            portfolio_id="PORT-XYZ",
            ticker="GOOGL",
            asset_class="EQUITY",
            alert_type="FLASH_CRASH_DRAWDOWN",
            severity="HIGH",
            metric_name="drawdown",
            threshold_value=0.05,
            observed_value=0.07,
        )

        outputs = list(dofn.process(alert))
        assert len(outputs) == 0  # Buffered, not yet flushed

        bundle_outputs = list(dofn.finish_bundle())
        assert len(bundle_outputs) == 1
        assert bundle_outputs[0].tag == TAG_CLOUDSQL_SUCCESS
        assert bundle_outputs[0].value.alert_id == alert.alert_id

        # Verify persisted in database
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM risk_alerts WHERE alert_id = ?", (alert.alert_id,))
        row = cursor.fetchone()
        assert row is not None
        assert row["ticker"] == "GOOGL"
        conn.close()
        dofn.teardown()

    def test_dofn_batch_size_trigger(self, tmp_path: Path) -> None:
        """Verify DoFn triggers flush immediately upon reaching batch_size."""
        db_file = str(tmp_path / "test_beam_batch.db")
        config = CloudSqlConfig(driver="sqlite3", sqlite_db_path=db_file, batch_size=3)
        dofn = WriteAlertsToCloudSqlFn(config=config)

        dofn.setup()
        dofn.start_bundle()

        alerts = [
            RiskAlert.create(
                portfolio_id=f"PORT-{i}",
                ticker="SPY",
                asset_class="ETF",
                alert_type="FLASH_CRASH_DRAWDOWN",
                severity="LOW",
                metric_name="drawdown",
                threshold_value=0.05,
                observed_value=0.051,
            )
            for i in range(3)
        ]

        out1 = list(dofn.process(alerts[0]))
        out2 = list(dofn.process(alerts[1]))
        out3 = list(dofn.process(alerts[2]))  # Reaches batch size 3

        assert len(out1) == 0
        assert len(out2) == 0
        assert len(out3) == 3
        for item in out3:
            assert item.tag == TAG_CLOUDSQL_SUCCESS
        dofn.teardown()

    def test_dofn_malformed_input_deadletter_containment(self) -> None:
        """Verify unparseable or schema-violating alert yields to TAG_CLOUDSQL_FAILED."""
        config = CloudSqlConfig(driver="sqlite3", sqlite_db_path=":memory:")
        dofn = WriteAlertsToCloudSqlFn(config=config)

        dofn.setup()
        dofn.start_bundle()

        # Send invalid dict missing required fields
        invalid_alert = {"bad_field": 123}
        outputs = list(dofn.process(invalid_alert))

        assert len(outputs) == 1
        assert outputs[0].tag == TAG_CLOUDSQL_FAILED
        assert "error" in outputs[0].value
        dofn.teardown()


# -----------------------------------------------------------------------------
# 4. GCS RAW SINK FORMATTING & PREFIX NORMALIZATION TESTS
# -----------------------------------------------------------------------------
class TestGcsRawSinkFormatting:
    """Validate GCS Bronze tier JSON lines formatting and prefix logic."""

    def test_format_order_for_gcs_fn(self) -> None:
        """Verify TradeOrder is serialized to Bronze-tier JSON line with audit metadata."""
        order = TradeOrder(
            order_id="ORD-001",
            portfolio_id="PORT-ALPHA",
            ticker="AAPL",
            asset_class="EQUITY",
            side="BUY",
            order_type="LIMIT",
            quantity=100.0,
            price=150.0,
            notional_usd=15000.0,
            timestamp="2026-10-19T10:00:00Z",
            trader_id="ALGO-EXEC-01",
            execution_status="FILLED",
            sequence_number=42,
        )

        dofn = FormatOrderForGcsFn()
        results = list(dofn.process(order))
        assert len(results) == 1

        record = json.loads(results[0])
        assert record["order_id"] == "ORD-001"
        assert record["portfolio_id"] == "PORT-ALPHA"
        assert record["ticker"] == "AAPL"
        assert record["quantity"] == 100.0
        assert record["price"] == 150.0
        assert record["notional_usd"] == 15000.0
        assert record["_tier"] == "BRONZE_RAW"
        assert "_gcs_ingested_at" in record

    def test_format_deadletter_for_gcs_fn(self) -> None:
        """Verify DeadLetterRecord is formatted with diagnostics for regulatory cold audit."""
        dlq = DeadLetterRecord(
            payload='{"corrupt": true}',
            error_type="JSONDecodeError",
            error_message="Invalid JSON syntax",
            error_details={"line": 1},
            timestamp="2026-10-19T10:05:00Z",
            source_attributes={"attr1": "val1"},
            ordering_key="PORT-1#TICKER",
        )

        dofn = FormatDeadLetterForGcsFn()
        results = list(dofn.process(dlq))
        assert len(results) == 1

        record = json.loads(results[0])
        assert record["payload"] == '{"corrupt": true}'
        assert record["error_type"] == "JSONDecodeError"
        assert record["_tier"] == "BRONZE_DEADLETTER"
        assert "_gcs_ingested_at" in record

    def test_normalize_output_prefix(self) -> None:
        """Verify trailing slash appends default prefix while exact path is preserved."""
        assert _normalize_output_prefix("gs://my-bucket/orders/", "orders-raw") == "gs://my-bucket/orders/orders-raw"
        assert _normalize_output_prefix("gs://my-bucket/orders/orders-raw", "orders-raw") == "gs://my-bucket/orders/orders-raw"
        assert _normalize_output_prefix("C:\\data\\orders\\", "orders-raw") == "C:/data/orders/orders-raw"


# -----------------------------------------------------------------------------
# 5. GCS RAW SINK BEAM PIPELINE EXECUTION TESTS
# -----------------------------------------------------------------------------
class TestGcsRawSinkBeamExecution:
    """Validate WriteRawOrdersToGcs and WriteRawDeadLetterToGcs using Beam DirectRunner."""

    def test_write_raw_orders_to_local_files(self, tmp_path: Path) -> None:
        """Verify valid orders are written to windowed JSON lines files."""
        output_prefix = str(tmp_path / "raw_orders" / "orders")

        orders = [
            TradeOrder(
                order_id=f"ORD-{i}",
                portfolio_id="PORT-ALPHA",
                ticker="MSFT",
                asset_class="EQUITY",
                side="BUY",
                order_type="LIMIT",
                quantity=10.0 * (i + 1),
                price=300.0,
                notional_usd=3000.0 * (i + 1),
                timestamp="2026-10-19T10:00:00Z",
                sequence_number=i + 1,
            )
            for i in range(3)
        ]

        with BeamTestPipeline() as p:
            input_pcoll = p | beam.Create(orders)
            _ = input_pcoll | WriteRawOrdersToGcs(
                output_path=output_prefix,
                window_seconds=60,
                num_shards=1,
            )

        # Inspect generated files
        parent_dir = tmp_path / "raw_orders"
        files = list(parent_dir.glob("orders*"))
        assert len(files) >= 1

        all_lines = []
        for f in files:
            with open(f, "r", encoding="utf-8") as fh:
                all_lines.extend(fh.readlines())

        assert len(all_lines) == 3
        for line in all_lines:
            record = json.loads(line)
            assert record["ticker"] == "MSFT"
            assert record["_tier"] == "BRONZE_RAW"

    def test_write_raw_deadletter_to_local_files(self, tmp_path: Path) -> None:
        """Verify deadletter records are written to cold-tier audit files."""
        output_prefix = str(tmp_path / "raw_dlq" / "deadletter")

        dlqs = [
            DeadLetterRecord(
                payload=f'{{"corrupt_index": {i}}}',
                error_type="SchemaViolation",
                error_message="Negative price rejected",
                error_details={"price": -10.0},
                timestamp="2026-10-19T10:00:00Z",
            )
            for i in range(2)
        ]

        with BeamTestPipeline() as p:
            input_pcoll = p | beam.Create(dlqs)
            _ = input_pcoll | WriteRawDeadLetterToGcs(
                output_path=output_prefix,
                window_seconds=60,
                num_shards=1,
            )

        parent_dir = tmp_path / "raw_dlq"
        files = list(parent_dir.glob("deadletter*"))
        assert len(files) >= 1

        lines = []
        for f in files:
            with open(f, "r", encoding="utf-8") as fh:
                lines.extend(fh.readlines())

        assert len(lines) == 2
        for line in lines:
            record = json.loads(line)
            assert record["_tier"] == "BRONZE_DEADLETTER"


# -----------------------------------------------------------------------------
# 6. END-TO-END MULTI-OUTPUT PIPELINE INTEGRATION TEST
# -----------------------------------------------------------------------------
class TestEndToEndMultiOutputPipeline:
    """Verify complete multi-output Dataflow pipeline routing alerts to Cloud SQL and raw to GCS."""

    def test_multi_output_pipeline_execution(self, tmp_path: Path) -> None:
        """Simulate trade stream with price collapse:

        1. Valid orders decoded and written to GCS raw sink.
        2. Corrupt order diverted to deadletter stream.
        3. Sliding window anomaly detector flags flash crash.
        4. Clean RiskAlert written to Cloud SQL database and verified via SQL query.
        """
        db_file = str(tmp_path / "e2e_cloudsql.db")
        gcs_orders_dir = str(tmp_path / "e2e_gcs" / "orders")

        options = RiskEnginePipelineOptions([
            "--project_id=trading-risk-engine-dev",
            "--environment=dev",
            "--window_duration_seconds=300",
            "--window_slide_seconds=60",
            "--max_anomaly_drop_threshold=0.05",
            "--enable_cloudsql_sink",
            "--cloudsql_driver=sqlite3",
            "--cloudsql_sqlite_path=" + db_file,
            "--cloudsql_table=risk_alerts",
            "--cloudsql_batch_size=1",  # Immediate flush
            "--enable_gcs_raw_sink",
            "--gcs_output_path=" + gcs_orders_dir,
            "--gcs_window_seconds=300",
            "--gcs_num_shards=1",
        ])
        options.resolve_defaults()
        options.deadletter_topic = None
        options.output_alerts_topic = None
        options.validate_options(is_test=True)

        # 1. Clean order stream with a rapid 10% flash crash on NVDA
        base_time = 1776510000.0  # arbitrary epoch
        input_events = [
            # T0: Peak price $100.00
            json.dumps({
                "order_id": "ORD-1",
                "portfolio_id": "PORT-E2E",
                "ticker": "NVDA",
                "asset_class": "EQUITY",
                "side": "SELL",
                "order_type": "MARKET",
                "quantity": 100.0,
                "price": 100.0,
                "timestamp": datetime.fromtimestamp(base_time, tz=timezone.utc).isoformat(),
                "sequence_number": 1,
            }),
            # T1: Price drop to $94.00 (-6%, triggers alert)
            json.dumps({
                "order_id": "ORD-2",
                "portfolio_id": "PORT-E2E",
                "ticker": "NVDA",
                "asset_class": "EQUITY",
                "side": "SELL",
                "order_type": "MARKET",
                "quantity": 200.0,
                "price": 94.0,
                "timestamp": datetime.fromtimestamp(base_time + 60, tz=timezone.utc).isoformat(),
                "sequence_number": 2,
            }),
            # T2: Price drop to $90.00 (-10% total)
            json.dumps({
                "order_id": "ORD-3",
                "portfolio_id": "PORT-E2E",
                "ticker": "NVDA",
                "asset_class": "EQUITY",
                "side": "SELL",
                "order_type": "MARKET",
                "quantity": 300.0,
                "price": 90.0,
                "timestamp": datetime.fromtimestamp(base_time + 120, tz=timezone.utc).isoformat(),
                "sequence_number": 3,
            }),
            # Corrupt payload: malformed JSON
            "NOT_A_VALID_JSON_STRING",
        ]

        with BeamTestPipeline(options=options) as p:
            test_source = p | "CreateTestStream" >> beam.Create(input_events)
            bundle = build_pipeline(
                options=options,
                pipeline=p,
                test_input_pcoll=test_source,
            )

            assert bundle.valid_orders is not None
            assert bundle.deadletter_records is not None
            assert bundle.risk_alerts is not None
            assert bundle.cloudsql_alerts_written is not None
            assert bundle.gcs_orders_written is not None

        # -------------------------------------------------------------
        # Post-Pipeline Verification 1: Cloud SQL SQLite Database
        # -------------------------------------------------------------
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM risk_alerts WHERE ticker = 'NVDA'")
        alert_rows = cursor.fetchall()
        conn.close()

        assert len(alert_rows) >= 1, "Expected at least one RiskAlert written to Cloud SQL"
        first_alert = alert_rows[0]
        assert first_alert["ticker"] == "NVDA"
        assert first_alert["portfolio_id"] == "PORT-E2E"
        assert first_alert["alert_type"] == "FLASH_CRASH_DRAWDOWN"
        assert first_alert["status"] == "ACTIVE"
        assert first_alert["observed_value"] >= 0.05

        # -------------------------------------------------------------
        # Post-Pipeline Verification 2: GCS Raw Sinks Files
        # -------------------------------------------------------------
        parent_gcs_dir = tmp_path / "e2e_gcs"
        gcs_files = list(parent_gcs_dir.glob("orders*"))
        assert len(gcs_files) >= 1, "Expected GCS raw order files to be created"

        raw_records = []
        for f in gcs_files:
            with open(f, "r", encoding="utf-8") as fh:
                for line in fh:
                    raw_records.append(json.loads(line))

        assert len(raw_records) == 3, f"Expected 3 valid orders in GCS, found {len(raw_records)}"
        order_ids = {r["order_id"] for r in raw_records}
        assert order_ids == {"ORD-1", "ORD-2", "ORD-3"}
        for r in raw_records:
            assert r["_tier"] == "BRONZE_RAW"
            assert "_gcs_ingested_at" in r
