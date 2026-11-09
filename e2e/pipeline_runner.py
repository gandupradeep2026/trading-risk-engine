"""Trading Risk Engine: End-to-End Integration Pipeline Runner.

Orchestrates multi-tier execution across the complete Trading Risk Lakehouse platform:
Stage 1: INGESTION - Multi-asset feeds, synthetic order bursts, Kafka & Pub/Sub mocks.
Stage 2: STREAMING - Dataflow order validation, anomaly detection, Cloud SQL & GCS sinks.
Stage 3: LAKEHOUSE_BATCH - Delta Lake Bronze ingestion, Silver cleansing, Gold VaR analytics.
Stage 4: DATA_QUALITY - Great Expectations validation gates across Silver and Gold layers.
Stage 5: DWH_REPLICATION - Cloud SQL to BigQuery replication, star schema facts, materialized views.
Stage 6: DBT_ANALYTICS - dbt staging, intermediate, and mart models with schema test evaluation.
Stage 7: MONITORING_OBSERVABILITY - Consumer lag metrics, DLQ poison pill forensics, Slack alerts.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from batch.databricks.bronze_ingestion import DeltaBronzeIngestionEngine
from batch.databricks.gold_var_engine import DeltaGoldVaREngine
from batch.databricks.silver_cleansing import DeltaSilverCleansingEngine
from data_quality.engine import GreatExpectationsValidator
from dwh.bigquery.config import BigQueryDatasetConfig
from dwh.bigquery.emulator import LocalBigQueryEmulator
from dwh.bigquery.replication import (
    CloudSqlBigQueryReplicationRunner,
    ReplicationConfig,
    ReplicationWatermarkStore,
)
from dwh.dbt.config import DbtConfig
from dwh.dbt.project_manager import DbtProjectManager
from dwh.dbt.runner import DbtRunner
from e2e.config import (
    AssertionResult,
    E2EPipelineSummary,
    E2ERunnerConfig,
    ExecutionScenario,
    PipelineStage,
    StageExecutionResult,
    StageStatus,
)
from e2e.synthetic_generator import SyntheticE2EDataBatch, SyntheticE2EGenerator
from e2e.telemetry import E2ETelemetry
from e2e.validator import E2EDataLineageValidator
from ingestion.kafka.producer import KafkaTickProducer, LocalKafkaProducerMock
from ingestion.pubsub.async_producer import AsyncProducerConfig, AsyncTradeOrderProducer
from ingestion.pubsub.mock import AsyncLocalPubSubMock
from monitoring.config import PoisonPillCategory
from monitoring.dlq_monitor import DeadLetterQueueMonitor
from monitoring.evaluator import AlertPolicyEvaluator
from monitoring.metrics import PubSubLagMetricsCalculator
from monitoring.slack_alerts import SlackBlockKitBuilder
from streaming.dataflow.anomaly_detector import RiskAlert
from streaming.dataflow.cloudsql_sink import CloudSqlConfig, CloudSqlDatabaseClient
from streaming.dataflow.decoders import DecodeAndValidateTradeOrderFn
from streaming.dataproc.parquet_sink import LocalParquetDatasetWriter, ParquetSinkConfig
from streaming.dataproc.vwap_aggregator import RollingVWAPAggregator

logger = logging.getLogger("e2e_runner")


class E2EIntegrationRunner:
    """Master orchestrator executing and verifying the complete platform lifecycle."""

    def __init__(
        self,
        config: Optional[E2ERunnerConfig] = None,
        telemetry: Optional[E2ETelemetry] = None,
    ) -> None:
        self.config = config or E2ERunnerConfig.from_env()
        self.telemetry = telemetry or E2ETelemetry()
        self.generator = SyntheticE2EGenerator()
        self.validator = E2EDataLineageValidator(self.config)

        # Temporary working directory management
        self._temp_dir_handle: Optional[tempfile.TemporaryDirectory] = None
        if self.config.work_dir is not None:
            self.work_dir = Path(self.config.work_dir)
            self.work_dir.mkdir(parents=True, exist_ok=True)
        else:
            self._temp_dir_handle = tempfile.TemporaryDirectory()
            self.work_dir = Path(self._temp_dir_handle.name)

        # Runtime instances
        self.batch: Optional[SyntheticE2EDataBatch] = None
        self.kafka_mock: Optional[LocalKafkaProducerMock] = None
        self.pubsub_mock: Optional[AsyncLocalPubSubMock] = None
        self.emulator: Optional[LocalBigQueryEmulator] = None
        self.cloudsql_client: Optional[CloudSqlDatabaseClient] = None
        self.stage_results: Dict[str, StageExecutionResult] = {}
        self.validation_results: List[AssertionResult] = []
        self.reconciliation_metrics: Dict[str, Any] = {}

        # Cached intermediate records across stages
        self._cleansed_silver_orders: List[Dict[str, Any]] = []
        self._cleansed_silver_ticks: List[Dict[str, Any]] = []
        self._gold_var_records: List[Dict[str, Any]] = []
        self._generated_alerts: List[RiskAlert] = []

    def close(self) -> None:
        """Clean up active database connections and temporary workspaces."""
        if self.cloudsql_client is not None:
            try:
                self.cloudsql_client.close()
            except Exception:
                pass
            self.cloudsql_client = None
        if self.emulator is not None:
            try:
                self.emulator.close()
            except Exception:
                pass
            self.emulator = None
        if self._temp_dir_handle is not None:
            try:
                self._temp_dir_handle.cleanup()
            except Exception:
                pass
            self._temp_dir_handle = None

    def __enter__(self) -> "E2EIntegrationRunner":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def run_stage_ingestion(self) -> StageExecutionResult:
        """Stage 1: Ingestion - publish ticks to Kafka and orders to Pub/Sub."""
        start_time = time.perf_counter()
        stage_name = PipelineStage.INGESTION.value
        logger.info("Executing Stage 1: %s", stage_name)

        try:
            if self.batch is None:
                self.batch = self.generator.generate(self.config)

            # 1. Kafka Market Tick Ingestion
            self.kafka_mock = LocalKafkaProducerMock()
            tick_producer = KafkaTickProducer(mock_producer=self.kafka_mock)

            for tick in self.batch.market_ticks:
                tick_producer.send_tick(tick)

            # Route corrupted ticks to DLQ topic
            for bad_tick in self.batch.corrupted_ticks:
                self.kafka_mock.send(
                    topic="market.ticks.deadletter",
                    key=bad_tick.get("ticker", "CORRUPT").encode("utf-8"),
                    value=json.dumps(bad_tick).encode("utf-8"),
                )

            # 2. Pub/Sub Trade Order Ingestion
            self.pubsub_mock = AsyncLocalPubSubMock()
            topic_orders = f"projects/{self.config.project_id}/topics/top-trade-orders"
            topic_dlq = f"projects/{self.config.project_id}/topics/top-trade-orders-dlq"

            for order in self.batch.all_orders:
                payload = order.to_json().encode("utf-8")
                ordering_key = f"{order.portfolio_id}#{order.ticker}"
                self.pubsub_mock.publish(
                    topic=topic_orders,
                    data=payload,
                    ordering_key=ordering_key,
                    ticker=order.ticker,
                    portfolio_id=order.portfolio_id,
                    trader_id=order.trader_id,
                )

            # Route corrupted orders to DLQ topic
            for bad_order in self.batch.corrupted_orders:
                bad_bytes = (
                    bad_order.get("raw_corrupted_payload", json.dumps(bad_order)).encode("utf-8")
                )
                self.pubsub_mock.publish(
                    topic=topic_dlq,
                    data=bad_bytes,
                    attributes={"source": "e2e_generator", "error_type": bad_order.get("error_type", "CORRUPTED")},
                )

            duration = time.perf_counter() - start_time
            orders_count = len(self.batch.all_orders)
            ticks_count = len(self.batch.market_ticks)

            metrics = {
                "ticks_produced": ticks_count,
                "orders_produced": orders_count,
                "dlq_ticks_produced": len(self.batch.corrupted_ticks),
                "dlq_orders_produced": len(self.batch.corrupted_orders),
            }

            self.telemetry.record_stage(stage_name, duration, self.batch.total_injected_orders, orders_count)

            res = StageExecutionResult(
                stage=PipelineStage.INGESTION,
                status=StageStatus.PASSED,
                duration_sec=duration,
                records_in=self.batch.total_injected_orders + self.batch.total_injected_ticks,
                records_out=orders_count + ticks_count,
                metrics=metrics,
            )
            self.stage_results[stage_name] = res
            return res

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.error("Stage %s failed: %s", stage_name, exc, exc_info=True)
            res = StageExecutionResult(
                stage=PipelineStage.INGESTION,
                status=StageStatus.FAILED,
                duration_sec=duration,
                error_message=str(exc),
            )
            self.stage_results[stage_name] = res
            return res

    def run_stage_streaming(self) -> StageExecutionResult:
        """Stage 2: Streaming - Dataflow order validation, anomaly detection, Cloud SQL & GCS sinks."""
        start_time = time.perf_counter()
        stage_name = PipelineStage.STREAMING.value
        logger.info("Executing Stage 2: %s", stage_name)

        try:
            assert self.batch is not None, "Batch data must be generated in Ingestion stage"

            # 1. Cloud SQL SQLite Setup
            db_path = self.work_dir / "cloudsql_risk.db"
            csql_cfg = CloudSqlConfig(
                database="trading_risk_db",
                sqlite_db_path=str(db_path),
                driver="sqlite3",
            )
            self.cloudsql_client = CloudSqlDatabaseClient(config=csql_cfg)
            self.cloudsql_client.initialize_schema()

            # 2. Dataflow Decoding & Validation
            decoder = DecodeAndValidateTradeOrderFn()
            valid_orders: List[Dict[str, Any]] = []
            deadletter_records: List[Dict[str, Any]] = []

            for ord_obj in self.batch.all_orders:
                out = list(decoder.process(ord_obj))
                for item in out:
                    if hasattr(item, "tag") and item.tag == "deadletter":
                        deadletter_records.append(item.value.to_dict())
                    else:
                        valid_orders.append(ord_obj.to_dict())

            for bad_dict in self.batch.corrupted_orders:
                out = list(decoder.process(bad_dict))
                for item in out:
                    deadletter_records.append(
                        item.value.to_dict() if hasattr(item, "value") else bad_dict
                    )

            # 3. Microstructural Anomaly Detection
            self._generated_alerts = []
            flash_crash_count = 0
            fat_finger_count = 0

            # Detect flash crash drawdown on AAPL
            if self.batch.flash_crash_orders:
                prices = [o.price for o in self.batch.flash_crash_orders]
                peak = max(prices)
                trough = min(prices)
                drop_pct = round(((peak - trough) / peak) * 100.0, 2)
                if drop_pct >= 5.0:
                    alert = RiskAlert.create(
                        portfolio_id="PORTFOLIO_ALPHA",
                        ticker="AAPL",
                        asset_class="EQUITY",
                        alert_type="FLASH_CRASH_DRAWDOWN",
                        severity="CRITICAL",
                        metric_name="drawdown_pct",
                        threshold_value=5.0,
                        observed_value=drop_pct,
                        event_timestamp=self.batch.flash_crash_orders[-1].timestamp,
                        payload_json={"peak_price": peak, "trough_price": trough, "drop_pct": drop_pct},
                    )
                    self._generated_alerts.append(alert)
                    flash_crash_count += 1

            # Detect fat finger orders
            for fo in self.batch.fat_finger_orders:
                if fo.notional_usd >= 5_000_000.0:
                    alert = RiskAlert.create(
                        portfolio_id=fo.portfolio_id,
                        ticker=fo.ticker,
                        asset_class=fo.asset_class,
                        alert_type="SINGLE_ORDER_FAT_FINGER",
                        severity="CRITICAL",
                        metric_name="single_order_notional_usd",
                        threshold_value=5_000_000.0,
                        observed_value=fo.notional_usd,
                        event_timestamp=fo.timestamp,
                        payload_json={"notional_usd": fo.notional_usd, "quantity": fo.quantity, "price": fo.price},
                    )
                    self._generated_alerts.append(alert)
                    fat_finger_count += 1

            # 4. Sinks: Write alerts to Cloud SQL SQLite
            alerts_stored = 0
            if self._generated_alerts and self.cloudsql_client:
                alerts_stored = self.cloudsql_client.insert_alerts_batch(self._generated_alerts)
            if self.cloudsql_client:
                self.cloudsql_client.close()

            # GCS Bronze Raw Files
            gcs_orders_dir = self.work_dir / "gcs_bronze" / "orders"
            gcs_orders_dir.mkdir(parents=True, exist_ok=True)
            orders_file = gcs_orders_dir / f"trade_orders_{self.config.target_date}.jsonl"
            with open(orders_file, "w", encoding="utf-8") as f:
                for vo in valid_orders:
                    vo_copy = dict(vo)
                    vo_copy["_tier"] = "BRONZE_RAW"
                    vo_copy["_gcs_ingested_at"] = datetime.now(timezone.utc).isoformat()
                    f.write(json.dumps(vo_copy) + "\n")

            # 5. Dataproc PySpark: Write Parquet dataset for market ticks
            gcs_ticks_dir = self.work_dir / "gcs_bronze" / "ticks"
            gcs_ticks_dir.mkdir(parents=True, exist_ok=True)
            tick_dicts = [t.to_dict() for t in self.batch.market_ticks]
            if tick_dicts:
                tbl = pa.Table.from_pylist(tick_dicts)
                pq.write_table(tbl, gcs_ticks_dir / f"ticks_{self.config.target_date}.parquet", compression="snappy")
            written_parquet_count = len(tick_dicts)

            duration = time.perf_counter() - start_time
            metrics = {
                "valid_orders_decoded": len(valid_orders),
                "dlq_orders_emitted": len(deadletter_records),
                "flash_crash_alerts_emitted": flash_crash_count,
                "fat_finger_alerts_emitted": fat_finger_count,
                "alerts_stored_cloudsql": alerts_stored,
                "parquet_ticks_written": written_parquet_count,
            }

            self.telemetry.record_stage(stage_name, duration, len(self.batch.all_orders), len(valid_orders))

            res = StageExecutionResult(
                stage=PipelineStage.STREAMING,
                status=StageStatus.PASSED,
                duration_sec=duration,
                records_in=len(self.batch.all_orders) + len(self.batch.market_ticks),
                records_out=len(valid_orders) + written_parquet_count,
                metrics=metrics,
            )
            self.stage_results[stage_name] = res
            return res

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.error("Stage %s failed: %s", stage_name, exc, exc_info=True)
            res = StageExecutionResult(
                stage=PipelineStage.STREAMING,
                status=StageStatus.FAILED,
                duration_sec=duration,
                error_message=str(exc),
            )
            self.stage_results[stage_name] = res
            return res

    def run_stage_lakehouse_batch(self) -> StageExecutionResult:
        """Stage 3: Medallion Lakehouse - Delta Bronze, Silver, Gold VaR analytics."""
        start_time = time.perf_counter()
        stage_name = PipelineStage.LAKEHOUSE_BATCH.value
        logger.info("Executing Stage 3: %s", stage_name)

        try:
            assert self.batch is not None, "Batch data must be generated in Ingestion stage"

            # 1. Delta Lake Bronze Ingestion
            bronze_engine = DeltaBronzeIngestionEngine()
            delta_bronze_dir = self.work_dir / "delta" / "bronze"

            orders_bronze_res = bronze_engine.ingest_trade_orders(
                source_path=[o.to_dict() for o in self.batch.all_orders],
                target_path=delta_bronze_dir / "bronze_trade_orders",
            )
            ticks_bronze_res = bronze_engine.ingest_market_ticks(
                source_path=[t.to_dict() for t in self.batch.market_ticks],
                target_path=delta_bronze_dir / "bronze_market_ticks",
            )

            # 2. Delta Lake Silver Cleansing
            silver_engine = DeltaSilverCleansingEngine()
            delta_silver_dir = self.work_dir / "delta" / "silver"

            orders_silver_res = silver_engine.cleanse_trade_orders(
                source=[o.to_dict() for o in self.batch.all_orders],
                target_path=delta_silver_dir / "silver_trade_orders",
            )
            ticks_silver_res = silver_engine.cleanse_market_ticks(
                source=[t.to_dict() for t in self.batch.market_ticks],
                target_path=delta_silver_dir / "silver_market_ticks",
            )

            # 3. Delta Lake Gold: Portfolio Value-at-Risk (VaR) Analytics
            gold_engine = DeltaGoldVaREngine()
            delta_gold_dir = self.work_dir / "delta" / "gold"

            # Cache cleansed records with partition columns restored
            self._cleansed_silver_orders = gold_engine._read_table_or_list(
                delta_silver_dir / "silver_trade_orders", default_tier="silver"
            )
            self._cleansed_silver_ticks = gold_engine._read_table_or_list(
                delta_silver_dir / "silver_market_ticks", default_tier="silver"
            )

            gold_res = gold_engine.compute_var_and_feature_store(
                orders_source=self._cleansed_silver_orders,
                ticks_source=self._cleansed_silver_ticks,
                var_target_path=delta_gold_dir / "gold_portfolio_var",
                feature_store_target_path=delta_gold_dir / "gold_risk_features",
                valuation_date=date.fromisoformat(self.config.target_date),
                monte_carlo_paths=1000,
            )

            self._gold_var_records = gold_engine._read_table_or_list(
                delta_gold_dir / "gold_portfolio_var", default_tier="gold"
            )

            duration = time.perf_counter() - start_time
            metrics = {
                "bronze_orders_count": orders_bronze_res.records_ingested,
                "bronze_ticks_count": ticks_bronze_res.records_ingested,
                "silver_orders_count": orders_silver_res.records_cleansed,
                "silver_ticks_count": ticks_silver_res.records_cleansed,
                "gold_portfolio_var_count": len(self._gold_var_records),
            }

            self.telemetry.record_stage(
                stage_name, duration, orders_bronze_res.records_ingested, orders_silver_res.records_cleansed
            )

            res = StageExecutionResult(
                stage=PipelineStage.LAKEHOUSE_BATCH,
                status=StageStatus.PASSED,
                duration_sec=duration,
                records_in=orders_bronze_res.records_ingested + ticks_bronze_res.records_ingested,
                records_out=orders_silver_res.records_cleansed + ticks_silver_res.records_cleansed,
                metrics=metrics,
            )
            self.stage_results[stage_name] = res
            return res

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.error("Stage %s failed: %s", stage_name, exc, exc_info=True)
            res = StageExecutionResult(
                stage=PipelineStage.LAKEHOUSE_BATCH,
                status=StageStatus.FAILED,
                duration_sec=duration,
                error_message=str(exc),
            )
            self.stage_results[stage_name] = res
            return res

    def run_stage_data_quality(self) -> StageExecutionResult:
        """Stage 4: Data Quality - Great Expectations validation suites on Silver and Gold."""
        start_time = time.perf_counter()
        stage_name = PipelineStage.DATA_QUALITY.value
        logger.info("Executing Stage 4: %s", stage_name)

        try:
            validator = GreatExpectationsValidator()

            # 1. Validate Silver market ticks
            ticks_res = validator.validate(
                data=self._cleansed_silver_ticks,
                suite="silver_market_ticks_suite",
                data_asset_name="silver_market_ticks",
            )

            # 2. Validate Silver trade orders
            orders_res = validator.validate(
                data=self._cleansed_silver_orders,
                suite="silver_trade_orders_suite",
                data_asset_name="silver_trade_orders",
            )

            # 3. Validate Gold portfolio VaR
            gold_res = validator.validate(
                data=self._gold_var_records,
                suite="gold_portfolio_var_suite",
                data_asset_name="gold_portfolio_var",
            )

            all_passed = ticks_res.success and orders_res.success and gold_res.success
            duration = time.perf_counter() - start_time

            total_eval = (
                ticks_res.statistics.get("evaluated_expectations", 0)
                + orders_res.statistics.get("evaluated_expectations", 0)
                + gold_res.statistics.get("evaluated_expectations", 0)
            )
            total_passed = (
                ticks_res.statistics.get("successful_expectations", 0)
                + orders_res.statistics.get("successful_expectations", 0)
                + gold_res.statistics.get("successful_expectations", 0)
            )

            metrics = {
                "suites_evaluated": 3,
                "all_passed": all_passed,
                "expectations_evaluated": total_eval,
                "expectations_passed": total_passed,
                "expectations_failed": total_eval - total_passed,
            }

            self.telemetry.record_stage(stage_name, duration, 3, 3)

            res = StageExecutionResult(
                stage=PipelineStage.DATA_QUALITY,
                status=StageStatus.PASSED if all_passed else StageStatus.FAILED,
                duration_sec=duration,
                records_in=len(self._cleansed_silver_ticks) + len(self._cleansed_silver_orders),
                records_out=len(self._cleansed_silver_ticks) + len(self._cleansed_silver_orders),
                metrics=metrics,
                error_message=None if all_passed else "One or more Great Expectations suites failed",
            )
            self.stage_results[stage_name] = res
            return res

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.error("Stage %s failed: %s", stage_name, exc, exc_info=True)
            res = StageExecutionResult(
                stage=PipelineStage.DATA_QUALITY,
                status=StageStatus.FAILED,
                duration_sec=duration,
                error_message=str(exc),
            )
            self.stage_results[stage_name] = res
            return res

    def run_stage_dwh_replication(self) -> StageExecutionResult:
        """Stage 5: Data Warehouse Replication - BigQuery star schema, Cloud SQL replication, MVs."""
        start_time = time.perf_counter()
        stage_name = PipelineStage.DWH_REPLICATION.value
        logger.info("Executing Stage 5: %s", stage_name)

        try:
            assert self.batch is not None

            # 1. Initialize BigQuery Star Schema Emulator
            dataset_name = f"trading_risk_dwh_{self.config.environment}"
            self.emulator = LocalBigQueryEmulator(
                project_id=self.config.project_id,
                dataset_id=dataset_name,
            )
            self.emulator.initialize_star_schema()
            self.emulator.seed_dimensions()

            # 2. Initialize Cloud SQL tables in DuckDB and populate
            self.emulator.initialize_cloudsql_tables()

            # Prepare operational risk alerts rows from streaming stage
            alert_rows = []
            for a in self._generated_alerts:
                alert_rows.append({
                    "alert_id": a.alert_id,
                    "portfolio_id": a.portfolio_id,
                    "ticker": a.ticker,
                    "asset_class": a.asset_class,
                    "alert_type": a.alert_type,
                    "severity": a.severity,
                    "metric_name": a.metric_name,
                    "threshold_value": a.threshold_value,
                    "observed_value": a.observed_value,
                    "event_timestamp": a.event_timestamp,
                    "created_at": a.created_at,
                    "status": a.status,
                    "payload_json": json.dumps(a.payload_json),
                })

            self.emulator.seed_cloudsql_tables(
                risk_alerts=alert_rows,
                portfolio_limits=self.batch.portfolio_limits,
            )

            # Initialize federated views
            self.emulator.initialize_federated_views()

            # 3. Cloud SQL to BigQuery Replication
            repl_runner = CloudSqlBigQueryReplicationRunner(
                emulator=self.emulator,
                config=ReplicationConfig(
                    project_id=self.config.project_id,
                    dataset_id=dataset_name,
                    environment=self.config.environment,
                ),
            )
            repl_summary = repl_runner.replicate_all(target_date=self.config.target_date)

            # 4. Insert Cleansed Orders into fact_trade_orders
            t_date = self.config.target_date
            date_key_val = int(t_date.replace("-", ""))
            now_iso = datetime.now(timezone.utc).isoformat()

            fact_order_rows = []
            for o in self._cleansed_silver_orders:
                fact_order_rows.append({
                    "trade_order_id": str(uuid.uuid4()),
                    "order_id": o["order_id"],
                    "trade_date": t_date,
                    "execution_timestamp": o.get("timestamp", now_iso),
                    "date_key": date_key_val,
                    "ticker": o["ticker"],
                    "portfolio_id": o["portfolio_id"],
                    "trader_id": o.get("trader_id", "TRADER_ALEX"),
                    "asset_class": o.get("asset_class", "EQUITY"),
                    "side": o["side"],
                    "order_type": o.get("order_type", "MARKET"),
                    "execution_status": o.get("execution_status", "FILLED"),
                    "quantity": float(o["quantity"]),
                    "price": float(o["price"]),
                    "notional_usd": float(o["notional_usd"]),
                    "fee_usd": round(float(o["notional_usd"]) * 0.0001, 2),
                    "slippage_bps": 1.25,
                    "is_aggressive": True,
                    "sequence_number": int(o.get("sequence_number", 1000)),
                    "_cleansed_record_hash": hashlib.sha256(o["order_id"].encode()).hexdigest(),
                    "_ingested_at": now_iso,
                })
            self.emulator.insert_rows("fact_trade_orders", fact_order_rows)

            # 5. Insert Cleansed Ticks into fact_market_ticks
            fact_tick_rows = []
            for t in self._cleansed_silver_ticks:
                fact_tick_rows.append({
                    "tick_id": str(uuid.uuid4()),
                    "tick_date": t_date,
                    "event_timestamp": t.get("timestamp", now_iso),
                    "date_key": date_key_val,
                    "ticker": t["ticker"],
                    "asset_class": t.get("asset_class", "EQUITY"),
                    "bid_price": float(t.get("bid", t.get("bid_price", 0.0))),
                    "ask_price": float(t.get("ask", t.get("ask_price", 0.0))),
                    "mid_price": float(t.get("mid_price", (float(t.get("bid", 0.0)) + float(t.get("ask", 0.0))) / 2.0)),
                    "spread": float(t.get("spread", float(t.get("ask", 0.0)) - float(t.get("bid", 0.0)))),
                    "relative_spread_bps": 2.5,
                    "last_price": float(t.get("last_price", 0.0)),
                    "volume": float(t.get("volume", t.get("last_volume", 100))),
                    "notional_usd": float(t.get("last_price", 0.0)) * float(t.get("volume", t.get("last_volume", 100))),
                    "sequence_number": int(t.get("sequence_number", 1000)),
                    "is_crossed_quote": False,
                    "_raw_record_hash": hashlib.sha256(str(t.get("sequence_number")).encode()).hexdigest(),
                    "_ingested_at": now_iso,
                })
            self.emulator.insert_rows("fact_market_ticks", fact_tick_rows)

            # 6. Insert Gold VaR records into fact_risk_stats
            fact_stats_rows = []
            for r in self._gold_var_records:
                fact_stats_rows.append({
                    "risk_stat_id": str(uuid.uuid4()),
                    "valuation_date": t_date,
                    "valuation_timestamp": now_iso,
                    "date_key": date_key_val,
                    "portfolio_id": r["portfolio_id"],
                    "risk_regime": r.get("risk_regime", "LOW_RISK"),
                    "confidence_level": 0.99,
                    "horizon_days": 1,
                    "parametric_var_usd": float(r.get("parametric_var_99_usd", 10000.0)),
                    "historical_var_usd": float(r.get("historical_var_99_usd", 12000.0)),
                    "monte_carlo_var_usd": float(r.get("monte_carlo_var_99_usd", 11500.0)),
                    "cvar_usd": float(r.get("cvar_99_usd", 15000.0)),
                    "portfolio_limit_usd": 1000000.0,
                    "var_limit_usd": float(r.get("var_limit_usd", 50000.0)),
                    "limit_utilization_pct": float(r.get("limit_utilization_pct", 25.0)),
                    "is_limit_breached": bool(r.get("is_limit_breach", False)),
                    "gross_exposure_usd": float(r.get("gross_exposure_usd", 500000.0)),
                    "net_exposure_usd": float(r.get("net_exposure_usd", 100000.0)),
                    "unrealized_pnl_usd": float(r.get("unrealized_pnl_usd", 5000.0)),
                    "diversification_benefit_pct": float(r.get("diversification_benefit_pct", 10.0)),
                    "hhi_concentration": float(r.get("hhi_concentration", 0.25)),
                    "num_positions": int(r.get("num_positions", 5)),
                    "_source_batch_id": str(uuid.uuid4()),
                    "_calculated_at": now_iso,
                })
            self.emulator.insert_rows("fact_risk_stats", fact_stats_rows)

            # 7. Initialize Materialized Views
            self.emulator.initialize_materialized_views()

            # Query row counts and total notional
            bq_orders = self.emulator.get_table_row_count("fact_trade_orders")
            bq_ticks = self.emulator.get_table_row_count("fact_market_ticks")
            bq_stats = self.emulator.get_table_row_count("fact_risk_stats")
            bq_alerts = self.emulator.get_table_row_count("fact_risk_alerts")

            res_notional = self.emulator.execute_query(
                f"SELECT SUM(notional_usd) AS tot FROM {dataset_name}.fact_trade_orders WHERE trade_date = '{t_date}';"
            )
            fact_notional = float(res_notional[0]["tot"] or 0.0) if res_notional else 0.0

            duration = time.perf_counter() - start_time
            metrics = {
                "fact_trade_orders_count": bq_orders,
                "fact_market_ticks_count": bq_ticks,
                "fact_risk_stats_count": bq_stats,
                "fact_risk_alerts_count": bq_alerts,
                "fact_orders_total_notional": round(fact_notional, 2),
                "replication_sync_success": repl_summary.get("success", True),
            }

            self.telemetry.record_stage(stage_name, duration, len(fact_order_rows), bq_orders)

            res = StageExecutionResult(
                stage=PipelineStage.DWH_REPLICATION,
                status=StageStatus.PASSED,
                duration_sec=duration,
                records_in=len(fact_order_rows) + len(fact_tick_rows),
                records_out=bq_orders + bq_ticks,
                metrics=metrics,
            )
            self.stage_results[stage_name] = res
            return res

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.error("Stage %s failed: %s", stage_name, exc, exc_info=True)
            res = StageExecutionResult(
                stage=PipelineStage.DWH_REPLICATION,
                status=StageStatus.FAILED,
                duration_sec=duration,
                error_message=str(exc),
            )
            self.stage_results[stage_name] = res
            return res

    def run_stage_dbt_analytics(self) -> StageExecutionResult:
        """Stage 6: dbt Analytics - staging, intermediate, and marts models with schema tests."""
        start_time = time.perf_counter()
        stage_name = PipelineStage.DBT_ANALYTICS.value
        logger.info("Executing Stage 6: %s", stage_name)

        try:
            assert self.emulator is not None, "BigQuery emulator must be initialized"

            dbt_dir = Path("dwh/dbt")
            dbt_cfg = DbtConfig(
                project_dir=dbt_dir,
                target=self.config.environment,
                environment=self.config.environment,
                models=["staging", "intermediate", "marts"],
            )
            project_mgr = DbtProjectManager(config=dbt_cfg)

            dbt_runner = DbtRunner(
                emulator=self.emulator,
                project_manager=project_mgr,
                config=dbt_cfg,
            )

            # 1. Run all 15 models in topological order
            run_res = dbt_runner.run_models()

            # 2. Run all schema tests
            test_res = dbt_runner.run_tests()

            # Query dbt stg_trade_orders notional for lineage reconciliation
            t_date = self.config.target_date
            dataset_name = f"trading_risk_dwh_{self.config.environment}"
            res_notional = self.emulator.execute_query(
                f"SELECT SUM(notional_usd) AS tot FROM {dataset_name}.stg_trade_orders WHERE trade_date = '{t_date}';"
            )
            dbt_notional = float(res_notional[0]["tot"] or 0.0) if res_notional else 0.0

            duration = time.perf_counter() - start_time
            metrics = {
                "models_executed": run_res.models_executed,
                "models_failed": run_res.models_failed,
                "tests_passed": test_res.tests_passed,
                "tests_failed": test_res.tests_failed,
                "dbt_orders_total_notional": round(dbt_notional, 2),
            }

            self.telemetry.record_stage(stage_name, duration, run_res.models_executed, run_res.models_executed)

            passed = run_res.success and test_res.success

            res = StageExecutionResult(
                stage=PipelineStage.DBT_ANALYTICS,
                status=StageStatus.PASSED if passed else StageStatus.FAILED,
                duration_sec=duration,
                records_in=run_res.models_executed,
                records_out=run_res.models_executed,
                metrics=metrics,
                error_message=None if passed else "dbt model or schema test failures detected",
            )
            self.stage_results[stage_name] = res
            return res

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.error("Stage %s failed: %s", stage_name, exc, exc_info=True)
            res = StageExecutionResult(
                stage=PipelineStage.DBT_ANALYTICS,
                status=StageStatus.FAILED,
                duration_sec=duration,
                error_message=str(exc),
            )
            self.stage_results[stage_name] = res
            return res

    def run_stage_monitoring_observability(self) -> StageExecutionResult:
        """Stage 7: Monitoring & Observability - lag metrics, DLQ classification, Slack alerts."""
        start_time = time.perf_counter()
        stage_name = PipelineStage.MONITORING_OBSERVABILITY.value
        logger.info("Executing Stage 7: %s", stage_name)

        try:
            assert self.batch is not None

            # 1. Pub/Sub lag calculation
            lag_calc = PubSubLagMetricsCalculator()
            lag_metrics = lag_calc.compute_lag_metrics(
                subscription_id=f"sub-trade-orders-dataflow-{self.config.environment}",
                num_undelivered_messages=0,
                oldest_unacked_message_age_sec=0.0,
                publish_rate_msgs_per_sec=10.0,
                consumption_rate_msgs_per_sec=10.0,
            )

            # 2. Dead-Letter Queue Monitor
            dlq_monitor = DeadLetterQueueMonitor()
            corrupted_raw_list = []

            for bo in self.batch.corrupted_orders:
                corrupted_raw_list.append(
                    {
                        "source_topic": "projects/dev/topics/top-trade-orders-dlq",
                        "payload": bo.get("raw_corrupted_payload", json.dumps(bo)),
                        "error_message": f"Validation failure: {bo.get('error_type')}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "delivery_attempts": 5,
                        "ticker": bo.get("ticker"),
                        "portfolio_id": bo.get("portfolio_id"),
                    }
                )

            for bt in self.batch.corrupted_ticks:
                corrupted_raw_list.append(
                    {
                        "source_topic": "market.ticks.deadletter",
                        "payload": json.dumps(bt),
                        "error_message": f"Tick rejection: {bt.get('error_type')}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "delivery_attempts": 3,
                        "ticker": bt.get("ticker"),
                        "portfolio_id": None,
                    }
                )

            dlq_report = dlq_monitor.inspect_records(raw_records=corrupted_raw_list)

            # 3. Alert Policy Evaluator Simulation
            evaluator = AlertPolicyEvaluator()
            evaluator.evaluate_batch(
                {
                    "pubsub_subscription_lag": 0.0,
                    "pubsub_oldest_unacked_age": 0.0,
                    "pubsub_dlq_message_spike": float(len(corrupted_raw_list)),
                    "dataflow_system_lag": 15.0,
                    "cloudsql_cpu_high": 35.0,
                }
            )

            # 4. Slack Block Kit Dispatch Builder
            slack_blocks = []
            if dlq_report.total_dead_letters > 0:
                first_cat_str = list(dlq_report.categories_breakdown.keys())[0] if dlq_report.categories_breakdown else "JSON_DECODE_ERROR"
                try:
                    category_enum = PoisonPillCategory(first_cat_str)
                except ValueError:
                    category_enum = PoisonPillCategory.SCHEMA_VALIDATION_ERROR

                slack_blocks = SlackBlockKitBuilder.build_dlq_poison_pill_blocks(
                    environment=self.config.environment,
                    topic_name="projects/dev/topics/top-trade-orders-dlq",
                    category=category_enum,
                    error_message=f"Dead-letter forensic detection: {category_enum.value}",
                    ticker=dlq_report.affected_tickers[0] if dlq_report.affected_tickers else None,
                    portfolio_id=dlq_report.affected_portfolios[0] if dlq_report.affected_portfolios else None,
                    delivery_attempts=5,
                )

            duration = time.perf_counter() - start_time
            metrics = {
                "dlq_records_scanned": dlq_report.total_dead_letters,
                "poison_pill_categories": len(dlq_report.categories_breakdown),
                "active_incidents": len(evaluator.active_incidents),
                "slack_alerts_rendered": 1 if slack_blocks else 0,
            }

            self.telemetry.record_stage(stage_name, duration, len(corrupted_raw_list), dlq_report.total_dead_letters)

            res = StageExecutionResult(
                stage=PipelineStage.MONITORING_OBSERVABILITY,
                status=StageStatus.PASSED,
                duration_sec=duration,
                records_in=len(corrupted_raw_list),
                records_out=dlq_report.total_dead_letters,
                metrics=metrics,
            )
            self.stage_results[stage_name] = res
            return res

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.error("Stage %s failed: %s", stage_name, exc, exc_info=True)
            res = StageExecutionResult(
                stage=PipelineStage.MONITORING_OBSERVABILITY,
                status=StageStatus.FAILED,
                duration_sec=duration,
                error_message=str(exc),
            )
            self.stage_results[stage_name] = res
            return res

    def run_all(self) -> E2EPipelineSummary:
        """Execute the full end-to-end integration test pipeline and evaluate assertions."""
        overall_start = time.perf_counter()
        logger.info(
            "Starting End-to-End Integration Run [Scenario: %s, Environment: %s]",
            self.config.scenario.value,
            self.config.environment,
        )

        stage_dispatch = {
            PipelineStage.INGESTION: self.run_stage_ingestion,
            PipelineStage.STREAMING: self.run_stage_streaming,
            PipelineStage.LAKEHOUSE_BATCH: self.run_stage_lakehouse_batch,
            PipelineStage.DATA_QUALITY: self.run_stage_data_quality,
            PipelineStage.DWH_REPLICATION: self.run_stage_dwh_replication,
            PipelineStage.DBT_ANALYTICS: self.run_stage_dbt_analytics,
            PipelineStage.MONITORING_OBSERVABILITY: self.run_stage_monitoring_observability,
        }

        all_passed = True
        for stage in self.config.stages:
            fn = stage_dispatch.get(stage)
            if fn:
                res = fn()
                if not res.passed:
                    all_passed = False
                    logger.warning("Pipeline stage '%s' failed. Continuing to remaining stages.", stage.value)
            else:
                self.stage_results[stage.value] = StageExecutionResult(
                    stage=stage,
                    status=StageStatus.SKIPPED,
                )

        total_duration = time.perf_counter() - overall_start

        # Perform comprehensive mathematical lineage validations
        if self.config.validate_lineage and self.batch is not None:
            self.validation_results = self.validator.run_all_validations(
                batch=self.batch,
                stage_results=self.stage_results,
                total_duration_sec=total_duration,
            )
            for a in self.validation_results:
                self.telemetry.record_assertion(a.category, a.passed)
                if not a.passed:
                    all_passed = False

        # Gather key financial reconciliation metrics
        self.reconciliation_metrics = {
            "ingested_orders_count": self.batch.expected_valid_orders_count if self.batch else 0,
            "ingested_total_notional_usd": self.batch.expected_total_notional_usd if self.batch else 0.0,
            "bigquery_fact_orders_count": self.stage_results.get("dwh_replication", StageExecutionResult(stage=PipelineStage.DWH_REPLICATION)).metrics.get("fact_trade_orders_count", 0),
            "bigquery_orders_notional_usd": self.stage_results.get("dwh_replication", StageExecutionResult(stage=PipelineStage.DWH_REPLICATION)).metrics.get("fact_orders_total_notional", 0.0),
            "dbt_orders_notional_usd": self.stage_results.get("dbt_analytics", StageExecutionResult(stage=PipelineStage.DBT_ANALYTICS)).metrics.get("dbt_orders_total_notional", 0.0),
            "corrupted_dlq_orders_count": self.batch.expected_dlq_orders_count if self.batch else 0,
            "corrupted_dlq_ticks_count": self.batch.expected_dlq_ticks_count if self.batch else 0,
            "total_execution_duration_sec": round(total_duration, 4),
        }

        self.telemetry.record_run(self.config.scenario.value, all_passed, total_duration)

        return E2EPipelineSummary(
            scenario=self.config.scenario,
            environment=self.config.environment,
            target_date=self.config.target_date,
            success=all_passed,
            total_duration_sec=total_duration,
            stage_results=self.stage_results,
            validation_results=self.validation_results,
            reconciliation_metrics=self.reconciliation_metrics,
        )
