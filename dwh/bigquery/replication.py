"""Cloud SQL to BigQuery Replication Engine via External / Federated Queries (Day 25).

Provides:
- ReplicationMode: INCREMENTAL_WATERMARK and SNAPSHOT_REPLACE synchronization strategies.
- TableReplicationSpec: Specification for replicating operational Cloud SQL tables to BigQuery.
- REPLICATION_CATALOG: Canonical specifications for risk_alerts -> fact_risk_alerts and
  portfolio_limits -> dim_portfolios.
- ReplicationWatermarkStore: Thread-safe high-watermark state tracking per table.
- ReplicationTelemetry: Operational telemetry tracking synced records, transferred bytes, and Prometheus metrics.
- CloudSqlBigQueryReplicationRunner: Enterprise synchronization engine supporting both live GCP
  BigQuery EXTERNAL_QUERY execution and zero-cost local DuckDB emulation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field

from dwh.bigquery.config import BigQueryConnectionConfig, BigQueryDatasetConfig
from dwh.bigquery.emulator import LocalBigQueryEmulator

logger = logging.getLogger(__name__)


class ReplicationMode(str, Enum):
    """Replication synchronization strategy."""
    INCREMENTAL_WATERMARK = "INCREMENTAL_WATERMARK"
    SNAPSHOT_REPLACE = "SNAPSHOT_REPLACE"


@dataclass
class TableReplicationSpec:
    """Specification for replicating a Cloud SQL table to BigQuery via federated query."""

    source_table: str
    target_table: str
    primary_keys: List[str]
    replication_mode: ReplicationMode
    watermark_column: Optional[str] = None
    date_partition_column: Optional[str] = None
    pushdown_columns: List[str] = field(default_factory=list)
    description: str = ""


# -----------------------------------------------------------------------------
# CANONICAL REPLICATION CATALOG
# -----------------------------------------------------------------------------

REPLICATION_RISK_ALERTS_SPEC = TableReplicationSpec(
    source_table="risk_alerts",
    target_table="fact_risk_alerts",
    primary_keys=["alert_id"],
    watermark_column="event_timestamp",
    date_partition_column="alert_date",
    replication_mode=ReplicationMode.INCREMENTAL_WATERMARK,
    pushdown_columns=[
        "alert_id::text AS alert_id",
        "portfolio_id",
        "ticker",
        "asset_class",
        "alert_type",
        "severity",
        "metric_name",
        "threshold_value::numeric AS threshold_value",
        "observed_value::numeric AS observed_value",
        "event_timestamp",
        "created_at",
        "status",
        "payload_json::text AS payload_json",
    ],
    description="Replicates streaming anomaly alerts from Cloud SQL hot store to BigQuery fact_risk_alerts.",
)

REPLICATION_PORTFOLIO_LIMITS_SPEC = TableReplicationSpec(
    source_table="portfolio_limits",
    target_table="dim_portfolios",
    primary_keys=["portfolio_id"],
    watermark_column="updated_at",
    replication_mode=ReplicationMode.SNAPSHOT_REPLACE,
    pushdown_columns=[
        "limit_id::text AS limit_id",
        "portfolio_id",
        "asset_class",
        "max_notional_usd::numeric AS max_notional_usd",
        "max_drawdown_pct::numeric AS max_drawdown_pct",
        "max_var_99_usd::numeric AS max_var_99_usd",
        "is_active",
        "updated_at",
    ],
    description="Replicates operational mandate limits from Cloud SQL to BigQuery dim_portfolios.",
)

REPLICATION_CATALOG: Dict[str, TableReplicationSpec] = {
    REPLICATION_RISK_ALERTS_SPEC.source_table: REPLICATION_RISK_ALERTS_SPEC,
    REPLICATION_PORTFOLIO_LIMITS_SPEC.source_table: REPLICATION_PORTFOLIO_LIMITS_SPEC,
}


class ReplicationConfig(BaseModel):
    """Configuration for Cloud SQL to BigQuery federated replication."""

    project_id: str = Field(default="trading-risk-dev-001", description="GCP project ID")
    dataset_id: str = Field(default="trading_risk_dwh_dev", description="Target BigQuery dataset")
    region: str = Field(default="us-central1", description="GCP region")
    connection_id: str = Field(
        default="conn_cloudsql_risk_db_dev", description="BigQuery external connection ID"
    )
    batch_size: int = Field(default=5000, description="Max batch size for pushdown queries")
    max_retries: int = Field(default=3, description="Maximum replication retry attempts")
    retry_backoff_sec: float = Field(default=1.0, description="Backoff multiplier in seconds")
    dry_run: bool = Field(default=False, description="Dry run mode (compile queries without execution)")

    @classmethod
    def from_env(cls) -> ReplicationConfig:
        """Instantiate configuration from environment variables."""
        env = os.getenv("ENVIRONMENT", "dev").lower()
        project_id = os.getenv("GCP_PROJECT_ID", f"trading-risk-{env}-001")
        dataset_id = os.getenv("BIGQUERY_DATASET_ID", f"trading_risk_dwh_{env}")
        region = os.getenv("GCP_REGION", "us-central1")
        conn_id = os.getenv("BIGQUERY_CONNECTION_ID", f"conn_cloudsql_risk_db_{env}")

        return cls(
            project_id=project_id,
            dataset_id=dataset_id,
            region=region,
            connection_id=conn_id,
            batch_size=int(os.getenv("REPLICATION_BATCH_SIZE", "5000")),
            max_retries=int(os.getenv("REPLICATION_MAX_RETRIES", "3")),
            retry_backoff_sec=float(os.getenv("REPLICATION_RETRY_BACKOFF_SEC", "1.0")),
            dry_run=os.getenv("REPLICATION_DRY_RUN", "false").lower() == "true",
        )

    def get_connection_path(self) -> str:
        """Return the fully qualified BigQuery external connection path."""
        return f"{self.project_id}.{self.region}.{self.connection_id}"


class ReplicationWatermarkStore:
    """Thread-safe state store for tracking high-watermark timestamps per replicated table."""

    def __init__(self) -> None:
        self._watermarks: Dict[str, datetime] = {}
        self._lock = threading.Lock()

    def get_watermark(self, table_name: str) -> Optional[datetime]:
        """Retrieve current high-watermark timestamp for a table."""
        with self._lock:
            return self._watermarks.get(table_name)

    def set_watermark(self, table_name: str, watermark: datetime) -> None:
        """Update high-watermark timestamp for a table."""
        with self._lock:
            self._watermarks[table_name] = watermark

    def reset(self, table_name: Optional[str] = None) -> None:
        """Reset watermark state for a table or all tables."""
        with self._lock:
            if table_name:
                self._watermarks.pop(table_name, None)
            else:
                self._watermarks.clear()

    def to_dict(self) -> Dict[str, str]:
        """Export watermarks as ISO 8601 string mapping."""
        with self._lock:
            return {tbl: ts.isoformat() for tbl, ts in self._watermarks.items()}


@dataclass
class ReplicationTelemetry:
    """Operational metrics store tracking replication jobs."""

    syncs_executed: int = 0
    syncs_failed: int = 0
    records_synced: Dict[str, int] = field(default_factory=dict)
    total_duration_ms: float = 0.0
    bytes_transferred: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_sync(
        self, table_name: str, count: int, duration_ms: float, success: bool, est_bytes: int = 0
    ) -> None:
        with self._lock:
            self.syncs_executed += 1
            if not success:
                self.syncs_failed += 1
            else:
                self.records_synced[table_name] = self.records_synced.get(table_name, 0) + count
            self.total_duration_ms += duration_ms
            self.bytes_transferred += est_bytes

    def format_prometheus(self) -> str:
        """Format metrics in standard Prometheus text exposition format."""
        lines = [
            "# HELP bigquery_replication_syncs_executed_total Total replication syncs executed",
            "# TYPE bigquery_replication_syncs_executed_total counter",
            f"bigquery_replication_syncs_executed_total {self.syncs_executed}",
            "# HELP bigquery_replication_syncs_failed_total Total failed replication syncs",
            "# TYPE bigquery_replication_syncs_failed_total counter",
            f"bigquery_replication_syncs_failed_total {self.syncs_failed}",
            "# HELP bigquery_replication_bytes_transferred_bytes Total estimated bytes replicated via federation",
            "# TYPE bigquery_replication_bytes_transferred_bytes counter",
            f"bigquery_replication_bytes_transferred_bytes {self.bytes_transferred}",
        ]
        for tbl, count in sorted(self.records_synced.items()):
            lines.append(f'bigquery_replication_records_synced_total{{table="{tbl}"}} {count}')
        return "\n".join(lines) + "\n"


class CloudSqlBigQueryReplicationRunner:
    """Enterprise replication orchestrator replicating Cloud SQL tables to BigQuery via EXTERNAL_QUERY."""

    def __init__(
        self,
        emulator: Optional[LocalBigQueryEmulator] = None,
        config: Optional[ReplicationConfig] = None,
        watermark_store: Optional[ReplicationWatermarkStore] = None,
    ) -> None:
        self.config = config or ReplicationConfig.from_env()
        self.emulator = emulator or LocalBigQueryEmulator(
            project_id=self.config.project_id,
            dataset_id=self.config.dataset_id,
        )
        self.watermark_store = watermark_store or ReplicationWatermarkStore()
        self.telemetry = ReplicationTelemetry()

    def build_pushdown_query(
        self,
        spec: TableReplicationSpec,
        since_watermark: Optional[datetime] = None,
        target_date: Optional[Union[date, str]] = None,
    ) -> str:
        """Construct the optimized SQL query to push down into Cloud SQL PostgreSQL."""
        col_list = ", ".join(spec.pushdown_columns) if spec.pushdown_columns else "*"
        where_clauses = []

        if since_watermark is not None and spec.watermark_column:
            wm_str = since_watermark.strftime("%Y-%m-%d %H:%M:%S")
            where_clauses.append(f"{spec.watermark_column} > '{wm_str}'")

        if target_date is not None and spec.watermark_column:
            t_date = str(target_date)
            where_clauses.append(f"DATE({spec.watermark_column}) = '{t_date}'")

        query = f"SELECT {col_list} FROM {spec.source_table}"
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        if spec.watermark_column:
            query += f" ORDER BY {spec.watermark_column} ASC"

        return query

    def build_merge_statement(
        self,
        spec: TableReplicationSpec,
        since_watermark: Optional[datetime] = None,
        target_date: Optional[Union[date, str]] = None,
    ) -> str:
        """Construct the idempotent BigQuery MERGE statement using EXTERNAL_QUERY."""
        pushdown = self.build_pushdown_query(
            spec, since_watermark=since_watermark, target_date=target_date
        )
        conn_path = self.config.get_connection_path()
        target_table = f"{self.config.project_id}.{self.config.dataset_id}.{spec.target_table}"

        if spec.source_table == "risk_alerts":
            # Target is fact_risk_alerts
            return f"""
MERGE INTO `{target_table}` T
USING (
    SELECT
        alert_id,
        CAST(event_timestamp AS DATE) AS alert_date,
        created_at AS detected_at,
        event_timestamp,
        CAST(EXTRACT(YEAR FROM event_timestamp) * 10000 + EXTRACT(MONTH FROM event_timestamp) * 100 + EXTRACT(DAY FROM event_timestamp) AS BIGINT) AS date_key,
        portfolio_id,
        ticker,
        asset_class,
        alert_type,
        severity,
        metric_name,
        threshold_value,
        observed_value,
        payload_json,
        CURRENT_TIMESTAMP AS _ingested_at
    FROM EXTERNAL_QUERY("{conn_path}", "{pushdown}")
) S
ON T.alert_id = S.alert_id
WHEN MATCHED THEN
    UPDATE SET
        observed_value = S.observed_value,
        threshold_value = S.threshold_value,
        severity = S.severity,
        payload_json = S.payload_json,
        _ingested_at = S._ingested_at
WHEN NOT MATCHED THEN
    INSERT (alert_id, alert_date, detected_at, event_timestamp, date_key, portfolio_id, ticker, asset_class, alert_type, severity, metric_name, threshold_value, observed_value, payload_json, _ingested_at)
    VALUES (S.alert_id, S.alert_date, S.detected_at, S.event_timestamp, S.date_key, S.portfolio_id, S.ticker, S.asset_class, S.alert_type, S.severity, S.metric_name, S.threshold_value, S.observed_value, S.payload_json, S._ingested_at);
""".strip()

        elif spec.source_table == "portfolio_limits":
            # Target is dim_portfolios
            return f"""
MERGE INTO `{target_table}` T
USING (
    SELECT
        limit_id,
        portfolio_id,
        asset_class,
        max_notional_usd,
        max_drawdown_pct,
        max_var_99_usd,
        is_active,
        updated_at
    FROM EXTERNAL_QUERY("{conn_path}", "{pushdown}")
) S
ON T.portfolio_id = S.portfolio_id
WHEN MATCHED THEN
    UPDATE SET
        risk_limit_usd = S.max_notional_usd,
        max_var_limit_usd = S.max_var_99_usd,
        daily_loss_limit_usd = ROUND(S.max_notional_usd * S.max_drawdown_pct, 2),
        is_active = S.is_active,
        updated_at = S.updated_at
WHEN NOT MATCHED THEN
    INSERT (portfolio_key, portfolio_id, portfolio_name, desk, portfolio_manager, risk_limit_usd, daily_loss_limit_usd, max_var_limit_usd, base_currency, strategy_type, is_active, created_at, updated_at)
    VALUES (S.portfolio_id, S.portfolio_id, S.portfolio_id, 'MULTI_ASSET', 'SYSTEM', S.max_notional_usd, ROUND(S.max_notional_usd * S.max_drawdown_pct, 2), S.max_var_99_usd, 'USD', 'STAT_ARB', S.is_active, S.updated_at, S.updated_at);
""".strip()


        else:
            raise ValueError(f"No MERGE template defined for source table '{spec.source_table}'")

    def replicate_table(
        self,
        table_name: str,
        target_date: Optional[Union[date, str]] = None,
        full_refresh: bool = False,
    ) -> Dict[str, Any]:
        """Execute replication for a single table with watermark management and error handling."""
        if table_name not in REPLICATION_CATALOG:
            raise KeyError(
                f"Unknown replication table '{table_name}'. Catalog contains: {list(REPLICATION_CATALOG.keys())}"
            )

        spec = REPLICATION_CATALOG[table_name]
        since_wm = None
        if not full_refresh and spec.replication_mode == ReplicationMode.INCREMENTAL_WATERMARK:
            since_wm = self.watermark_store.get_watermark(table_name)

        t0 = time.perf_counter()
        merge_sql = self.build_merge_statement(
            spec, since_watermark=since_wm, target_date=target_date
        )

        if self.config.dry_run:
            logger.info("DRY-RUN: Replicate table %s:\n%s", table_name, merge_sql)
            return {
                "table": table_name,
                "status": "DRY_RUN",
                "rows_synced": 0,
                "duration_ms": 0.0,
                "query": merge_sql,
            }

        last_error = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                # Ensure target table exists in emulator
                self.emulator.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.dataset_id};")

                # Get row count before merge
                count_before = self.emulator.get_table_row_count(spec.target_table)

                # Execute merge query
                self.emulator.execute_query(merge_sql, bypass_partition_filter=True)

                # Get row count after merge
                count_after = self.emulator.get_table_row_count(spec.target_table)
                rows_synced = max(0, count_after - count_before)

                # Check max watermark from source to update watermark store
                if spec.watermark_column:
                    max_wm_sql = f"SELECT MAX({spec.watermark_column}) AS max_wm FROM {self.config.dataset_id}.{spec.source_table}"
                    if since_wm:
                        max_wm_sql += f" WHERE {spec.watermark_column} > '{since_wm.strftime('%Y-%m-%d %H:%M:%S')}'"
                    wm_res = self.emulator.execute_query(max_wm_sql, bypass_partition_filter=True)
                    if wm_res and wm_res[0].get("max_wm") is not None:
                        val = wm_res[0]["max_wm"]
                        if isinstance(val, str):
                            new_wm = datetime.fromisoformat(val.replace("Z", "+00:00"))
                        elif isinstance(val, datetime):
                            new_wm = val
                        else:
                            new_wm = datetime.now(timezone.utc)
                        self.watermark_store.set_watermark(table_name, new_wm)

                dur_ms = (time.perf_counter() - t0) * 1000.0
                est_bytes = rows_synced * 256
                self.telemetry.record_sync(table_name, rows_synced, dur_ms, success=True, est_bytes=est_bytes)

                return {
                    "table": table_name,
                    "status": "SUCCESS",
                    "rows_synced": rows_synced,
                    "target_table": spec.target_table,
                    "total_target_rows": count_after,
                    "duration_ms": dur_ms,
                    "watermark": self.watermark_store.get_watermark(table_name),
                }
            except Exception as e:
                last_error = e
                logger.warning(
                    "Replication attempt %d failed for table %s: %s", attempt, table_name, e
                )
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_backoff_sec * (2 ** (attempt - 1)))

        dur_ms = (time.perf_counter() - t0) * 1000.0
        self.telemetry.record_sync(table_name, 0, dur_ms, success=False)
        raise RuntimeError(
            f"Replication failed for table '{table_name}' after {self.config.max_retries} attempts: {last_error}"
        ) from last_error

    def replicate_all(
        self,
        target_date: Optional[Union[date, str]] = None,
        full_refresh: bool = False,
    ) -> Dict[str, Any]:
        """Synchronize all tables in the replication catalog in dependency order."""
        t0 = time.perf_counter()
        results = {}
        # Order: portfolio_limits first (reference dimension), then risk_alerts (fact table)
        execution_order = ["portfolio_limits", "risk_alerts"]
        for tbl in execution_order:
            results[tbl] = self.replicate_table(
                tbl, target_date=target_date, full_refresh=full_refresh
            )

        total_rows = sum(r.get("rows_synced", 0) for r in results.values())
        total_duration = (time.perf_counter() - t0) * 1000.0

        return {
            "status": "SUCCESS",
            "tables": results,
            "total_rows_synced": total_rows,
            "total_duration_ms": total_duration,
        }

    def seed_mock_cloudsql_data(
        self, alerts_count: int = 25, limits_count: int = 3
    ) -> Dict[str, int]:
        """Convenience method to seed operational tables for testing."""
        self.emulator.initialize_cloudsql_tables()
        return self.emulator.seed_cloudsql_tables(alerts_count=alerts_count)
