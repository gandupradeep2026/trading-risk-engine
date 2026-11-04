"""Local BigQuery Emulator & DuckDB Execution Engine (Day 22).

Provides zero-cost, hermetic local emulation of Google Cloud BigQuery using DuckDB.
Executes BigQuery GoogleSQL DDL, manages partitioned and clustered star-schema tables,
enforces partition filter requirements (require_partition_filter = TRUE), and supports
full star-schema analytical queries and telemetry tracking.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import decimal
import json
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import duckdb
import pyarrow as pa

from dwh.bigquery.config import (
    FEDERATED_VIEWS_CATALOG,
    MATERIALIZED_VIEWS_CATALOG,
    STAR_SCHEMA_CATALOG,
    TablePartitionClusteringSpec,
)
from dwh.bigquery.schema_manager import BigQuerySchemaManager
from dwh.bigquery.seed_data import (
    get_dim_date_records,
    get_dim_portfolios_records,
    get_dim_tickers_records,
    get_dim_traders_records,
)


class BigQueryEmulatorError(Exception):
    """Base exception for BigQuery emulation errors."""
    pass


class BigQueryPartitionFilterRequiredError(BigQueryEmulatorError):
    """Raised when a query against a partitioned table omits the mandatory partition filter."""
    pass


@dataclass
class BigQueryTelemetry:
    """Operational telemetry store tracking simulated BigQuery activity."""

    queries_executed: int = 0
    queries_failed: int = 0
    partition_filter_violations: int = 0
    rows_inserted: Dict[str, int] = field(default_factory=dict)
    simulated_bytes_scanned: int = 0
    total_execution_time_ms: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_query(self, success: bool, duration_ms: float, bytes_scanned: int = 0) -> None:
        with self._lock:
            self.queries_executed += 1
            if not success:
                self.queries_failed += 1
            self.total_execution_time_ms += duration_ms
            self.simulated_bytes_scanned += bytes_scanned

    def record_insert(self, table_name: str, count: int) -> None:
        with self._lock:
            self.rows_inserted[table_name] = self.rows_inserted.get(table_name, 0) + count

    def record_filter_violation(self) -> None:
        with self._lock:
            self.partition_filter_violations += 1

    def format_prometheus(self) -> str:
        """Render operational metrics in standard Prometheus text exposition format."""
        lines = [
            "# HELP bigquery_queries_executed_total Total BigQuery queries executed",
            "# TYPE bigquery_queries_executed_total counter",
            f"bigquery_queries_executed_total {self.queries_executed}",
            "# HELP bigquery_queries_failed_total Total failed BigQuery queries",
            "# TYPE bigquery_queries_failed_total counter",
            f"bigquery_queries_failed_total {self.queries_failed}",
            "# HELP bigquery_partition_filter_violations_total Total partition filter rejections",
            "# TYPE bigquery_partition_filter_violations_total counter",
            f"bigquery_partition_filter_violations_total {self.partition_filter_violations}",
            "# HELP bigquery_simulated_bytes_scanned_bytes Total simulated bytes scanned",
            "# TYPE bigquery_simulated_bytes_scanned_bytes counter",
            f"bigquery_simulated_bytes_scanned_bytes {self.simulated_bytes_scanned}",
        ]
        for tbl, count in sorted(self.rows_inserted.items()):
            lines.append(f'bigquery_rows_inserted_total{{table="{tbl}"}} {count}')
        return "\n".join(lines) + "\n"


def _strip_options_clauses(text: str) -> str:
    """Strip BigQuery OPTIONS(...) clauses taking nested parentheses and string literals into account."""
    result = []
    i = 0
    n = len(text)
    while i < n:
        if text[i:i+7].upper() == "OPTIONS" and (i == 0 or not (text[i-1].isalnum() or text[i-1] == '_')):
            j = i + 7
            while j < n and text[j].isspace():
                j += 1
            if j < n and text[j] == '(':
                depth = 1
                in_quote = None
                k = j + 1
                while k < n and depth > 0:
                    ch = text[k]
                    if in_quote:
                        if ch == in_quote and (k == 0 or text[k-1] != '\\'):
                            in_quote = None
                    else:
                        if ch in ('"', "'"):
                            in_quote = ch
                        elif ch == '(':
                            depth += 1
                        elif ch == ')':
                            depth -= 1
                    k += 1
                i = k
                continue
        result.append(text[i])
        i += 1
    return "".join(result)


def _translate_external_query(sql: str) -> str:
    """Translate GoogleSQL EXTERNAL_QUERY("connection", "subquery") to standard SQL subquery."""
    pattern = r'EXTERNAL_QUERY\s*\(\s*[^,]+\s*,\s*(?:"""([\s\S]*?)"""|\'\'\'([\s\S]*?)\'\'\'|"([^"]*)"|\'([^\']*)\')\s*\)'

    def _replace_match(m: re.Match) -> str:
        inner_sql = m.group(1) or m.group(2) or m.group(3) or m.group(4) or ""
        # Auto alias any col::type that does not have an AS alias
        inner_sql = re.sub(r'(\b\w+)::(?:text|varchar)\b(?!\s+AS\b)', r'\1::VARCHAR AS \1', inner_sql, flags=re.IGNORECASE)
        inner_sql = re.sub(r'(\b\w+)::numeric\b(?!\s+AS\b)', r'\1::DECIMAL(18, 4) AS \1', inner_sql, flags=re.IGNORECASE)
        inner_sql = re.sub(r'(\b\w+)::jsonb\b(?!\s+AS\b)', r'\1::VARCHAR AS \1', inner_sql, flags=re.IGNORECASE)
        # Any remaining casts with AS alias
        inner_sql = re.sub(r"::text\b", "::VARCHAR", inner_sql, flags=re.IGNORECASE)
        inner_sql = re.sub(r"::numeric\b", "::DECIMAL(18, 4)", inner_sql, flags=re.IGNORECASE)
        inner_sql = re.sub(r"::jsonb\b", "::VARCHAR", inner_sql, flags=re.IGNORECASE)
        return f"({inner_sql.strip()})"

    return re.sub(pattern, _replace_match, sql, flags=re.IGNORECASE)



class LocalBigQueryEmulator:
    """In-memory DuckDB-backed Google Cloud BigQuery emulator."""

    def __init__(
        self,
        project_id: str = "trading-risk-dev-001",
        dataset_id: str = "trading_risk_dwh_dev",
        database_path: str = ":memory:",
    ) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.database_path = database_path
        self.connection = duckdb.connect(database_path)
        self.schema_manager = BigQuerySchemaManager()
        self.telemetry = BigQueryTelemetry()
        self.table_specs: Dict[str, TablePartitionClusteringSpec] = dict(STAR_SCHEMA_CATALOG)
        self.table_specs.update(MATERIALIZED_VIEWS_CATALOG)
        self.table_specs.update(FEDERATED_VIEWS_CATALOG)
        self._lock = threading.Lock()

        # Initialize dataset schema in DuckDB
        self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self.dataset_id};")
        self.connection.execute(f"SET schema = '{self.dataset_id}';")

        # Initialize Cloud SQL operational tables for federated query emulation
        self.initialize_cloudsql_tables()


    def translate_bigquery_ddl_to_duckdb(self, bq_ddl: str) -> str:
        """Translate GoogleSQL BigQuery DDL statement into DuckDB compatible SQL."""
        # Clean comments
        sql = re.sub(r"--.*", "", bq_ddl)

        # Replace project and dataset references
        sql = sql.replace("${project_id}", self.project_id)
        sql = sql.replace("${dataset_id}", self.dataset_id)
        sql = sql.replace(f"`{self.project_id}.{self.dataset_id}.", f"{self.dataset_id}.")
        sql = sql.replace(f"`{self.project_id}.{self.dataset_id}", f"{self.dataset_id}")
        sql = sql.replace("`", "")

        # Strip all OPTIONS clauses with nested parenthesis support
        sql = _strip_options_clauses(sql)

        # Truncate at last closing parenthesis of column definitions and terminate with semicolon
        last_paren = sql.rfind(")")
        if last_paren != -1:
            sql = sql[:last_paren + 1] + ";"

        # Translate BigQuery data types to DuckDB
        type_mappings = [
            (r"\bSTRING\b", "VARCHAR"),
            (r"\bINT64\b", "BIGINT"),
            (r"\bFLOAT64\b", "DOUBLE"),
            (r"\bNUMERIC\b", "DECIMAL(18, 4)"),
            (r"\bBIGNUMERIC\b", "DECIMAL(38, 9)"),
            (r"\bBOOL\b", "BOOLEAN"),
            (r"\bTIMESTAMP\b", "TIMESTAMPTZ"),
            (r"\bJSON\b", "VARCHAR"),
        ]
        for pattern, replacement in type_mappings:
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

        return sql.strip()

    def create_table_from_ddl(self, table_name: str) -> None:
        """Create a table in DuckDB using the rendered BigQuery DDL script."""
        raw_ddl = self.schema_manager.load_raw_ddl(table_name)
        duck_sql = self.translate_bigquery_ddl_to_duckdb(raw_ddl)
        with self._lock:
            self.connection.execute(duck_sql)

    def initialize_star_schema(self) -> None:
        """Deploy all 8 star schema tables in dependency order."""
        creation_order = [
            "dim_date",
            "dim_tickers",
            "dim_portfolios",
            "dim_traders",
            "fact_trade_orders",
            "fact_market_ticks",
            "fact_risk_stats",
            "fact_risk_alerts",
        ]
        for tbl in creation_order:
            self.create_table_from_ddl(tbl)

    def translate_materialized_view_ddl_to_duckdb(self, bq_ddl: str) -> str:
        """Translate GoogleSQL BigQuery Materialized View DDL into DuckDB compatible SQL."""
        # Clean comments
        sql = re.sub(r"--.*", "", bq_ddl)

        # Replace project and dataset references
        sql = sql.replace("${project_id}", self.project_id)
        sql = sql.replace("${dataset_id}", self.dataset_id)
        sql = sql.replace(f"`{self.project_id}.{self.dataset_id}.", f"{self.dataset_id}.")
        sql = sql.replace(f"`{self.project_id}.{self.dataset_id}", f"{self.dataset_id}")
        sql = sql.replace("`", "")

        # Strip all OPTIONS clauses
        sql = _strip_options_clauses(sql)

        # Strip CLUSTER BY clause: CLUSTER BY col1, col2, ...
        sql = re.sub(r"\bCLUSTER\s+BY\s+[a-zA-Z0-9_,\s]+(?=\bOPTIONS\b|\bAS\b)", "", sql, flags=re.IGNORECASE)

        # Strip PARTITION BY clause if present
        sql = re.sub(r"\bPARTITION\s+BY\s+[a-zA-Z0-9_,\s]+(?=\bCLUSTER\b|\bOPTIONS\b|\bAS\b)", "", sql, flags=re.IGNORECASE)

        # Replace CREATE MATERIALIZED VIEW [IF NOT EXISTS] with CREATE OR REPLACE VIEW
        sql = re.sub(
            r"CREATE\s+MATERIALIZED\s+VIEW(?:\s+IF\s+NOT\s+EXISTS)?\s+",
            "CREATE OR REPLACE VIEW ",
            sql,
            flags=re.IGNORECASE,
        )

        return sql.strip()

    def create_materialized_view_from_ddl(self, mv_name: str) -> None:
        """Create a materialized view in DuckDB from rendered GoogleSQL DDL."""
        raw_ddl = self.schema_manager.load_raw_ddl(mv_name)
        duck_sql = self.translate_materialized_view_ddl_to_duckdb(raw_ddl)
        with self._lock:
            self.connection.execute(duck_sql)

    def initialize_materialized_views(self) -> None:
        """Deploy all 3 BigQuery materialized views."""
        views = [
            "mv_realtime_market_performance",
            "mv_realtime_trader_exposure",
            "mv_realtime_risk_alerts",
        ]
        for mv in views:
            self.create_materialized_view_from_ddl(mv)

    def initialize_cloudsql_tables(self) -> None:
        """Initialize operational Cloud SQL PostgreSQL tables in DuckDB for zero-cost federated queries."""
        ddl_risk_alerts = f"""
        CREATE TABLE IF NOT EXISTS {self.dataset_id}.risk_alerts (
            alert_id VARCHAR PRIMARY KEY,
            portfolio_id VARCHAR NOT NULL,
            ticker VARCHAR NOT NULL,
            asset_class VARCHAR NOT NULL,
            alert_type VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,
            metric_name VARCHAR NOT NULL,
            threshold_value DECIMAL(18, 6) NOT NULL,
            observed_value DECIMAL(18, 6) NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'ACTIVE',
            payload_json VARCHAR DEFAULT '{{}}'
        );
        """
        ddl_portfolio_limits = f"""
        CREATE TABLE IF NOT EXISTS {self.dataset_id}.portfolio_limits (
            limit_id VARCHAR PRIMARY KEY,
            portfolio_id VARCHAR NOT NULL UNIQUE,
            asset_class VARCHAR NOT NULL,
            max_notional_usd DECIMAL(18, 2) NOT NULL,
            max_drawdown_pct DECIMAL(6, 4) NOT NULL,
            max_var_99_usd DECIMAL(18, 2) NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at TIMESTAMPTZ NOT NULL
        );
        """
        with self._lock:
            self.connection.execute(ddl_risk_alerts)
            self.connection.execute(ddl_portfolio_limits)

    def seed_cloudsql_tables(
        self,
        risk_alerts: Optional[List[Dict[str, Any]]] = None,
        portfolio_limits: Optional[List[Dict[str, Any]]] = None,
        alerts_count: int = 25,
    ) -> Dict[str, int]:
        """Seed Cloud SQL operational tables with sample records for federated query execution."""
        counts = {}
        if risk_alerts is not None:
            self.insert_rows("risk_alerts", risk_alerts)
            counts["risk_alerts"] = len(risk_alerts)
        else:
            from dwh.bigquery.seed_data import generate_mock_risk_alerts
            sample_alerts = generate_mock_risk_alerts(count=alerts_count)
            csql_alerts = []
            for a in sample_alerts:
                csql_alerts.append({
                    "alert_id": a["alert_id"],
                    "portfolio_id": a["portfolio_id"],
                    "ticker": a.get("ticker", "AAPL"),
                    "asset_class": a.get("asset_class", "EQUITY"),
                    "alert_type": a["alert_type"],
                    "severity": a["severity"],
                    "metric_name": a["metric_name"],
                    "threshold_value": a["threshold_value"],
                    "observed_value": a["observed_value"],
                    "event_timestamp": a["event_timestamp"],
                    "created_at": a.get("detected_at", a["event_timestamp"]),
                    "status": "ACTIVE",
                    "payload_json": a.get("payload_json", "{}"),
                })
            self.insert_rows("risk_alerts", csql_alerts)
            counts["risk_alerts"] = len(csql_alerts)

        if portfolio_limits is not None:
            self.insert_rows("portfolio_limits", portfolio_limits)
            counts["portfolio_limits"] = len(portfolio_limits)
        else:
            now_iso = datetime.now(timezone.utc).isoformat()
            limits = [
                {
                    "limit_id": "lim-001",
                    "portfolio_id": "PORTFOLIO_ALPHA",
                    "asset_class": "EQUITY",
                    "max_notional_usd": 10000000.0,
                    "max_drawdown_pct": 0.05,
                    "max_var_99_usd": 250000.0,
                    "is_active": True,
                    "updated_at": now_iso,
                },
                {
                    "limit_id": "lim-002",
                    "portfolio_id": "PORTFOLIO_BETA",
                    "asset_class": "FX",
                    "max_notional_usd": 25000000.0,
                    "max_drawdown_pct": 0.03,
                    "max_var_99_usd": 400000.0,
                    "is_active": True,
                    "updated_at": now_iso,
                },
                {
                    "limit_id": "lim-003",
                    "portfolio_id": "PORTFOLIO_GAMMA",
                    "asset_class": "CRYPTO",
                    "max_notional_usd": 5000000.0,
                    "max_drawdown_pct": 0.10,
                    "max_var_99_usd": 500000.0,
                    "is_active": True,
                    "updated_at": now_iso,
                },
            ]
            self.insert_rows("portfolio_limits", limits)
            counts["portfolio_limits"] = len(limits)

        return counts

    def translate_federated_view_ddl_to_duckdb(
        self,
        bq_ddl: str,
        region: str = "us-central1",
        connection_id: str = "conn_cloudsql_risk_db_dev",
    ) -> str:
        """Translate GoogleSQL federated view DDL using EXTERNAL_QUERY into DuckDB view."""
        sql = re.sub(r"--.*", "", bq_ddl)
        sql = sql.replace("${project_id}", self.project_id)
        sql = sql.replace("${dataset_id}", self.dataset_id)
        sql = sql.replace("${region}", region)
        sql = sql.replace("${connection_id}", connection_id)
        sql = sql.replace(f"`{self.project_id}.{self.dataset_id}.", f"{self.dataset_id}.")
        sql = sql.replace(f"`{self.project_id}.{self.dataset_id}", f"{self.dataset_id}")
        sql = sql.replace("`", "")

        # Strip OPTIONS(...)
        sql = _strip_options_clauses(sql)

        # Translate EXTERNAL_QUERY
        sql = _translate_external_query(sql)

        # Replace CREATE VIEW with CREATE OR REPLACE VIEW
        sql = re.sub(r"CREATE\s+VIEW(?:\s+IF\s+NOT\s+EXISTS)?\s+", "CREATE OR REPLACE VIEW ", sql, flags=re.IGNORECASE)
        sql = re.sub(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+", "CREATE OR REPLACE VIEW ", sql, flags=re.IGNORECASE)

        return sql.strip()

    def create_federated_view_from_ddl(
        self,
        view_name: str,
        region: str = "us-central1",
        connection_id: str = "conn_cloudsql_risk_db_dev",
    ) -> None:
        """Deploy a federated view in DuckDB from rendered GoogleSQL DDL."""
        raw_ddl = self.schema_manager.load_raw_ddl(view_name)
        duck_sql = self.translate_federated_view_ddl_to_duckdb(
            raw_ddl, region=region, connection_id=connection_id
        )
        with self._lock:
            self.connection.execute(duck_sql)

    def initialize_federated_views(
        self,
        region: str = "us-central1",
        connection_id: str = "conn_cloudsql_risk_db_dev",
    ) -> None:
        """Deploy all BigQuery federated views over Cloud SQL."""
        for v in ["v_federated_risk_alerts", "v_federated_portfolio_limits"]:
            self.create_federated_view_from_ddl(v, region=region, connection_id=connection_id)


    def load_seed_dimensions(
        self,
        date_start: date = date(2026, 1, 1),
        date_end: date = date(2026, 12, 31),
    ) -> Dict[str, int]:
        """Load deterministic seed data for all dimension tables."""
        counts = {}
        # 1. dim_date
        date_rows = get_dim_date_records(date_start, date_end)
        self.insert_rows("dim_date", date_rows)
        counts["dim_date"] = len(date_rows)

        # 2. dim_tickers
        ticker_rows = get_dim_tickers_records()
        self.insert_rows("dim_tickers", ticker_rows)
        counts["dim_tickers"] = len(ticker_rows)

        # 3. dim_portfolios
        portfolio_rows = get_dim_portfolios_records()
        self.insert_rows("dim_portfolios", portfolio_rows)
        counts["dim_portfolios"] = len(portfolio_rows)

        # 4. dim_traders
        trader_rows = get_dim_traders_records()
        self.insert_rows("dim_traders", trader_rows)
        counts["dim_traders"] = len(trader_rows)

        return counts

    def insert_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Insert a batch of dictionary records into the specified table."""
        if not rows:
            return 0

        target_table = f"{self.dataset_id}.{table_name}"
        cols = list(rows[0].keys())
        col_list = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))
        insert_sql = f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders});"

        tuples = []
        for r in rows:
            row_vals = []
            for c in cols:
                val = r.get(c)
                if isinstance(val, (dict, list)):
                    val = json.dumps(val)
                row_vals.append(val)
            tuples.append(tuple(row_vals))

        with self._lock:
            self.connection.executemany(insert_sql, tuples)
        self.telemetry.record_insert(table_name, len(rows))
        return len(rows)

    def validate_partition_filter(self, query: str) -> None:
        """Simulate BigQuery's require_partition_filter constraint on partitioned fact tables."""
        clean_q = re.sub(r"--.*", "", query)

        for tbl_name, spec in self.table_specs.items():
            if spec.require_partition_filter and spec.partition_column:
                # Check if the query references this table
                pattern = rf"\b(?:{self.dataset_id}\.)?{tbl_name}\b"
                if re.search(pattern, clean_q, re.IGNORECASE):
                    # Table is referenced. Must contain WHERE and the partition_column
                    pcol = spec.partition_column
                    where_match = re.search(r"\bWHERE\b(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|;|\Z)", clean_q, re.DOTALL | re.IGNORECASE)
                    if not where_match:
                        self.telemetry.record_filter_violation()
                        raise BigQueryPartitionFilterRequiredError(
                            f"Cannot query table '{tbl_name}' without a filter over partition column '{pcol}' "
                            f"(require_partition_filter = TRUE)."
                        )
                    where_clause = where_match.group(1)
                    if not re.search(rf"\b{pcol}\b", where_clause, re.IGNORECASE):
                        self.telemetry.record_filter_violation()
                        raise BigQueryPartitionFilterRequiredError(
                            f"Cannot query table '{tbl_name}' without a filter over partition column '{pcol}' "
                            f"in WHERE clause: '{where_clause.strip()}' (require_partition_filter = TRUE)."
                        )

    def execute_query(
        self,
        query: str,
        parameters: Optional[List[Any]] = None,
        bypass_partition_filter: bool = False,
    ) -> List[Dict[str, Any]]:
        """Execute query locally in DuckDB with BigQuery emulation rules."""
        if not bypass_partition_filter:
            self.validate_partition_filter(query)

        # Normalize project.dataset and dataset prefixes
        clean_q = query.replace(f"`{self.project_id}.{self.dataset_id}.", f"{self.dataset_id}.")
        clean_q = clean_q.replace(f"{self.project_id}.{self.dataset_id}.", f"{self.dataset_id}.")
        clean_q = clean_q.replace("`", "")
        clean_q = _translate_external_query(clean_q)

        t0 = time.perf_counter()
        success = False
        try:
            with self._lock:
                if parameters:
                    rel = self.connection.execute(clean_q, parameters)
                else:
                    rel = self.connection.execute(clean_q)
                if rel.description:
                    columns = [desc[0] for desc in rel.description]
                    rows = rel.fetchall()
                else:
                    columns = []
                    rows = []
            success = True
            result = [dict(zip(columns, r)) for r in rows]
            # Simulated byte scan estimation
            simulated_bytes = len(rows) * len(columns) * 32
            return result
        finally:
            dur_ms = (time.perf_counter() - t0) * 1000.0
            self.telemetry.record_query(success, dur_ms, simulated_bytes if success else 0)

    def execute_query_arrow(
        self, query: str, bypass_partition_filter: bool = False
    ) -> pa.Table:
        """Execute query and return results as an Apache PyArrow Table."""
        if not bypass_partition_filter:
            self.validate_partition_filter(query)

        clean_q = query.replace(f"`{self.project_id}.{self.dataset_id}.", f"{self.dataset_id}.")
        clean_q = clean_q.replace(f"{self.project_id}.{self.dataset_id}.", f"{self.dataset_id}.")
        clean_q = clean_q.replace("`", "")
        clean_q = _translate_external_query(clean_q)


        t0 = time.perf_counter()
        success = False
        arrow_table: Optional[pa.Table] = None
        try:
            with self._lock:
                arrow_table = self.connection.sql(clean_q).to_arrow_table()
            success = True
            return arrow_table
        finally:
            dur_ms = (time.perf_counter() - t0) * 1000.0
            bytes_scanned = arrow_table.nbytes if (success and arrow_table is not None) else 0
            self.telemetry.record_query(success, dur_ms, bytes_scanned)

    def get_table_row_count(self, table_name: str) -> int:
        """Return the current row count of a specified table."""
        target_table = f"{self.dataset_id}.{table_name}"
        res = self.execute_query(
            f"SELECT COUNT(*) AS cnt FROM {target_table};",
            bypass_partition_filter=True,
        )
        return int(res[0]["cnt"])

    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Retrieve column names and data types from DuckDB information schema."""
        target_table = table_name
        res = self.execute_query(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{self.dataset_id}' AND table_name = '{target_table}';",
            bypass_partition_filter=True,
        )
        return res

    def close(self) -> None:
        """Close database connection handle."""
        self.connection.close()
