"""CLI Tool for Cloud SQL to BigQuery Federated Replication (Day 25).

Usage:
    python -m dwh.bigquery.replication_cli --help
    python -m dwh.bigquery.replication_cli --sync-all --seed-mock-data
    python -m dwh.bigquery.replication_cli --table risk_alerts --dry-run
    python -m dwh.bigquery.replication_cli --sync-all --export-prometheus
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
import sys
from typing import Any, Dict, List, Optional

from dwh.bigquery.emulator import LocalBigQueryEmulator
from dwh.bigquery.replication import (
    CloudSqlBigQueryReplicationRunner,
    REPLICATION_CATALOG,
    ReplicationConfig,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("dwh.bigquery.replication_cli")


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line interface argument parser."""
    parser = argparse.ArgumentParser(
        description="Synchronize operational Cloud SQL PostgreSQL data to BigQuery via federated queries."
    )
    parser.add_argument(
        "--sync-all",
        action="store_true",
        help="Replicate all tables in the replication catalog in dependency order.",
    )
    parser.add_argument(
        "--table",
        choices=["risk_alerts", "portfolio_limits", "all"],
        default=None,
        help="Specific table to replicate (risk_alerts, portfolio_limits, or all).",
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        type=str,
        default=None,
        help="Target partition date to synchronize (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Perform full table refresh, bypassing incremental watermark check.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and validate SQL MERGE queries without executing against database.",
    )
    parser.add_argument(
        "--seed-mock-data",
        action="store_true",
        help="Seed operational Cloud SQL tables with realistic mock data before replication.",
    )
    parser.add_argument(
        "--json-report",
        action="store_true",
        help="Output structured JSON replication execution report.",
    )
    parser.add_argument(
        "--export-prometheus",
        action="store_true",
        help="Render operational Prometheus telemetry metrics after replication.",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        default="trading-risk-dev-001",
        help="Google Cloud Project ID.",
    )
    parser.add_argument(
        "--dataset-id",
        type=str,
        default="trading_risk_dwh_dev",
        help="Target BigQuery analytical dataset.",
    )
    parser.add_argument(
        "--connection-id",
        type=str,
        default="conn_cloudsql_risk_db_dev",
        help="BigQuery external connection ID for Cloud SQL.",
    )
    return parser


def run_replication_cli(args: Optional[List[str]] = None) -> int:
    """Execute the replication CLI runner."""
    parser = build_arg_parser()
    opts = parser.parse_args(args)

    config = ReplicationConfig(
        project_id=opts.project_id,
        dataset_id=opts.dataset_id,
        connection_id=opts.connection_id,
        dry_run=opts.dry_run,
    )

    emulator = LocalBigQueryEmulator(
        project_id=opts.project_id,
        dataset_id=opts.dataset_id,
    )
    emulator.initialize_star_schema()
    emulator.load_seed_dimensions()
    emulator.initialize_cloudsql_tables()

    runner = CloudSqlBigQueryReplicationRunner(emulator=emulator, config=config)

    if opts.seed_mock_data:
        counts = runner.seed_mock_cloudsql_data()
        logger.info("Seeded mock Cloud SQL operational data: %s", counts)

    target_table = opts.table or ("all" if opts.sync_all else None)
    if not target_table and not opts.export_prometheus and not opts.dry_run:
        target_table = "all"

    results: Dict[str, Any] = {}
    success = True

    try:
        if target_table in ("all", None):
            res = runner.replicate_all(
                target_date=opts.target_date, full_refresh=opts.full_refresh
            )
            results = res
        elif target_table in REPLICATION_CATALOG:
            res = runner.replicate_table(
                target_table,
                target_date=opts.target_date,
                full_refresh=opts.full_refresh,
            )
            results = {
                "status": res.get("status", "SUCCESS"),
                "tables": {target_table: res},
                "total_rows_synced": res.get("rows_synced", 0),
                "total_duration_ms": res.get("duration_ms", 0.0),
            }
        else:
            logger.error("Unknown table '%s'", target_table)
            return 1
    except Exception as e:
        logger.error("Replication failed: %s", e)
        results = {"status": "FAILED", "error": str(e)}
        success = False

    if opts.json_report:
        print(json.dumps(results, indent=2, default=str))
    else:
        logger.info(
            "Replication execution complete: status=%s, total_rows_synced=%s",
            results.get("status"),
            results.get("total_rows_synced", 0),
        )

    if opts.export_prometheus:
        prom = runner.telemetry.format_prometheus()
        print("\n# --- Prometheus Telemetry Exposition ---")
        print(prom)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(run_replication_cli())
