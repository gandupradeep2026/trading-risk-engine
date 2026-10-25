"""Databricks Production Notebook / Task Runner for Delta Silver Cleansing.

Can be run:
1. Inside Databricks cluster as a notebook / python task extracting parameters
   via `dbutils.widgets` or `sys.argv`.
2. Locally or in CI workflows as a standalone Python CLI script.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from batch.databricks.config import (
    DatabricksWorkspaceConfig,
    SILVER_MARKET_TICKS_CONFIG,
    SILVER_ROLLING_VWAP_CONFIG,
    SILVER_TRADE_ORDERS_CONFIG,
)
from batch.databricks.silver_cleansing import (
    DeltaSilverCleansingEngine,
    SilverCleansingResult,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("silver_cleansing_runner")


def get_parameter(param_name: str, default_val: str = "") -> str:
    """Extract parameter from dbutils.widgets if running in Databricks, else environment or default."""
    try:
        import IPython  # type: ignore
        dbutils = IPython.get_ipython().user_ns.get("dbutils")  # type: ignore
        if dbutils:
            return str(dbutils.widgets.get(param_name))
    except Exception:
        pass
    env_key = param_name.upper().replace("-", "_")
    return os.getenv(env_key, default_val)


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line flags with Databricks widget fallback."""
    parser = argparse.ArgumentParser(
        description="Run Databricks Delta Lake Silver Cleansing & Deduplication for Trading Risk Engine."
    )
    parser.add_argument(
        "--environment",
        type=str,
        default=get_parameter("environment", "dev"),
        help="Deployment environment (dev, staging, prod)",
    )
    parser.add_argument(
        "--source-type",
        type=str,
        default=get_parameter("source_type", "market_ticks"),
        choices=["market_ticks", "trade_orders", "rolling_vwap", "all"],
        help="Source type to cleanse into Silver Delta",
    )
    parser.add_argument(
        "--ingestion-date",
        type=str,
        default=get_parameter("ingestion_date", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
        help="Target ingestion date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--source-path",
        type=str,
        default=get_parameter("source_path", ""),
        help="Override raw data input path or Bronze directory",
    )
    parser.add_argument(
        "--target-path",
        type=str,
        default=get_parameter("target_path", ""),
        help="Override target Delta table path or directory",
    )
    parser.add_argument(
        "--gcs-silver-bucket",
        type=str,
        default=get_parameter("gcs_silver_bucket", ""),
        help="GCS Silver bucket name",
    )
    parser.add_argument(
        "--run-optimize",
        type=lambda v: str(v).lower() in {"true", "1", "yes"},
        default=get_parameter("run_optimize", "true").lower() in {"true", "1", "yes"},
        help="Execute Delta OPTIMIZE and Z-Ordering after cleansing",
    )
    parser.add_argument(
        "--run-vacuum",
        type=lambda v: str(v).lower() in {"true", "1", "yes"},
        default=get_parameter("run_vacuum", "true").lower() in {"true", "1", "yes"},
        help="Execute Delta VACUUM retention pruning",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=get_parameter("dry_run", "false").lower() in {"true", "1", "yes"},
        help="Validate parameters and print planned actions without modifying tables",
    )
    return parser.parse_args(args)


def run_silver_cleansing(args: argparse.Namespace) -> Dict[str, Any]:
    """Execute Silver cleansing pipeline based on parsed arguments."""
    workspace_cfg = DatabricksWorkspaceConfig(
        environment=args.environment,
        silver_bucket_name=args.gcs_silver_bucket,
    )
    target_date = datetime.strptime(args.ingestion_date, "%Y-%m-%d").date()

    logger.info(
        f"Starting Silver Cleansing Runner [env={args.environment}, "
        f"source_type={args.source_type}, date={target_date}, dry_run={args.dry_run}]"
    )

    if args.dry_run:
        plan = {
            "status": "DRY_RUN_SUCCESS",
            "environment": args.environment,
            "source_type": args.source_type,
            "ingestion_date": str(target_date),
            "run_optimize": args.run_optimize,
            "run_vacuum": args.run_vacuum,
            "silver_delta_root": workspace_cfg.silver_delta_path,
        }
        logger.info(f"Dry run plan completed: {json.dumps(plan)}")
        return plan

    engine = DeltaSilverCleansingEngine(workspace_config=workspace_cfg)
    results: List[Dict[str, Any]] = []

    sources_to_run = (
        ["market_ticks", "trade_orders", "rolling_vwap"]
        if args.source_type == "all"
        else [args.source_type]
    )

    for src in sources_to_run:
        target_dir = args.target_path or f"silver_{src}"

        if src == "market_ticks":
            bronze_src = args.source_path or "bronze_market_ticks"
            res = engine.cleanse_market_ticks(
                source=bronze_src,
                target_path=target_dir,
                ingest_date=target_date,
            )
            results.append(res.to_dict())

            if args.run_optimize:
                opt = engine.optimize_table(
                    res.target_table_path,
                    z_order_columns=SILVER_MARKET_TICKS_CONFIG.z_order_columns,
                )
                logger.info(f"Optimized {src}: {opt}")

            if args.run_vacuum:
                vac = engine.vacuum_table(res.target_table_path, retention_hours=168)
                logger.info(f"Vacuumed {src}: {vac}")

        elif src == "trade_orders":
            bronze_src = args.source_path or "bronze_trade_orders"
            res = engine.cleanse_trade_orders(
                source=bronze_src,
                target_path=target_dir,
                ingest_date=target_date,
            )
            results.append(res.to_dict())

            if args.run_optimize:
                opt = engine.optimize_table(
                    res.target_table_path,
                    z_order_columns=SILVER_TRADE_ORDERS_CONFIG.z_order_columns,
                )
                logger.info(f"Optimized {src}: {opt}")

            if args.run_vacuum:
                vac = engine.vacuum_table(res.target_table_path, retention_hours=168)
                logger.info(f"Vacuumed {src}: {vac}")

        elif src == "rolling_vwap":
            bronze_src = args.source_path or "bronze_rolling_vwap"
            res = engine.cleanse_rolling_vwap(
                source=bronze_src,
                target_path=target_dir,
                ingest_date=target_date,
            )
            results.append(res.to_dict())

            if args.run_optimize:
                opt = engine.optimize_table(
                    res.target_table_path,
                    z_order_columns=SILVER_ROLLING_VWAP_CONFIG.z_order_columns,
                )
                logger.info(f"Optimized {src}: {opt}")

            if args.run_vacuum:
                vac = engine.vacuum_table(res.target_table_path, retention_hours=168)
                logger.info(f"Vacuumed {src}: {vac}")

    summary = {
        "status": "SUCCESS",
        "environment": args.environment,
        "runs": results,
        "telemetry_metrics": engine.telemetry.format_prometheus(),
    }
    return summary


def main(argv: Optional[List[str]] = None) -> None:
    """CLI Entry point for notebook task."""
    args = parse_args(argv)
    output = run_silver_cleansing(args)

    output_json = json.dumps(output, indent=2, default=str)
    print(output_json)

    # If executed within Databricks notebook environment, signal success
    try:
        import IPython  # type: ignore
        dbutils = IPython.get_ipython().user_ns.get("dbutils")  # type: ignore
        if dbutils:
            dbutils.notebook.exit(output_json)
    except Exception:
        pass


if __name__ == "__main__":
    main()
