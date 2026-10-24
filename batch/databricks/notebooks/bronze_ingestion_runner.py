"""Databricks Production Notebook / Task Runner for Delta Bronze Ingestion.

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

from batch.databricks.bronze_ingestion import (
    BronzeIngestionResult,
    DeltaBronzeIngestionEngine,
)
from batch.databricks.config import (
    BRONZE_MARKET_TICKS_CONFIG,
    BRONZE_ROLLING_VWAP_CONFIG,
    BRONZE_TRADE_ORDERS_CONFIG,
    DatabricksWorkspaceConfig,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("bronze_ingestion_runner")


def get_parameter(param_name: str, default_val: str = "") -> str:
    """Extract parameter from dbutils.widgets if running in Databricks, else environment or default."""
    try:
        # Databricks dbutils check
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
        description="Run Databricks Delta Lake Bronze Ingestion for Trading Risk Engine."
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
        help="Source type to ingest into Bronze Delta",
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
        help="Override raw data input path or directory",
    )
    parser.add_argument(
        "--target-path",
        type=str,
        default=get_parameter("target_path", ""),
        help="Override target Delta table path or directory",
    )
    parser.add_argument(
        "--gcs-bronze-bucket",
        type=str,
        default=get_parameter("gcs_bronze_bucket", ""),
        help="GCS Bronze bucket name",
    )
    parser.add_argument(
        "--run-optimize",
        type=lambda v: str(v).lower() in {"true", "1", "yes"},
        default=get_parameter("run_optimize", "true").lower() in {"true", "1", "yes"},
        help="Execute Delta OPTIMIZE and Z-Ordering after ingestion",
    )
    parser.add_argument(
        "--run-vacuum",
        type=lambda v: str(v).lower() in {"true", "1", "yes"},
        default=get_parameter("run_vacuum", "true").lower() in {"true", "1", "yes"},
        help="Execute Delta VACUUM retention cleanup",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate parameters and plan without writing files",
    )
    return parser.parse_args(args)


def run_bronze_ingestion(parsed_args: argparse.Namespace) -> Dict[str, Any]:
    """Execute Bronze ingestion pipeline according to provided arguments."""
    workspace_cfg = DatabricksWorkspaceConfig(
        environment=parsed_args.environment,
        bronze_bucket_name=parsed_args.gcs_bronze_bucket,
    )
    engine = DeltaBronzeIngestionEngine(workspace_config=workspace_cfg)

    ingest_d = datetime.strptime(parsed_args.ingestion_date, "%Y-%m-%d").date()

    if parsed_args.dry_run:
        summary = {
            "status": "DRY_RUN",
            "environment": parsed_args.environment,
            "source_type": parsed_args.source_type,
            "ingestion_date": parsed_args.ingestion_date,
            "source_path": parsed_args.source_path,
            "target_path": parsed_args.target_path,
            "run_optimize": parsed_args.run_optimize,
            "run_vacuum": parsed_args.run_vacuum,
        }
        logger.info(f"Dry run execution plan: {json.dumps(summary, indent=2)}")
        return summary

    results: List[Dict[str, Any]] = []
    sources = [parsed_args.source_type] if parsed_args.source_type != "all" else ["market_ticks", "trade_orders", "rolling_vwap"]

    for src in sources:
        logger.info(f"Initiating Bronze ingestion for source: {src}")
        target_table_path = parsed_args.target_path or f"./data/delta/bronze/bronze_{src}"

        if src == "market_ticks":
            source_input = parsed_args.source_path or f"./data/raw/ticks/year={ingest_d.year}/month={ingest_d.month:02d}/day={ingest_d.day:02d}"
            res = engine.ingest_market_ticks(
                source_path=source_input,
                target_path=target_table_path,
                ingest_date=ingest_d,
            )
            config = BRONZE_MARKET_TICKS_CONFIG
        elif src == "trade_orders":
            source_input = parsed_args.source_path or f"./data/raw/orders/year={ingest_d.year}/month={ingest_d.month:02d}/day={ingest_d.day:02d}"
            res = engine.ingest_trade_orders(
                source_path=source_input,
                target_path=target_table_path,
                ingest_date=ingest_d,
            )
            config = BRONZE_TRADE_ORDERS_CONFIG
        elif src == "rolling_vwap":
            source_input = parsed_args.source_path or f"./data/raw/vwap/year={ingest_d.year}/month={ingest_d.month:02d}/day={ingest_d.day:02d}"
            res = engine.ingest_rolling_vwap(
                source_path=source_input,
                target_path=target_table_path,
                ingest_date=ingest_d,
            )
            config = BRONZE_ROLLING_VWAP_CONFIG
        else:
            raise ValueError(f"Unsupported source type: {src}")

        res_dict = res.to_dict()

        # Maintenance steps
        if parsed_args.run_optimize:
            opt_res = engine.optimize_table(res.target_table_path, z_order_columns=config.z_order_columns)
            res_dict["optimize"] = opt_res

        if parsed_args.run_vacuum:
            vac_res = engine.vacuum_table(res.target_table_path, retention_hours=config.vacuum_retention_hours)
            res_dict["vacuum"] = vac_res

        results.append(res_dict)
        logger.info(res.summary_markdown())

    output = {
        "status": "SUCCESS",
        "environment": parsed_args.environment,
        "ingestion_date": parsed_args.ingestion_date,
        "results": results,
    }

    # Exit notebook if running in Databricks
    try:
        import IPython  # type: ignore
        dbutils = IPython.get_ipython().user_ns.get("dbutils")  # type: ignore
        if dbutils:
            dbutils.notebook.exit(json.dumps(output))
    except Exception:
        pass

    return output


def main() -> None:
    """CLI execution entrypoint."""
    args = parse_args()
    res = run_bronze_ingestion(args)
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
