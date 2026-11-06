"""CLI Runner for BigQuery Star Schema DDL Deployment & Validation (Day 22).

Usage:
    python -m dwh.bigquery.ddl_runner --environment dev --local_emulator --seed_dimensions
    python -m dwh.bigquery.ddl_runner --validate_only
    python -m dwh.bigquery.ddl_runner --dry_run
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Dict, Optional

from dwh.bigquery.config import BigQueryDatasetConfig, STAR_SCHEMA_CATALOG
from dwh.bigquery.emulator import LocalBigQueryEmulator
from dwh.bigquery.schema_manager import BigQuerySchemaManager


def run_ddl_tool(
    environment: str = "dev",
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    validate_only: bool = False,
    dry_run: bool = False,
    local_emulator: bool = True,
    seed_dimensions: bool = False,
    output_json: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute DDL validation, deployment, and dimension seeding."""
    config = BigQueryDatasetConfig.from_env()
    if environment:
        config.environment = environment
    if project_id:
        config.project_id = project_id
    if dataset_id:
        config.dataset_id = dataset_id

    schema_manager = BigQuerySchemaManager()
    summary: Dict[str, Any] = {
        "status": "SUCCESS",
        "environment": config.environment,
        "project_id": config.project_id,
        "dataset_id": config.dataset_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tables": {},
    }

    def _save_and_return(res: Dict[str, Any]) -> Dict[str, Any]:
        if output_json:
            out_path = Path(output_json)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(res, f, indent=2)
        return res

    # Step 1: Schema & DDL Validation
    try:
        schema_manager.validate_catalog()
        summary["validation"] = "PASSED"
    except Exception as e:
        summary["status"] = "FAILED"
        summary["error"] = f"Catalog validation error: {e}"
        return _save_and_return(summary)

    if validate_only:
        summary["mode"] = "validate_only"
        return _save_and_return(summary)

    # Step 2: Dry Run Mode
    if dry_run:
        summary["mode"] = "dry_run"
        master_ddl = schema_manager.render_master_ddl(config.project_id, config.dataset_id)
        summary["rendered_ddl_bytes"] = len(master_ddl)
        summary["tables_to_create"] = list(STAR_SCHEMA_CATALOG.keys())
        return _save_and_return(summary)

    # Step 3: Local Emulator Execution
    if local_emulator:
        summary["mode"] = "local_emulator"
        emulator = LocalBigQueryEmulator(
            project_id=config.project_id, dataset_id=config.dataset_id
        )
        try:
            emulator.initialize_star_schema()

            if seed_dimensions:
                seed_counts = emulator.load_seed_dimensions()
                summary["seed_counts"] = seed_counts

            for tbl in STAR_SCHEMA_CATALOG.keys():
                row_count = emulator.get_table_row_count(tbl)
                cols = emulator.get_table_columns(tbl)
                summary["tables"][tbl] = {
                    "row_count": row_count,
                    "column_count": len(cols),
                }

            summary["telemetry"] = {
                "queries_executed": emulator.telemetry.queries_executed,
                "rows_inserted": emulator.telemetry.rows_inserted,
            }
        finally:
            emulator.close()

    return _save_and_return(summary)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="BigQuery Star Schema DDL Deployment & Validation Runner"
    )
    parser.add_argument("--environment", default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument("--project_id", default=None)
    parser.add_argument("--dataset_id", default=None)
    parser.add_argument("--validate_only", action="store_true")
    parser.add_argument("--dry_run", action="store_true")
    parser.add_argument("--local_emulator", action="store_true", default=True)
    parser.add_argument("--seed_dimensions", action="store_true")
    parser.add_argument("--output_json", default=None)

    args = parser.parse_args()
    summary = run_ddl_tool(
        environment=args.environment,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        validate_only=args.validate_only,
        dry_run=args.dry_run,
        local_emulator=args.local_emulator,
        seed_dimensions=args.seed_dimensions,
        output_json=args.output_json,
    )

    print(json.dumps(summary, indent=2))
    if summary.get("status") != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
