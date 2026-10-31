"""Command-Line Runner for Great Expectations Quality Gates.

Executes expectation suites against partitioned Lakehouse datasets:
    python -m data_quality.run_checkpoints --suite silver_market_ticks_suite --data-path /path/to/ticks.parquet
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, Optional

from data_quality.engine import GreatExpectationsValidator
from data_quality.suite_definitions import get_expectation_suite

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("trading_risk.data_quality.runner")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Great Expectations Lakehouse Quality Gate Runner")
    parser.add_argument("--suite", required=True, help="Expectation suite name or JSON file path")
    parser.add_argument("--data-path", required=False, default=None, help="Path to Parquet file or directory to validate")
    parser.add_argument("--target-date", required=False, default=None, help="Target execution date (YYYY-MM-DD)")
    parser.add_argument("--environment", default="dev", help="Target GCP environment (dev, prod)")
    parser.add_argument("--data-docs-dir", default="uncommitted/data_docs", help="Directory to output HTML Data Docs")
    parser.add_argument("--fail-on-error", action="store_true", default=True, help="Exit code 1 if validation fails")
    parser.add_argument("--dry-run", action="store_true", help="Print suite metadata and exit without execution")
    return parser.parse_args()


def run_quality_gate(
    suite_name: str,
    data_path: Optional[str] = None,
    target_date: Optional[str] = None,
    environment: str = "dev",
    data_docs_dir: str = "uncommitted/data_docs",
    fail_on_error: bool = True,
    dry_run: bool = False,
) -> int:
    """Execute quality gate validation."""
    logger.info("Initializing Data Quality Gate for suite: %s (env=%s, date=%s)", suite_name, environment, target_date)
    suite = get_expectation_suite(suite_name)

    if dry_run:
        logger.info("[DRY RUN] Loaded suite '%s' with %d expectations", suite.expectation_suite_name, len(suite.expectations))
        print(suite.to_json())
        return 0

    if not data_path:
        logger.error("Data path must be specified when not running in dry-run mode")
        return 2

    validator = GreatExpectationsValidator()
    result = validator.validate(
        data=data_path,
        suite=suite,
        data_docs_dir=data_docs_dir,
    )

    stats = result.statistics
    logger.info(
        "Validation completed: success=%s, evaluated=%d, passed=%d, failed=%d (success_percent=%.2f%%)",
        result.success,
        stats["evaluated_expectations"],
        stats["successful_expectations"],
        stats["unsuccessful_expectations"],
        stats["success_percent"],
    )

    if result.data_docs_path:
        logger.info("HTML Data Docs report generated at: %s", result.data_docs_path)

    print(result.to_json())

    if not result.success and fail_on_error:
        logger.error("Quality Gate BREACH: %d expectations failed", stats["unsuccessful_expectations"])
        return 1

    return 0


def main() -> None:
    args = parse_args()
    code = run_quality_gate(
        suite_name=args.suite,
        data_path=args.data_path,
        target_date=args.target_date,
        environment=args.environment,
        data_docs_dir=args.data_docs_dir,
        fail_on_error=args.fail_on_error,
        dry_run=args.dry_run,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
