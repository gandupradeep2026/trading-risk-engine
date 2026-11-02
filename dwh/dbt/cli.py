"""dbt Command-Line Interface (CLI) Runner (Day 23).

Provides CLI entry points for compiling dbt models, executing transformations against
LocalBigQueryEmulator, evaluating schema tests, and exporting Prometheus telemetry.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import List, Optional

from dwh.bigquery.emulator import LocalBigQueryEmulator
from dwh.dbt.config import DbtConfig
from dwh.dbt.project_manager import DbtProjectManager
from dwh.dbt.runner import DbtRunner


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="dbt-risk-engine",
        description="Trading Risk Engine: dbt Core Project Manager & Local Runner (Day 23)",
    )
    parser.add_argument(
        "--project-dir",
        type=str,
        default=str(Path(__file__).resolve().parent),
        help="Path to dbt project root directory containing dbt_project.yml",
    )
    parser.add_argument(
        "--models",
        nargs="+",
        default=["staging", "intermediate"],
        help="Model layers or model names to run (e.g. staging intermediate stg_trade_orders all)",
    )
    parser.add_argument(
        "--target",
        type=str,
        default="dev",
        help="Target profile name (dev, prod, duckdb)",
    )
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        help="Deployment environment: dev or prod",
    )
    parser.add_argument(
        "--compile-only",
        action="store_true",
        help="Compile model SQL and print DAG execution order without executing relations",
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed emulator with dimension master data and synthetic facts before execution",
    )
    parser.add_argument(
        "--run-tests",
        action="store_true",
        help="Execute schema tests after compiling and running models",
    )
    parser.add_argument(
        "--export-prometheus",
        type=str,
        default=None,
        help="File path to write Prometheus metric text exposition",
    )
    parser.add_argument(
        "--json-report",
        type=str,
        default=None,
        help="File path to write JSON execution summary",
    )
    return parser


def main(args: Optional[List[str]] = None) -> int:
    """Main execution function for dbt CLI."""
    parser = build_parser()
    opts = parser.parse_args(args)

    cfg = DbtConfig(
        project_dir=Path(opts.project_dir),
        target=opts.target,
        environment=opts.environment,
        gcp_project_id=f"trading-risk-{opts.environment}-001",
        dataset_id=f"trading_risk_dwh_{opts.environment}",
        models=opts.models,
    )

    pm = DbtProjectManager(cfg)
    order = pm.get_execution_order(opts.models)

    print("=" * 70)
    print(f"TRADING RISK ENGINE: dbt Project Manager (Day 23)")
    print(f"Project: {cfg.project_name} | Environment: {cfg.environment} | Target: {cfg.target}")
    print(f"Models Selected: {opts.models}")
    print(f"Topological Execution Order ({len(order)} models):")
    for i, m in enumerate(order, 1):
        node = pm.models[m]
        print(f"  [{i}/{len(order)}] {m} ({node.layer}, {node.materialized}) -> refs: {node.refs}")
    print("=" * 70)

    if opts.compile_only:
        print("\n--- COMPILED SQL OUTPUT ---")
        for m in order:
            print(f"\n/* --- Model: {m} --- */")
            print(pm.compile_model_sql(m))
        return 0

    # Initialize emulator and seed if requested
    emulator = LocalBigQueryEmulator(
        project_id=cfg.gcp_project_id,
        dataset_id=cfg.dataset_id,
    )

    if opts.seed:
        print("\nInitializing BigQuery star-schema and loading seed data...")
        emulator.initialize_star_schema()
        counts = emulator.load_seed_dimensions()
        print(f"Seeded dimension records: {counts}")
        from dwh.bigquery.seed_data import (
            generate_mock_trade_orders,
            generate_mock_market_ticks,
            generate_mock_risk_stats,
            generate_mock_risk_alerts,
        )
        t_count = emulator.insert_rows("fact_trade_orders", generate_mock_trade_orders(100))
        k_count = emulator.insert_rows("fact_market_ticks", generate_mock_market_ticks(100))
        s_count = emulator.insert_rows("fact_risk_stats", generate_mock_risk_stats("2026-11-02"))
        a_count = emulator.insert_rows("fact_risk_alerts", generate_mock_risk_alerts(20))
        print(f"Seeded fact records: trades={t_count}, ticks={k_count}, risk_stats={s_count}, alerts={a_count}")

    runner = DbtRunner(emulator=emulator, project_manager=pm, config=cfg)

    if opts.run_tests:
        run_res = runner.run_all(opts.models)
    else:
        run_res = runner.run_models(opts.models)

    print("\n--- EXECUTION RESULTS ---")
    print(f"Status: {'SUCCESS' if run_res.success else 'FAILED'}")
    print(f"Models: {run_res.models_executed} executed, {run_res.models_failed} failed")
    if opts.run_tests:
        print(f"Tests: {run_res.tests_executed} executed, {run_res.tests_passed} passed, {run_res.tests_failed} failed")
    print(f"Duration: {run_res.total_duration_ms:.2f} ms")

    if opts.json_report:
        report_path = Path(opts.json_report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(run_res.to_dict(), f, indent=2)
        print(f"JSON report written to: {report_path}")

    if opts.export_prometheus:
        prom_path = Path(opts.export_prometheus)
        prom_path.parent.mkdir(parents=True, exist_ok=True)
        with open(prom_path, "w", encoding="utf-8") as f:
            f.write(runner.telemetry.format_prometheus())
        print(f"Prometheus metrics exported to: {prom_path}")

    return 0 if run_res.success else 1


if __name__ == "__main__":
    sys.exit(main())
