"""CLI Runner for BigQuery Performance Benchmarks (Day 26).

Usage:
    python -m dwh.bigquery.benchmark_cli --scale-factor 1.0 --iterations 3 --output-json report.json --output-markdown report.md
    python -m dwh.bigquery.benchmark_cli --dry-run
    python -m dwh.bigquery.benchmark_cli --scenarios partition_pruning_fact_market_ticks,materialized_view_market_performance
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from dwh.bigquery.benchmark import (
    BENCHMARK_SCENARIOS_CATALOG,
    BenchmarkRunner,
    BenchmarkSuiteResult,
)


def run_benchmark_cli(
    environment: str = "dev",
    project_id: str = "trading-risk-engine-dev",
    dataset_id: Optional[str] = None,
    scale_factor: float = 1.0,
    iterations: int = 5,
    scenarios: Optional[List[str]] = None,
    dry_run: bool = False,
    output_json: Optional[str] = None,
    output_markdown: Optional[str] = None,
    export_prometheus: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute benchmarking suite and output results."""
    resolved_dataset = dataset_id or f"trading_risk_dwh_{environment}"

    if dry_run:
        available = list(BENCHMARK_SCENARIOS_CATALOG.keys())
        summary = {
            "status": "DRY_RUN",
            "environment": environment,
            "dataset_id": resolved_dataset,
            "scale_factor": scale_factor,
            "iterations": iterations,
            "scenarios_available": available,
            "scenarios_selected": scenarios or available,
        }
        if output_json:
            out_p = Path(output_json)
            out_p.parent.mkdir(parents=True, exist_ok=True)
            with open(out_p, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
        return summary

    runner = BenchmarkRunner(
        dataset_id=resolved_dataset,
        project_id=project_id,
    )

    # Setup benchmark data
    runner.setup_benchmark_data(scale_factor=scale_factor)

    # Run the benchmark suite
    suite_result: BenchmarkSuiteResult = runner.run_suite(
        scenario_names=scenarios,
        iterations=iterations,
        scale_factor=scale_factor,
    )

    result_dict = suite_result.to_dict()

    if output_json:
        out_p = Path(output_json)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        with open(out_p, "w", encoding="utf-8") as f:
            json.dump(result_dict, f, indent=2)

    if output_markdown:
        out_md = Path(output_markdown)
        out_md.parent.mkdir(parents=True, exist_ok=True)
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(suite_result.to_markdown())

    if export_prometheus:
        prom_p = Path(export_prometheus)
        prom_p.parent.mkdir(parents=True, exist_ok=True)
        with open(prom_p, "w", encoding="utf-8") as f:
            f.write(runner.telemetry.format_prometheus())

    return result_dict


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run BigQuery Star-Schema & Materialized View Benchmarks",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--environment", default="dev", choices=["dev", "prod"], help="Environment name")
    parser.add_argument("--project-id", default="trading-risk-engine-dev", help="GCP Project ID")
    parser.add_argument("--dataset-id", default=None, help="BigQuery Dataset ID")
    parser.add_argument("--scale-factor", type=float, default=1.0, help="Scale factor for generated datasets")
    parser.add_argument("--iterations", type=int, default=5, help="Number of repetitions per scenario")
    parser.add_argument("--scenarios", type=str, default=None, help="Comma-separated list of scenario names")
    parser.add_argument("--dry-run", action="store_true", help="Print selected scenarios without executing")
    parser.add_argument("--output-json", default=None, help="Path to write JSON benchmark report")
    parser.add_argument("--output-markdown", default=None, help="Path to write Markdown benchmark report")
    parser.add_argument("--export-prometheus", default=None, help="Path to write Prometheus metrics file")

    args = parser.parse_args()

    scen_list = [s.strip() for s in args.scenarios.split(",") if s.strip()] if args.scenarios else None

    result = run_benchmark_cli(
        environment=args.environment,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        scale_factor=args.scale_factor,
        iterations=args.iterations,
        scenarios=scen_list,
        dry_run=args.dry_run,
        output_json=args.output_json,
        output_markdown=args.output_markdown,
        export_prometheus=args.export_prometheus,
    )

    print(f"Benchmark finished: {result.get('status', 'SUCCESS')}")
    if "total_queries_executed" in result:
        print(f"Total queries executed: {result['total_queries_executed']}")
        print(f"Total duration: {result['total_duration_seconds']:.2f}s")


if __name__ == "__main__":
    main()
