"""Trading Risk Engine: End-to-End Integration Test Runner CLI.

Command-line entry point supporting:
- Scenario selection: happy_path, anomaly_injection, corrupted_dlq, full_e2e
- Stage filtering: all, or comma-separated list of individual stages
- Scale factor adjustments for volumetric testing
- Dry-run execution planning
- Automated report exports: JSON, Markdown, and Prometheus OpenMetrics
"""

from __future__ import annotations

import argparse
from datetime import date
import json
import logging
import os
from pathlib import Path
import sys
from typing import List, Optional

from e2e.config import E2ERunnerConfig, ExecutionScenario, PipelineStage
from e2e.pipeline_runner import E2EIntegrationRunner
from e2e.synthetic_generator import SyntheticE2EGenerator

logger = logging.getLogger("e2e_cli")


def build_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser for the E2E test runner."""
    parser = argparse.ArgumentParser(
        prog="python -m e2e.cli",
        description="Trading Risk Engine - End-to-End Integration Test Runner CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default="full_e2e",
        choices=[s.value for s in ExecutionScenario],
        help="End-to-end testing scenario to execute",
    )
    parser.add_argument(
        "--stages",
        type=str,
        default="all",
        help="Comma-separated list of stages to run (e.g. 'ingestion,streaming') or 'all'",
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=1.0,
        help="Scaling multiplier for synthetic record volume",
    )
    parser.add_argument(
        "--target-date",
        type=str,
        default=date.today().isoformat(),
        help="Valuation calendar date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Target platform environment",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate synthetic data batch and display execution plan without running stages",
    )
    parser.add_argument(
        "--json-report",
        type=str,
        default=None,
        help="File path to write JSON execution summary report",
    )
    parser.add_argument(
        "--markdown-report",
        type=str,
        default=None,
        help="File path to write GitHub Flavored Markdown summary report",
    )
    parser.add_argument(
        "--export-prometheus",
        type=str,
        default=None,
        help="File path to write Prometheus OpenMetrics text file",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging output",
    )
    return parser


def parse_stages_arg(stages_arg: str) -> List[PipelineStage]:
    """Parse comma-separated stages string into PipelineStage list."""
    if stages_arg.strip().lower() == "all":
        return list(PipelineStage)

    parsed: List[PipelineStage] = []
    tokens = [t.strip().lower() for t in stages_arg.split(",") if t.strip()]
    for token in tokens:
        try:
            parsed.append(PipelineStage(token))
        except ValueError:
            valid_stages = [s.value for s in PipelineStage]
            raise ValueError(f"Unknown stage '{token}'. Valid options: {valid_stages}")
    return parsed


def run_e2e_cli(argv: Optional[List[str]] = None) -> int:
    """Execute E2E CLI command and return process exit code."""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "%(name)s", "message": "%(message)s"}',
    )

    try:
        scenario = ExecutionScenario(args.scenario)
        stages = parse_stages_arg(args.stages)
    except ValueError as err:
        print(f"Error: {err}", file=sys.stderr)
        return 2

    config = E2ERunnerConfig(
        environment=args.environment,
        project_id=f"trading-risk-engine-{args.environment}",
        target_date=args.target_date,
        scenario=scenario,
        stages=stages,
        scale_factor=args.scale_factor,
        inject_flash_crash=scenario in (ExecutionScenario.ANOMALY_INJECTION, ExecutionScenario.FULL_E2E),
        inject_fat_finger=scenario in (ExecutionScenario.ANOMALY_INJECTION, ExecutionScenario.FULL_E2E),
        inject_corrupted_orders=scenario in (ExecutionScenario.CORRUPTED_DLQ, ExecutionScenario.FULL_E2E),
        inject_corrupted_ticks=scenario in (ExecutionScenario.CORRUPTED_DLQ, ExecutionScenario.FULL_E2E),
    )

    if args.dry_run:
        generator = SyntheticE2EGenerator()
        batch = generator.generate(config)
        dry_run_info = {
            "dry_run": True,
            "scenario": config.scenario.value,
            "environment": config.environment,
            "target_date": config.target_date,
            "scale_factor": config.scale_factor,
            "planned_stages": [s.value for s in config.stages],
            "synthetic_batch": {
                "expected_valid_orders": batch.expected_valid_orders_count,
                "expected_dlq_orders": batch.expected_dlq_orders_count,
                "expected_valid_ticks": batch.expected_valid_ticks_count,
                "expected_dlq_ticks": batch.expected_dlq_ticks_count,
                "expected_total_notional_usd": batch.expected_total_notional_usd,
                "expected_flash_crash_alerts": batch.expected_flash_crash_alerts_count,
                "expected_fat_finger_alerts": batch.expected_fat_finger_alerts_count,
            },
        }
        print(json.dumps(dry_run_info, indent=2))
        return 0

    runner = E2EIntegrationRunner(config=config)
    try:
        summary = runner.run_all()
        md_text = summary.to_markdown()
        try:
            print(md_text)
        except UnicodeEncodeError:
            print(md_text.encode("ascii", errors="replace").decode("ascii"))

        if args.json_report:
            out_json = Path(args.json_report)
            out_json.parent.mkdir(parents=True, exist_ok=True)
            out_json.write_text(summary.to_json(indent=2), encoding="utf-8")
            print(f"Exported JSON report to {out_json}")

        if args.markdown_report:
            out_md = Path(args.markdown_report)
            out_md.parent.mkdir(parents=True, exist_ok=True)
            out_md.write_text(summary.to_markdown(), encoding="utf-8")
            print(f"Exported Markdown report to {out_md}")

        if args.export_prometheus:
            out_prom = Path(args.export_prometheus)
            out_prom.parent.mkdir(parents=True, exist_ok=True)
            out_prom.write_text(runner.telemetry.format_prometheus(), encoding="utf-8")
            print(f"Exported Prometheus metrics to {out_prom}")

        return 0 if summary.success else 1

    finally:
        runner.close()


if __name__ == "__main__":
    sys.exit(run_e2e_cli())
