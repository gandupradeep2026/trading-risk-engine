"""Trading Risk Engine: Case Study CLI Runner.

Production CLI interface for executing portfolio case study shock simulations,
exporting executive markdown summaries, generating JSON audit reports,
and rendering Prometheus metrics.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import List, Optional

from case_study.config import CaseStudyConfig
from case_study.reporter import CaseStudyReportGenerator
from case_study.simulator import CaseStudySimulator
from case_study.telemetry import CaseStudyTelemetry


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="case-study-runner",
        description="Execute executive portfolio case study simulations and audit reporting.",
    )
    parser.add_argument(
        "--run-scenario",
        action="store_true",
        help="Run the multi-asset flash crash and rogue trader incident simulation.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate scenario configuration without running the full simulation.",
    )
    parser.add_argument(
        "--output-markdown",
        type=str,
        default=None,
        help="File path to save the generated executive markdown report.",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="File path to save the structured JSON audit results.",
    )
    parser.add_argument(
        "--export-prometheus",
        action="store_true",
        help="Print operational Prometheus metrics to stdout.",
    )

    return parser.parse_args(args)


def main(args: Optional[List[str]] = None) -> int:
    parsed = parse_args(args)

    config = CaseStudyConfig()
    telemetry = CaseStudyTelemetry()

    if parsed.dry_run:
        print(f"[DRY-RUN] Case study config validated: {config.scenario_name}")
        print(f"[DRY-RUN] Portfolio books configured: {len(config.books)}")
        print(f"[DRY-RUN] Total Initial NAV: ${config.total_initial_nav_usd / 1e6:,.1f}M USD")
        return 0

    simulator = CaseStudySimulator(config=config, telemetry=telemetry)
    result = simulator.run_simulation()

    print(f"=== Executive Portfolio Case Study: {result.scenario_name} ===")
    print(f"Outcome: {'PASSED' if result.is_success else 'FAILED'}")
    print(f"Capital Preserved: ${result.total_capital_preserved_usd / 1e6:,.2f}M USD")
    print(f"P95 Containment Latency: {result.p95_containment_latency_ms:.1f} ms")
    print(f"Reconciliation Passed: {result.reconciliation_passed}")

    # Generate Markdown
    md_content = CaseStudyReportGenerator.render_markdown(result)
    if parsed.output_markdown:
        out_path = Path(parsed.output_markdown)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(md_content, encoding="utf-8")
        print(f"Markdown report written to: {out_path}")

    # Generate JSON
    if parsed.output_json:
        json_path = Path(parsed.output_json)
        CaseStudyReportGenerator.export_json(result, output_path=json_path)
        print(f"JSON audit report written to: {json_path}")

    # Export Prometheus
    if parsed.export_prometheus:
        print("\n--- Prometheus Metrics ---")
        print(telemetry.format_prometheus())

    return 0 if result.is_success else 1


if __name__ == "__main__":
    sys.exit(main())
