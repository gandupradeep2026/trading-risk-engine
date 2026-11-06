"""Trading Risk Engine: CI CLI Tool & Pipeline Validator.

Command-line interface for local and automated validation of workflows, linters,
code quality, AST integrity, and test suites.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from ci.config import CiPipelineConfig
from ci.linter_runner import LocalCiRunner
from ci.telemetry import CiTelemetry
from ci.workflow_validator import WorkflowValidator


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trading Risk Engine CI Pipeline Runner & Workflow Validator")
    parser.add_argument(
        "--validate-workflows",
        action="store_true",
        help="Validate all GitHub Actions YAML workflow files against enterprise platform standards.",
    )
    parser.add_argument(
        "--ast-scan",
        action="store_true",
        help="Run Python AST syntax and hygiene scan across repository.",
    )
    parser.add_argument(
        "--yaml-check",
        action="store_true",
        help="Validate YAML file syntax across the repository.",
    )
    parser.add_argument(
        "--black",
        action="store_true",
        help="Run Black formatting check (--check --diff).",
    )
    parser.add_argument(
        "--flake8",
        action="store_true",
        help="Run Flake8 style and complexity linter.",
    )
    parser.add_argument(
        "--mypy",
        action="store_true",
        help="Run Mypy static type checker.",
    )
    parser.add_argument(
        "--pytest",
        action="store_true",
        help="Execute Pytest test suite.",
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Execute all validation checks, linters, and AST scans.",
    )
    parser.add_argument(
        "--json-report",
        type=str,
        default=None,
        help="Path to export structured JSON verification summary.",
    )
    parser.add_argument(
        "--markdown-report",
        type=str,
        default=None,
        help="Path to export GitHub Flavored Markdown verification report.",
    )
    parser.add_argument(
        "--export-prometheus",
        type=str,
        default=None,
        help="Path to export Prometheus text exposition metrics.",
    )
    return parser


def _run_workflows_check(ci_config: CiPipelineConfig, telemetry: CiTelemetry, report: Dict[str, Any]) -> bool:
    validator = WorkflowValidator(config=ci_config)
    wf_report = validator.validate_all()
    telemetry.record_workflow_validation(wf_report.is_valid, len(wf_report.errors))
    report["workflows"] = wf_report.to_dict()

    if wf_report.is_valid:
        print(f"[CI] Workflows: {wf_report.total_workflows} workflow files validated successfully.")
        for r in wf_report.results:
            print(f"     * {r.file_name} -> {len(r.topological_order)} jobs: {' -> '.join(r.topological_order)}")
        return True

    print("[CI ERROR] Workflow validation failed:")
    for err in wf_report.errors:
        print(f"     ! {err}")
    return False


def _run_ast_check(runner: LocalCiRunner, telemetry: CiTelemetry, report: Dict[str, Any]) -> bool:
    ast_res = runner.scan_codebase_ast()
    telemetry.record_ast_scan(
        files_scanned=ast_res.details.get("files_scanned", 0),
        issues_found=ast_res.issue_count,
    )
    report["linters"]["ast_syntax"] = {
        "status": ast_res.status,
        "duration_sec": ast_res.duration_sec,
        "issues": ast_res.issue_count,
    }
    if not ast_res.passed:
        print(f"[CI ERROR] AST scan failed ({ast_res.issue_count} issues):")
        print(ast_res.stderr)
        return False

    print(f"[CI] AST Scan: {ast_res.stdout}")
    return True


def _run_yaml_check(runner: LocalCiRunner, telemetry: CiTelemetry, report: Dict[str, Any]) -> bool:
    yaml_res = runner.validate_yaml_files()
    telemetry.record_linter_run(
        tool="yaml_syntax",
        passed=yaml_res.passed,
        duration_sec=yaml_res.duration_sec,
        issue_count=yaml_res.issue_count,
    )
    report["linters"]["yaml_syntax"] = {
        "status": yaml_res.status,
        "duration_sec": yaml_res.duration_sec,
        "issues": yaml_res.issue_count,
    }
    if not yaml_res.passed:
        print(f"[CI ERROR] YAML check failed ({yaml_res.issue_count} errors):")
        print(yaml_res.stderr)
        return False

    print(f"[CI] YAML Check: {yaml_res.stdout}")
    return True


def _run_black_check(runner: LocalCiRunner, telemetry: CiTelemetry, report: Dict[str, Any]) -> bool:
    black_res = runner.run_black(check_only=True)
    telemetry.record_linter_run(
        tool="black",
        passed=black_res.passed,
        duration_sec=black_res.duration_sec,
        issue_count=black_res.issue_count,
    )
    report["linters"]["black"] = {
        "status": black_res.status,
        "duration_sec": black_res.duration_sec,
        "issues": black_res.issue_count,
    }
    if not black_res.passed and black_res.status != "SKIPPED":
        print("[CI ERROR] Black formatting check failed.")
        if black_res.stderr:
            print(black_res.stderr[:500])
        return False

    print(f"[CI] Black Check: {black_res.status} ({black_res.duration_sec:.2f}s)")
    return True


def _run_flake8_check(runner: LocalCiRunner, telemetry: CiTelemetry, report: Dict[str, Any]) -> bool:
    flake8_res = runner.run_flake8()
    telemetry.record_linter_run(
        tool="flake8",
        passed=flake8_res.passed,
        duration_sec=flake8_res.duration_sec,
        issue_count=flake8_res.issue_count,
    )
    report["linters"]["flake8"] = {
        "status": flake8_res.status,
        "duration_sec": flake8_res.duration_sec,
        "issues": flake8_res.issue_count,
    }
    if not flake8_res.passed and flake8_res.status != "SKIPPED":
        print(f"[CI ERROR] Flake8 check failed ({flake8_res.issue_count} issues).")
        if flake8_res.stdout:
            print(flake8_res.stdout[:500])
        return False

    print(f"[CI] Flake8 Check: {flake8_res.status} ({flake8_res.duration_sec:.2f}s)")
    return True


def _run_mypy_check(runner: LocalCiRunner, telemetry: CiTelemetry, report: Dict[str, Any]) -> bool:
    mypy_res = runner.run_mypy()
    telemetry.record_linter_run(
        tool="mypy",
        passed=mypy_res.passed,
        duration_sec=mypy_res.duration_sec,
        issue_count=mypy_res.issue_count,
    )
    report["linters"]["mypy"] = {
        "status": mypy_res.status,
        "duration_sec": mypy_res.duration_sec,
        "issues": mypy_res.issue_count,
    }
    if not mypy_res.passed and mypy_res.status != "SKIPPED":
        print(f"[CI ERROR] Mypy type check failed ({mypy_res.issue_count} issues).")
        if mypy_res.stdout:
            print(mypy_res.stdout[:500])
        return False

    print(f"[CI] Mypy Check: {mypy_res.status} ({mypy_res.duration_sec:.2f}s)")
    return True


def _run_pytest_check(runner: LocalCiRunner, telemetry: CiTelemetry, report: Dict[str, Any]) -> bool:
    pytest_res = runner.run_pytest(verbose=True)
    telemetry.record_pytest_run(passed=pytest_res.passed, duration_sec=pytest_res.duration_sec)
    report["linters"]["pytest"] = {
        "status": pytest_res.status,
        "duration_sec": pytest_res.duration_sec,
        "issues": pytest_res.issue_count,
    }
    if not pytest_res.passed:
        print("[CI ERROR] Pytest suite failed.")
        return False

    print(f"[CI] Pytest Suite: PASSED ({pytest_res.duration_sec:.2f}s)")
    return True


def _export_reports(
    args: argparse.Namespace,
    report_data: Dict[str, Any],
    telemetry: CiTelemetry,
    runner: LocalCiRunner,
) -> None:
    if args.json_report:
        out_path = Path(args.json_report)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2)
        print(f"[CI] JSON report exported to: {out_path}")

    if args.markdown_report:
        out_path = Path(args.markdown_report)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        summary = runner.run_all(run_pytest=False)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(summary.render_markdown())
        print(f"[CI] Markdown report exported to: {out_path}")

    if args.export_prometheus:
        out_path = Path(args.export_prometheus)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(telemetry.format_prometheus())
        print(f"[CI] Prometheus metrics exported to: {out_path}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if not (
        args.validate_workflows
        or args.ast_scan
        or args.yaml_check
        or args.black
        or args.flake8
        or args.mypy
        or args.pytest
        or args.run_all
    ):
        args.run_all = True

    repo_root = Path.cwd()
    ci_config = CiPipelineConfig(repo_root=repo_root)
    telemetry = CiTelemetry()
    runner = LocalCiRunner(repo_root=repo_root)

    report_data: Dict[str, Any] = {
        "status": "PASS",
        "workflows": {},
        "linters": {},
        "issues_count": 0,
    }

    success = True
    if args.validate_workflows or args.run_all:
        if not _run_workflows_check(ci_config, telemetry, report_data):
            success = False

    if args.ast_scan or args.run_all:
        if not _run_ast_check(runner, telemetry, report_data):
            success = False

    if args.yaml_check or args.run_all:
        if not _run_yaml_check(runner, telemetry, report_data):
            success = False

    if args.black or args.run_all:
        if not _run_black_check(runner, telemetry, report_data):
            success = False

    if args.flake8 or args.run_all:
        if not _run_flake8_check(runner, telemetry, report_data):
            success = False

    if args.mypy or args.run_all:
        if not _run_mypy_check(runner, telemetry, report_data):
            success = False

    if args.pytest:
        if not _run_pytest_check(runner, telemetry, report_data):
            success = False

    report_data["status"] = "PASS" if success else "FAIL"
    report_data["issues_count"] = telemetry.total_issues

    _export_reports(args, report_data, telemetry, runner)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
