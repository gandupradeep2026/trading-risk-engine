"""Trading Risk Engine: Continuous Integration Telemetry & Prometheus Metrics.

Collects and formats operational telemetry for CI/CD runs, including workflow validation,
linter execution rates, AST scan results, and test metrics.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class CiTelemetry:
    """Thread-safe operational telemetry collector for CI pipeline execution."""

    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    workflows_validated: int = 0
    workflow_validation_errors: int = 0
    linter_runs: Dict[str, int] = field(default_factory=dict)
    linter_failures: Dict[str, int] = field(default_factory=dict)
    linter_durations_sec: Dict[str, float] = field(default_factory=dict)
    ast_files_scanned: int = 0
    ast_issues_detected: int = 0
    pytest_runs: int = 0
    pytest_passed: int = 0
    pytest_failed: int = 0
    total_issues: int = 0

    def record_workflow_validation(self, is_valid: bool, error_count: int = 0) -> None:
        with self._lock:
            self.workflows_validated += 1
            if not is_valid:
                self.workflow_validation_errors += max(1, error_count)

    def record_linter_run(self, tool: str, passed: bool, duration_sec: float, issue_count: int = 0) -> None:
        with self._lock:
            self.linter_runs[tool] = self.linter_runs.get(tool, 0) + 1
            if not passed:
                self.linter_failures[tool] = self.linter_failures.get(tool, 0) + 1
            self.linter_durations_sec[tool] = self.linter_durations_sec.get(tool, 0.0) + duration_sec
            self.total_issues += issue_count

    def record_ast_scan(self, files_scanned: int, issues_found: int) -> None:
        with self._lock:
            self.ast_files_scanned += files_scanned
            self.ast_issues_detected += issues_found
            self.total_issues += issues_found

    def record_pytest_run(self, passed: bool, duration_sec: float) -> None:
        with self._lock:
            self.pytest_runs += 1
            if passed:
                self.pytest_passed += 1
            else:
                self.pytest_failed += 1
            self.linter_durations_sec["pytest"] = self.linter_durations_sec.get("pytest", 0.0) + duration_sec

    def format_prometheus(self) -> str:
        """Renders metrics in Prometheus text exposition format."""
        with self._lock:
            lines = [
                "# HELP ci_workflows_validated_total Total count of GitHub Actions workflows validated.",
                "# TYPE ci_workflows_validated_total counter",
                f"ci_workflows_validated_total {self.workflows_validated}",
                "",
                "# HELP ci_workflow_validation_errors_total Total workflow validation errors detected.",
                "# TYPE ci_workflow_validation_errors_total counter",
                f"ci_workflow_validation_errors_total {self.workflow_validation_errors}",
                "",
                "# HELP ci_ast_files_scanned_total Total Python files scanned for AST syntax and hygiene.",
                "# TYPE ci_ast_files_scanned_total counter",
                f"ci_ast_files_scanned_total {self.ast_files_scanned}",
                "",
                "# HELP ci_ast_issues_detected_total Total AST issues and code smells detected.",
                "# TYPE ci_ast_issues_detected_total counter",
                f"ci_ast_issues_detected_total {self.ast_issues_detected}",
                "",
                "# HELP ci_pytest_runs_total Total Pytest executions completed.",
                "# TYPE ci_pytest_runs_total counter",
                f"ci_pytest_runs_total {self.pytest_runs}",
                f"ci_pytest_passed_total {self.pytest_passed}",
                f"ci_pytest_failed_total {self.pytest_failed}",
                "",
                "# HELP ci_linter_runs_total Total linter tool executions by tool name.",
                "# TYPE ci_linter_runs_total counter",
            ]
            for tool, count in sorted(self.linter_runs.items()):
                lines.append(f'ci_linter_runs_total{{tool="{tool}"}} {count}')

            lines.extend(
                [
                    "",
                    "# HELP ci_linter_failures_total Total failures recorded per linter tool.",
                    "# TYPE ci_linter_failures_total counter",
                ]
            )
            for tool, count in sorted(self.linter_failures.items()):
                lines.append(f'ci_linter_failures_total{{tool="{tool}"}} {count}')

            lines.extend(
                [
                    "",
                    "# HELP ci_linter_duration_seconds Total execution time per linter tool.",
                    "# TYPE ci_linter_duration_seconds counter",
                ]
            )
            for tool, duration in sorted(self.linter_durations_sec.items()):
                lines.append(f'ci_linter_duration_seconds{{tool="{tool}"}} {duration:.4f}')

            lines.append("")
            return "\n".join(lines)
