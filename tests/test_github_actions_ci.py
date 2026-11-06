"""Unit and Integration Test Suite for GitHub Actions CI Workflow (Day 27).

Validates:
1. GitHub Actions workflow specifications (.github/workflows/ci.yml, scheduled-audit.yml).
2. Linter & static analysis tool configurations (pyproject.toml, .flake8, mypy.ini, requirements-dev.txt).
3. DAG topological sorting and cycle detection of workflow jobs.
4. Python AST syntax, code smells, and bare exception prevention across all repository files.
5. LocalCiRunner execution for Black, Flake8, Mypy, and YAML validation.
6. Operational CI telemetry and Prometheus metrics exposition.
7. CI CLI runner execution and structured report generation.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Dict, List
import pytest
import yaml

from ci.config import (
    CiPipelineConfig,
    WorkflowDefinition,
    WorkflowJob,
    WorkflowParser,
    WorkflowStep,
)
from ci.linter_runner import (
    AstIssue,
    CiRunSummary,
    CodebaseAstScanner,
    LinterResult,
    LinterType,
    LocalCiRunner,
)
from ci.telemetry import CiTelemetry
from ci.workflow_validator import (
    CyclicWorkflowDependencyError,
    JobDependencyGraph,
    MissingJobDependencyError,
    WorkflowValidator,
)
from ci.cli import main as ci_cli_main


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture
def ci_config(repo_root: Path) -> CiPipelineConfig:
    return CiPipelineConfig(repo_root=repo_root)


@pytest.fixture
def ci_runner(repo_root: Path) -> LocalCiRunner:
    return LocalCiRunner(repo_root=repo_root)


# ==============================================================================
# 1. Configuration Manifest Tests
# ==============================================================================


def test_ci_workflow_file_exists(repo_root: Path) -> None:
    ci_file = repo_root / ".github" / "workflows" / "ci.yml"
    assert ci_file.is_file(), f"Expected workflow file at {ci_file}"
    content = ci_file.read_text(encoding="utf-8")
    assert len(content) > 100
    assert "Trading Risk Engine CI" in content


def test_scheduled_audit_workflow_file_exists(repo_root: Path) -> None:
    audit_file = repo_root / ".github" / "workflows" / "scheduled-audit.yml"
    assert audit_file.is_file(), f"Expected scheduled workflow at {audit_file}"
    content = audit_file.read_text(encoding="utf-8")
    assert "schedule:" in content or "cron:" in content


def test_pyproject_toml_configuration(repo_root: Path) -> None:
    pyproject_file = repo_root / "pyproject.toml"
    assert pyproject_file.is_file(), f"Missing pyproject.toml at {pyproject_file}"
    content = pyproject_file.read_text(encoding="utf-8")
    assert "[tool.black]" in content
    assert "[tool.mypy]" in content
    assert "[tool.pytest.ini_options]" in content
    assert "line-length = 120" in content
    assert "extend-exclude" in content


def test_flake8_configuration(repo_root: Path) -> None:
    flake8_file = repo_root / ".flake8"
    assert flake8_file.is_file(), f"Missing .flake8 at {flake8_file}"
    content = flake8_file.read_text(encoding="utf-8")
    assert "[flake8]" in content
    assert "max-line-length = 120" in content
    assert "exclude =" in content


def test_mypy_configuration(repo_root: Path) -> None:
    mypy_file = repo_root / "mypy.ini"
    assert mypy_file.is_file(), f"Missing mypy.ini at {mypy_file}"
    content = mypy_file.read_text(encoding="utf-8")
    assert "[mypy]" in content
    assert "python_version = 3.12" in content
    assert "ignore_missing_imports = True" in content
    assert "files = ci" in content


def test_requirements_dev_file(repo_root: Path) -> None:
    dev_reqs = repo_root / "requirements-dev.txt"
    assert dev_reqs.is_file(), f"Missing requirements-dev.txt at {dev_reqs}"
    content = dev_reqs.read_text(encoding="utf-8")
    for req in ["black", "flake8", "mypy", "pytest", "pytest-cov", "pyyaml"]:
        assert req in content, f"Expected '{req}' in requirements-dev.txt"


# ==============================================================================
# 2. Workflow Structure, Triggers, & Security Tests
# ==============================================================================


def test_workflow_yaml_parsing(repo_root: Path) -> None:
    ci_file = repo_root / ".github" / "workflows" / "ci.yml"
    wf = WorkflowParser.parse_file(ci_file)
    assert wf.name == "Trading Risk Engine CI"
    assert "push" in wf.on_triggers
    assert "pull_request" in wf.on_triggers
    assert "workflow_dispatch" in wf.on_triggers

    # Concurrency and permissions
    assert wf.concurrency is not None
    assert wf.permissions is not None
    assert wf.permissions.get("contents") == "read"


def test_ci_jobs_and_dag_topology(repo_root: Path) -> None:
    ci_file = repo_root / ".github" / "workflows" / "ci.yml"
    wf = WorkflowParser.parse_file(ci_file)

    expected_jobs = [
        "lint-and-format",
        "security-scan",
        "terraform-validate",
        "test-matrix",
        "ci-status-gate",
    ]
    for job_id in expected_jobs:
        assert job_id in wf.jobs, f"Missing expected job '{job_id}' in ci.yml"

    # Verify DAG topology
    graph = JobDependencyGraph.from_workflow(wf)
    topo_order = graph.topological_order
    assert len(topo_order) == len(expected_jobs)

    # test-matrix must come after lint-and-format
    assert topo_order.index("lint-and-format") < topo_order.index("test-matrix")
    # ci-status-gate must be the terminal node
    assert topo_order[-1] == "ci-status-gate"


def test_ci_test_matrix_configuration(repo_root: Path) -> None:
    ci_file = repo_root / ".github" / "workflows" / "ci.yml"
    wf = WorkflowParser.parse_file(ci_file)
    py_versions = wf.get_matrix_python_versions("test-matrix")
    assert "3.11" in py_versions
    assert "3.12" in py_versions


def test_job_timeouts_and_fail_fast_guardrails(repo_root: Path) -> None:
    ci_file = repo_root / ".github" / "workflows" / "ci.yml"
    wf = WorkflowParser.parse_file(ci_file)

    for job_id, job in wf.jobs.items():
        assert job.timeout_minutes is not None, f"Job '{job_id}' missing timeout-minutes"
        assert 5 <= job.timeout_minutes <= 45, f"Job '{job_id}' timeout {job.timeout_minutes}m out of bounds"

    test_job = wf.get_job("test-matrix")
    assert test_job is not None
    assert test_job.strategy is not None
    assert test_job.strategy.get("fail-fast") is False


def test_workflow_step_requirements_references(repo_root: Path) -> None:
    ci_file = repo_root / ".github" / "workflows" / "ci.yml"
    wf = WorkflowParser.parse_file(ci_file)
    validator = WorkflowValidator(config=CiPipelineConfig(repo_root=repo_root))
    errors: List[str] = []
    validator._validate_referenced_files(wf, errors)
    assert not errors, f"Unresolved files referenced in workflow: {errors}"


# ==============================================================================
# 3. Workflow Validator Error Handling & Anomaly Detection Tests
# ==============================================================================


def test_workflow_validator_catches_cycles() -> None:
    cyclic_yaml = """
name: Cyclic Workflow
on: [push]
jobs:
  job-a:
    runs-on: ubuntu-latest
    needs: [job-b]
    steps:
      - run: echo "A"
  job-b:
    runs-on: ubuntu-latest
    needs: [job-a]
    steps:
      - run: echo "B"
"""
    wf = WorkflowParser.parse_yaml_str(cyclic_yaml, "cyclic.yml")
    with pytest.raises(CyclicWorkflowDependencyError) as exc_info:
        JobDependencyGraph.from_workflow(wf)
    assert "Cyclic dependency detected" in str(exc_info.value)


def test_workflow_validator_catches_missing_upstream_job() -> None:
    broken_yaml = """
name: Broken Workflow
on: [push]
jobs:
  job-a:
    runs-on: ubuntu-latest
    needs: [ghost-job]
    steps:
      - run: echo "A"
"""
    wf = WorkflowParser.parse_yaml_str(broken_yaml, "broken.yml")
    with pytest.raises(MissingJobDependencyError) as exc_info:
        JobDependencyGraph.from_workflow(wf)
    assert "unknown job 'ghost-job'" in str(exc_info.value)


def test_workflow_validator_catches_excessive_timeout(repo_root: Path) -> None:
    huge_timeout_yaml = """
name: Long Workflow
on: [push]
jobs:
  job-a:
    runs-on: ubuntu-latest
    timeout-minutes: 120
    steps:
      - run: echo "A"
"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file = Path(tmp_dir) / "long.yml"
        tmp_file.write_text(huge_timeout_yaml, encoding="utf-8")
        validator = WorkflowValidator(config=CiPipelineConfig(repo_root=repo_root, max_timeout_minutes=45))
        res = validator.validate_workflow_file(tmp_file)
        assert not res.is_valid
        assert any("exceeds platform ceiling" in e for e in res.errors)


def test_workflow_validator_catches_missing_triggers(repo_root: Path) -> None:
    no_push_yaml = """
name: Trading Risk Engine CI
on:
  schedule:
    - cron: "0 0 * * *"
jobs:
  lint-and-format:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo "test"
"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file = Path(tmp_dir) / "ci.yml"
        tmp_file.write_text(no_push_yaml, encoding="utf-8")
        validator = WorkflowValidator(config=CiPipelineConfig(repo_root=repo_root))
        res = validator.validate_workflow_file(tmp_file)
        assert not res.is_valid
        assert any("must trigger on 'push'" in e for e in res.errors)
        assert any("must trigger on 'pull_request'" in e for e in res.errors)


# ==============================================================================
# 4. AST Codebase Scanner & Static Analysis Tests
# ==============================================================================


def test_ast_codebase_scanner_clean(ci_runner: LocalCiRunner) -> None:
    result = ci_runner.scan_codebase_ast()
    assert result.status == "PASS"
    assert result.issue_count == 0
    assert result.details.get("files_scanned", 0) >= 120


def test_ast_scanner_detects_bare_except() -> None:
    snippet = """
def sample_func():
    try:
        x = 1 / 0
    except:
        pass
"""
    import ast

    tree = ast.parse(snippet)
    scanner = CodebaseAstScanner("test_sample.py")
    scanner.visit(tree)
    assert len(scanner.issues) == 1
    assert scanner.issues[0].rule_id == "AST-001"
    assert "Bare 'except:'" in scanner.issues[0].message


def test_ast_scanner_detects_mutable_default() -> None:
    snippet = """
def bad_func(val, items=[]):
    items.append(val)
    return items
"""
    import ast

    tree = ast.parse(snippet)
    scanner = CodebaseAstScanner("test_sample.py")
    scanner.visit(tree)
    assert len(scanner.issues) == 1
    assert scanner.issues[0].rule_id == "AST-002"
    assert "Mutable default argument" in scanner.issues[0].message


def test_ast_scanner_detects_wildcard_import() -> None:
    snippet = "from math import *"
    import ast

    tree = ast.parse(snippet)
    scanner = CodebaseAstScanner("sample_service.py")
    scanner.visit(tree)
    assert len(scanner.issues) == 1
    assert scanner.issues[0].rule_id == "AST-003"
    assert "Wildcard import" in scanner.issues[0].message


# ==============================================================================
# 5. Local CI Linters & Tools Execution Tests
# ==============================================================================


def test_yaml_validation_across_repository(ci_runner: LocalCiRunner) -> None:
    res = ci_runner.validate_yaml_files()
    assert res.status == "PASS"
    assert res.issue_count == 0
    assert res.details.get("files_scanned", 0) >= 14


def test_black_code_formatting_ci(ci_runner: LocalCiRunner) -> None:
    res = ci_runner.run_black(check_only=True)
    assert res.status == "PASS"
    assert res.return_code == 0


def test_flake8_clean_codebase(ci_runner: LocalCiRunner) -> None:
    res = ci_runner.run_flake8()
    assert res.status == "PASS"
    assert res.return_code == 0
    assert res.issue_count == 0


def test_mypy_static_typing_clean(ci_runner: LocalCiRunner) -> None:
    res = ci_runner.run_mypy()
    assert res.status == "PASS"
    assert res.return_code == 0


# ==============================================================================
# 6. Telemetry & Reporting Tests
# ==============================================================================


def test_ci_telemetry_prometheus_formatting() -> None:
    telem = CiTelemetry()
    telem.record_workflow_validation(is_valid=True)
    telem.record_workflow_validation(is_valid=False, error_count=2)
    telem.record_linter_run(tool="black", passed=True, duration_sec=1.2)
    telem.record_linter_run(tool="flake8", passed=False, duration_sec=3.4, issue_count=3)
    telem.record_ast_scan(files_scanned=125, issues_found=0)
    telem.record_pytest_run(passed=True, duration_sec=42.5)

    prom_text = telem.format_prometheus()
    assert "ci_workflows_validated_total 2" in prom_text
    assert "ci_workflow_validation_errors_total 2" in prom_text
    assert 'ci_linter_runs_total{tool="black"} 1' in prom_text
    assert 'ci_linter_failures_total{tool="flake8"} 1' in prom_text
    assert "ci_ast_files_scanned_total 125" in prom_text
    assert "ci_pytest_runs_total 1" in prom_text
    assert "ci_pytest_passed_total 1" in prom_text


def test_ci_run_summary_markdown_rendering() -> None:
    results = [
        LinterResult(
            tool=LinterType.BLACK,
            status="PASS",
            duration_sec=1.5,
            return_code=0,
            stdout="clean",
            stderr="",
            issue_count=0,
        ),
        LinterResult(
            tool=LinterType.FLAKE8,
            status="PASS",
            duration_sec=2.8,
            return_code=0,
            stdout="",
            stderr="",
            issue_count=0,
        ),
    ]
    summary = CiRunSummary(
        is_success=True,
        results=results,
        total_duration_sec=4.3,
        total_issues=0,
    )
    md = summary.render_markdown()
    assert "## Trading Risk Engine CI Verification [PASS]" in md
    assert "| `black` | OK `PASS` | 1.50s | 0 | 0 |" in md
    assert "| `flake8` | OK `PASS` | 2.80s | 0 | 0 |" in md


# ==============================================================================
# 7. CI CLI Integration Tests
# ==============================================================================


def test_ci_cli_validate_workflows_mode() -> None:
    exit_code = ci_cli_main(["--validate-workflows"])
    assert exit_code == 0


def test_ci_cli_ast_scan_mode() -> None:
    exit_code = ci_cli_main(["--ast-scan"])
    assert exit_code == 0


def test_ci_cli_yaml_check_mode() -> None:
    exit_code = ci_cli_main(["--yaml-check"])
    assert exit_code == 0


def test_ci_cli_export_reports() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        json_out = Path(tmp_dir) / "reports" / "ci_summary.json"
        md_out = Path(tmp_dir) / "reports" / "ci_summary.md"
        prom_out = Path(tmp_dir) / "reports" / "metrics.prom"

        exit_code = ci_cli_main(
            [
                "--validate-workflows",
                "--ast-scan",
                "--yaml-check",
                "--json-report",
                str(json_out),
                "--markdown-report",
                str(md_out),
                "--export-prometheus",
                str(prom_out),
            ]
        )
        assert exit_code == 0

        assert json_out.is_file()
        with open(json_out, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["status"] == "PASS"

        assert md_out.is_file()
        assert "Trading Risk Engine CI" in md_out.read_text(encoding="utf-8")

        assert prom_out.is_file()
        assert "ci_workflows_validated_total" in prom_out.read_text(encoding="utf-8")


def test_full_ci_runner_orchestration(ci_runner: LocalCiRunner) -> None:
    summary = ci_runner.run_all(run_pytest=False)
    assert summary.is_success
    assert summary.total_issues == 0
    assert len(summary.results) >= 5
    tool_names = {r.tool for r in summary.results}
    assert LinterType.AST_SYNTAX in tool_names
    assert LinterType.YAML_SYNTAX in tool_names
    assert LinterType.BLACK in tool_names
    assert LinterType.FLAKE8 in tool_names
    assert LinterType.MYPY in tool_names
