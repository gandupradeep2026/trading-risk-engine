"""Trading Risk Engine: Local CI Runner & Code Quality Engine.

Provides unified orchestration for code formatting (Black), style checking (Flake8),
static type checking (Mypy), Python AST syntax and antipattern analysis,
YAML syntax verification, and Pytest test suite execution.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml


class LinterType(str, Enum):
    BLACK = "black"
    FLAKE8 = "flake8"
    MYPY = "mypy"
    PYTEST = "pytest"
    BANDIT = "bandit"
    AST_SYNTAX = "ast_syntax"
    YAML_SYNTAX = "yaml_syntax"


@dataclass
class AstIssue:
    file_path: str
    line_number: int
    rule_id: str
    message: str


@dataclass
class LinterResult:
    tool: LinterType
    status: str  # "PASS", "FAIL", "SKIPPED"
    duration_sec: float
    return_code: int
    stdout: str
    stderr: str
    issue_count: int
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


@dataclass
class CiRunSummary:
    is_success: bool
    results: List[LinterResult]
    total_duration_sec: float
    total_issues: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_success": self.is_success,
            "total_duration_sec": round(self.total_duration_sec, 3),
            "total_issues": self.total_issues,
            "tools": {
                r.tool.value: {
                    "status": r.status,
                    "duration_sec": round(r.duration_sec, 3),
                    "return_code": r.return_code,
                    "issue_count": r.issue_count,
                    "stdout_excerpt": r.stdout[:500] if r.stdout else "",
                    "stderr_excerpt": r.stderr[:500] if r.stderr else "",
                }
                for r in self.results
            },
        }

    def render_markdown(self) -> str:
        status_icon = "PASS" if self.is_success else "FAIL"
        lines = [
            f"## Trading Risk Engine CI Verification [{status_icon}]",
            "",
            f"- **Overall Status**: `{'PASSED' if self.is_success else 'FAILED'}`",
            f"- **Total Duration**: {self.total_duration_sec:.2f}s",
            f"- **Total Issues**: {self.total_issues}",
            "",
            "| Tool | Status | Duration (s) | Issues | Return Code |",
            "| :--- | :---: | :---: | :---: | :---: |",
        ]
        for r in self.results:
            icon = "OK" if r.passed else "ERR"
            lines.append(
                f"| `{r.tool.value}` | {icon} `{r.status}` | {r.duration_sec:.2f}s | {r.issue_count} | {r.return_code} |"
            )
        lines.append("")
        return "\n".join(lines)


class CodebaseAstScanner(ast.NodeVisitor):
    """Static AST visitor detecting syntax errors, bare exceptions, and mutable default arguments."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.issues: List[AstIssue] = []

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.type is None:
            self.issues.append(
                AstIssue(
                    file_path=self.file_path,
                    line_number=node.lineno,
                    rule_id="AST-001",
                    message="Bare 'except:' handler found. Use 'except Exception:' instead.",
                )
            )
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_mutable_defaults(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_mutable_defaults(node)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if not self.file_path.endswith("__init__.py"):
            for alias in node.names:
                if alias.name == "*":
                    self.issues.append(
                        AstIssue(
                            file_path=self.file_path,
                            line_number=node.lineno,
                            rule_id="AST-003",
                            message=f"Wildcard import 'from {node.module} import *' detected outside __init__.py.",
                        )
                    )
        self.generic_visit(node)

    def _check_mutable_defaults(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for default in node.args.defaults + node.args.kw_defaults:
            if default is None:
                continue
            if isinstance(default, (ast.List, ast.Dict, ast.Set)):
                self.issues.append(
                    AstIssue(
                        file_path=self.file_path,
                        line_number=default.lineno,
                        rule_id="AST-002",
                        message=f"Mutable default argument detected in function '{node.name}'.",
                    )
                )


class LocalCiRunner:
    """Orchestrates linting, AST analysis, YAML validation, and tests."""

    def __init__(self, repo_root: Optional[Path] = None) -> None:
        self.repo_root = repo_root or Path(os.getcwd())

    def scan_codebase_ast(self) -> LinterResult:
        start_time = time.perf_counter()
        py_files = sorted(
            [
                p
                for p in self.repo_root.rglob("*.py")
                if not any(part.startswith(".") or part in ("venv", ".venv", "build", "dist") for part in p.parts)
            ]
        )

        all_issues: List[AstIssue] = []
        syntax_errors = 0

        for py_path in py_files:
            rel_path = str(py_path.relative_to(self.repo_root))
            try:
                with open(py_path, "rb") as f:
                    content = f.read()
                tree = ast.parse(content, filename=str(py_path))
                scanner = CodebaseAstScanner(rel_path)
                scanner.visit(tree)
                all_issues.extend(scanner.issues)
            except SyntaxError as e:
                syntax_errors += 1
                all_issues.append(
                    AstIssue(
                        file_path=rel_path,
                        line_number=e.lineno or 0,
                        rule_id="AST-000",
                        message=f"SyntaxError: {e.msg}",
                    )
                )
            except Exception as e:
                all_issues.append(
                    AstIssue(
                        file_path=rel_path,
                        line_number=0,
                        rule_id="AST-ERR",
                        message=f"Failed to parse file: {e}",
                    )
                )

        duration = time.perf_counter() - start_time
        status = "PASS" if not all_issues else "FAIL"
        stdout_msg = f"AST scan: inspected {len(py_files)} Python files. Found {len(all_issues)} issues."

        return LinterResult(
            tool=LinterType.AST_SYNTAX,
            status=status,
            duration_sec=duration,
            return_code=0 if status == "PASS" else 1,
            stdout=stdout_msg,
            stderr="\n".join(f"{i.file_path}:{i.line_number} [{i.rule_id}] {i.message}" for i in all_issues[:20]),
            issue_count=len(all_issues),
            details={"files_scanned": len(py_files), "issues": [vars(i) for i in all_issues]},
        )

    def validate_yaml_files(self) -> LinterResult:
        start_time = time.perf_counter()
        yaml_files = sorted(
            [
                p
                for p in list(self.repo_root.rglob("*.yml")) + list(self.repo_root.rglob("*.yaml"))
                if not any(part.startswith(".") and part != ".github" for part in p.parts)
                and not any(part in ("venv", ".venv", "build", "dist") for part in p.parts)
            ]
        )

        errors: List[str] = []
        for y_path in yaml_files:
            rel_path = str(y_path.relative_to(self.repo_root))
            try:
                with open(y_path, "r", encoding="utf-8") as f:
                    yaml.safe_load(f)
            except Exception as e:
                errors.append(f"{rel_path}: {e}")

        duration = time.perf_counter() - start_time
        status = "PASS" if not errors else "FAIL"

        return LinterResult(
            tool=LinterType.YAML_SYNTAX,
            status=status,
            duration_sec=duration,
            return_code=0 if not errors else 1,
            stdout=f"Validated {len(yaml_files)} YAML files successfully.",
            stderr="\n".join(errors),
            issue_count=len(errors),
            details={"files_scanned": len(yaml_files), "errors": errors},
        )

    def run_command(self, cmd: List[str], tool: LinterType, timeout: int = 120) -> LinterResult:
        start_time = time.perf_counter()
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(self.repo_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
            )
            duration = time.perf_counter() - start_time
            status = "PASS" if proc.returncode == 0 else "FAIL"

            # Parse issue counts where applicable
            issue_count = 0
            if proc.returncode != 0:
                output_lines = [line for line in (proc.stdout + "\n" + proc.stderr).splitlines() if line.strip()]
                issue_count = max(1, len(output_lines))

            return LinterResult(
                tool=tool,
                status=status,
                duration_sec=duration,
                return_code=proc.returncode,
                stdout=proc.stdout,
                stderr=proc.stderr,
                issue_count=issue_count,
            )
        except FileNotFoundError:
            duration = time.perf_counter() - start_time
            return LinterResult(
                tool=tool,
                status="SKIPPED",
                duration_sec=duration,
                return_code=127,
                stdout="",
                stderr=f"Executable '{cmd[0]}' not found in PATH.",
                issue_count=0,
            )
        except subprocess.TimeoutExpired:
            duration = time.perf_counter() - start_time
            return LinterResult(
                tool=tool,
                status="FAIL",
                duration_sec=duration,
                return_code=124,
                stdout="",
                stderr=f"Command '{' '.join(cmd)}' timed out after {timeout} seconds.",
                issue_count=1,
            )

    def run_black(self, check_only: bool = True) -> LinterResult:
        cmd = [sys.executable, "-m", "black"]
        if check_only:
            cmd.extend(["--check", "--diff"])
        cmd.append(".")
        return self.run_command(cmd, LinterType.BLACK)

    def run_flake8(self) -> LinterResult:
        cmd = [sys.executable, "-m", "flake8", "."]
        return self.run_command(cmd, LinterType.FLAKE8)

    def run_mypy(self, targets: Optional[List[str]] = None) -> LinterResult:
        cmd = [sys.executable, "-m", "mypy"]
        if targets:
            cmd.extend(targets)
        return self.run_command(cmd, LinterType.MYPY)

    def run_pytest(
        self, test_path: str = "tests/", verbose: bool = False, max_fail: Optional[int] = None
    ) -> LinterResult:
        cmd = [sys.executable, "-m", "pytest", test_path]
        if verbose:
            cmd.append("-v")
        else:
            cmd.append("-q")
        if max_fail:
            cmd.extend(["--maxfail", str(max_fail)])
        return self.run_command(cmd, LinterType.PYTEST, timeout=300)

    def run_all(self, run_pytest: bool = False) -> CiRunSummary:
        start_time = time.perf_counter()
        results: List[LinterResult] = []

        # 1. AST syntax & hygiene
        results.append(self.scan_codebase_ast())

        # 2. YAML validity
        results.append(self.validate_yaml_files())

        # 3. Black formatting
        results.append(self.run_black(check_only=True))

        # 4. Flake8
        results.append(self.run_flake8())

        # 5. Mypy
        results.append(self.run_mypy())

        # 6. Pytest (optional during quick lint)
        if run_pytest:
            results.append(self.run_pytest())

        total_duration = time.perf_counter() - start_time
        total_issues = sum(r.issue_count for r in results)
        is_success = all(r.passed or r.status == "SKIPPED" for r in results)

        return CiRunSummary(
            is_success=is_success,
            results=results,
            total_duration_sec=total_duration,
            total_issues=total_issues,
        )
