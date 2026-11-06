"""Trading Risk Engine: Continuous Integration & Automated Verification Engine.

This module provides strongly typed data models and YAML parser for GitHub Actions
workflows, ensuring schema conformance, trigger validation, and configuration integrity.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import yaml


@dataclass
class WorkflowStep:
    """Represents a single step in a GitHub Actions workflow job."""

    name: str
    uses: Optional[str] = None
    run: Optional[str] = None
    with_params: Dict[str, Any] = field(default_factory=dict)
    env: Dict[str, str] = field(default_factory=dict)
    id: Optional[str] = None
    if_condition: Optional[str] = None
    continue_on_error: bool = False
    timeout_minutes: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> WorkflowStep:
        return cls(
            name=str(data.get("name", "Unnamed Step")),
            uses=data.get("uses"),
            run=data.get("run"),
            with_params=data.get("with", {}),
            env=data.get("env", {}),
            id=data.get("id"),
            if_condition=data.get("if"),
            continue_on_error=bool(data.get("continue-on-error", False)),
            timeout_minutes=data.get("timeout-minutes"),
        )


@dataclass
class WorkflowJob:
    """Represents a job in a GitHub Actions workflow."""

    job_id: str
    name: str
    runs_on: Union[str, List[str]]
    steps: List[WorkflowStep]
    needs: List[str] = field(default_factory=list)
    strategy: Optional[Dict[str, Any]] = None
    timeout_minutes: Optional[int] = None
    if_condition: Optional[str] = None
    concurrency: Optional[Any] = None
    permissions: Optional[Dict[str, str]] = None
    outputs: Optional[Dict[str, str]] = None

    @classmethod
    def from_dict(cls, job_id: str, data: Dict[str, Any]) -> WorkflowJob:
        raw_needs = data.get("needs", [])
        if isinstance(raw_needs, str):
            needs = [raw_needs]
        elif isinstance(raw_needs, list):
            needs = [str(n) for n in raw_needs]
        else:
            needs = []

        raw_steps = data.get("steps", [])
        steps = [WorkflowStep.from_dict(s) for s in raw_steps if isinstance(s, dict)]

        return cls(
            job_id=job_id,
            name=str(data.get("name", job_id)),
            runs_on=data.get("runs-on", "ubuntu-latest"),
            steps=steps,
            needs=needs,
            strategy=data.get("strategy"),
            timeout_minutes=data.get("timeout-minutes"),
            if_condition=data.get("if"),
            concurrency=data.get("concurrency"),
            permissions=data.get("permissions"),
            outputs=data.get("outputs"),
        )


@dataclass
class WorkflowDefinition:
    """Represents a fully parsed GitHub Actions workflow document."""

    name: str
    on_triggers: Dict[str, Any]
    jobs: Dict[str, WorkflowJob]
    file_path: str = ""
    env: Dict[str, str] = field(default_factory=dict)
    concurrency: Optional[Any] = None
    permissions: Optional[Dict[str, str]] = None

    def get_job(self, job_id: str) -> Optional[WorkflowJob]:
        return self.jobs.get(job_id)

    def job_ids(self) -> List[str]:
        return list(self.jobs.keys())

    def get_matrix_python_versions(self, job_id: str) -> List[str]:
        job = self.get_job(job_id)
        if not job or not job.strategy:
            return []
        matrix = job.strategy.get("matrix", {})
        versions = matrix.get("python-version", [])
        if isinstance(versions, list):
            return [str(v) for v in versions]
        return []


class WorkflowParser:
    """Parses and normalizes GitHub Actions YAML workflow files."""

    @staticmethod
    def parse_file(file_path: Union[str, Path]) -> WorkflowDefinition:
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Workflow file does not exist: {path}")

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        return WorkflowParser.parse_yaml_str(content, file_path=str(path))

    @staticmethod
    def parse_yaml_str(content: str, file_path: str = "") -> WorkflowDefinition:
        try:
            data = yaml.safe_load(content)
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid YAML syntax in {file_path}: {exc}") from exc

        if not isinstance(data, dict):
            raise ValueError(f"Workflow root in {file_path} must be a YAML mapping")

        # Handle PyYAML parsing 'on:' as boolean True if unquoted
        on_triggers: Dict[str, Any] = {}
        if "on" in data:
            raw_on = data["on"]
        elif True in data:
            raw_on = data[True]
        else:
            raw_on = {}

        if isinstance(raw_on, str):
            on_triggers = {raw_on: {}}
        elif isinstance(raw_on, list):
            on_triggers = {str(k): {} for k in raw_on}
        elif isinstance(raw_on, dict):
            on_triggers = raw_on
        else:
            on_triggers = {}

        raw_jobs = data.get("jobs", {})
        if not isinstance(raw_jobs, dict):
            raise ValueError(f"'jobs' in {file_path} must be a dictionary")

        jobs = {
            job_id: WorkflowJob.from_dict(job_id, job_data)
            for job_id, job_data in raw_jobs.items()
            if isinstance(job_data, dict)
        }

        return WorkflowDefinition(
            name=str(data.get("name", Path(file_path).stem if file_path else "Workflow")),
            on_triggers=on_triggers,
            jobs=jobs,
            file_path=file_path,
            env=data.get("env", {}) if isinstance(data.get("env"), dict) else {},
            concurrency=data.get("concurrency"),
            permissions=data.get("permissions") if isinstance(data.get("permissions"), dict) else None,
        )


@dataclass
class CiPipelineConfig:
    """Enterprise platform CI configuration and standards specification."""

    repo_root: Path = field(default_factory=lambda: Path(os.getcwd()))
    workflows_dir: Path = field(default_factory=lambda: Path(os.getcwd()) / ".github" / "workflows")
    required_workflows: List[str] = field(default_factory=lambda: ["ci.yml"])
    required_jobs: List[str] = field(
        default_factory=lambda: [
            "lint-and-format",
            "security-scan",
            "terraform-validate",
            "test-matrix",
            "ci-status-gate",
        ]
    )
    target_python_versions: List[str] = field(default_factory=lambda: ["3.11", "3.12"])
    max_timeout_minutes: int = 45
    min_timeout_minutes: int = 5
