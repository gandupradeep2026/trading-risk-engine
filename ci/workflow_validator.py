"""Trading Risk Engine: GitHub Actions Workflow Validator & DAG Analyzer.

Validates GitHub Actions YAML specifications against enterprise platform standards,
detects cyclic job dependencies, verifies matrix strategies, and enforces security
best practices (timeouts, concurrency, least-privilege permissions).
"""

from __future__ import annotations

import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from ci.config import CiPipelineConfig, WorkflowDefinition, WorkflowParser


class WorkflowValidationError(Exception):
    """Raised when a GitHub Actions workflow violates validation rules."""

    pass


class CyclicWorkflowDependencyError(WorkflowValidationError):
    """Raised when jobs form a cyclic dependency loop."""

    pass


class MissingJobDependencyError(WorkflowValidationError):
    """Raised when a job references a non-existent upstream job in 'needs'."""

    pass


@dataclass
class JobDependencyGraph:
    """Represents the directed acyclic graph of jobs in a workflow."""

    nodes: List[str]
    edges: Dict[str, List[str]]  # node -> list of dependent downstream nodes
    in_degree: Dict[str, int]
    topological_order: List[str] = field(default_factory=list)

    @classmethod
    def from_workflow(cls, workflow: WorkflowDefinition) -> JobDependencyGraph:
        nodes = list(workflow.jobs.keys())
        edges: Dict[str, List[str]] = defaultdict(list)
        in_degree: Dict[str, int] = {node: 0 for node in nodes}

        for job_id, job in workflow.jobs.items():
            for upstream in job.needs:
                if upstream not in workflow.jobs:
                    raise MissingJobDependencyError(
                        f"Job '{job_id}' in workflow '{workflow.name}' depends on " f"unknown job '{upstream}'."
                    )
                edges[upstream].append(job_id)
                in_degree[job_id] += 1

        # Kahn's algorithm for topological sorting and cycle detection
        queue = deque([node for node in nodes if in_degree[node] == 0])
        topological_order: List[str] = []

        temp_in_degree = dict(in_degree)
        while queue:
            current = queue.popleft()
            topological_order.append(current)

            for downstream in edges.get(current, []):
                temp_in_degree[downstream] -= 1
                if temp_in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(topological_order) != len(nodes):
            unresolved = [node for node in nodes if node not in topological_order]
            raise CyclicWorkflowDependencyError(
                f"Cyclic dependency detected in workflow '{workflow.name}'. " f"Unresolved jobs: {unresolved}"
            )

        return cls(
            nodes=nodes,
            edges=dict(edges),
            in_degree=in_degree,
            topological_order=topological_order,
        )


@dataclass
class WorkflowValidationResult:
    """Validation outcome for a single workflow file."""

    file_name: str
    workflow_name: str
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    topological_order: List[str] = field(default_factory=list)
    jobs_count: int = 0
    steps_count: int = 0


@dataclass
class ValidationReport:
    """Aggregate validation report across all repository workflows."""

    is_valid: bool
    results: List[WorkflowValidationResult]
    total_workflows: int
    total_jobs: int
    total_steps: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "total_workflows": self.total_workflows,
            "total_jobs": self.total_jobs,
            "total_steps": self.total_steps,
            "errors": self.errors,
            "warnings": self.warnings,
            "results": [
                {
                    "file_name": r.file_name,
                    "workflow_name": r.workflow_name,
                    "is_valid": r.is_valid,
                    "errors": r.errors,
                    "warnings": r.warnings,
                    "topological_order": r.topological_order,
                    "jobs_count": r.jobs_count,
                    "steps_count": r.steps_count,
                }
                for r in self.results
            ],
        }


class WorkflowValidator:
    """Validates GitHub Actions workflows against enterprise production standards."""

    def __init__(self, config: Optional[CiPipelineConfig] = None) -> None:
        self.config = config or CiPipelineConfig()

    def validate_workflow_file(self, file_path: Path) -> WorkflowValidationResult:
        errors: List[str] = []
        warnings: List[str] = []
        file_name = file_path.name

        try:
            workflow = WorkflowParser.parse_file(file_path)
        except Exception as exc:
            return WorkflowValidationResult(
                file_name=file_name,
                workflow_name="Unknown",
                is_valid=False,
                errors=[f"Failed to parse workflow YAML: {exc}"],
                jobs_count=0,
                steps_count=0,
            )

        jobs_count = len(workflow.jobs)
        steps_count = sum(len(job.steps) for job in workflow.jobs.values())

        # Validate DAG & dependencies
        topological_order: List[str] = []
        try:
            graph = JobDependencyGraph.from_workflow(workflow)
            topological_order = graph.topological_order
        except WorkflowValidationError as e:
            errors.append(str(e))

        # Check concurrency controls
        if not workflow.concurrency:
            warnings.append(f"Workflow '{workflow.name}' does not specify concurrency controls (cancel-in-progress).")

        # Check permissions
        if not workflow.permissions:
            warnings.append(f"Workflow '{workflow.name}' does not declare explicit top-level permissions.")

        # Check job timeouts
        for job_id, job in workflow.jobs.items():
            if job.timeout_minutes is None:
                warnings.append(f"Job '{job_id}' in '{workflow.name}' does not specify 'timeout-minutes'.")
            elif job.timeout_minutes > self.config.max_timeout_minutes:
                errors.append(
                    f"Job '{job_id}' timeout ({job.timeout_minutes}m) exceeds "
                    f"platform ceiling ({self.config.max_timeout_minutes}m)."
                )

        # Specialized checks for primary CI workflow (ci.yml)
        if file_name == "ci.yml":
            self._validate_primary_ci_workflow(workflow, errors, warnings)

        # Verify referenced local files in workflow steps
        self._validate_referenced_files(workflow, errors)

        is_valid = len(errors) == 0
        return WorkflowValidationResult(
            file_name=file_name,
            workflow_name=workflow.name,
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            topological_order=topological_order,
            jobs_count=jobs_count,
            steps_count=steps_count,
        )

    def _validate_primary_ci_workflow(
        self, workflow: WorkflowDefinition, errors: List[str], warnings: List[str]
    ) -> None:
        # Verify required triggers
        triggers = workflow.on_triggers
        if "push" not in triggers:
            errors.append("ci.yml must trigger on 'push' events.")
        if "pull_request" not in triggers:
            errors.append("ci.yml must trigger on 'pull_request' events.")

        # Verify required jobs exist
        for required_job in self.config.required_jobs:
            if required_job not in workflow.jobs:
                errors.append(f"ci.yml missing required job: '{required_job}'")

        # Verify test matrix has required Python versions
        test_job = workflow.get_job("test-matrix")
        if test_job:
            versions = workflow.get_matrix_python_versions("test-matrix")
            for req_ver in self.config.target_python_versions:
                if req_ver not in versions:
                    errors.append(
                        f"test-matrix must include target Python version '{req_ver}'. "
                        f"Configured versions: {versions}"
                    )

        # Verify gatekeeper job depends on other jobs
        gate_job = workflow.get_job("ci-status-gate")
        if gate_job:
            other_jobs = set(self.config.required_jobs) - {"ci-status-gate"}
            missing_needs = other_jobs - set(gate_job.needs)
            if missing_needs:
                errors.append(f"ci-status-gate must depend on upstream jobs: {sorted(list(missing_needs))}")

    def _validate_referenced_files(self, workflow: WorkflowDefinition, errors: List[str]) -> None:
        repo_root = self.config.repo_root
        req_pattern = re.compile(r"pip install -r\s+([^\s\n\r&|;]+)")

        for job_id, job in workflow.jobs.items():
            for step in job.steps:
                if not step.run:
                    continue
                matches = req_pattern.findall(step.run)
                for req_file in matches:
                    clean_path = req_file.strip("\"'")
                    target = repo_root / clean_path
                    if not target.is_file():
                        errors.append(
                            f"Job '{job_id}', step '{step.name}' references non-existent "
                            f"requirements file: '{clean_path}'"
                        )

    def validate_all(self) -> ValidationReport:
        workflows_dir = self.config.workflows_dir
        if not workflows_dir.exists():
            return ValidationReport(
                is_valid=False,
                results=[],
                total_workflows=0,
                total_jobs=0,
                total_steps=0,
                errors=[f"Workflows directory does not exist: {workflows_dir}"],
                warnings=[],
            )

        workflow_files = sorted(list(workflows_dir.glob("*.yml")) + list(workflows_dir.glob("*.yaml")))
        if not workflow_files:
            return ValidationReport(
                is_valid=False,
                results=[],
                total_workflows=0,
                total_jobs=0,
                total_steps=0,
                errors=[f"No workflow YAML files found in {workflows_dir}"],
                warnings=[],
            )

        results: List[WorkflowValidationResult] = []
        all_errors: List[str] = []
        all_warnings: List[str] = []

        # Check required workflows exist
        file_names = {f.name for f in workflow_files}
        for req_wf in self.config.required_workflows:
            if req_wf not in file_names:
                all_errors.append(f"Required workflow '{req_wf}' is missing from {workflows_dir}")

        total_jobs = 0
        total_steps = 0

        for wf_file in workflow_files:
            res = self.validate_workflow_file(wf_file)
            results.append(res)
            total_jobs += res.jobs_count
            total_steps += res.steps_count
            all_errors.extend([f"[{res.file_name}] {e}" for e in res.errors])
            all_warnings.extend([f"[{res.file_name}] {w}" for w in res.warnings])

        is_valid = len(all_errors) == 0

        return ValidationReport(
            is_valid=is_valid,
            results=results,
            total_workflows=len(workflow_files),
            total_jobs=total_jobs,
            total_steps=total_steps,
            errors=all_errors,
            warnings=all_warnings,
        )
