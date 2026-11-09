"""Trading Risk Engine: End-to-End Integration Testing & Orchestration Package.

Exports:
- E2ERunnerConfig, PipelineStage, ExecutionScenario, StageStatus, StageExecutionResult, AssertionResult, E2EPipelineSummary
- SyntheticE2EDataBatch, SyntheticE2EGenerator
- E2EDataLineageValidator
- E2ETelemetry
- E2EIntegrationRunner
- run_e2e_cli
"""

from __future__ import annotations

from e2e.cli import run_e2e_cli
from e2e.config import (
    AssertionResult,
    E2EPipelineSummary,
    E2ERunnerConfig,
    ExecutionScenario,
    PipelineStage,
    StageExecutionResult,
    StageStatus,
)
from e2e.pipeline_runner import E2EIntegrationRunner
from e2e.synthetic_generator import SyntheticE2EDataBatch, SyntheticE2EGenerator
from e2e.telemetry import E2ETelemetry
from e2e.validator import E2EDataLineageValidator

__all__ = [
    "AssertionResult",
    "E2EDataLineageValidator",
    "E2EIntegrationRunner",
    "E2EPipelineSummary",
    "E2ERunnerConfig",
    "E2ETelemetry",
    "ExecutionScenario",
    "PipelineStage",
    "StageExecutionResult",
    "StageStatus",
    "SyntheticE2EDataBatch",
    "SyntheticE2EGenerator",
    "run_e2e_cli",
]
