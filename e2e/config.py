"""Trading Risk Engine: End-to-End Integration Test Runner Configuration.

Defines:
- PipelineStage: Discrete stages of the trading risk platform lifecycle.
- ExecutionScenario: Pre-configured end-to-end execution scenarios.
- StageStatus: Execution lifecycle status for individual pipeline stages.
- StageExecutionResult: Metrics, status, and duration for each executed stage.
- AssertionResult: Formal record for end-to-end data lineage and integrity checks.
- E2ERunnerConfig: Master configuration model for orchestrating end-to-end integration runs.
- E2EPipelineSummary: Comprehensive execution and verification report with Markdown export.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union


class PipelineStage(str, Enum):
    """Discrete architectural stages in the Trading Risk Lakehouse platform."""

    INGESTION = "ingestion"
    STREAMING = "streaming"
    LAKEHOUSE_BATCH = "lakehouse_batch"
    DATA_QUALITY = "data_quality"
    DWH_REPLICATION = "dwh_replication"
    DBT_ANALYTICS = "dbt_analytics"
    MONITORING_OBSERVABILITY = "monitoring_observability"


class ExecutionScenario(str, Enum):
    """Predefined end-to-end integration testing scenarios."""

    HAPPY_PATH = "happy_path"
    ANOMALY_INJECTION = "anomaly_injection"
    CORRUPTED_DLQ = "corrupted_dlq"
    FULL_E2E = "full_e2e"


class StageStatus(str, Enum):
    """Execution status for an individual pipeline stage."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class StageExecutionResult:
    """Execution telemetry and outcome for a single pipeline stage."""

    stage: PipelineStage
    status: StageStatus = StageStatus.PENDING
    duration_sec: float = 0.0
    records_in: int = 0
    records_out: int = 0
    metrics: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.status == StageStatus.PASSED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.stage.value,
            "status": self.status.value,
            "duration_sec": round(self.duration_sec, 4),
            "records_in": self.records_in,
            "records_out": self.records_out,
            "metrics": self.metrics,
            "error_message": self.error_message,
        }


@dataclass
class AssertionResult:
    """Outcome of an end-to-end data integrity or lineage reconciliation rule."""

    name: str
    category: str  # e.g., RECORD_CONSERVATION, NOTIONAL_RECONCILIATION, ANOMALY_DETECTION, DLQ_INTEGRITY
    passed: bool
    expected: Any
    actual: Any
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "passed": self.passed,
            "expected": self.expected,
            "actual": self.actual,
            "message": self.message,
        }


@dataclass
class E2ERunnerConfig:
    """Master configuration for orchestrating the end-to-end integration test runner."""

    environment: str = "dev"
    project_id: str = "trading-risk-engine-dev"
    target_date: str = field(default_factory=lambda: date.today().isoformat())
    scenario: ExecutionScenario = ExecutionScenario.FULL_E2E
    stages: List[PipelineStage] = field(
        default_factory=lambda: [
            PipelineStage.INGESTION,
            PipelineStage.STREAMING,
            PipelineStage.LAKEHOUSE_BATCH,
            PipelineStage.DATA_QUALITY,
            PipelineStage.DWH_REPLICATION,
            PipelineStage.DBT_ANALYTICS,
            PipelineStage.MONITORING_OBSERVABILITY,
        ]
    )
    scale_factor: float = 1.0
    inject_flash_crash: bool = True
    inject_fat_finger: bool = True
    inject_corrupted_orders: bool = True
    inject_corrupted_ticks: bool = True
    work_dir: Optional[Path] = None
    sla_total_seconds: float = 60.0
    sla_stage_seconds: float = 15.0
    tolerance_usd: float = 0.05
    validate_lineage: bool = True
    export_prometheus: bool = True

    @classmethod
    def from_env(cls) -> E2ERunnerConfig:
        """Create configuration instance from environment variables with safe defaults."""
        scenario_str = os.getenv("E2E_SCENARIO", "full_e2e").lower()
        try:
            scenario = ExecutionScenario(scenario_str)
        except ValueError:
            scenario = ExecutionScenario.FULL_E2E

        scale = float(os.getenv("E2E_SCALE_FACTOR", "1.0"))
        env = os.getenv("ENVIRONMENT", "dev")
        proj = os.getenv("GCP_PROJECT_ID", f"trading-risk-engine-{env}")
        t_date = os.getenv("E2E_TARGET_DATE", date.today().isoformat())

        return cls(
            environment=env,
            project_id=proj,
            target_date=t_date,
            scenario=scenario,
            scale_factor=scale,
            inject_flash_crash=os.getenv("E2E_INJECT_FLASH_CRASH", "true").lower() in ("true", "1"),
            inject_fat_finger=os.getenv("E2E_INJECT_FAT_FINGER", "true").lower() in ("true", "1"),
            inject_corrupted_orders=os.getenv("E2E_INJECT_CORRUPTED_ORDERS", "true").lower() in ("true", "1"),
            inject_corrupted_ticks=os.getenv("E2E_INJECT_CORRUPTED_TICKS", "true").lower() in ("true", "1"),
            sla_total_seconds=float(os.getenv("E2E_SLA_TOTAL_SECONDS", "60.0")),
            sla_stage_seconds=float(os.getenv("E2E_SLA_STAGE_SECONDS", "15.0")),
        )


@dataclass
class E2EPipelineSummary:
    """Comprehensive summary report of the end-to-end integration test execution."""

    scenario: ExecutionScenario
    environment: str
    target_date: str
    success: bool
    total_duration_sec: float
    stage_results: Dict[str, StageExecutionResult] = field(default_factory=dict)
    validation_results: List[AssertionResult] = field(default_factory=list)
    reconciliation_metrics: Dict[str, Any] = field(default_factory=dict)

    @property
    def passed_assertions(self) -> int:
        return sum(1 for a in self.validation_results if a.passed)

    @property
    def failed_assertions(self) -> int:
        return sum(1 for a in self.validation_results if not a.passed)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scenario": self.scenario.value,
            "environment": self.environment,
            "target_date": self.target_date,
            "success": self.success,
            "total_duration_sec": round(self.total_duration_sec, 4),
            "stages": {name: res.to_dict() for name, res in self.stage_results.items()},
            "validations": {
                "total": len(self.validation_results),
                "passed": self.passed_assertions,
                "failed": self.failed_assertions,
                "details": [a.to_dict() for a in self.validation_results],
            },
            "reconciliation_metrics": self.reconciliation_metrics,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_markdown(self) -> str:
        """Render GitHub-Flavored Markdown summary report."""
        status_badge = "✅ PASSED" if self.success else "❌ FAILED"
        lines = [
            "# Trading Risk Engine - End-to-End Integration Test Report",
            "",
            f"**Execution Status**: {status_badge}  ",
            f"**Scenario**: `{self.scenario.value}`  ",
            f"**Environment**: `{self.environment}`  ",
            f"**Valuation Date**: `{self.target_date}`  ",
            f"**Total Duration**: `{self.total_duration_sec:.3f}s`  ",
            f"**Assertions Passed**: `{self.passed_assertions}/{len(self.validation_results)}`  ",
            "",
            "---",
            "",
            "## 1. Stage Execution Summary",
            "",
            "| Stage | Status | Duration (s) | In | Out | Key Metric |",
            "| :--- | :--- | :---: | :---: | :---: | :--- |",
        ]

        for stage_name, res in self.stage_results.items():
            st_icon = "✅" if res.passed else ("⏭️" if res.status == StageStatus.SKIPPED else "❌")
            metric_str = ", ".join(f"{k}={v}" for k, v in list(res.metrics.items())[:2]) or "OK"
            lines.append(
                f"| `{stage_name}` | {st_icon} {res.status.value} | {res.duration_sec:.3f}s | "
                f"{res.records_in} | {res.records_out} | {metric_str} |"
            )

        lines.extend([
            "",
            "## 2. End-to-End Lineage & Reconciliation Invariants",
            "",
            "| Assertion Rule | Category | Expected | Actual | Outcome |",
            "| :--- | :--- | :---: | :---: | :---: |",
        ])

        for a in self.validation_results:
            outcome = "✅ PASS" if a.passed else "❌ FAIL"
            lines.append(
                f"| `{a.name}` | `{a.category}` | `{a.expected}` | `{a.actual}` | {outcome} |"
            )

        if self.reconciliation_metrics:
            lines.extend([
                "",
                "## 3. Financial Reconciliations",
                "",
                "| Metric | Value |",
                "| :--- | :--- |",
            ])
            for k, v in self.reconciliation_metrics.items():
                if isinstance(v, float):
                    lines.append(f"| `{k}` | `${v:,.2f}` |" if "notional" in k or "usd" in k else f"| `{k}` | `{v:.4f}` |")
                else:
                    lines.append(f"| `{k}` | `{v}` |")

        lines.append("")
        return "\n".join(lines)
