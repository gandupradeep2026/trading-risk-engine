"""Trading Risk Engine: End-to-End Operational Telemetry & Metrics.

Tracks real-time execution statistics, stage latencies, records processed,
reconciliation assertion outcomes, and formats Prometheus OpenMetrics text exposition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Any, Dict, List, Tuple


@dataclass
class E2ETelemetry:
    """Thread-safe operational metrics store for the End-to-End Integration Test Runner."""

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    runs_total: Dict[Tuple[str, str], int] = field(default_factory=dict)
    stage_durations: Dict[str, float] = field(default_factory=dict)
    records_processed: Dict[Tuple[str, str], int] = field(default_factory=dict)
    assertions_evaluated: Dict[Tuple[str, str], int] = field(default_factory=dict)
    reconciliation_diffs: Dict[str, float] = field(default_factory=dict)
    sla_violations_total: int = 0

    def record_run(self, scenario: str, success: bool, duration_sec: float) -> None:
        """Record overall pipeline execution run."""
        status = "SUCCESS" if success else "FAILURE"
        key = (scenario, status)
        with self._lock:
            self.runs_total[key] = self.runs_total.get(key, 0) + 1

    def record_stage(self, stage: str, duration_sec: float, records_in: int, records_out: int) -> None:
        """Record telemetry for a single completed stage."""
        with self._lock:
            self.stage_durations[stage] = duration_sec
            key_in = (stage, "records_in")
            key_out = (stage, "records_out")
            self.records_processed[key_in] = self.records_processed.get(key_in, 0) + records_in
            self.records_processed[key_out] = self.records_processed.get(key_out, 0) + records_out

    def record_assertion(self, category: str, passed: bool) -> None:
        """Record single assertion evaluation outcome."""
        outcome = "PASS" if passed else "FAIL"
        key = (category, outcome)
        with self._lock:
            self.assertions_evaluated[key] = self.assertions_evaluated.get(key, 0) + 1

    def record_reconciliation_diff(self, metric: str, diff: float) -> None:
        """Record numerical difference for reconciliation tracking."""
        with self._lock:
            self.reconciliation_diffs[metric] = diff

    def record_sla_violation(self) -> None:
        """Increment SLA violation counter."""
        with self._lock:
            self.sla_violations_total += 1

    def format_prometheus(self) -> str:
        """Format metrics into Prometheus / OpenMetrics text exposition format."""
        with self._lock:
            lines = [
                "# HELP e2e_pipeline_runs_total Total number of E2E integration test runs executed",
                "# TYPE e2e_pipeline_runs_total counter",
            ]
            for (scenario, status), count in sorted(self.runs_total.items()):
                lines.append(f'e2e_pipeline_runs_total{{scenario="{scenario}",status="{status}"}} {count}')

            lines.extend([
                "# HELP e2e_stage_duration_seconds Latency of each E2E pipeline stage in seconds",
                "# TYPE e2e_stage_duration_seconds gauge",
            ])
            for stage, dur in sorted(self.stage_durations.items()):
                lines.append(f'e2e_stage_duration_seconds{{stage="{stage}"}} {dur:.4f}')

            lines.extend([
                "# HELP e2e_records_processed_total Total records processed per stage and direction",
                "# TYPE e2e_records_processed_total counter",
            ])
            for (stage, direction), count in sorted(self.records_processed.items()):
                lines.append(f'e2e_records_processed_total{{stage="{stage}",direction="{direction}"}} {count}')

            lines.extend([
                "# HELP e2e_validation_assertions_total Total assertions evaluated by category and outcome",
                "# TYPE e2e_validation_assertions_total counter",
            ])
            for (category, outcome), count in sorted(self.assertions_evaluated.items()):
                lines.append(f'e2e_validation_assertions_total{{category="{category}",outcome="{outcome}"}} {count}')

            lines.extend([
                "# HELP e2e_sla_violations_total Total number of SLA latency violations",
                "# TYPE e2e_sla_violations_total counter",
                f"e2e_sla_violations_total {self.sla_violations_total}",
            ])

            return "\n".join(lines) + "\n"
