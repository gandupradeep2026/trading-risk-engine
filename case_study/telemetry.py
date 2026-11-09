"""Trading Risk Engine: Case Study Telemetry & Metrics.

Provides thread-safe operational metrics collection and Prometheus exposition
for the Executive Portfolio Case Study simulation and reporting engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Dict


@dataclass
class CaseStudyTelemetry:
    """Thread-safe operational telemetry for case study simulations."""

    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    simulations_executed: int = 0
    incidents_evaluated: int = 0
    incidents_contained: int = 0
    incidents_escalated: int = 0
    total_capital_preserved_usd: float = 0.0
    max_containment_latency_ms: float = 0.0
    last_reconciliation_delta_usd: float = 0.0
    stage_latencies_sec: Dict[str, float] = field(default_factory=dict)

    def record_simulation(
        self,
        incidents_evaluated: int,
        incidents_contained: int,
        incidents_escalated: int,
        capital_preserved_usd: float,
        max_containment_latency_ms: float,
        reconciliation_delta_usd: float,
        duration_sec: float,
    ) -> None:
        with self._lock:
            self.simulations_executed += 1
            self.incidents_evaluated += incidents_evaluated
            self.incidents_contained += incidents_contained
            self.incidents_escalated += incidents_escalated
            self.total_capital_preserved_usd += capital_preserved_usd
            if max_containment_latency_ms > self.max_containment_latency_ms:
                self.max_containment_latency_ms = max_containment_latency_ms
            self.last_reconciliation_delta_usd = reconciliation_delta_usd
            self.stage_latencies_sec["total_simulation"] = duration_sec

    def record_stage_latency(self, stage_name: str, latency_sec: float) -> None:
        with self._lock:
            self.stage_latencies_sec[stage_name] = latency_sec

    def format_prometheus(self) -> str:
        """Render metrics in standard Prometheus / OpenMetrics text exposition format."""
        with self._lock:
            lines = [
                "# HELP case_study_simulations_total Total number of case study simulations executed.",
                "# TYPE case_study_simulations_total counter",
                f"case_study_simulations_total {self.simulations_executed}",
                "",
                "# HELP case_study_incidents_evaluated_total Total risk incidents evaluated in case studies.",
                "# TYPE case_study_incidents_evaluated_total counter",
                f"case_study_incidents_evaluated_total {self.incidents_evaluated}",
                "",
                "# HELP case_study_incidents_contained_total Total risk incidents automatically contained.",
                "# TYPE case_study_incidents_contained_total counter",
                f"case_study_incidents_contained_total {self.incidents_contained}",
                "",
                "# HELP case_study_incidents_escalated_total Total risk incidents escalated for intervention.",
                "# TYPE case_study_incidents_escalated_total counter",
                f"case_study_incidents_escalated_total {self.incidents_escalated}",
                "",
                "# HELP case_study_capital_preserved_usd Estimated portfolio capital preserved by containment.",
                "# TYPE case_study_capital_preserved_usd gauge",
                f"case_study_capital_preserved_usd {self.total_capital_preserved_usd:.2f}",
                "",
                "# HELP case_study_max_containment_latency_ms Maximum containment latency in milliseconds.",
                "# TYPE case_study_max_containment_latency_ms gauge",
                f"case_study_max_containment_latency_ms {self.max_containment_latency_ms:.2f}",
                "",
                "# HELP case_study_reconciliation_delta_usd Absolute reconciliation delta across data tiers.",
                "# TYPE case_study_reconciliation_delta_usd gauge",
                f"case_study_reconciliation_delta_usd {self.last_reconciliation_delta_usd:.4f}",
            ]

            for stage, lat in sorted(self.stage_latencies_sec.items()):
                lines.extend(
                    [
                        f'# HELP case_study_stage_latency_seconds Latency for stage {stage}.',
                        "# TYPE case_study_stage_latency_seconds gauge",
                        f'case_study_stage_latency_seconds{{stage="{stage}"}} {lat:.4f}',
                    ]
                )

            return "\n".join(lines) + "\n"
