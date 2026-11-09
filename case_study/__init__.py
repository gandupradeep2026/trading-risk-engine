"""Trading Risk Engine: Executive Portfolio Case Study Package.

Provides simulation models, quantitative risk profiling, automated containment verification,
and executive report generation for portfolio case studies.
"""

from __future__ import annotations

from case_study.config import (
    DEFAULT_PORTFOLIO_BOOKS,
    CaseStudyConfig,
    ContainmentStatus,
    IncidentScenarioSpec,
    IncidentType,
    PortfolioBookSpec,
)
from case_study.reporter import CaseStudyReportGenerator
from case_study.simulator import (
    BookRiskSnapshot,
    CaseStudySimulationResult,
    CaseStudySimulator,
    IncidentEventLog,
)
from case_study.telemetry import CaseStudyTelemetry

__all__ = [
    "CaseStudyConfig",
    "CaseStudyReportGenerator",
    "CaseStudySimulationResult",
    "CaseStudySimulator",
    "CaseStudyTelemetry",
    "ContainmentStatus",
    "DEFAULT_PORTFOLIO_BOOKS",
    "IncidentEventLog",
    "IncidentScenarioSpec",
    "IncidentType",
    "PortfolioBookSpec",
    "BookRiskSnapshot",
]
