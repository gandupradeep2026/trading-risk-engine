"""Unit and Integration Test Suite for Day 30: Executive Architecture & Portfolio Case Study.

Validates:
1. Executive README.md structure, Mermaid diagrams, benchmark tables, and case study narrative.
2. docs/PORTFOLIO_CASE_STUDY.md whitepaper completeness, mathematical models, and regulatory compliance.
3. Case study configuration models, portfolio book specs, and default thresholds.
4. Case study simulator execution: flash crash shock, rogue trader fat-finger detection, and DLQ isolation.
5. Case study report generator: Markdown executive report rendering and JSON audit export.
6. Case study telemetry store and Prometheus / OpenMetrics formatting.
7. Case study CLI runner execution across dry-run, simulation, and report export modes.
8. Packaging and public symbol exports in case_study/__init__.py.
"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
from typing import Dict
import pytest

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
from case_study.cli import main as case_study_cli_main


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture
def sample_config() -> CaseStudyConfig:
    return CaseStudyConfig()


@pytest.fixture
def sample_telemetry() -> CaseStudyTelemetry:
    return CaseStudyTelemetry()


# ==============================================================================
# 1. Executive README and Documentation Verification Tests
# ==============================================================================


def test_readme_file_exists_and_executive_structure(repo_root: Path) -> None:
    readme_path = repo_root / "README.md"
    assert readme_path.is_file(), f"Expected README.md at {readme_path}"
    content = readme_path.read_text(encoding="utf-8")

    assert len(content) > 5000, "README.md should be an exhaustive executive-grade document"

    # Core Section Headings
    assert "## 1. Executive Summary & Enterprise Value Proposition" in content
    assert "## 2. Enterprise System Architecture" in content
    assert "## 3. Layer-by-Layer Architectural Deep-Dive" in content
    assert "## 4. Performance Benchmarks & FinOps Cost Optimization" in content
    assert "## 5. Executive Portfolio Case Study: Flash Crash & Rogue Trader Incident" in content
    assert "## 6. Zero-Cost Local Emulation & Operator CLI Guide" in content
    assert "## 7. Quality Assurance & Continuous Integration" in content
    assert "## 8. Directory Layout" in content

    # Key Badges & Architecture mentions
    assert "Zero-Cost" in content
    assert "Apache Beam" in content
    assert "PySpark" in content
    assert "Databricks Medallion" in content
    assert "BigQuery" in content
    assert "dbt Core" in content


def test_readme_mermaid_diagrams_completeness(repo_root: Path) -> None:
    readme_path = repo_root / "README.md"
    content = readme_path.read_text(encoding="utf-8")

    # Verify at least 5 Mermaid diagrams are present
    mermaid_blocks = [block for block in content.split("```mermaid") if "```" in block]
    assert len(mermaid_blocks) >= 5, f"Expected at least 5 Mermaid diagrams, found {len(mermaid_blocks)}"

    # Specific diagram types
    assert "flowchart TD" in content
    assert "graph TB" in content
    assert "flowchart LR" in content
    assert "sequenceDiagram" in content


def test_readme_benchmark_and_cli_tables(repo_root: Path) -> None:
    readme_path = repo_root / "README.md"
    content = readme_path.read_text(encoding="utf-8")

    # Benchmark results table markers
    assert "`partition_pruning_fact_market_ticks`" in content
    assert "`materialized_view_market_performance`" in content
    assert "16.8x" in content
    assert "96.8%" in content

    # High-concurrency read table markers
    assert "16 Readers" in content
    assert "3,310.5 QPS" in content

    # Local CLI operator table markers
    assert "python -m ingestion.worker" in content
    assert "python -m streaming.dataflow.run_pipeline" in content
    assert "python -m dwh.bigquery.benchmark_cli" in content
    assert "python -m case_study.cli" in content


def test_portfolio_case_study_whitepaper_doc(repo_root: Path) -> None:
    doc_path = repo_root / "docs" / "PORTFOLIO_CASE_STUDY.md"
    assert doc_path.is_file(), f"Expected case study whitepaper at {doc_path}"
    content = doc_path.read_text(encoding="utf-8")

    assert len(content) > 3000
    assert "## 1. Portfolio Structure & Risk Mandate Specifications" in content
    assert "## 2. Mathematical Risk Modeling Framework" in content
    assert "## 3. Chronological Incident Timeline & Audit Trail" in content
    assert "## 4. Pre- vs. Post-Incident Quantitative Risk Profile" in content
    assert "## 5. Post-Incident Regulatory & Supervisory Reporting" in content
    assert "## 6. Business Impact & FinOps Return on Investment (ROI)" in content

    # Mathematical models
    assert "Peak-to-Trough Drawdown" in content
    assert "Parametric Value-at-Risk" in content
    assert "Expected Shortfall" in content
    assert "Bollinger Volatility Bands" in content

    # Regulatory standards
    assert "SEC Rule 15c3-5" in content
    assert "Dodd-Frank Section 731" in content
    assert "MiFID II RTS 28" in content


# ==============================================================================
# 2. Case Study Configuration Tests
# ==============================================================================


def test_portfolio_book_specs_and_defaults() -> None:
    assert len(DEFAULT_PORTFOLIO_BOOKS) == 4
    total_nav = sum(b.initial_nav_usd for b in DEFAULT_PORTFOLIO_BOOKS)
    assert total_nav == 500_000_000.0

    book_dict = DEFAULT_PORTFOLIO_BOOKS[0].to_dict()
    assert book_dict["book_id"] == "PORTFOLIO_ALPHA"
    assert book_dict["initial_nav_usd"] == 150_000_000.0
    assert "AAPL" in book_dict["primary_tickers"]


def test_case_study_config_invariants(sample_config: CaseStudyConfig) -> None:
    assert sample_config.total_initial_nav_usd == 500_000_000.0
    assert sample_config.get_book("PORTFOLIO_ALPHA") is not None
    assert sample_config.get_book("NON_EXISTENT") is None
    assert sample_config.flash_crash_drop_pct == 7.5
    assert sample_config.fat_finger_notional_usd == 6_250_000.0


def test_incident_scenario_spec_serialization() -> None:
    spec = IncidentScenarioSpec(
        incident_id="INC-TEST-001",
        incident_type=IncidentType.SINGLE_ORDER_FAT_FINGER,
        book_id="PORTFOLIO_ALPHA",
        ticker="AAPL",
        severity="CRITICAL",
        description="Rogue order testing",
        simulated_at_offset_sec=15.0,
        trigger_threshold=5_000_000.0,
        observed_value=6_250_000.0,
    )
    d = spec.to_dict()
    assert d["incident_id"] == "INC-TEST-001"
    assert d["incident_type"] == "SINGLE_ORDER_FAT_FINGER"
    assert d["observed_value"] == 6_250_000.0


# ==============================================================================
# 3. Case Study Simulator Execution Tests
# ==============================================================================


def test_case_study_simulator_run(
    sample_config: CaseStudyConfig,
    sample_telemetry: CaseStudyTelemetry,
) -> None:
    simulator = CaseStudySimulator(config=sample_config, telemetry=sample_telemetry)
    result = simulator.run_simulation()

    assert isinstance(result, CaseStudySimulationResult)
    assert result.is_success is True
    assert result.scenario_name == sample_config.scenario_name
    assert result.total_initial_nav_usd == 500_000_000.0
    assert result.total_capital_preserved_usd == 12_500_000.0
    assert result.reconciliation_passed is True
    assert result.reconciliation_delta_usd <= 0.05
    assert result.corrupt_records_isolated == 5
    assert result.clean_orders_processed == 1_250
    assert result.circuit_breakers_tripped >= 2

    # Containment latency SLA (< 250ms)
    assert result.max_containment_latency_ms <= 250.0
    assert result.p95_containment_latency_ms <= 250.0

    # Incident logs verification
    assert len(result.incident_logs) == 3
    fat_finger_log = result.incident_logs[0]
    assert fat_finger_log.incident_type == IncidentType.SINGLE_ORDER_FAT_FINGER
    assert fat_finger_log.containment_status == ContainmentStatus.CONTAINED
    assert fat_finger_log.detection_latency_ms <= 100.0

    flash_crash_log = result.incident_logs[1]
    assert flash_crash_log.incident_type == IncidentType.FLASH_CRASH_DRAWDOWN
    assert flash_crash_log.containment_status == ContainmentStatus.CONTAINED
    assert flash_crash_log.observed_value == 7.5

    poison_pill_log = result.incident_logs[2]
    assert poison_pill_log.incident_type == IncidentType.POISON_PILL_CORRUPTION
    assert poison_pill_log.observed_value == 5.0

    # Book Risk Snapshots
    assert len(result.pre_incident_snapshots) == 4
    assert len(result.post_incident_snapshots) == 4
    for book_id in ["PORTFOLIO_ALPHA", "PORTFOLIO_BETA", "PORTFOLIO_GAMMA", "PORTFOLIO_OMEGA"]:
        assert book_id in result.pre_incident_snapshots
        assert book_id in result.post_incident_snapshots
        assert result.post_incident_snapshots[book_id].is_breached is False


def test_simulation_result_serialization(sample_config: CaseStudyConfig) -> None:
    simulator = CaseStudySimulator(config=sample_config)
    result = simulator.run_simulation()

    res_dict = result.to_dict()
    assert res_dict["is_success"] is True
    assert "incident_logs" in res_dict
    assert "pre_incident_snapshots" in res_dict
    assert "post_incident_snapshots" in res_dict
    assert res_dict["total_capital_preserved_usd"] == 12_500_000.0


# ==============================================================================
# 4. Case Study Report Generator Tests
# ==============================================================================


def test_case_study_report_generator_markdown(sample_config: CaseStudyConfig) -> None:
    simulator = CaseStudySimulator(config=sample_config)
    result = simulator.run_simulation()

    markdown = CaseStudyReportGenerator.render_markdown(result)
    assert "# Portfolio Case Study: Multi-Asset Flash Crash & Rogue Trader Incident" in markdown
    assert "## 1. Incident Chronology & Containment Audit Trail" in markdown
    assert "## 2. Pre- vs. Post-Incident Quantitative Risk Matrix (Value-at-Risk)" in markdown
    assert "## 3. Business ROI & Capital Preservation Analysis" in markdown
    assert "## 4. End-to-End Data Lineage & Financial Reconciliation" in markdown
    assert "$12.50 Million USD" in markdown
    assert "PORTFOLIO_ALPHA" in markdown
    assert "CONTAINED" in markdown


def test_case_study_report_generator_json_export(sample_config: CaseStudyConfig) -> None:
    simulator = CaseStudySimulator(config=sample_config)
    result = simulator.run_simulation()

    with tempfile.TemporaryDirectory() as tmp_dir:
        json_file = Path(tmp_dir) / "audit_report.json"
        json_str = CaseStudyReportGenerator.export_json(result, output_path=json_file)

        assert json_file.is_file()
        parsed = json.loads(json_str)
        assert parsed["scenario_name"] == sample_config.scenario_name
        assert parsed["is_success"] is True
        assert len(parsed["incident_logs"]) == 3


# ==============================================================================
# 5. Case Study Telemetry & Prometheus Exposition Tests
# ==============================================================================


def test_case_study_telemetry_recording_and_prometheus() -> None:
    telem = CaseStudyTelemetry()
    telem.record_simulation(
        incidents_evaluated=3,
        incidents_contained=3,
        incidents_escalated=0,
        capital_preserved_usd=12_500_000.0,
        max_containment_latency_ms=85.0,
        reconciliation_delta_usd=0.0,
        duration_sec=0.15,
    )
    telem.record_stage_latency("market_shock", 0.045)
    telem.record_stage_latency("circuit_breaker", 0.035)

    prom_text = telem.format_prometheus()
    assert "case_study_simulations_total 1" in prom_text
    assert "case_study_incidents_contained_total 3" in prom_text
    assert "case_study_capital_preserved_usd 12500000.00" in prom_text
    assert "case_study_max_containment_latency_ms 85.00" in prom_text
    assert "case_study_reconciliation_delta_usd 0.0000" in prom_text
    assert 'case_study_stage_latency_seconds{stage="market_shock"} 0.0450' in prom_text


# ==============================================================================
# 6. Case Study CLI Runner Tests
# ==============================================================================


def test_case_study_cli_dry_run_mode(capsys: pytest.CaptureFixture[str]) -> None:
    code = case_study_cli_main(["--dry-run"])
    assert code == 0
    captured = capsys.readouterr().out
    assert "[DRY-RUN]" in captured
    assert "Total Initial NAV: $500.0M USD" in captured


def test_case_study_cli_run_scenario_mode(capsys: pytest.CaptureFixture[str]) -> None:
    code = case_study_cli_main(["--run-scenario"])
    assert code == 0
    captured = capsys.readouterr().out
    assert "Outcome: PASSED" in captured
    assert "Capital Preserved: $12.50M USD" in captured


def test_case_study_cli_report_exports() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        md_path = str(Path(tmp_dir) / "case_study.md")
        json_path = str(Path(tmp_dir) / "case_study.json")

        code = case_study_cli_main(
            [
                "--run-scenario",
                "--output-markdown",
                md_path,
                "--output-json",
                json_path,
                "--export-prometheus",
            ]
        )
        assert code == 0
        assert Path(md_path).is_file()
        assert Path(json_path).is_file()
        assert Path(md_path).read_text(encoding="utf-8").startswith("# Portfolio Case Study")


# ==============================================================================
# 7. Package Exports Verification
# ==============================================================================


def test_case_study_package_exports() -> None:
    import case_study

    assert hasattr(case_study, "CaseStudyConfig")
    assert hasattr(case_study, "CaseStudySimulator")
    assert hasattr(case_study, "CaseStudyReportGenerator")
    assert hasattr(case_study, "CaseStudyTelemetry")
    assert hasattr(case_study, "DEFAULT_PORTFOLIO_BOOKS")
    assert hasattr(case_study, "IncidentType")
    assert hasattr(case_study, "ContainmentStatus")
