"""Zero-Cost Unit & Integration Test Suite for Performance Benchmarks & BigQuery Cost Optimization (Day 26).

Validates:
1. Benchmark Catalog & Scenarios: All canonical scenarios, categories, query templates, and baselines.
2. Metrics & Statistical Derivations: Percentiles (p50, p90, p95, p99), stddev, throughput QPS,
   speedup ratios, byte savings, latency histograms, and Prometheus telemetry.
3. Local Benchmark Runner Execution: Partition pruning acceleration, clustering efficiency,
   Materialized View query acceleration, 4-table Star-Schema analytical joins, and multi-threaded
   reader concurrency testing.
4. BigQuery Pricing Model: On-Demand ($6.25/TB), Editions (Standard, Enterprise, Enterprise Plus),
   Active/Long-Term Logical and Physical storage models, and Materialized View ROI equations.
5. Query Antipattern Analyzer: Static detection of SELECT *, missing partition filters, unclustered
   lookups, Cartesian cross joins, unbounded ORDER BY, leading wildcards, and non-sargiable functions.
6. Budget Guardrail Manager: maximum_bytes_billed enforcement, daily/monthly spend tracking, and alerts.
7. Runaway Query Incident Remediator: Emergency bq cancel commands and INFORMATION_SCHEMA diagnostic SQL.
8. CLI Runners: Benchmark and Cost optimization CLI command suites.
9. Runbook & Documentation Integrity: Markdown formatting, Mermaid diagrams, and GitHub alerts.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Dict, List
import pytest

from dwh.bigquery.benchmark import (
    BENCHMARK_SCENARIOS_CATALOG,
    BenchmarkCategory,
    BenchmarkMetricResult,
    BenchmarkRunner,
    BenchmarkScenario,
    BenchmarkSuiteResult,
    BenchmarkTelemetry,
)
from dwh.bigquery.benchmark_cli import run_benchmark_cli
from dwh.bigquery.cost_optimizer import (
    AntipatternFinding,
    BigQueryPricingModel,
    BudgetCheckResult,
    BudgetGuardrailManager,
    BudgetPolicy,
    CostEstimationResult,
    FindingSeverity,
    PricingTier,
    QueryAntipatternAnalyzer,
    RunawayQueryRemediator,
    StorageBillingModel,
)
from dwh.bigquery.cost_cli import run_cost_cli
from dwh.bigquery.emulator import LocalBigQueryEmulator


# =============================================================================
# 1. BENCHMARK CATALOG & SCENARIO SPECIFICATIONS
# =============================================================================

class TestBenchmarkCatalogAndScenarios:
    """Validate completeness and schema integrity of the canonical benchmark catalog."""

    def test_canonical_catalog_contains_required_scenarios(self):
        required_scenarios = {
            "partition_pruning_fact_market_ticks",
            "clustering_fact_trade_orders",
            "materialized_view_market_performance",
            "materialized_view_trader_exposure",
            "star_schema_multidimensional_turnover",
            "hourly_microstructure_ohlcv_vwap",
            "gold_portfolio_var_compliance",
            "federated_vs_replicated_alerts",
        }
        assert required_scenarios.issubset(set(BENCHMARK_SCENARIOS_CATALOG.keys()))

    def test_all_scenarios_have_valid_attributes(self):
        for name, scen in BENCHMARK_SCENARIOS_CATALOG.items():
            assert scen.name == name
            assert isinstance(scen.category, BenchmarkCategory)
            assert len(scen.description.strip()) > 10
            assert "${dataset}" in scen.query_template or "SELECT" in scen.query_template
            assert scen.default_iterations >= 1
            assert scen.warmup_runs >= 1

    def test_comparative_scenarios_define_baselines(self):
        comparative = [
            "partition_pruning_fact_market_ticks",
            "clustering_fact_trade_orders",
            "materialized_view_market_performance",
            "materialized_view_trader_exposure",
            "federated_vs_replicated_alerts",
        ]
        for name in comparative:
            scen = BENCHMARK_SCENARIOS_CATALOG[name]
            assert scen.baseline_query_template is not None
            assert len(scen.baseline_query_template.strip()) > 0


# =============================================================================
# 2. METRICS & STATISTICAL DERIVATIONS
# =============================================================================

class TestBenchmarkMetricsAndStatistics:
    """Validate statistical derivations, latency distributions, and exports."""

    @pytest.fixture
    def sample_metric_result(self) -> BenchmarkMetricResult:
        durations = [1.2, 1.5, 1.8, 2.0, 2.5]
        return BenchmarkMetricResult(
            scenario_name="test_scenario",
            category=BenchmarkCategory.PARTITION_PRUNING.value,
            description="Testing metrics calculation",
            iterations=len(durations),
            durations_ms=durations,
            p50_ms=1.8,
            p90_ms=2.5,
            p95_ms=2.5,
            p99_ms=2.5,
            min_ms=1.2,
            max_ms=2.5,
            avg_ms=1.8,
            stddev_ms=0.48,
            throughput_qps=555.5,
            bytes_scanned_bytes=20480,
            simulated_cost_usd=0.000128,
            baseline_p50_ms=9.0,
            baseline_avg_ms=9.0,
            baseline_bytes_scanned=204800,
            speedup_ratio=5.0,
            bytes_savings_pct=90.0,
            histogram_buckets={"1ms - 5ms": 5},
        )

    def test_metric_result_serialization_to_dict(self, sample_metric_result):
        d = sample_metric_result.to_dict()
        assert d["scenario_name"] == "test_scenario"
        assert d["p50_ms"] == 1.8
        assert d["speedup_ratio"] == 5.0
        assert d["bytes_savings_pct"] == 90.0
        assert d["throughput_qps"] == 555.5
        assert isinstance(d["durations_ms"], list)

    def test_suite_result_serialization_and_markdown(self, sample_metric_result):
        suite = BenchmarkSuiteResult(
            timestamp=datetime.now(timezone.utc).isoformat(),
            environment="dev",
            scale_factor=1.0,
            total_queries_executed=5,
            total_duration_seconds=0.009,
            scenarios={"test_scenario": sample_metric_result},
            system_info={"engine": "DuckDB"},
        )
        s_dict = suite.to_dict()
        assert s_dict["environment"] == "dev"
        assert "test_scenario" in s_dict["scenarios"]

        md = suite.to_markdown()
        assert "# BigQuery Performance Benchmark Report" in md
        assert "| `test_scenario` |" in md
        assert "**5.0x**" in md
        assert "**90.0%**" in md

    def test_benchmark_telemetry_prometheus_exposition(self):
        telemetry = BenchmarkTelemetry()
        telemetry.record_run(queries=10, bytes_scanned=1048576, duration_ms=250.0)
        telemetry.record_run(queries=5, bytes_scanned=524288, duration_ms=120.0)

        assert telemetry.scenarios_executed == 2
        assert telemetry.total_queries == 15
        assert telemetry.total_bytes_scanned == 1572864

        prom = telemetry.format_prometheus()
        assert "bigquery_benchmark_scenarios_executed_total 2" in prom
        assert "bigquery_benchmark_queries_total 15" in prom
        assert "bigquery_benchmark_bytes_scanned_bytes 1572864" in prom


# =============================================================================
# 3. BENCHMARK RUNNER LOCAL EXECUTION
# =============================================================================

class TestBenchmarkRunnerExecution:
    """Validate local execution of benchmark scenarios against DuckDB BigQuery emulator."""

    @pytest.fixture(scope="class")
    def runner(self) -> BenchmarkRunner:
        runner = BenchmarkRunner(
            dataset_id="trading_risk_dwh_dev",
            project_id="trading-risk-engine-dev",
        )
        runner.setup_benchmark_data(
            scale_factor=0.2,
            seed_dimensions=True,
            num_ticks_per_day=30,
            num_orders_per_day=15,
            days=10,
        )
        return runner

    def test_setup_benchmark_data_populates_all_tables(self, runner):
        assert runner._is_setup is True
        assert runner.emulator.get_table_row_count("dim_tickers") >= 17
        assert runner.emulator.get_table_row_count("dim_portfolios") >= 5
        assert runner.emulator.get_table_row_count("fact_market_ticks") > 0
        assert runner.emulator.get_table_row_count("fact_trade_orders") > 0

    def test_run_partition_pruning_scenario(self, runner):
        scen = BENCHMARK_SCENARIOS_CATALOG["partition_pruning_fact_market_ticks"]
        result = runner.run_scenario(scen, iterations=3, warmup_runs=1)

        assert result.scenario_name == scen.name
        assert result.category == BenchmarkCategory.PARTITION_PRUNING.value
        assert result.iterations == 3
        assert result.p50_ms > 0
        assert result.throughput_qps > 0
        assert result.bytes_scanned_bytes > 0
        # Pruned query should scan fewer bytes than multi-day baseline
        if result.baseline_bytes_scanned is not None:
            assert result.baseline_bytes_scanned >= result.bytes_scanned_bytes

    def test_run_clustering_scenario(self, runner):
        scen = BENCHMARK_SCENARIOS_CATALOG["clustering_fact_trade_orders"]
        result = runner.run_scenario(scen, iterations=3, warmup_runs=1)

        assert result.scenario_name == scen.name
        assert result.category == BenchmarkCategory.CLUSTERING.value
        assert result.p50_ms > 0
        assert result.bytes_scanned_bytes > 0

    def test_run_materialized_view_market_performance_scenario(self, runner):
        scen = BENCHMARK_SCENARIOS_CATALOG["materialized_view_market_performance"]
        result = runner.run_scenario(scen, iterations=3, warmup_runs=1)

        assert result.scenario_name == scen.name
        assert result.category == BenchmarkCategory.MATERIALIZED_VIEW.value
        assert result.p50_ms > 0
        assert result.baseline_p50_ms is not None

    def test_run_star_schema_multidimensional_turnover_scenario(self, runner):
        scen = BENCHMARK_SCENARIOS_CATALOG["star_schema_multidimensional_turnover"]
        result = runner.run_scenario(scen, iterations=3, warmup_runs=1)

        assert result.scenario_name == scen.name
        assert result.category == BenchmarkCategory.STAR_SCHEMA_JOIN.value
        assert result.p50_ms > 0

    def test_run_suite_execution(self, runner):
        selected = [
            "clustering_fact_trade_orders",
            "materialized_view_trader_exposure",
        ]
        suite_res = runner.run_suite(scenario_names=selected, iterations=2, scale_factor=0.2)

        assert suite_res.total_queries_executed == 4
        assert len(suite_res.scenarios) == 2
        assert "clustering_fact_trade_orders" in suite_res.scenarios
        assert "materialized_view_trader_exposure" in suite_res.scenarios
        assert suite_res.total_duration_seconds > 0

    def test_run_concurrency_test(self, runner):
        scen = BENCHMARK_SCENARIOS_CATALOG["clustering_fact_trade_orders"]
        concurrency_levels = [1, 2, 4]
        res = runner.run_concurrency_test(scen, concurrency_levels=concurrency_levels, iterations_per_thread=2)

        assert 1 in res
        assert 2 in res
        assert 4 in res
        assert res[1]["total_queries"] == 2
        assert res[2]["total_queries"] == 4
        assert res[4]["total_queries"] == 8
        assert res[4]["aggregate_qps"] > 0


# =============================================================================
# 4. BIGQUERY PRICING MODEL & FINOPS CALCULATOR
# =============================================================================

class TestBigQueryPricingModel:
    """Validate mathematical accuracy of BigQuery pricing calculations."""

    def test_on_demand_query_cost_calculation(self):
        # 1 TB scanned should equal exactly $6.25
        one_tb = 1024 ** 4
        cost = BigQueryPricingModel.calculate_query_cost(one_tb, pricing_tier=PricingTier.ON_DEMAND)
        assert pytest.approx(cost, rel=1e-4) == 6.25

        # 10 GB scanned
        ten_gb = 10 * (1024 ** 3)
        cost_10gb = BigQueryPricingModel.calculate_query_cost(ten_gb, pricing_tier=PricingTier.ON_DEMAND)
        assert pytest.approx(cost_10gb, rel=1e-3) == (10 / 1024) * 6.25

    def test_editions_slot_hour_pricing(self):
        # 3600 seconds with 100 slots = 100 slot-hours
        slot_ms = 100 * 3600 * 1000
        cost_std = BigQueryPricingModel.calculate_query_cost(0, pricing_tier=PricingTier.EDITION_STANDARD, slot_ms=slot_ms)
        cost_ent = BigQueryPricingModel.calculate_query_cost(0, pricing_tier=PricingTier.EDITION_ENTERPRISE, slot_ms=slot_ms)
        cost_plus = BigQueryPricingModel.calculate_query_cost(0, pricing_tier=PricingTier.EDITION_ENTERPRISE_PLUS, slot_ms=slot_ms)

        assert pytest.approx(cost_std, rel=1e-4) == 100 * 0.040
        assert pytest.approx(cost_ent, rel=1e-4) == 100 * 0.060
        assert pytest.approx(cost_plus, rel=1e-4) == 100 * 0.100

    def test_logical_and_physical_storage_cost(self):
        # 100 GB storage
        bytes_100gb = 100 * (1024 ** 3)

        # Active logical: 100 * $0.02 = $2.00
        active_log = BigQueryPricingModel.calculate_storage_cost(bytes_100gb, is_long_term=False, storage_billing_model=StorageBillingModel.LOGICAL)
        assert pytest.approx(active_log, rel=1e-4) == 2.00

        # Long-term logical (50% discount): 100 * $0.01 = $1.00
        lt_log = BigQueryPricingModel.calculate_storage_cost(bytes_100gb, is_long_term=True, storage_billing_model=StorageBillingModel.LOGICAL)
        assert pytest.approx(lt_log, rel=1e-4) == 1.00

        # Active physical: 100 * $0.04 = $4.00
        active_phy = BigQueryPricingModel.calculate_storage_cost(bytes_100gb, is_long_term=False, storage_billing_model=StorageBillingModel.PHYSICAL)
        assert pytest.approx(active_phy, rel=1e-4) == 4.00

    def test_partition_pruning_savings_calculation(self):
        total_bytes = 1000 * (1024 ** 3)  # 1 TB table
        pruned_bytes = 30 * (1024 ** 3)    # 30 GB day partition
        savings = BigQueryPricingModel.calculate_partition_pruning_savings(
            total_table_bytes=total_bytes,
            pruned_bytes_scanned=pruned_bytes,
            query_frequency_per_day=50,
        )

        assert savings["savings_pct"] == 97.0
        assert savings["cost_unpruned_usd"] > savings["cost_pruned_usd"]
        assert savings["projected_monthly_savings_usd"] > 0

    def test_materialized_view_roi_calculation(self):
        raw_bytes = 50 * (1024 ** 3)   # 50 GB per raw query
        mv_bytes = 100 * (1024 ** 2)   # 100 MB per MV query
        roi = BigQueryPricingModel.calculate_materialized_view_roi(
            raw_query_bytes=raw_bytes,
            mv_query_bytes=mv_bytes,
            queries_per_day=200,
        )

        assert roi["is_cost_effective"] is True
        assert roi["monthly_net_savings_usd"] > 0
        assert roi["breakeven_queries_per_day"] > 0


# =============================================================================
# 5. QUERY ANTIPATTERN ANALYZER
# =============================================================================

class TestQueryAntipatternAnalyzer:
    """Validate detection of performance and cost antipatterns in SQL."""

    @pytest.fixture
    def analyzer(self) -> QueryAntipatternAnalyzer:
        return QueryAntipatternAnalyzer()

    def test_detect_select_star_antipattern(self, analyzer):
        sql = "SELECT * FROM fact_trade_orders WHERE trade_date = '2026-03-01';"
        findings = analyzer.analyze(sql)

        rule_names = [f.rule_name for f in findings]
        assert "SELECT_STAR_PROJECTION" in rule_names
        finding = next(f for f in findings if f.rule_name == "SELECT_STAR_PROJECTION")
        assert finding.severity == FindingSeverity.CRITICAL
        assert finding.estimated_scan_reduction_pct > 50.0

    def test_detect_missing_partition_filter(self, analyzer):
        sql = "SELECT order_id, ticker, notional_usd FROM fact_trade_orders WHERE portfolio_id = 'PORTFOLIO_ALPHA';"
        findings = analyzer.analyze(sql)

        rule_names = [f.rule_name for f in findings]
        assert "MISSING_PARTITION_FILTER" in rule_names
        finding = next(f for f in findings if f.rule_name == "MISSING_PARTITION_FILTER")
        assert finding.severity == FindingSeverity.CRITICAL

    def test_detect_missing_cluster_key(self, analyzer):
        # fact_trade_orders is clustered by portfolio_id, ticker, trader_id, execution_status
        # Filtering by trade_date but omitting portfolio_id
        sql = "SELECT order_id, notional_usd FROM fact_trade_orders WHERE trade_date = '2026-03-01' AND side = 'BUY';"
        findings = analyzer.analyze(sql)

        rule_names = [f.rule_name for f in findings]
        assert "UNCLUSTERED_LOOKUP" in rule_names
        finding = next(f for f in findings if f.rule_name == "UNCLUSTERED_LOOKUP")
        assert finding.severity == FindingSeverity.WARNING

    def test_detect_cartesian_join(self, analyzer):
        sql = "SELECT a.order_id, b.portfolio_name FROM fact_trade_orders a CROSS JOIN dim_portfolios b WHERE a.trade_date = '2026-03-01';"
        findings = analyzer.analyze(sql)

        rule_names = [f.rule_name for f in findings]
        assert "CARTESIAN_JOIN" in rule_names
        finding = next(f for f in findings if f.rule_name == "CARTESIAN_JOIN")
        assert finding.severity == FindingSeverity.CRITICAL

    def test_detect_unbounded_order_by(self, analyzer):
        sql = "SELECT order_id, notional_usd FROM fact_trade_orders WHERE trade_date = '2026-03-01' AND portfolio_id = 'PORTFOLIO_ALPHA' ORDER BY notional_usd DESC;"
        findings = analyzer.analyze(sql)

        rule_names = [f.rule_name for f in findings]
        assert "UNBOUNDED_ORDER_BY" in rule_names

    def test_detect_leading_wildcard_like(self, analyzer):
        sql = "SELECT order_id FROM fact_trade_orders WHERE trade_date = '2026-03-01' AND portfolio_id LIKE '%ALPHA';"
        findings = analyzer.analyze(sql)

        rule_names = [f.rule_name for f in findings]
        assert "LEADING_WILDCARD_LIKE" in rule_names

    def test_detect_non_sargiable_date_function(self, analyzer):
        sql = "SELECT ticker FROM fact_market_ticks WHERE DATE(event_timestamp) = '2026-03-01';"
        findings = analyzer.analyze(sql)

        rule_names = [f.rule_name for f in findings]
        assert "NON_SARGIABLE_DATE_FUNCTION" in rule_names

    def test_clean_optimized_query_has_no_critical_findings(self, analyzer):
        sql = """
            SELECT order_id, trader_id, quantity, price, notional_usd
            FROM fact_trade_orders
            WHERE trade_date = '2026-03-01'
              AND portfolio_id = 'PORTFOLIO_ALPHA'
              AND ticker = 'AAPL'
            ORDER BY notional_usd DESC
            LIMIT 50;
        """
        findings = analyzer.analyze(sql)
        critical_findings = [f for f in findings if f.severity == FindingSeverity.CRITICAL]
        assert len(critical_findings) == 0


# =============================================================================
# 6. BUDGET GUARDRAIL MANAGER
# =============================================================================

class TestBudgetGuardrailManager:
    """Validate query budget policies and byte scanning limit enforcement."""

    @pytest.fixture
    def manager(self) -> BudgetGuardrailManager:
        policy = BudgetPolicy(
            project_id="trading-risk-engine-dev",
            daily_budget_usd=50.0,
            max_bytes_billed_per_query=10 * (1024 ** 3),  # 10 GB
            enforce_strict_quota=True,
        )
        return BudgetGuardrailManager(policy)

    def test_query_within_limits_allowed(self, manager):
        one_gb = 1024 ** 3
        res = manager.evaluate_query(one_gb, current_daily_spend_usd=5.0)

        assert res.allowed is True
        assert res.status == "OK"
        assert res.is_bytes_limit_exceeded is False

    def test_query_exceeding_max_bytes_billed_rejected(self, manager):
        twelve_gb = 12 * (1024 ** 3)  # Exceeds 10 GB limit
        res = manager.evaluate_query(twelve_gb, current_daily_spend_usd=5.0)

        assert res.allowed is False
        assert res.status == "BREACH"
        assert res.is_bytes_limit_exceeded is True
        assert "maximum_bytes_billed" in res.reason

    def test_query_exceeding_daily_budget_rejected(self, manager):
        one_gb = 1024 ** 3
        # Current spend is already $49.99, query cost adds to exceed $50.00
        res = manager.evaluate_query(one_gb, current_daily_spend_usd=50.50)

        assert res.allowed is False
        assert res.status == "BREACH"
        assert "daily budget limit" in res.reason

    def test_warning_threshold_triggered(self, manager):
        one_gb = 1024 ** 3
        # Current spend $46.00 = 92% utilization
        res = manager.evaluate_query(one_gb, current_daily_spend_usd=46.00)

        assert res.allowed is True
        assert res.status == "WARNING"
        assert res.alert_threshold_triggered == 90.0


# =============================================================================
# 7. RUNAWAY QUERY INCIDENT REMEDIATOR
# =============================================================================

class TestRunawayQueryRemediator:
    """Validate incident response command generators and diagnostic SQL."""

    def test_generate_cancel_command(self):
        cmd = RunawayQueryRemediator.generate_cancel_command(
            job_id="bqjob_r12345678_001",
            project_id="trading-risk-engine-dev",
            location="us-central1",
        )
        assert cmd == "bq --project_id=trading-risk-engine-dev --location=us-central1 cancel bqjob_r12345678_001"

    def test_generate_investigation_sql(self):
        sql = RunawayQueryRemediator.generate_investigation_sql(
            project_id="trading-risk-engine-dev",
            region="us-central1",
            min_bytes_billed=5000000000,
        )
        assert "INFORMATION_SCHEMA.JOBS_BY_PROJECT" in sql
        assert "`trading-risk-engine-dev`.`region-us-central1`" in sql
        assert "total_bytes_billed >= 5000000000" in sql
        assert "state = 'RUNNING'" in sql

    def test_generate_session_guardrail_sql(self):
        sql = RunawayQueryRemediator.generate_session_guardrail_sql(max_bytes_billed=10737418240)
        assert "SET @@maximum_bytes_billed = 10737418240;" in sql
        assert "cost_center=risk_engine" in sql


# =============================================================================
# 8. BENCHMARK & COST CLI RUNNERS
# =============================================================================

class TestBenchmarkAndCostCLIs:
    """Validate command-line runners for benchmarking and cost analysis."""

    def test_benchmark_cli_dry_run_mode(self, tmp_path):
        json_path = tmp_path / "benchmark_dry_run.json"
        res = run_benchmark_cli(
            environment="dev",
            dry_run=True,
            output_json=str(json_path),
        )

        assert res["status"] == "DRY_RUN"
        assert len(res["scenarios_available"]) >= 8
        assert json_path.exists()
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            assert data["status"] == "DRY_RUN"

    def test_benchmark_cli_execution_with_reports(self, tmp_path):
        json_path = tmp_path / "bench_report.json"
        md_path = tmp_path / "bench_report.md"
        prom_path = tmp_path / "bench_prom.txt"

        res = run_benchmark_cli(
            environment="dev",
            scale_factor=0.1,
            iterations=2,
            scenarios=["clustering_fact_trade_orders"],
            output_json=str(json_path),
            output_markdown=str(md_path),
            export_prometheus=str(prom_path),
        )

        assert "total_queries_executed" in res
        assert json_path.exists()
        assert md_path.exists()
        assert prom_path.exists()

        md_content = md_path.read_text(encoding="utf-8")
        assert "# BigQuery Performance Benchmark Report" in md_content
        assert "`clustering_fact_trade_orders`" in md_content

    def test_cost_cli_analyze_sql_mode(self, tmp_path):
        json_path = tmp_path / "cost_analysis.json"
        sql = "SELECT * FROM fact_trade_orders;"
        res = run_cost_cli(
            analyze_sql=sql,
            json_output=str(json_path),
        )

        assert res["status"] == "SUCCESS"
        assert "query_analysis" in res
        assert res["query_analysis"]["findings_count"] > 0
        assert json_path.exists()

    def test_cost_cli_budget_check_and_incident_query(self):
        res = run_cost_cli(
            query_bytes=5 * (1024 ** 3),
            pricing_model="ON_DEMAND",
            check_budget=True,
            daily_spend=10.0,
            generate_runbook_query=True,
        )

        assert res["status"] == "SUCCESS"
        assert "cost_estimation" in res
        assert res["cost_estimation"]["cost_usd"] > 0
        assert "budget_check" in res
        assert res["budget_check"]["allowed"] is True
        assert "runbook_queries" in res
        assert "investigation_sql" in res["runbook_queries"]


# =============================================================================
# 9. RUNBOOK & DOCUMENTATION INTEGRITY
# =============================================================================

class TestRunbookAndDocumentationIntegrity:
    """Validate documentation structure, Mermaid syntax, and GitHub alerts."""

    def test_performance_benchmarks_doc_exists_and_valid(self):
        bench_doc_path = Path("docs/PERFORMANCE_BENCHMARKS.md")
        assert bench_doc_path.exists(), "docs/PERFORMANCE_BENCHMARKS.md must exist."
        content = bench_doc_path.read_text(encoding="utf-8")
        assert len(content) > 1000

        # Required sections
        assert "# BigQuery Performance Benchmarks & Engineering Report" in content
        assert "## Executive Summary" in content
        assert "## 1. Benchmarking Methodology & Test Harness" in content
        assert "## 2. Benchmark Results Matrix" in content
        assert "## 3. Detailed Benchmark Deep Dives" in content
        assert "## 4. Key Performance Takeaways & Production Sizing" in content

        # Diagrams & alerts
        assert "```mermaid" in content
        assert "> [!IMPORTANT]" in content
        assert "> [!TIP]" in content

    def test_cost_optimization_runbook_exists_and_valid(self):
        runbook_path = Path("docs/BIGQUERY_COST_OPTIMIZATION_RUNBOOK.md")
        assert runbook_path.exists(), "docs/BIGQUERY_COST_OPTIMIZATION_RUNBOOK.md must exist."
        content = runbook_path.read_text(encoding="utf-8")
        assert len(content) > 1000

        # Required sections
        assert "# BigQuery Cost Optimization & FinOps Runbook" in content
        assert "## Executive Summary" in content
        assert "## 1. BigQuery Cost Architecture & Pricing Mechanics" in content
        assert "## 2. Architecture-Level Cost Controls" in content
        assert "## 3. Materialized View FinOps Strategy & ROI" in content
        assert "## 4. Query Anti-Pattern & Cost Remediation Catalog" in content
        assert "## 5. Guardrails, Quotas, and Proactive Alerting" in content
        assert "## 6. Emergency Incident Response: Runaway Query Remediation" in content

        # Runaway query remediation elements
        assert "INFORMATION_SCHEMA.JOBS_BY_PROJECT" in content
        assert "bq" in content and "cancel" in content
        assert "> [!IMPORTANT]" in content
        assert "> [!TIP]" in content
