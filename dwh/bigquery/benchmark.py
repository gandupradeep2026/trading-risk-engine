"""BigQuery Performance Benchmarking Engine & Suite (Day 26).

Provides an enterprise benchmarking framework for the Trading Risk Engine data warehouse:
- Evaluates query latency percentiles (p50, p90, p95, p99), throughput (QPS), and data scanned.
- Benchmarks partition pruning efficiency (30-day table vs 1-day pruned query).
- Benchmarks clustering efficiency on multi-column financial dimensions.
- Benchmarks Materialized View query acceleration vs raw fact aggregations.
- Benchmarks multi-table Star-Schema joins across facts and dimensions.
- Evaluates concurrent reader throughput, contention, and latency distributions.
- Exports structured JSON reports and executive Markdown benchmarking summaries.
- Emits Prometheus telemetry metrics for observability dashboards.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import math
import re
import statistics
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pyarrow as pa

from dwh.bigquery.config import (
    BigQueryDatasetConfig,
    MATERIALIZED_VIEWS_CATALOG,
    STAR_SCHEMA_CATALOG,
)
from dwh.bigquery.emulator import (
    BigQueryTelemetry,
    LocalBigQueryEmulator,
)
from dwh.bigquery.seed_data import (
    generate_mock_market_ticks,
    generate_mock_risk_alerts,
    generate_mock_risk_stats,
    generate_mock_trade_orders,
    get_dim_date_records,
    get_dim_portfolios_records,
    get_dim_tickers_records,
    get_dim_traders_records,
)


class BenchmarkCategory(str, Enum):
    """Classification of benchmark scenarios."""

    PARTITION_PRUNING = "PARTITION_PRUNING"
    CLUSTERING = "CLUSTERING"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    STAR_SCHEMA_JOIN = "STAR_SCHEMA_JOIN"
    HIGH_FREQUENCY_AGGREGATION = "HIGH_FREQUENCY_AGGREGATION"
    FEDERATION = "FEDERATION"
    CONCURRENCY = "CONCURRENCY"


@dataclass
class BenchmarkScenario:
    """Specification of a benchmark scenario to measure."""

    name: str
    category: BenchmarkCategory
    description: str
    query_template: str
    baseline_query_template: Optional[str] = None
    parameters: Optional[List[Any]] = None
    warmup_runs: int = 1
    default_iterations: int = 5
    require_partition_filter_bypass: bool = False


@dataclass
class BenchmarkMetricResult:
    """Metrics captured during benchmark execution."""

    scenario_name: str
    category: str
    description: str
    iterations: int
    durations_ms: List[float]
    p50_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    avg_ms: float
    stddev_ms: float
    throughput_qps: float
    bytes_scanned_bytes: int
    simulated_cost_usd: float
    baseline_p50_ms: Optional[float] = None
    baseline_avg_ms: Optional[float] = None
    baseline_bytes_scanned: Optional[int] = None
    speedup_ratio: Optional[float] = None
    bytes_savings_pct: Optional[float] = None
    histogram_buckets: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "scenario_name": self.scenario_name,
            "category": self.category,
            "description": self.description,
            "iterations": self.iterations,
            "durations_ms": [round(d, 3) for d in self.durations_ms],
            "p50_ms": round(self.p50_ms, 3),
            "p90_ms": round(self.p90_ms, 3),
            "p95_ms": round(self.p95_ms, 3),
            "p99_ms": round(self.p99_ms, 3),
            "min_ms": round(self.min_ms, 3),
            "max_ms": round(self.max_ms, 3),
            "avg_ms": round(self.avg_ms, 3),
            "stddev_ms": round(self.stddev_ms, 3),
            "throughput_qps": round(self.throughput_qps, 2),
            "bytes_scanned_bytes": self.bytes_scanned_bytes,
            "simulated_cost_usd": round(self.simulated_cost_usd, 6),
            "baseline_p50_ms": round(self.baseline_p50_ms, 3) if self.baseline_p50_ms is not None else None,
            "baseline_avg_ms": round(self.baseline_avg_ms, 3) if self.baseline_avg_ms is not None else None,
            "baseline_bytes_scanned": self.baseline_bytes_scanned,
            "speedup_ratio": round(self.speedup_ratio, 2) if self.speedup_ratio is not None else None,
            "bytes_savings_pct": round(self.bytes_savings_pct, 2) if self.bytes_savings_pct is not None else None,
            "histogram_buckets": self.histogram_buckets,
        }


@dataclass
class BenchmarkSuiteResult:
    """Aggregated results across all executed benchmark scenarios."""

    timestamp: str
    environment: str
    scale_factor: float
    total_queries_executed: int
    total_duration_seconds: float
    scenarios: Dict[str, BenchmarkMetricResult]
    system_info: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Export suite results as dictionary."""
        return {
            "timestamp": self.timestamp,
            "environment": self.environment,
            "scale_factor": self.scale_factor,
            "total_queries_executed": self.total_queries_executed,
            "total_duration_seconds": round(self.total_duration_seconds, 3),
            "system_info": self.system_info,
            "scenarios": {k: v.to_dict() for k, v in self.scenarios.items()},
        }

    def to_markdown(self) -> str:
        """Render benchmarking results as a GitHub Flavored Markdown report."""
        lines = [
            f"# BigQuery Performance Benchmark Report",
            f"",
            f"- **Execution Timestamp**: `{self.timestamp}`",
            f"- **Environment**: `{self.environment}`",
            f"- **Dataset Scale Factor**: `{self.scale_factor}x`",
            f"- **Total Queries Executed**: `{self.total_queries_executed}`",
            f"- **Total Benchmark Wall Duration**: `{self.total_duration_seconds:.2f}s`",
            f"",
            f"## Executive Benchmark Summary",
            f"",
            f"| Scenario Name | Category | p50 (ms) | p90 (ms) | p99 (ms) | QPS | Data Scanned | Speedup | Cost Savings |",
            f"| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
        ]

        for s_name, res in self.scenarios.items():
            speedup_str = f"**{res.speedup_ratio:.1f}x**" if res.speedup_ratio else "Baseline / N/A"
            savings_str = f"**{res.bytes_savings_pct:.1f}%**" if res.bytes_savings_pct else "N/A"
            bytes_kb = res.bytes_scanned_bytes / 1024.0
            data_str = f"{bytes_kb:.1f} KB" if bytes_kb < 1024 else f"{(bytes_kb / 1024.0):.2f} MB"

            lines.append(
                f"| `{s_name}` | {res.category} | {res.p50_ms:.2f} | {res.p90_ms:.2f} | "
                f"{res.p99_ms:.2f} | {res.throughput_qps:.1f} | {data_str} | {speedup_str} | {savings_str} |"
            )

        lines.extend([
            f"",
            f"## Detailed Scenario Observations",
            f"",
        ])

        for s_name, res in self.scenarios.items():
            lines.extend([
                f"### Scenario: `{s_name}`",
                f"- **Category**: `{res.category}`",
                f"- **Description**: {res.description}",
                f"- **Latency Distribution**: min={res.min_ms:.2f}ms, avg={res.avg_ms:.2f}ms, p50={res.p50_ms:.2f}ms, p95={res.p95_ms:.2f}ms, p99={res.p99_ms:.2f}ms, max={res.max_ms:.2f}ms (stddev={res.stddev_ms:.2f}ms)",
                f"- **Throughput**: `{res.throughput_qps:.1f} queries/sec`",
                f"- **Data Scanned per Query**: `{res.bytes_scanned_bytes} bytes` (simulated cost: `${res.simulated_cost_usd:.6f}`)",
            ])
            if res.speedup_ratio is not None:
                lines.append(f"- **Acceleration vs Baseline**: **{res.speedup_ratio:.2f}x faster** execution")
            if res.bytes_savings_pct is not None:
                lines.append(f"- **Bytes Scanned Reduction**: **{res.bytes_savings_pct:.2f}% reduction** in data read")
            lines.append("")

        return "\n".join(lines)


@dataclass
class BenchmarkTelemetry:
    """Operational telemetry store tracking benchmark runs and metrics."""

    scenarios_executed: int = 0
    total_queries: int = 0
    total_bytes_scanned: int = 0
    total_duration_ms: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_run(self, queries: int, bytes_scanned: int, duration_ms: float) -> None:
        with self._lock:
            self.scenarios_executed += 1
            self.total_queries += queries
            self.total_bytes_scanned += bytes_scanned
            self.total_duration_ms += duration_ms

    def format_prometheus(self) -> str:
        """Render benchmark operational metrics in Prometheus text exposition format."""
        with self._lock:
            lines = [
                "# HELP bigquery_benchmark_scenarios_executed_total Total benchmark scenarios executed",
                "# TYPE bigquery_benchmark_scenarios_executed_total counter",
                f"bigquery_benchmark_scenarios_executed_total {self.scenarios_executed}",
                "# HELP bigquery_benchmark_queries_total Total benchmark queries executed",
                "# TYPE bigquery_benchmark_queries_total counter",
                f"bigquery_benchmark_queries_total {self.total_queries}",
                "# HELP bigquery_benchmark_bytes_scanned_bytes Total bytes scanned during benchmarks",
                "# TYPE bigquery_benchmark_bytes_scanned_bytes counter",
                f"bigquery_benchmark_bytes_scanned_bytes {self.total_bytes_scanned}",
                "# HELP bigquery_benchmark_total_duration_seconds Total benchmark execution duration in seconds",
                "# TYPE bigquery_benchmark_total_duration_seconds counter",
                f"bigquery_benchmark_total_duration_seconds {(self.total_duration_ms / 1000.0):.4f}",
            ]
            return "\n".join(lines) + "\n"


# -----------------------------------------------------------------------------
# CANONICAL BENCHMARK SCENARIO CATALOG
# -----------------------------------------------------------------------------

BENCHMARK_SCENARIOS_CATALOG: Dict[str, BenchmarkScenario] = {
    "partition_pruning_fact_market_ticks": BenchmarkScenario(
        name="partition_pruning_fact_market_ticks",
        category=BenchmarkCategory.PARTITION_PRUNING,
        description="Measures latency and data scan savings of filtering a single day partition vs multi-day scans on fact_market_ticks.",
        query_template="""
            SELECT ticker, asset_class, COUNT(*) AS tick_count,
                   AVG(mid_price) AS avg_mid, SUM(volume) AS total_vol
            FROM ${dataset}.fact_market_ticks
            WHERE tick_date = '2026-03-01'
            GROUP BY ticker, asset_class
            ORDER BY total_vol DESC;
        """,
        baseline_query_template="""
            SELECT ticker, asset_class, COUNT(*) AS tick_count,
                   AVG(mid_price) AS avg_mid, SUM(volume) AS total_vol
            FROM ${dataset}.fact_market_ticks
            WHERE tick_date BETWEEN '2026-03-01' AND '2026-03-31'
            GROUP BY ticker, asset_class
            ORDER BY total_vol DESC;
        """,
        default_iterations=5,
    ),
    "clustering_fact_trade_orders": BenchmarkScenario(
        name="clustering_fact_trade_orders",
        category=BenchmarkCategory.CLUSTERING,
        description="Measures lookup latency when filtering on cluster keys (portfolio_id, ticker) vs unclustered scan.",
        query_template="""
            SELECT order_id, trader_id, side, order_type, quantity, price, notional_usd
            FROM ${dataset}.fact_trade_orders
            WHERE trade_date = '2026-03-01'
              AND portfolio_id = 'PORTFOLIO_ALPHA'
              AND ticker = 'AAPL'
            ORDER BY notional_usd DESC;
        """,
        baseline_query_template="""
            SELECT order_id, trader_id, side, order_type, quantity, price, notional_usd
            FROM ${dataset}.fact_trade_orders
            WHERE trade_date = '2026-03-01'
            ORDER BY notional_usd DESC;
        """,
        default_iterations=5,
    ),
    "materialized_view_market_performance": BenchmarkScenario(
        name="materialized_view_market_performance",
        category=BenchmarkCategory.MATERIALIZED_VIEW,
        description="Compares querying precomputed mv_realtime_market_performance vs raw aggregation over fact_market_ticks.",
        query_template="""
            SELECT ticker, asset_class, tick_count, total_volume, gross_notional_usd
            FROM ${dataset}.mv_realtime_market_performance
            WHERE ticker = 'AAPL';
        """,
        baseline_query_template="""
            SELECT ticker, asset_class,
                   COUNT(*) AS tick_count,
                   SUM(volume) AS total_volume,
                   ROUND(SUM(last_price * volume), 4) AS gross_notional_usd
            FROM ${dataset}.fact_market_ticks
            WHERE tick_date = '2026-03-01' AND ticker = 'AAPL'
            GROUP BY ticker, asset_class;
        """,
        default_iterations=5,
    ),
    "materialized_view_trader_exposure": BenchmarkScenario(
        name="materialized_view_trader_exposure",
        category=BenchmarkCategory.MATERIALIZED_VIEW,
        description="Compares querying mv_realtime_trader_exposure vs raw aggregations over fact_trade_orders.",
        query_template="""
            SELECT portfolio_id, trader_id, order_count, gross_notional_usd, total_fees_usd
            FROM ${dataset}.mv_realtime_trader_exposure
            WHERE portfolio_id = 'PORTFOLIO_ALPHA';
        """,
        baseline_query_template="""
            SELECT portfolio_id, trader_id,
                   COUNT(*) AS order_count,
                   ROUND(SUM(notional_usd), 2) AS gross_notional_usd,
                   ROUND(SUM(fee_usd), 2) AS total_fees_usd
            FROM ${dataset}.fact_trade_orders
            WHERE trade_date = '2026-03-01' AND portfolio_id = 'PORTFOLIO_ALPHA'
            GROUP BY portfolio_id, trader_id;
        """,
        default_iterations=5,
    ),
    "star_schema_multidimensional_turnover": BenchmarkScenario(
        name="star_schema_multidimensional_turnover",
        category=BenchmarkCategory.STAR_SCHEMA_JOIN,
        description="Executes a 4-table star-schema analytical join between fact_trade_orders and dimensions.",
        query_template="""
            SELECT p.desk, p.portfolio_name, t.asset_class, tr.role,
                   COUNT(f.order_id) AS total_orders,
                   SUM(f.notional_usd) AS total_turnover_usd,
                   AVG(f.notional_usd) AS avg_ticket_size_usd
            FROM ${dataset}.fact_trade_orders f
            INNER JOIN ${dataset}.dim_portfolios p ON f.portfolio_id = p.portfolio_id
            INNER JOIN ${dataset}.dim_tickers t ON f.ticker = t.ticker
            INNER JOIN ${dataset}.dim_traders tr ON f.trader_id = tr.trader_id
            WHERE f.trade_date = '2026-03-01'
            GROUP BY p.desk, p.portfolio_name, t.asset_class, tr.role
            ORDER BY total_turnover_usd DESC;
        """,
        default_iterations=5,
    ),
    "hourly_microstructure_ohlcv_vwap": BenchmarkScenario(
        name="hourly_microstructure_ohlcv_vwap",
        category=BenchmarkCategory.HIGH_FREQUENCY_AGGREGATION,
        description="Calculates OHLCV, session VWAP, and spread metrics grouped by hour and ticker.",
        query_template="""
            SELECT ticker,
                   strftime(event_timestamp, '%Y-%m-%d %H:00:00') AS window_hour,
                   COUNT(*) AS tick_count,
                   MIN(mid_price) AS low_price,
                   MAX(mid_price) AS high_price,
                   AVG(relative_spread_bps) AS avg_spread_bps,
                   SUM(volume) AS total_volume,
                   SUM(mid_price * volume) / NULLIF(SUM(volume), 0) AS vwap
            FROM ${dataset}.fact_market_ticks
            WHERE tick_date = '2026-03-01'
            GROUP BY ticker, strftime(event_timestamp, '%Y-%m-%d %H:00:00')
            ORDER BY ticker, window_hour;
        """,
        default_iterations=5,
    ),
    "gold_portfolio_var_compliance": BenchmarkScenario(
        name="gold_portfolio_var_compliance",
        category=BenchmarkCategory.STAR_SCHEMA_JOIN,
        description="Joins fact_risk_stats with dim_portfolios to evaluate VaR mandate limits and headroom.",
        query_template="""
            SELECT s.portfolio_id, p.portfolio_name, p.max_var_limit_usd,
                   s.parametric_var_usd, s.monte_carlo_var_usd,
                   (p.max_var_limit_usd - s.monte_carlo_var_usd) AS var_headroom_usd,
                   s.risk_regime
            FROM ${dataset}.fact_risk_stats s
            INNER JOIN ${dataset}.dim_portfolios p ON s.portfolio_id = p.portfolio_id
            WHERE s.valuation_date = '2026-03-01' AND s.confidence_level = 0.99
            ORDER BY var_headroom_usd ASC;
        """,
        default_iterations=5,
    ),
    "federated_vs_replicated_alerts": BenchmarkScenario(
        name="federated_vs_replicated_alerts",
        category=BenchmarkCategory.FEDERATION,
        description="Compares federated Cloud SQL view v_federated_risk_alerts vs replicated native table fact_risk_alerts.",
        query_template="""
            SELECT alert_id, portfolio_id, ticker, severity, metric_name, observed_value, threshold_value
            FROM ${dataset}.fact_risk_alerts
            WHERE alert_date = '2026-03-01' AND severity = 'CRITICAL';
        """,
        baseline_query_template="""
            SELECT alert_id, portfolio_id, ticker, severity, metric_name, observed_value, threshold_value
            FROM ${dataset}.v_federated_risk_alerts
            WHERE severity = 'CRITICAL';
        """,
        default_iterations=5,
    ),
}


# -----------------------------------------------------------------------------
# BENCHMARK RUNNER IMPLEMENTATION
# -----------------------------------------------------------------------------

class BenchmarkRunner:
    """Orchestrates benchmark dataset provisioning, execution, and statistical profiling."""

    def __init__(
        self,
        emulator: Optional[LocalBigQueryEmulator] = None,
        dataset_id: str = "trading_risk_dwh_dev",
        project_id: str = "trading-risk-engine-dev",
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.emulator = emulator or LocalBigQueryEmulator(
            dataset_id=dataset_id,
            project_id=project_id,
        )
        self.telemetry = BenchmarkTelemetry()
        self._is_setup = False

    def setup_benchmark_data(
        self,
        scale_factor: float = 1.0,
        seed_dimensions: bool = True,
        num_ticks_per_day: int = 100,
        num_orders_per_day: int = 40,
        days: int = 31,
    ) -> Dict[str, int]:
        """Deploy star schema, materialized views, federated views, and seed synthetic data."""
        self.emulator.initialize_star_schema()

        counts: Dict[str, int] = {}
        if seed_dimensions:
            dim_counts = self.emulator.seed_dimensions()
            counts.update(dim_counts)

        # Deploy Materialized Views and Federated Views
        self.emulator.initialize_materialized_views()
        self.emulator.initialize_cloudsql_tables()
        self.emulator.initialize_federated_views()
        self.emulator.seed_cloudsql_tables()

        # Seed Multi-Day Fact Data (starting 2026-03-01)
        start_d = date(2026, 3, 1)
        all_ticks = []
        all_orders = []
        all_stats = []
        all_alerts = []

        curr = start_d
        day_count = 0
        while day_count < days:
            date_str = curr.isoformat()
            ticks_n = max(5, int(num_ticks_per_day * scale_factor))
            orders_n = max(3, int(num_orders_per_day * scale_factor))
            all_ticks.extend(generate_mock_market_ticks(count=ticks_n, tick_date=date_str))
            all_orders.extend(generate_mock_trade_orders(count=orders_n, trade_date=date_str))
            all_stats.extend(generate_mock_risk_stats(valuation_date=date_str))
            all_alerts.extend(generate_mock_risk_alerts(count=3, alert_date=date_str))
            curr += timedelta(days=1)
            day_count += 1

        counts["fact_market_ticks"] = self.emulator.insert_rows("fact_market_ticks", all_ticks)
        counts["fact_trade_orders"] = self.emulator.insert_rows("fact_trade_orders", all_orders)
        counts["fact_risk_stats"] = self.emulator.insert_rows("fact_risk_stats", all_stats)
        counts["fact_risk_alerts"] = self.emulator.insert_rows("fact_risk_alerts", all_alerts)

        self._is_setup = True
        return counts

    def _render_query(self, template: str) -> str:
        """Substitute dataset and project tokens in query templates."""
        q = template.replace("${dataset}", self.dataset_id)
        q = q.replace("${project_id}", self.project_id)
        return q.strip()

    def _calculate_histogram(self, values: List[float]) -> Dict[str, int]:
        """Compute latency distribution histogram across logarithmic-ish buckets."""
        buckets = {
            "< 1ms": 0,
            "1ms - 5ms": 0,
            "5ms - 20ms": 0,
            "20ms - 50ms": 0,
            "50ms - 100ms": 0,
            ">= 100ms": 0,
        }
        for v in values:
            if v < 1.0:
                buckets["< 1ms"] += 1
            elif v < 5.0:
                buckets["1ms - 5ms"] += 1
            elif v < 20.0:
                buckets["5ms - 20ms"] += 1
            elif v < 50.0:
                buckets["20ms - 50ms"] += 1
            elif v < 100.0:
                buckets["50ms - 100ms"] += 1
            else:
                buckets[">= 100ms"] += 1
        return buckets

    def run_scenario(
        self,
        scenario: BenchmarkScenario,
        iterations: Optional[int] = None,
        warmup_runs: Optional[int] = None,
    ) -> BenchmarkMetricResult:
        """Execute a single benchmark scenario and measure latency and throughput."""
        num_iterations = iterations if iterations is not None else scenario.default_iterations
        num_warmup = warmup_runs if warmup_runs is not None else scenario.warmup_runs

        rendered_q = self._render_query(scenario.query_template)

        # Warmup executions (discarded)
        for _ in range(num_warmup):
            self.emulator.execute_query(
                rendered_q,
                parameters=scenario.parameters,
                bypass_partition_filter=scenario.require_partition_filter_bypass,
            )

        # Measured executions
        durations: List[float] = []
        bytes_scanned_list: List[int] = []

        for _ in range(num_iterations):
            t0 = time.perf_counter()
            arrow_tbl = self.emulator.execute_query_arrow(
                rendered_q,
                bypass_partition_filter=scenario.require_partition_filter_bypass,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            durations.append(elapsed_ms)
            bytes_scanned_list.append(arrow_tbl.nbytes if arrow_tbl is not None else 0)

        # Baseline execution if specified
        baseline_p50_ms: Optional[float] = None
        baseline_avg_ms: Optional[float] = None
        baseline_bytes: Optional[int] = None
        speedup_ratio: Optional[float] = None
        savings_pct: Optional[float] = None

        if scenario.baseline_query_template:
            rendered_baseline = self._render_query(scenario.baseline_query_template)
            # Warmup baseline
            self.emulator.execute_query(
                rendered_baseline,
                bypass_partition_filter=True,
            )
            base_durations: List[float] = []
            base_bytes_list: List[int] = []
            for _ in range(num_iterations):
                t0 = time.perf_counter()
                b_tbl = self.emulator.execute_query_arrow(
                    rendered_baseline,
                    bypass_partition_filter=True,
                )
                b_ms = (time.perf_counter() - t0) * 1000.0
                base_durations.append(b_ms)
                base_bytes_list.append(b_tbl.nbytes if b_tbl is not None else 0)

            base_durations.sort()
            baseline_p50_ms = statistics.median(base_durations)
            baseline_avg_ms = statistics.mean(base_durations)
            baseline_bytes = int(statistics.mean(base_bytes_list)) if base_bytes_list else 0

        # Statistical calculations
        durations.sort()
        p50 = statistics.median(durations)
        # BigQuery percentile approximations
        p90_idx = max(0, min(len(durations) - 1, math.ceil(len(durations) * 0.90) - 1))
        p95_idx = max(0, min(len(durations) - 1, math.ceil(len(durations) * 0.95) - 1))
        p99_idx = max(0, min(len(durations) - 1, math.ceil(len(durations) * 0.99) - 1))
        p90 = durations[p90_idx]
        p95 = durations[p95_idx]
        p99 = durations[p99_idx]
        avg_val = statistics.mean(durations)
        stddev_val = statistics.stdev(durations) if len(durations) > 1 else 0.0
        min_val = durations[0]
        max_val = durations[-1]

        total_elapsed_sec = sum(durations) / 1000.0
        qps = (len(durations) / total_elapsed_sec) if total_elapsed_sec > 0 else 0.0

        avg_bytes = int(statistics.mean(bytes_scanned_list)) if bytes_scanned_list else 0
        # BigQuery on-demand analysis cost: $6.25 per TB ($6.25 / 10^12 bytes)
        simulated_cost = (avg_bytes / 1e12) * 6.25

        if baseline_avg_ms and avg_val > 0:
            speedup_ratio = baseline_avg_ms / avg_val
        if baseline_bytes and baseline_bytes > 0:
            savings_pct = max(0.0, ((baseline_bytes - avg_bytes) / baseline_bytes) * 100.0)

        hist = self._calculate_histogram(durations)

        self.telemetry.record_run(
            queries=num_iterations,
            bytes_scanned=sum(bytes_scanned_list),
            duration_ms=sum(durations),
        )

        return BenchmarkMetricResult(
            scenario_name=scenario.name,
            category=scenario.category.value,
            description=scenario.description,
            iterations=num_iterations,
            durations_ms=durations,
            p50_ms=p50,
            p90_ms=p90,
            p95_ms=p95,
            p99_ms=p99,
            min_ms=min_val,
            max_ms=max_val,
            avg_ms=avg_val,
            stddev_ms=stddev_val,
            throughput_qps=qps,
            bytes_scanned_bytes=avg_bytes,
            simulated_cost_usd=simulated_cost,
            baseline_p50_ms=baseline_p50_ms,
            baseline_avg_ms=baseline_avg_ms,
            baseline_bytes_scanned=baseline_bytes,
            speedup_ratio=speedup_ratio,
            bytes_savings_pct=savings_pct,
            histogram_buckets=hist,
        )

    def run_suite(
        self,
        scenario_names: Optional[List[str]] = None,
        iterations: int = 5,
        scale_factor: float = 1.0,
    ) -> BenchmarkSuiteResult:
        """Run a collection or all canonical benchmark scenarios."""
        if not self._is_setup:
            self.setup_benchmark_data(scale_factor=scale_factor)

        selected_names = scenario_names or list(BENCHMARK_SCENARIOS_CATALOG.keys())
        results: Dict[str, BenchmarkMetricResult] = {}

        t_start = time.perf_counter()
        total_queries = 0

        for name in selected_names:
            if name not in BENCHMARK_SCENARIOS_CATALOG:
                raise KeyError(f"Unknown benchmark scenario: '{name}'")
            scen = BENCHMARK_SCENARIOS_CATALOG[name]
            metric_res = self.run_scenario(scen, iterations=iterations)
            results[name] = metric_res
            total_queries += metric_res.iterations

        total_wall_sec = time.perf_counter() - t_start

        return BenchmarkSuiteResult(
            timestamp=datetime.now(timezone.utc).isoformat(),
            environment=self.dataset_id.split("_")[-1] if "_" in self.dataset_id else "dev",
            scale_factor=scale_factor,
            total_queries_executed=total_queries,
            total_duration_seconds=total_wall_sec,
            scenarios=results,
            system_info={
                "engine": "DuckDB-LocalBigQueryEmulator",
                "star_schema_tables": len(STAR_SCHEMA_CATALOG),
                "materialized_views": len(MATERIALIZED_VIEWS_CATALOG),
            },
        )

    def run_concurrency_test(
        self,
        scenario: BenchmarkScenario,
        concurrency_levels: Optional[List[int]] = None,
        iterations_per_thread: int = 5,
    ) -> Dict[int, Dict[str, Any]]:
        """Evaluate query throughput and contention across multiple concurrent reader threads."""
        levels = concurrency_levels or [1, 2, 4, 8]
        concurrency_results: Dict[int, Dict[str, Any]] = {}
        rendered_q = self._render_query(scenario.query_template)

        for num_threads in levels:
            thread_latencies: List[float] = []
            lock = threading.Lock()

            def _worker():
                local_durs: List[float] = []
                for _ in range(iterations_per_thread):
                    t0 = time.perf_counter()
                    self.emulator.execute_query(
                        rendered_q,
                        bypass_partition_filter=scenario.require_partition_filter_bypass,
                    )
                    local_durs.append((time.perf_counter() - t0) * 1000.0)
                with lock:
                    thread_latencies.extend(local_durs)

            threads = [threading.Thread(target=_worker) for _ in range(num_threads)]
            t_wall_start = time.perf_counter()
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            wall_duration_sec = time.perf_counter() - t_wall_start

            total_queries = num_threads * iterations_per_thread
            qps = (total_queries / wall_duration_sec) if wall_duration_sec > 0 else 0.0

            concurrency_results[num_threads] = {
                "concurrency": num_threads,
                "total_queries": total_queries,
                "wall_duration_sec": round(wall_duration_sec, 4),
                "aggregate_qps": round(qps, 2),
                "avg_latency_ms": round(statistics.mean(thread_latencies), 3) if thread_latencies else 0.0,
                "p50_latency_ms": round(statistics.median(thread_latencies), 3) if thread_latencies else 0.0,
                "p95_latency_ms": round(statistics.quantiles(thread_latencies, n=20)[18], 3) if len(thread_latencies) >= 20 else 0.0,
            }

        return concurrency_results
