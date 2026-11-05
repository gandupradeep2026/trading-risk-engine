"""BigQuery Cost Optimization Engine, FinOps Calculator, and Query Antipattern Analyzer (Day 26).

Implements production-grade FinOps cost governance for the Trading Risk Engine:
1. BigQueryPricingModel: Models On-Demand, Editions (Standard, Enterprise, Enterprise Plus),
   Active/Long-Term Logical vs Physical Storage, and BI Engine costs.
2. QueryAntipatternAnalyzer: Inspects GoogleSQL queries for costly patterns:
   - SELECT * projections scanning redundant columns in columnar storage
   - Missing partition filters on partitioned fact tables
   - Missing cluster key filters on clustered tables
   - Cartesian Cross Joins causing slot exhaustion
   - Non-sargiable functions on partition/cluster keys
   - Unbounded ORDER BY and leading wildcard LIKE statements
3. BudgetGuardrailManager: Enforces maximum_bytes_billed limits and daily/monthly spending quotas.
4. RunawayQueryRemediator: Generates emergency mitigation commands (bq cancel) and
   INFORMATION_SCHEMA diagnostic queries for incident response.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
import math
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from dwh.bigquery.config import (
    FACT_MARKET_TICKS_SPEC,
    FACT_RISK_ALERTS_SPEC,
    FACT_RISK_STATS_SPEC,
    FACT_TRADE_ORDERS_SPEC,
    STAR_SCHEMA_CATALOG,
    TablePartitionClusteringSpec,
)


class PricingTier(str, Enum):
    """BigQuery pricing tier options."""

    ON_DEMAND = "ON_DEMAND"
    EDITION_STANDARD = "EDITION_STANDARD"
    EDITION_ENTERPRISE = "EDITION_ENTERPRISE"
    EDITION_ENTERPRISE_PLUS = "EDITION_ENTERPRISE_PLUS"


class StorageBillingModel(str, Enum):
    """BigQuery storage pricing models."""

    LOGICAL = "LOGICAL"
    PHYSICAL = "PHYSICAL"


class FindingSeverity(str, Enum):
    """Severity classification of query antipattern findings."""

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class CostEstimationResult:
    """Detailed cost breakdown for a query or storage asset."""

    bytes_scanned: int
    on_demand_cost_usd: float
    enterprise_slot_cost_usd: float
    active_storage_cost_monthly_usd: float
    long_term_storage_cost_monthly_usd: float
    projected_monthly_cost_usd: float
    potential_savings_usd: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bytes_scanned": self.bytes_scanned,
            "on_demand_cost_usd": round(self.on_demand_cost_usd, 6),
            "enterprise_slot_cost_usd": round(self.enterprise_slot_cost_usd, 6),
            "active_storage_cost_monthly_usd": round(self.active_storage_cost_monthly_usd, 4),
            "long_term_storage_cost_monthly_usd": round(self.long_term_storage_cost_monthly_usd, 4),
            "projected_monthly_cost_usd": round(self.projected_monthly_cost_usd, 4),
            "potential_savings_usd": round(self.potential_savings_usd, 4),
        }


@dataclass
class AntipatternFinding:
    """A detected antipattern in a SQL query with cost remediation recommendations."""

    finding_id: str
    rule_name: str
    severity: FindingSeverity
    title: str
    description: str
    recommendation: str
    suggested_rewrite: Optional[str] = None
    estimated_scan_reduction_pct: float = 0.0
    estimated_savings_usd_per_query: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "rule_name": self.rule_name,
            "severity": self.severity.value,
            "title": self.title,
            "description": self.description,
            "recommendation": self.recommendation,
            "suggested_rewrite": self.suggested_rewrite,
            "estimated_scan_reduction_pct": round(self.estimated_scan_reduction_pct, 2),
            "estimated_savings_usd_per_query": round(self.estimated_savings_usd_per_query, 6),
        }


# -----------------------------------------------------------------------------
# PRICING MODEL & FINOPS CALCULATOR
# -----------------------------------------------------------------------------

class BigQueryPricingModel:
    """Models Google Cloud BigQuery pricing mechanics across compute, storage, and caching."""

    # Compute: On-Demand ($6.25 per TB)
    ON_DEMAND_USD_PER_TB: float = 6.25
    ON_DEMAND_USD_PER_BYTE: float = 6.25 / (1024.0 ** 4)

    # Compute: Editions (Slot-Hour)
    SLOT_HOUR_USD_STANDARD: float = 0.040
    SLOT_HOUR_USD_ENTERPRISE: float = 0.060
    SLOT_HOUR_USD_ENTERPRISE_PLUS: float = 0.100

    # Storage: Logical (per GB / month)
    STORAGE_ACTIVE_LOGICAL_USD_PER_GB: float = 0.020
    STORAGE_LONG_TERM_LOGICAL_USD_PER_GB: float = 0.010  # 50% discount after 90 days unmodified

    # Storage: Physical (compressed, per GB / month)
    STORAGE_ACTIVE_PHYSICAL_USD_PER_GB: float = 0.040
    STORAGE_LONG_TERM_PHYSICAL_USD_PER_GB: float = 0.020

    # BI Engine: In-Memory Caching (per GB-hour)
    BI_ENGINE_USD_PER_GB_HOUR: float = 0.0416  # ~$30.37/GB/month

    @classmethod
    def calculate_query_cost(
        cls,
        bytes_scanned: int,
        pricing_tier: PricingTier = PricingTier.ON_DEMAND,
        slot_ms: int = 0,
    ) -> float:
        """Calculate query execution cost under the specified pricing tier."""
        if pricing_tier == PricingTier.ON_DEMAND:
            # First 1 TB / month is free in GCP free tier, but for billing models we calculate exact proportional cost
            return bytes_scanned * cls.ON_DEMAND_USD_PER_BYTE

        # Editions compute: slot_ms to slot-hours
        slot_hours = slot_ms / (1000.0 * 3600.0)
        if pricing_tier == PricingTier.EDITION_STANDARD:
            return slot_hours * cls.SLOT_HOUR_USD_STANDARD
        elif pricing_tier == PricingTier.EDITION_ENTERPRISE:
            return slot_hours * cls.SLOT_HOUR_USD_ENTERPRISE
        elif pricing_tier == PricingTier.EDITION_ENTERPRISE_PLUS:
            return slot_hours * cls.SLOT_HOUR_USD_ENTERPRISE_PLUS

        return bytes_scanned * cls.ON_DEMAND_USD_PER_BYTE

    @classmethod
    def calculate_storage_cost(
        cls,
        bytes_stored: int,
        is_long_term: bool = False,
        storage_billing_model: StorageBillingModel = StorageBillingModel.LOGICAL,
    ) -> float:
        """Calculate monthly storage cost for a dataset or table."""
        gb_stored = bytes_stored / (1024.0 ** 3)
        if storage_billing_model == StorageBillingModel.LOGICAL:
            rate = cls.STORAGE_LONG_TERM_LOGICAL_USD_PER_GB if is_long_term else cls.STORAGE_ACTIVE_LOGICAL_USD_PER_GB
        else:
            rate = cls.STORAGE_LONG_TERM_PHYSICAL_USD_PER_GB if is_long_term else cls.STORAGE_ACTIVE_PHYSICAL_USD_PER_GB
        return gb_stored * rate

    @classmethod
    def calculate_partition_pruning_savings(
        cls,
        total_table_bytes: int,
        pruned_bytes_scanned: int,
        query_frequency_per_day: int = 100,
    ) -> Dict[str, Any]:
        """Quantify cost and data savings enabled by partition filter pruning."""
        bytes_saved_per_query = max(0, total_table_bytes - pruned_bytes_scanned)
        savings_pct = (bytes_saved_per_query / total_table_bytes * 100.0) if total_table_bytes > 0 else 0.0

        cost_unpruned_query = cls.calculate_query_cost(total_table_bytes)
        cost_pruned_query = cls.calculate_query_cost(pruned_bytes_scanned)
        savings_per_query = max(0.0, cost_unpruned_query - cost_pruned_query)

        monthly_savings = savings_per_query * query_frequency_per_day * 30.0

        return {
            "total_table_bytes": total_table_bytes,
            "pruned_bytes_scanned": pruned_bytes_scanned,
            "bytes_saved_per_query": bytes_saved_per_query,
            "savings_pct": round(savings_pct, 2),
            "cost_unpruned_usd": round(cost_unpruned_query, 6),
            "cost_pruned_usd": round(cost_pruned_query, 6),
            "savings_per_query_usd": round(savings_per_query, 6),
            "projected_monthly_savings_usd": round(monthly_savings, 2),
        }

    @classmethod
    def calculate_materialized_view_roi(
        cls,
        raw_query_bytes: int,
        mv_query_bytes: int,
        queries_per_day: int = 500,
        mv_refresh_bytes_per_day: int = 10_000_000_000,
        mv_storage_bytes: int = 50_000_000,
    ) -> Dict[str, Any]:
        """Calculate return on investment (ROI) for maintaining a BigQuery Materialized View."""
        daily_raw_cost = cls.calculate_query_cost(raw_query_bytes) * queries_per_day
        daily_mv_query_cost = cls.calculate_query_cost(mv_query_bytes) * queries_per_day
        daily_refresh_cost = cls.calculate_query_cost(mv_refresh_bytes_per_day)
        monthly_storage_cost = cls.calculate_storage_cost(mv_storage_bytes)

        total_daily_mv_cost = daily_mv_query_cost + daily_refresh_cost
        daily_savings = daily_raw_cost - total_daily_mv_cost
        monthly_savings = (daily_savings * 30.0) - monthly_storage_cost

        breakeven_queries_per_day = 0
        per_query_savings = cls.calculate_query_cost(raw_query_bytes - mv_query_bytes)
        if per_query_savings > 0:
            breakeven_queries_per_day = math.ceil(daily_refresh_cost / per_query_savings)

        return {
            "daily_raw_queries_cost_usd": round(daily_raw_cost, 4),
            "daily_mv_queries_cost_usd": round(daily_mv_query_cost, 4),
            "daily_refresh_cost_usd": round(daily_refresh_cost, 4),
            "monthly_storage_cost_usd": round(monthly_storage_cost, 4),
            "daily_net_savings_usd": round(daily_savings, 4),
            "monthly_net_savings_usd": round(monthly_savings, 2),
            "is_cost_effective": monthly_savings > 0,
            "breakeven_queries_per_day": breakeven_queries_per_day,
        }


# -----------------------------------------------------------------------------
# QUERY ANTIPATTERN ANALYZER
# -----------------------------------------------------------------------------

class QueryAntipatternAnalyzer:
    """Static SQL analyzer detecting BigQuery anti-patterns and recommending optimizations."""

    def __init__(
        self,
        table_specs: Optional[Dict[str, TablePartitionClusteringSpec]] = None,
    ) -> None:
        self.table_specs = table_specs or STAR_SCHEMA_CATALOG

    def _strip_comments(self, sql: str) -> str:
        """Remove single-line and multi-line comments."""
        clean = re.sub(r"--.*", "", sql)
        clean = re.sub(r"/\*.*?\*/", "", clean, flags=re.DOTALL)
        return clean.strip()

    def analyze(self, sql_query: str) -> List[AntipatternFinding]:
        """Perform comprehensive static analysis of a GoogleSQL query."""
        findings: List[AntipatternFinding] = []
        clean_sql = self._strip_comments(sql_query)

        # 1. Detect SELECT * (Columnar expansion antipattern)
        if re.search(r"\bSELECT\s+(?:DISTINCT\s+)?\*", clean_sql, re.IGNORECASE):
            findings.append(
                AntipatternFinding(
                    finding_id="BQ-COST-001",
                    rule_name="SELECT_STAR_PROJECTION",
                    severity=FindingSeverity.CRITICAL,
                    title="Unbounded 'SELECT *' Scans Redundant Columnar Storage",
                    description=(
                        "BigQuery is a columnar database (Capacitor format) where costs scale strictly with "
                        "the byte size of columns read. 'SELECT *' reads every column across every row, "
                        "multiplying query costs and execution duration 5x-10x."
                    ),
                    recommendation=(
                        "Explicitly enumerate only the necessary business columns required by downstream consumers "
                        "(e.g., 'SELECT trade_date, portfolio_id, ticker, notional_usd...')."
                    ),
                    suggested_rewrite=re.sub(
                        r"\bSELECT\s+(?:DISTINCT\s+)?\*",
                        "SELECT <explicit_columns>",
                        clean_sql,
                        flags=re.IGNORECASE,
                    ),
                    estimated_scan_reduction_pct=75.0,
                    estimated_savings_usd_per_query=0.005,
                )
            )

        # 2. Detect missing partition filters on partitioned fact tables
        for tbl_name, spec in self.table_specs.items():
            if spec.partition_column:
                tbl_pattern = rf"\b(?:[a-zA-Z0-9_-]+\.)?{tbl_name}\b"
                if re.search(tbl_pattern, clean_sql, re.IGNORECASE):
                    pcol = spec.partition_column
                    where_match = re.search(r"\bWHERE\b(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|;|\Z)", clean_sql, re.DOTALL | re.IGNORECASE)
                    has_partition_filter = False
                    if where_match:
                        where_clause = where_match.group(1)
                        if re.search(rf"\b{pcol}\b", where_clause, re.IGNORECASE):
                            has_partition_filter = True

                    if not has_partition_filter:
                        findings.append(
                            AntipatternFinding(
                                finding_id="BQ-COST-002",
                                rule_name="MISSING_PARTITION_FILTER",
                                severity=FindingSeverity.CRITICAL if spec.require_partition_filter else FindingSeverity.WARNING,
                                title=f"Partitioned Table '{tbl_name}' Queried Without Partition Filter on '{pcol}'",
                                description=(
                                    f"Table '{tbl_name}' is partitioned by '{pcol}'. Omitting '{pcol}' in the WHERE clause "
                                    f"forces BigQuery to scan every single day partition across table history, triggering full scans."
                                ),
                                recommendation=(
                                    f"Filter explicitly on '{pcol}' (e.g., 'WHERE {pcol} = CURRENT_DATE()' or "
                                    f"'WHERE {pcol} BETWEEN @start_date AND @end_date')."
                                ),
                                suggested_rewrite=clean_sql + f" WHERE {pcol} = CURRENT_DATE()",
                                estimated_scan_reduction_pct=95.0,
                                estimated_savings_usd_per_query=0.050,
                            )
                        )

        # 3. Detect missing cluster key filters on clustered tables
        for tbl_name, spec in self.table_specs.items():
            if spec.clustering_columns:
                tbl_pattern = rf"\b(?:[a-zA-Z0-9_-]+\.)?{tbl_name}\b"
                if re.search(tbl_pattern, clean_sql, re.IGNORECASE):
                    where_match = re.search(r"\bWHERE\b(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|;|\Z)", clean_sql, re.DOTALL | re.IGNORECASE)
                    if where_match:
                        where_clause = where_match.group(1)
                        # Check if first clustering column is used
                        first_cluster_col = spec.clustering_columns[0]
                        if not re.search(rf"\b{first_cluster_col}\b", where_clause, re.IGNORECASE):
                            findings.append(
                                AntipatternFinding(
                                    finding_id="BQ-COST-003",
                                    rule_name="UNCLUSTERED_LOOKUP",
                                    severity=FindingSeverity.WARNING,
                                    title=f"Clustered Table '{tbl_name}' Filtered Without Primary Cluster Key '{first_cluster_col}'",
                                    description=(
                                        f"Table '{tbl_name}' is clustered by {spec.clustering_columns}. BigQuery clustering operates "
                                        f"hierarchically: filters that omit the leading cluster column '{first_cluster_col}' cannot "
                                        f"skip storage blocks efficiently."
                                    ),
                                    recommendation=(
                                        f"Include '{first_cluster_col}' in the filter condition whenever possible to enable "
                                        f"block pruning."
                                    ),
                                    estimated_scan_reduction_pct=50.0,
                                    estimated_savings_usd_per_query=0.010,
                                )
                            )

        # 4. Detect Cartesian Cross Join
        has_cross_join = bool(re.search(r"\bCROSS\s+JOIN\b", clean_sql, re.IGNORECASE))
        has_comma_join = bool(re.search(r"\bFROM\s+[a-zA-Z0-9_.]+\s*,\s*[a-zA-Z0-9_.]+", clean_sql, re.IGNORECASE))
        if has_cross_join or has_comma_join:
            findings.append(
                AntipatternFinding(
                    finding_id="BQ-COST-004",
                    rule_name="CARTESIAN_JOIN",
                    severity=FindingSeverity.CRITICAL,
                    title="Cartesian Cross Join Multiplies Record Rows and Exceeds Slot Memory Limits",
                    description=(
                        "Query performs a Cartesian product (CROSS JOIN or unconditioned comma join). "
                        "This produces N x M intermediate rows, causing massive slot memory consumption and query timeouts."
                    ),
                    recommendation=(
                        "Replace CROSS JOIN with an explicit INNER JOIN or LEFT JOIN specifying ON matching keys."
                    ),
                    estimated_scan_reduction_pct=90.0,
                    estimated_savings_usd_per_query=0.100,
                )
            )

        # 5. Detect Unbounded ORDER BY without LIMIT
        has_order_by = bool(re.search(r"\bORDER\s+BY\b", clean_sql, re.IGNORECASE))
        has_limit = bool(re.search(r"\bLIMIT\s+\d+\b", clean_sql, re.IGNORECASE))
        if has_order_by and not has_limit:
            findings.append(
                AntipatternFinding(
                    finding_id="BQ-COST-005",
                    rule_name="UNBOUNDED_ORDER_BY",
                    severity=FindingSeverity.WARNING,
                    title="Unbounded 'ORDER BY' Forces Full Dataset Sorting in Single Slot Memory",
                    description=(
                        "ORDER BY without a LIMIT clause requires BigQuery to collect and sort the entire dataset "
                        "on the primary worker node, leading to slot skew and potential 'Resources Exceeded' errors."
                    ),
                    recommendation=(
                        "Add a LIMIT clause if only top results are required, or perform sorting downstream in visualization tools."
                    ),
                    suggested_rewrite=clean_sql + " LIMIT 1000",
                    estimated_scan_reduction_pct=30.0,
                    estimated_savings_usd_per_query=0.002,
                )
            )

        # 6. Detect Leading Wildcard LIKE
        if re.search(r"\bLIKE\s+['\"][%_][^'\"]+['\"]", clean_sql, re.IGNORECASE):
            findings.append(
                AntipatternFinding(
                    finding_id="BQ-COST-006",
                    rule_name="LEADING_WILDCARD_LIKE",
                    severity=FindingSeverity.INFO,
                    title="Leading Wildcard in LIKE Predicate Prevents String Index Pruning",
                    description=(
                        "Predicates like `LIKE '%pattern'` prevent BigQuery from leveraging search optimization indexes "
                        "or clustered string dictionaries, forcing a full byte evaluation."
                    ),
                    recommendation=(
                        "Use suffix wildcards (`LIKE 'pattern%'`) or BigQuery SEARCH() function with Search Optimization."
                    ),
                    estimated_scan_reduction_pct=40.0,
                    estimated_savings_usd_per_query=0.001,
                )
            )

        # 7. Detect Non-Sargiable Functions on Date/Partition Columns
        if re.search(r"\bWHERE\b.*?\b(?:DATE|DATETIME|TIMESTAMP|EXTRACT)\s*\(\s*(?:tick_date|trade_date|valuation_date|alert_date|event_timestamp)\s*\)", clean_sql, re.IGNORECASE | re.DOTALL):
            findings.append(
                AntipatternFinding(
                    finding_id="BQ-COST-007",
                    rule_name="NON_SARGIABLE_DATE_FUNCTION",
                    severity=FindingSeverity.WARNING,
                    title="Function Call on Partition Column in WHERE Clause Defeats Partition Pruning",
                    description=(
                        "Wrapping a partition date column in a function (e.g. `DATE(event_timestamp) = '2026-03-01'`) "
                        "renders the expression non-sargiable, preventing the BigQuery query planner from pruning partitions."
                    ),
                    recommendation=(
                        "Rewrite using date boundaries (e.g., `event_timestamp >= '2026-03-01 00:00:00' AND "
                        "event_timestamp < '2026-03-02 00:00:00'`)."
                    ),
                    estimated_scan_reduction_pct=85.0,
                    estimated_savings_usd_per_query=0.020,
                )
            )

        return findings


# -----------------------------------------------------------------------------
# BUDGET GUARDRAILS & POLICY ENFORCER
# -----------------------------------------------------------------------------

@dataclass
class BudgetPolicy:
    """Enterprise policy specifying budget ceilings and query limits."""

    project_id: str = "trading-risk-engine-dev"
    environment: str = "dev"
    daily_budget_usd: float = 50.0
    monthly_budget_usd: float = 1500.0
    max_bytes_billed_per_query: int = 50 * (1024 ** 3)  # 50 GB default limit (~$0.31 per query)
    alert_threshold_percentages: List[float] = field(default_factory=lambda: [50.0, 75.0, 90.0, 100.0])
    enforce_strict_quota: bool = True


@dataclass
class BudgetCheckResult:
    """Outcome of evaluating a query against budget guardrails."""

    allowed: bool
    status: str  # OK, WARNING, BREACH
    reason: str
    projected_query_cost_usd: float
    projected_daily_total_usd: float
    daily_budget_usd: float
    daily_utilization_pct: float
    is_bytes_limit_exceeded: bool
    alert_threshold_triggered: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "status": self.status,
            "reason": self.reason,
            "projected_query_cost_usd": round(self.projected_query_cost_usd, 6),
            "projected_daily_total_usd": round(self.projected_daily_total_usd, 4),
            "daily_budget_usd": round(self.daily_budget_usd, 2),
            "daily_utilization_pct": round(self.daily_utilization_pct, 2),
            "is_bytes_limit_exceeded": self.is_bytes_limit_exceeded,
            "alert_threshold_triggered": self.alert_threshold_triggered,
        }


class BudgetGuardrailManager:
    """Enforces byte and financial quotas to prevent runaway BigQuery costs."""

    def __init__(self, policy: Optional[BudgetPolicy] = None) -> None:
        self.policy = policy or BudgetPolicy()

    def evaluate_query(
        self,
        query_bytes_scanned: int,
        current_daily_spend_usd: float = 0.0,
    ) -> BudgetCheckResult:
        """Evaluate whether a query complies with maximum_bytes_billed and daily budget."""
        query_cost = BigQueryPricingModel.calculate_query_cost(query_bytes_scanned)
        projected_total = current_daily_spend_usd + query_cost
        utilization_pct = (projected_total / self.policy.daily_budget_usd * 100.0) if self.policy.daily_budget_usd > 0 else 0.0

        is_bytes_exceeded = query_bytes_scanned > self.policy.max_bytes_billed_per_query

        # Check triggered budget alert thresholds
        triggered_threshold: Optional[float] = None
        for thresh in sorted(self.policy.alert_threshold_percentages, reverse=True):
            if utilization_pct >= thresh:
                triggered_threshold = thresh
                break

        # Enforcement decisions
        if is_bytes_exceeded and self.policy.enforce_strict_quota:
            max_gb = self.policy.max_bytes_billed_per_query / (1024 ** 3)
            scanned_gb = query_bytes_scanned / (1024 ** 3)
            return BudgetCheckResult(
                allowed=False,
                status="BREACH",
                reason=(
                    f"Query rejected: projected scan volume ({scanned_gb:.2f} GB) exceeds "
                    f"maximum_bytes_billed ceiling ({max_gb:.2f} GB)."
                ),
                projected_query_cost_usd=query_cost,
                projected_daily_total_usd=projected_total,
                daily_budget_usd=self.policy.daily_budget_usd,
                daily_utilization_pct=utilization_pct,
                is_bytes_limit_exceeded=True,
                alert_threshold_triggered=triggered_threshold,
            )

        if projected_total > self.policy.daily_budget_usd and self.policy.enforce_strict_quota:
            return BudgetCheckResult(
                allowed=False,
                status="BREACH",
                reason=(
                    f"Query rejected: projected daily spend (${projected_total:.2f}) exceeds "
                    f"daily budget limit (${self.policy.daily_budget_usd:.2f})."
                ),
                projected_query_cost_usd=query_cost,
                projected_daily_total_usd=projected_total,
                daily_budget_usd=self.policy.daily_budget_usd,
                daily_utilization_pct=utilization_pct,
                is_bytes_limit_exceeded=is_bytes_exceeded,
                alert_threshold_triggered=triggered_threshold,
            )

        if triggered_threshold and triggered_threshold >= 90.0:
            return BudgetCheckResult(
                allowed=True,
                status="WARNING",
                reason=f"Warning: Daily budget consumption is high ({utilization_pct:.1f}%).",
                projected_query_cost_usd=query_cost,
                projected_daily_total_usd=projected_total,
                daily_budget_usd=self.policy.daily_budget_usd,
                daily_utilization_pct=utilization_pct,
                is_bytes_limit_exceeded=is_bytes_exceeded,
                alert_threshold_triggered=triggered_threshold,
            )

        return BudgetCheckResult(
            allowed=True,
            status="OK",
            reason="Query scan volume and projected daily spend are within policy limits.",
            projected_query_cost_usd=query_cost,
            projected_daily_total_usd=projected_total,
            daily_budget_usd=self.policy.daily_budget_usd,
            daily_utilization_pct=utilization_pct,
            is_bytes_limit_exceeded=is_bytes_exceeded,
            alert_threshold_triggered=triggered_threshold,
        )


# -----------------------------------------------------------------------------
# RUNAWAY QUERY REMEDIATOR & INCIDENT RESPONSE TOOLKIT
# -----------------------------------------------------------------------------

class RunawayQueryRemediator:
    """Generates emergency operational commands and SQL queries to remediate runaway queries."""

    @staticmethod
    def generate_cancel_command(job_id: str, project_id: str, location: str = "us-central1") -> str:
        """Format the gcloud / bq CLI command to immediately terminate a runaway BigQuery job."""
        return f"bq --project_id={project_id} --location={location} cancel {job_id}"

    @staticmethod
    def generate_investigation_sql(
        project_id: str,
        region: str = "us-central1",
        min_bytes_billed: int = 10_000_000_000,
    ) -> str:
        """SQL query to identify active or recent expensive queries in INFORMATION_SCHEMA."""
        return f"""
-- BigQuery Emergency Incident Response: Identify Runaway Queries
SELECT
    project_id,
    job_id,
    user_email,
    state,
    creation_time,
    start_time,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND) AS running_duration_seconds,
    total_bytes_billed,
    ROUND(total_bytes_billed / POWER(1024, 4) * 6.25, 4) AS estimated_cost_usd,
    total_slot_ms,
    query
FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE state = 'RUNNING'
  AND (total_bytes_billed >= {min_bytes_billed} OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND) > 300)
ORDER BY total_bytes_billed DESC, running_duration_seconds DESC
LIMIT 10;
        """.strip()

    @staticmethod
    def generate_session_guardrail_sql(max_bytes_billed: int = 53_687_091_200) -> str:
        """SQL snippet setting session-level cost guardrails."""
        return f"""
-- Enforce Query Level Billing Cap
SET @@maximum_bytes_billed = {max_bytes_billed};
SET @@session.query_label = 'cost_center=risk_engine';
        """.strip()
