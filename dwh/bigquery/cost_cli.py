"""CLI Runner for BigQuery Cost Optimization & Query Antipattern Analysis (Day 26).

Usage:
    python -m dwh.bigquery.cost_cli --analyze-sql "SELECT * FROM fact_trade_orders"
    python -m dwh.bigquery.cost_cli --check-budget --query-bytes 10737418240 --daily-spend 15.0
    python -m dwh.bigquery.cost_cli --generate-runbook-query
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from dwh.bigquery.cost_optimizer import (
    BigQueryPricingModel,
    BudgetGuardrailManager,
    BudgetPolicy,
    PricingTier,
    QueryAntipatternAnalyzer,
    RunawayQueryRemediator,
)


def run_cost_cli(
    analyze_sql: Optional[str] = None,
    query_bytes: Optional[int] = None,
    pricing_model: str = "ON_DEMAND",
    check_budget: bool = False,
    daily_spend: float = 0.0,
    daily_budget: float = 50.0,
    max_bytes_billed: Optional[int] = None,
    generate_runbook_query: bool = False,
    project_id: str = "trading-risk-engine-dev",
    json_output: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute cost analysis, antipattern inspection, or budget checks."""
    summary: Dict[str, Any] = {
        "status": "SUCCESS",
        "project_id": project_id,
    }

    # 1. Antipattern analysis
    if analyze_sql:
        analyzer = QueryAntipatternAnalyzer()
        findings = analyzer.analyze(analyze_sql)
        summary["query_analysis"] = {
            "query": analyze_sql,
            "findings_count": len(findings),
            "findings": [f.to_dict() for f in findings],
        }

    # 2. Cost estimation
    if query_bytes is not None:
        tier = PricingTier(pricing_model)
        query_cost = BigQueryPricingModel.calculate_query_cost(query_bytes, pricing_tier=tier)
        summary["cost_estimation"] = {
            "bytes_scanned": query_bytes,
            "pricing_tier": tier.value,
            "cost_usd": round(query_cost, 6),
        }

    # 3. Budget check
    if check_budget and query_bytes is not None:
        policy = BudgetPolicy(
            project_id=project_id,
            daily_budget_usd=daily_budget,
        )
        if max_bytes_billed is not None:
            policy.max_bytes_billed_per_query = max_bytes_billed

        guardrail = BudgetGuardrailManager(policy)
        res = guardrail.evaluate_query(query_bytes, current_daily_spend_usd=daily_spend)
        summary["budget_check"] = res.to_dict()

    # 4. Generate incident response query
    if generate_runbook_query:
        summary["runbook_queries"] = {
            "investigation_sql": RunawayQueryRemediator.generate_investigation_sql(project_id),
            "cancel_command_example": RunawayQueryRemediator.generate_cancel_command("job_abc123", project_id),
            "session_guardrail_sql": RunawayQueryRemediator.generate_session_guardrail_sql(),
        }

    if json_output:
        out_p = Path(json_output)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        with open(out_p, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="BigQuery Cost Optimization & Query Antipattern Analyzer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--analyze-sql", type=str, default=None, help="SQL query to analyze for antipatterns")
    parser.add_argument("--query-bytes", type=int, default=None, help="Bytes scanned to estimate cost")
    parser.add_argument(
        "--pricing-model",
        default="ON_DEMAND",
        choices=["ON_DEMAND", "EDITION_STANDARD", "EDITION_ENTERPRISE", "EDITION_ENTERPRISE_PLUS"],
        help="BigQuery pricing model tier",
    )
    parser.add_argument("--check-budget", action="store_true", help="Check query against budget guardrails")
    parser.add_argument("--daily-spend", type=float, default=0.0, help="Current cumulative daily spend in USD")
    parser.add_argument("--daily-budget", type=float, default=50.0, help="Daily budget ceiling in USD")
    parser.add_argument("--max-bytes-billed", type=int, default=None, help="Maximum bytes billed per query ceiling")
    parser.add_argument("--generate-runbook-query", action="store_true", help="Generate incident response queries")
    parser.add_argument("--project-id", default="trading-risk-engine-dev", help="GCP Project ID")
    parser.add_argument("--json-output", default=None, help="Path to write JSON output")

    args = parser.parse_args()

    res = run_cost_cli(
        analyze_sql=args.analyze_sql,
        query_bytes=args.query_bytes,
        pricing_model=args.pricing_model,
        check_budget=args.check_budget,
        daily_spend=args.daily_spend,
        daily_budget=args.daily_budget,
        max_bytes_billed=args.max_bytes_billed,
        generate_runbook_query=args.generate_runbook_query,
        project_id=args.project_id,
        json_output=args.json_output,
    )

    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
