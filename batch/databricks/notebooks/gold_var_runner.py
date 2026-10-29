"""Databricks Production Notebook / Task Runner for Gold Portfolio VaR & Feature Store.

Can be executed:
1. Inside a Databricks cluster as a notebook / python task reading `dbutils.widgets`.
2. Locally or in CI/CD pipelines via CLI flags (`--environment`, `--valuation-date`, `--dry-run`, etc.).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from typing import Any, Dict, List, Optional

from batch.databricks.config import (
    DatabricksWorkspaceConfig,
    GOLD_PORTFOLIO_VAR_CONFIG,
    GOLD_RISK_FEATURE_STORE_CONFIG,
)
from batch.databricks.gold_var_engine import DeltaGoldVaREngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("gold_var_runner")


def get_parameter(param_name: str, default_val: str = "") -> str:
    """Extract parameter from dbutils.widgets if running in Databricks, else environment or default."""
    try:
        import IPython  # type: ignore
        dbutils = IPython.get_ipython().user_ns.get("dbutils")  # type: ignore
        if dbutils:
            return str(dbutils.widgets.get(param_name))
    except Exception:
        pass
    env_key = param_name.upper().replace("-", "_")
    return os.getenv(env_key, default_val)


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments with Databricks widget fallback."""
    parser = argparse.ArgumentParser(
        description="Run Databricks Delta Lake Gold Portfolio VaR & Risk Feature Store."
    )
    parser.add_argument(
        "--environment",
        type=str,
        default=get_parameter("environment", "dev"),
        help="Deployment environment (dev, staging, prod)",
    )
    parser.add_argument(
        "--valuation-date",
        type=str,
        default=get_parameter("valuation_date", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
        help="Target valuation date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--orders-source-path",
        type=str,
        default=get_parameter("orders_source_path", "silver_trade_orders"),
        help="Path or table name for Silver trade orders",
    )
    parser.add_argument(
        "--ticks-source-path",
        type=str,
        default=get_parameter("ticks_source_path", "silver_market_ticks"),
        help="Path or table name for Silver market ticks",
    )
    parser.add_argument(
        "--vwap-source-path",
        type=str,
        default=get_parameter("vwap_source_path", "silver_rolling_vwap"),
        help="Path or table name for Silver rolling VWAP",
    )
    parser.add_argument(
        "--var-target-path",
        type=str,
        default=get_parameter("var_target_path", "gold_portfolio_var"),
        help="Target path or table name for Gold Portfolio VaR",
    )
    parser.add_argument(
        "--feature-store-target-path",
        type=str,
        default=get_parameter("feature_store_target_path", "gold_risk_feature_store"),
        help="Target path or table name for Gold Risk Feature Store",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=int(get_parameter("horizon_days", "1")),
        help="VaR risk horizon in trading days",
    )
    parser.add_argument(
        "--monte-carlo-simulations",
        type=int,
        default=int(get_parameter("monte_carlo_simulations", "2000")),
        help="Number of Monte Carlo simulation paths per portfolio",
    )
    parser.add_argument(
        "--var-limit-usd",
        type=float,
        default=float(get_parameter("var_limit_usd", "250000.0")),
        help="Default portfolio 99% VaR limit in USD",
    )
    parser.add_argument(
        "--gcs-gold-bucket",
        type=str,
        default=get_parameter("gcs_gold_bucket", ""),
        help="GCS Gold bucket name override",
    )
    parser.add_argument(
        "--run-optimize",
        type=lambda v: str(v).lower() in {"true", "1", "yes"},
        default=get_parameter("run_optimize", "true").lower() in {"true", "1", "yes"},
        help="Run Delta OPTIMIZE and Z-Ordering on Gold tables",
    )
    parser.add_argument(
        "--run-vacuum",
        type=lambda v: str(v).lower() in {"true", "1", "yes"},
        default=get_parameter("run_vacuum", "true").lower() in {"true", "1", "yes"},
        help="Run Delta VACUUM on Gold tables",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=get_parameter("dry_run", "false").lower() in {"true", "1", "yes"},
        help="Validate configuration and output execution plan without modifying tables",
    )
    return parser.parse_args(args)


def run_gold_var_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    """Execute Gold Portfolio VaR & Feature Store calculation pipeline."""
    workspace_cfg = DatabricksWorkspaceConfig(
        environment=args.environment,
        gold_bucket_name=args.gcs_gold_bucket,
    )
    val_date = datetime.strptime(args.valuation_date, "%Y-%m-%d").date()

    if args.dry_run:
        plan = {
            "status": "DRY_RUN_SUCCESS",
            "environment": args.environment,
            "valuation_date": str(val_date),
            "horizon_days": args.horizon_days,
            "monte_carlo_simulations": args.monte_carlo_simulations,
            "var_limit_usd": args.var_limit_usd,
            "gold_delta_root": workspace_cfg.gold_delta_path,
            "run_optimize": args.run_optimize,
            "run_vacuum": args.run_vacuum,
        }
        logger.info(f"Gold VaR dry-run plan: {json.dumps(plan)}")
        return plan

    engine = DeltaGoldVaREngine(workspace_config=workspace_cfg)
    result = engine.compute_var_and_feature_store(
        orders_source=args.orders_source_path,
        ticks_source=args.ticks_source_path,
        vwap_source=args.vwap_source_path,
        var_target_path=args.var_target_path,
        feature_store_target_path=args.feature_store_target_path,
        valuation_date=val_date,
        horizon_days=args.horizon_days,
        monte_carlo_paths=args.monte_carlo_simulations,
        var_limit_usd=args.var_limit_usd,
    )

    if args.run_optimize:
        engine.optimize_table(
            result.var_table_path,
            z_order_columns=GOLD_PORTFOLIO_VAR_CONFIG.z_order_columns,
        )
        engine.optimize_table(
            result.feature_store_path,
            z_order_columns=GOLD_RISK_FEATURE_STORE_CONFIG.z_order_columns,
        )

    if args.run_vacuum:
        engine.vacuum_table(result.var_table_path, retention_hours=168)
        engine.vacuum_table(result.feature_store_path, retention_hours=168)

    return {
        "status": "SUCCESS",
        "environment": args.environment,
        "result": result.to_dict(),
        "telemetry_metrics": engine.telemetry.format_prometheus(),
    }


def main(argv: Optional[List[str]] = None) -> None:
    """CLI Entrypoint."""
    args = parse_args(argv)
    output = run_gold_var_pipeline(args)
    output_json = json.dumps(output, indent=2, default=str)
    print(output_json)

    try:
        import IPython  # type: ignore
        dbutils = IPython.get_ipython().user_ns.get("dbutils")  # type: ignore
        if dbutils:
            dbutils.notebook.exit(output_json)
    except Exception:
        pass


if __name__ == "__main__":
    main()
