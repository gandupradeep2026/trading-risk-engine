"""Configuration Models for Great Expectations Airflow Integration.

Encapsulates:
- GCP Lakehouse storage paths for Silver and Gold Delta tables.
- Data context and expectations store root paths.
- Checkpoint and suite identifiers.
- Threshold parameters (e.g. min_success_percent, fail_task_on_validation_failure).
- Automated retry parameters for transient storage failures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Any, Dict, Optional


@dataclass
class GreatExpectationsConfig:
    """Enterprise configuration for Great Expectations data quality validation gates."""
    environment: str = "dev"
    gcp_project_id: str = "trading-risk-dev-001"
    gcp_region: str = "us-central1"
    silver_bucket: str = "trading-risk-silver-dev"
    gold_bucket: str = "trading-risk-gold-dev"
    data_context_root_dir: str = "data_quality/great_expectations"
    gcs_data_docs_prefix: str = "great_expectations/data_docs"
    fail_task_on_validation_failure: bool = True
    min_success_percent: float = 100.0
    checkpoint_retry_limit: int = 2
    checkpoint_retry_delay_seconds: int = 30
    retry_exponential_backoff: bool = True
    max_retry_delay_seconds: int = 120

    # Canonical Checkpoints
    silver_checkpoint_name: str = "silver_quality_checkpoint"
    gold_checkpoint_name: str = "gold_quality_checkpoint"
    master_checkpoint_name: str = "lakehouse_master_checkpoint"

    # Canonical Suites
    silver_ticks_suite: str = "silver_market_ticks_suite"
    silver_orders_suite: str = "silver_trade_orders_suite"
    silver_vwap_suite: str = "silver_rolling_vwap_suite"
    gold_var_suite: str = "gold_portfolio_var_suite"
    gold_features_suite: str = "gold_risk_features_suite"

    def __post_init__(self) -> None:
        valid_envs = {"dev", "test", "staging", "prod"}
        if self.environment not in valid_envs:
            raise ValueError(f"Invalid environment '{self.environment}'. Must be one of {valid_envs}")
        if not (0.0 <= self.min_success_percent <= 100.0):
            raise ValueError(f"min_success_percent must be between 0.0 and 100.0, got {self.min_success_percent}")
        if self.checkpoint_retry_limit < 0:
            raise ValueError(f"checkpoint_retry_limit must be >= 0, got {self.checkpoint_retry_limit}")
        if self.checkpoint_retry_delay_seconds < 0:
            raise ValueError(f"checkpoint_retry_delay_seconds must be >= 0, got {self.checkpoint_retry_delay_seconds}")
        if not self.data_context_root_dir:
            raise ValueError("data_context_root_dir cannot be empty")
        if not self.silver_bucket or not self.gold_bucket:
            raise ValueError("silver_bucket and gold_bucket cannot be empty")

    @classmethod
    def from_env(cls) -> GreatExpectationsConfig:
        """Construct configuration from environment variables with sensible defaults."""
        env = os.environ.get("AIRFLOW_VAR_ENVIRONMENT", os.environ.get("GCP_ENVIRONMENT", "dev"))
        project = os.environ.get("AIRFLOW_VAR_GCP_PROJECT", os.environ.get("GCP_PROJECT_ID", f"trading-risk-{env}-001"))
        silver_b = os.environ.get("AIRFLOW_VAR_GCS_SILVER_BUCKET", f"trading-risk-silver-{env}")
        gold_b = os.environ.get("AIRFLOW_VAR_GCS_GOLD_BUCKET", f"trading-risk-gold-{env}")
        root_dir = os.environ.get("AIRFLOW_VAR_GREAT_EXPECTATIONS_ROOT", "data_quality/great_expectations")

        fail_on_fail_str = os.environ.get("AIRFLOW_VAR_FAIL_ON_DQ_FAILURE", "true").lower()
        fail_on_fail = fail_on_fail_str in ("true", "1", "yes")

        min_pct = float(os.environ.get("AIRFLOW_VAR_DQ_MIN_SUCCESS_PCT", "100.0"))

        return cls(
            environment=env,
            gcp_project_id=project,
            silver_bucket=silver_b,
            gold_bucket=gold_b,
            data_context_root_dir=root_dir,
            fail_task_on_validation_failure=fail_on_fail,
            min_success_percent=min_pct,
        )

    def get_silver_ticks_path(self, target_date: str) -> str:
        """Get GCS URI for Silver ticks partition."""
        # target_date expected YYYY-MM-DD
        parts = target_date.split("-")
        if len(parts) == 3:
            y, m, d = parts
            return f"gs://{self.silver_bucket}/delta/silver/silver_market_ticks/year={y}/month={m}/day={d}"
        return f"gs://{self.silver_bucket}/delta/silver/silver_market_ticks"

    def get_silver_orders_path(self, target_date: str) -> str:
        """Get GCS URI for Silver trade orders partition."""
        parts = target_date.split("-")
        if len(parts) == 3:
            y, m, d = parts
            return f"gs://{self.silver_bucket}/delta/silver/silver_trade_orders/year={y}/month={m}/day={d}"
        return f"gs://{self.silver_bucket}/delta/silver/silver_trade_orders"

    def get_silver_vwap_path(self, target_date: str) -> str:
        """Get GCS URI for Silver rolling VWAP partition."""
        parts = target_date.split("-")
        if len(parts) == 3:
            y, m, d = parts
            return f"gs://{self.silver_bucket}/delta/silver/silver_rolling_vwap/year={y}/month={m}/day={d}"
        return f"gs://{self.silver_bucket}/delta/silver/silver_rolling_vwap"

    def get_gold_var_path(self, target_date: str) -> str:
        """Get GCS URI for Gold portfolio VaR partition."""
        parts = target_date.split("-")
        if len(parts) == 3:
            y, m, d = parts
            return f"gs://{self.gold_bucket}/delta/gold/gold_portfolio_var/year={y}/month={m}/day={d}"
        return f"gs://{self.gold_bucket}/delta/gold/gold_portfolio_var"

    def get_gold_features_path(self, target_date: str) -> str:
        """Get GCS URI for Gold risk feature store partition."""
        parts = target_date.split("-")
        if len(parts) == 3:
            y, m, d = parts
            return f"gs://{self.gold_bucket}/delta/gold/gold_risk_features/year={y}/month={m}/day={d}"
        return f"gs://{self.gold_bucket}/delta/gold/gold_risk_features"

    def get_data_docs_gcs_url(self) -> str:
        """Get canonical GCS URL for Great Expectations Data Docs."""
        return f"https://storage.googleapis.com/{self.gold_bucket}/{self.gcs_data_docs_prefix}/index.html"
