"""Databricks Gold Pipeline & Job Orchestration Configuration.

Provides strongly typed configuration for orchestrating Databricks Delta Lake
Silver Cleansing, Gold Portfolio Value-at-Risk (VaR), and Risk Feature Store
pipelines from Apache Airflow / Google Cloud Composer.

Enforces automated retry policies (exponential backoff, retry on timeout),
GCP Spot executor cost optimization, Unity Catalog schema bindings, and
comprehensive parameterization for financial risk computations.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import os
from typing import Any, Dict, List, Optional


@dataclass
class DatabricksTaskRetryConfig:
    """Strongly typed automated retry policy for Databricks tasks and operators."""

    retries: int = 3
    retry_delay_seconds: int = 60
    retry_exponential_backoff: bool = True
    max_retry_delay_seconds: int = 300
    retry_on_timeout: bool = True
    min_retry_interval_millis: int = 60000

    def validate(self) -> None:
        """Validate retry configuration bounds."""
        if self.retries < 0:
            raise ValueError("retries cannot be negative")
        if self.retry_delay_seconds <= 0:
            raise ValueError("retry_delay_seconds must be positive")
        if self.max_retry_delay_seconds < self.retry_delay_seconds:
            raise ValueError("max_retry_delay_seconds must be >= retry_delay_seconds")
        if self.min_retry_interval_millis <= 0:
            raise ValueError("min_retry_interval_millis must be positive")

    def to_databricks_retry_dict(self) -> Dict[str, Any]:
        """Format as Databricks task retry policy dictionary."""
        return {
            "max_retries": self.retries,
            "min_retry_interval_millis": self.min_retry_interval_millis,
            "retry_on_timeout": self.retry_on_timeout,
        }


@dataclass
class DatabricksGoldJobConfig:
    """Strongly typed configuration for Databricks Gold Lakehouse orchestration."""

    environment: str = "dev"
    gcp_project_id: str = "trading-risk-dev-001"
    gcp_region: str = "us-central1"
    workspace_url: str = ""

    # Cluster Placement: existing cluster id OR new ephemeral job cluster
    existing_cluster_id: Optional[str] = None
    cluster_name: str = "tre-databricks-bronze-dev"
    spark_version: str = "15.4.x-scala2.12"
    node_type_id: str = "n2-standard-4"
    driver_node_type_id: str = "n2-standard-4"
    min_workers: int = 2
    max_workers: int = 8
    autotermination_minutes: int = 20
    use_spot_executors: bool = True

    # GCS Bucket Bindings
    bronze_bucket: str = ""
    silver_bucket: str = ""
    gold_bucket: str = ""

    # Unity Catalog & Schemas
    catalog_name: str = "trading_risk"
    bronze_schema_name: str = "bronze"
    silver_schema_name: str = "silver"
    gold_schema_name: str = "gold"

    # Quantitative Risk Modeling Parameters
    confidence_levels: str = "0.95,0.99"
    monte_carlo_simulations: int = 10000
    var_limit_usd: float = 250000.0
    horizon_days: int = 1
    run_optimize: bool = True
    run_vacuum: bool = True

    # Airflow Connection & Notebook Paths
    databricks_conn_id: str = "databricks_default"
    silver_notebook_path: str = "/Workspace/trading-risk/notebooks/02_silver_cleansing_runner"
    gold_notebook_path: str = "/Workspace/trading-risk/notebooks/03_gold_var_feature_store_runner"

    # Retry Policy
    retry_config: DatabricksTaskRetryConfig = field(default_factory=DatabricksTaskRetryConfig)

    def __post_init__(self) -> None:
        if not self.workspace_url:
            self.workspace_url = f"https://trading-risk-{self.environment}.cloud.databricks.com"
        if not self.bronze_bucket:
            self.bronze_bucket = f"trading-risk-bronze-{self.environment}"
        if not self.silver_bucket:
            self.silver_bucket = f"trading-risk-silver-{self.environment}"
        if not self.gold_bucket:
            self.gold_bucket = f"trading-risk-gold-{self.environment}"
        if not self.cluster_name:
            self.cluster_name = f"tre-databricks-bronze-{self.environment}"

        self.validate()

    def validate(self) -> None:
        """Validate configuration invariants."""
        if not self.environment:
            raise ValueError("environment cannot be empty")
        if self.min_workers < 1:
            raise ValueError("min_workers must be at least 1")
        if self.max_workers < self.min_workers:
            raise ValueError("max_workers must be >= min_workers")
        if self.monte_carlo_simulations <= 0:
            raise ValueError("monte_carlo_simulations must be positive")
        if self.var_limit_usd <= 0:
            raise ValueError("var_limit_usd must be positive")
        if self.horizon_days <= 0:
            raise ValueError("horizon_days must be positive")

        # Validate confidence levels format
        for lvl in self.confidence_levels.split(","):
            try:
                val = float(lvl.strip())
                if not (0.0 < val < 1.0):
                    raise ValueError(f"Confidence level {val} must be between 0.0 and 1.0")
            except ValueError as err:
                raise ValueError(f"Invalid confidence level specification '{self.confidence_levels}': {err}") from err

        self.retry_config.validate()

    def to_cluster_spec_dict(self) -> Dict[str, Any]:
        """Construct Databricks new_cluster dictionary matching production Terraform specification."""
        spec: Dict[str, Any] = {
            "spark_version": self.spark_version,
            "node_type_id": self.node_type_id,
            "driver_node_type_id": self.driver_node_type_id,
            "autoscale": {
                "min_workers": self.min_workers,
                "max_workers": self.max_workers,
            },
            "autotermination_minutes": self.autotermination_minutes,
            "spark_conf": {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.databricks.delta.autoCompact.enabled": "true",
                "spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled": "true",
                "spark.sql.streaming.schemaInference": "false",
                "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                "spark.hadoop.fs.gs.auth.service.account.enable": "true",
            },
            "spark_env_vars": {
                "ENVIRONMENT": self.environment,
                "GCP_PROJECT": self.gcp_project_id,
                "GCP_REGION": self.gcp_region,
            },
            "custom_tags": {
                "project": "trading-risk-engine",
                "environment": self.environment,
                "managed_by": "airflow",
                "tier": "gold-lakehouse",
            },
        }

        if self.use_spot_executors:
            spec["gcp_attributes"] = {
                "use_preemptible_executors": True,
                "availability": "SPOT_WITH_FALLBACK_GCP",
            }

        return spec

    def to_silver_submit_run_dict(
        self,
        target_date: str = "{{ ds }}",
        run_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Construct submit-run payload for Databricks Silver Cleansing task."""
        name = run_name or f"tre-silver-cleansing-{self.environment}-{target_date}"
        task_def: Dict[str, Any] = {
            "task_key": "cleanse_enforce_and_deduplicate_to_silver",
            "notebook_task": {
                "notebook_path": self.silver_notebook_path,
                "base_parameters": {
                    "environment": self.environment,
                    "source_type": "all",
                    "ingestion_date": target_date,
                    "gcs_silver_bucket": self.silver_bucket,
                    "catalog_name": self.catalog_name,
                    "bronze_schema_name": self.bronze_schema_name,
                    "silver_schema_name": self.silver_schema_name,
                    "run_optimize": "true" if self.run_optimize else "false",
                    "run_vacuum": "true" if self.run_vacuum else "false",
                },
            },
            **self.retry_config.to_databricks_retry_dict(),
        }

        if self.existing_cluster_id:
            task_def["existing_cluster_id"] = self.existing_cluster_id
        else:
            task_def["new_cluster"] = self.to_cluster_spec_dict()

        return {
            "run_name": name,
            "tasks": [task_def],
            "format": "MULTI_TASK",
        }

    def to_gold_var_submit_dict(
        self,
        target_date: str = "{{ ds }}",
        run_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Construct submit-run payload for Databricks Gold VaR & Feature Store task."""
        name = run_name or f"tre-gold-var-feature-store-{self.environment}-{target_date}"
        task_def: Dict[str, Any] = {
            "task_key": "compute_portfolio_var_and_feature_store",
            "notebook_task": {
                "notebook_path": self.gold_notebook_path,
                "base_parameters": {
                    "environment": self.environment,
                    "valuation_date": target_date,
                    "orders_source_path": f"{self.catalog_name}.{self.silver_schema_name}.silver_trade_orders",
                    "ticks_source_path": f"{self.catalog_name}.{self.silver_schema_name}.silver_market_ticks",
                    "vwap_source_path": f"{self.catalog_name}.{self.silver_schema_name}.silver_rolling_vwap",
                    "var_target_path": f"{self.catalog_name}.{self.gold_schema_name}.gold_portfolio_var",
                    "feature_store_target_path": f"{self.catalog_name}.{self.gold_schema_name}.gold_risk_feature_store",
                    "confidence_levels": self.confidence_levels,
                    "monte_carlo_simulations": str(self.monte_carlo_simulations),
                    "var_limit_usd": str(self.var_limit_usd),
                    "horizon_days": str(self.horizon_days),
                    "gcs_gold_bucket": self.gold_bucket,
                    "catalog_name": self.catalog_name,
                    "silver_schema_name": self.silver_schema_name,
                    "gold_schema_name": self.gold_schema_name,
                    "run_optimize": "true" if self.run_optimize else "false",
                    "run_vacuum": "true" if self.run_vacuum else "false",
                },
            },
            **self.retry_config.to_databricks_retry_dict(),
        }

        if self.existing_cluster_id:
            task_def["existing_cluster_id"] = self.existing_cluster_id
        else:
            task_def["new_cluster"] = self.to_cluster_spec_dict()

        return {
            "run_name": name,
            "tasks": [task_def],
            "format": "MULTI_TASK",
        }

    def to_gold_maintenance_submit_dict(
        self,
        target_date: str = "{{ ds }}",
        run_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Construct submit-run payload for Gold Delta table maintenance (OPTIMIZE / VACUUM)."""
        name = run_name or f"tre-gold-maintenance-{self.environment}-{target_date}"
        task_def: Dict[str, Any] = {
            "task_key": "optimize_and_vacuum_gold_tables",
            "notebook_task": {
                "notebook_path": self.gold_notebook_path,
                "base_parameters": {
                    "environment": self.environment,
                    "valuation_date": target_date,
                    "catalog_name": self.catalog_name,
                    "gold_schema_name": self.gold_schema_name,
                    "run_optimize": "true",
                    "run_vacuum": "true",
                    "dry_run": "false",
                },
            },
            **self.retry_config.to_databricks_retry_dict(),
        }

        if self.existing_cluster_id:
            task_def["existing_cluster_id"] = self.existing_cluster_id
        else:
            task_def["new_cluster"] = self.to_cluster_spec_dict()

        return {
            "run_name": name,
            "tasks": [task_def],
            "format": "MULTI_TASK",
        }

    def to_silver_run_now_params(self, target_date: str = "{{ ds }}") -> Dict[str, str]:
        """Format parameters for triggering an existing Silver cleansing Databricks job."""
        return {
            "environment": self.environment,
            "source_type": "all",
            "ingestion_date": target_date,
            "gcs_silver_bucket": self.silver_bucket,
            "catalog_name": self.catalog_name,
            "bronze_schema_name": self.bronze_schema_name,
            "silver_schema_name": self.silver_schema_name,
            "run_optimize": "true" if self.run_optimize else "false",
            "run_vacuum": "true" if self.run_vacuum else "false",
        }

    def to_gold_var_run_now_params(self, target_date: str = "{{ ds }}") -> Dict[str, str]:
        """Format parameters for triggering an existing Gold VaR Databricks job."""
        return {
            "environment": self.environment,
            "valuation_date": target_date,
            "catalog_name": self.catalog_name,
            "silver_schema_name": self.silver_schema_name,
            "gold_schema_name": self.gold_schema_name,
            "confidence_levels": self.confidence_levels,
            "monte_carlo_simulations": str(self.monte_carlo_simulations),
            "var_limit_usd": str(self.var_limit_usd),
            "horizon_days": str(self.horizon_days),
            "run_optimize": "true" if self.run_optimize else "false",
            "run_vacuum": "true" if self.run_vacuum else "false",
        }

    @classmethod
    def from_env(cls) -> DatabricksGoldJobConfig:
        """Construct DatabricksGoldJobConfig from environment variables / Airflow variables."""
        env = os.getenv("AIRFLOW_VAR_ENVIRONMENT", os.getenv("ENVIRONMENT", "dev")).lower()
        project_id = os.getenv("AIRFLOW_VAR_GCP_PROJECT", os.getenv("GCP_PROJECT_ID", f"trading-risk-{env}-001"))
        region = os.getenv("AIRFLOW_VAR_GCP_REGION", os.getenv("GCP_REGION", "us-central1"))

        retry_cfg = DatabricksTaskRetryConfig(
            retries=int(os.getenv("AIRFLOW_VAR_DATABRICKS_RETRIES", "3")),
            retry_delay_seconds=int(os.getenv("AIRFLOW_VAR_DATABRICKS_RETRY_DELAY_SEC", "60")),
            retry_exponential_backoff=os.getenv("AIRFLOW_VAR_DATABRICKS_RETRY_BACKOFF", "true").lower() in {"true", "1"},
            max_retry_delay_seconds=int(os.getenv("AIRFLOW_VAR_DATABRICKS_MAX_RETRY_DELAY_SEC", "300")),
        )

        return cls(
            environment=env,
            gcp_project_id=project_id,
            gcp_region=region,
            workspace_url=os.getenv("AIRFLOW_VAR_DATABRICKS_WORKSPACE_URL", f"https://trading-risk-{env}.cloud.databricks.com"),
            existing_cluster_id=os.getenv("AIRFLOW_VAR_DATABRICKS_CLUSTER_ID"),
            cluster_name=os.getenv("AIRFLOW_VAR_DATABRICKS_CLUSTER_NAME", f"tre-databricks-bronze-{env}"),
            bronze_bucket=os.getenv("AIRFLOW_VAR_GCS_BRONZE_BUCKET", f"trading-risk-bronze-{env}"),
            silver_bucket=os.getenv("AIRFLOW_VAR_GCS_SILVER_BUCKET", f"trading-risk-silver-{env}"),
            gold_bucket=os.getenv("AIRFLOW_VAR_GCS_GOLD_BUCKET", f"trading-risk-gold-{env}"),
            catalog_name=os.getenv("AIRFLOW_VAR_DATABRICKS_CATALOG", "trading_risk"),
            silver_schema_name=os.getenv("AIRFLOW_VAR_DATABRICKS_SILVER_SCHEMA", "silver"),
            gold_schema_name=os.getenv("AIRFLOW_VAR_DATABRICKS_GOLD_SCHEMA", "gold"),
            confidence_levels=os.getenv("AIRFLOW_VAR_VAR_CONFIDENCE_LEVELS", "0.95,0.99"),
            monte_carlo_simulations=int(os.getenv("AIRFLOW_VAR_VAR_MONTE_CARLO_SIMS", "10000")),
            var_limit_usd=float(os.getenv("AIRFLOW_VAR_VAR_LIMIT_USD", "250000.0")),
            horizon_days=int(os.getenv("AIRFLOW_VAR_VAR_HORIZON_DAYS", "1")),
            run_optimize=os.getenv("AIRFLOW_VAR_DELTA_OPTIMIZE", "true").lower() in {"true", "1"},
            run_vacuum=os.getenv("AIRFLOW_VAR_DELTA_VACUUM", "true").lower() in {"true", "1"},
            databricks_conn_id=os.getenv("AIRFLOW_VAR_DATABRICKS_CONN_ID", "databricks_default"),
            retry_config=retry_cfg,
        )
