"""Databricks Workspace and Delta Lake Configuration Models.

Provides strongly typed configuration models for:
- Databricks Lakehouse clusters on GCP (autoscale, spot instances, Delta Spark configs)
- Databricks workspace connection settings (host, auth, catalogs, GCS buckets)
- Delta Lake table specifications (Hive partitioning, Z-Ordering, CDF, vacuum retention)
"""

from __future__ import annotations

from dataclasses import dataclass, field
import os
import re
from typing import Any, Dict, List, Optional


DEFAULT_DELTA_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled": "true",
    "spark.sql.streaming.schemaInference": "false",
    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "spark.hadoop.fs.gs.auth.service.account.enable": "true",
}


@dataclass
class DatabricksClusterConfig:
    """Configuration for a Databricks Lakehouse compute cluster on GCP."""

    cluster_name: str = "tre-databricks-bronze-dev"
    spark_version: str = "15.4.x-scala2.12"
    node_type_id: str = "n2-standard-4"
    driver_node_type_id: Optional[str] = None
    min_workers: int = 1
    max_workers: int = 8
    autotermination_minutes: int = 20
    gcp_availability: str = "SPOT_WITH_FALLBACK_GCP"
    use_preemptible_executors: bool = True
    spark_conf: Dict[str, str] = field(default_factory=lambda: dict(DEFAULT_DELTA_SPARK_CONF))
    spark_env_vars: Dict[str, str] = field(default_factory=dict)
    custom_tags: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate cluster configuration invariants."""
        if not self.cluster_name or not self.cluster_name.strip():
            raise ValueError("cluster_name must be a non-empty string")
        if self.min_workers < 0:
            raise ValueError(f"min_workers must be non-negative, got {self.min_workers}")
        if self.max_workers < 1:
            raise ValueError(f"max_workers must be at least 1, got {self.max_workers}")
        if self.min_workers > self.max_workers:
            raise ValueError(
                f"min_workers ({self.min_workers}) cannot exceed max_workers ({self.max_workers})"
            )
        if self.autotermination_minutes < 0:
            raise ValueError(
                f"autotermination_minutes must be >= 0, got {self.autotermination_minutes}"
            )
        valid_availabilities = {"SPOT_GCP", "SPOT_WITH_FALLBACK_GCP", "ON_DEMAND_GCP"}
        if self.gcp_availability not in valid_availabilities:
            raise ValueError(
                f"Invalid gcp_availability '{self.gcp_availability}'. Expected one of {valid_availabilities}"
            )

    @classmethod
    def from_env(cls, prefix: str = "DATABRICKS_CLUSTER_") -> DatabricksClusterConfig:
        """Construct DatabricksClusterConfig from environment variables."""
        return cls(
            cluster_name=os.getenv(f"{prefix}NAME", "tre-databricks-bronze-dev"),
            spark_version=os.getenv(f"{prefix}SPARK_VERSION", "15.4.x-scala2.12"),
            node_type_id=os.getenv(f"{prefix}NODE_TYPE", "n2-standard-4"),
            driver_node_type_id=os.getenv(f"{prefix}DRIVER_NODE_TYPE") or None,
            min_workers=int(os.getenv(f"{prefix}MIN_WORKERS", "1")),
            max_workers=int(os.getenv(f"{prefix}MAX_WORKERS", "8")),
            autotermination_minutes=int(os.getenv(f"{prefix}AUTOTERMINATION_MINUTES", "20")),
            gcp_availability=os.getenv(f"{prefix}GCP_AVAILABILITY", "SPOT_WITH_FALLBACK_GCP"),
        )

    def to_api_payload(self) -> Dict[str, Any]:
        """Convert configuration into Databricks Clusters REST API payload."""
        driver_type = self.driver_node_type_id or self.node_type_id
        return {
            "cluster_name": self.cluster_name,
            "spark_version": self.spark_version,
            "node_type_id": self.node_type_id,
            "driver_node_type_id": driver_type,
            "autotermination_minutes": self.autotermination_minutes,
            "autoscale": {
                "min_workers": self.min_workers,
                "max_workers": self.max_workers,
            },
            "gcp_attributes": {
                "use_preemptible_executors": self.use_preemptible_executors,
                "availability": self.gcp_availability,
            },
            "spark_conf": dict(self.spark_conf),
            "spark_env_vars": dict(self.spark_env_vars),
            "custom_tags": dict(self.custom_tags),
        }


@dataclass
class DatabricksWorkspaceConfig:
    """Configuration for connecting to a Databricks Workspace on GCP."""

    workspace_url: str = "https://trading-risk-dev.cloud.databricks.com"
    token: str = ""
    environment: str = "dev"
    project_id: str = "trading-risk-dev-001"
    region: str = "us-central1"
    catalog_name: str = "trading_risk"
    bronze_bucket_name: str = ""
    silver_bucket_name: str = ""
    gold_bucket_name: str = ""

    def __post_init__(self) -> None:
        """Validate workspace configuration parameters."""
        if not self.workspace_url.startswith(("http://", "https://")):
            raise ValueError(f"workspace_url must start with http:// or https://: {self.workspace_url}")
        self.workspace_url = self.workspace_url.rstrip("/")

        if not self.bronze_bucket_name:
            self.bronze_bucket_name = f"tre-{self.project_id}-{self.environment}-bronze-raw"
        if not self.silver_bucket_name:
            self.silver_bucket_name = f"tre-{self.project_id}-{self.environment}-silver-processed"
        if not self.gold_bucket_name:
            self.gold_bucket_name = f"tre-{self.project_id}-{self.environment}-gold-curated"

    @property
    def bronze_delta_path(self) -> str:
        """Return the root GCS URI for Bronze Delta Lake tables."""
        return f"gs://{self.bronze_bucket_name}/delta/bronze"

    @property
    def silver_delta_path(self) -> str:
        """Return the root GCS URI for Silver Delta Lake tables."""
        return f"gs://{self.silver_bucket_name}/delta/silver"

    @property
    def gold_delta_path(self) -> str:
        """Return the root GCS URI for Gold Delta Lake tables."""
        return f"gs://{self.gold_bucket_name}/delta/gold"

    def get_table_uri(self, table_name: str, tier: str = "bronze") -> str:
        """Get canonical GCS Delta storage path for a table."""
        if tier.lower() == "bronze":
            return f"{self.bronze_delta_path}/{table_name}"
        elif tier.lower() == "silver":
            return f"{self.silver_delta_path}/{table_name}"
        elif tier.lower() == "gold":
            return f"{self.gold_delta_path}/{table_name}"
        else:
            raise ValueError(f"Unknown Medallion tier: {tier}")

    @classmethod
    def from_env(cls) -> DatabricksWorkspaceConfig:
        """Construct DatabricksWorkspaceConfig from environment variables."""
        return cls(
            workspace_url=os.getenv("DATABRICKS_HOST", "https://trading-risk-dev.cloud.databricks.com"),
            token=os.getenv("DATABRICKS_TOKEN", ""),
            environment=os.getenv("ENVIRONMENT", "dev"),
            project_id=os.getenv("GCP_PROJECT_ID", "trading-risk-dev-001"),
            region=os.getenv("GCP_REGION", "us-central1"),
            catalog_name=os.getenv("DATABRICKS_CATALOG", "trading_risk"),
            bronze_bucket_name=os.getenv("GCS_BRONZE_BUCKET", ""),
            silver_bucket_name=os.getenv("GCS_SILVER_BUCKET", ""),
            gold_bucket_name=os.getenv("GCS_GOLD_BUCKET", ""),
        )


@dataclass
class DeltaTableConfig:
    """Specification for a Delta Lake table in the Analytical Lakehouse."""

    table_name: str
    catalog: str = "trading_risk"
    schema: str = "bronze"
    partition_columns: List[str] = field(default_factory=list)
    z_order_columns: List[str] = field(default_factory=list)
    enable_change_data_feed: bool = True
    auto_optimize_write: bool = True
    auto_compact: bool = True
    vacuum_retention_hours: int = 168  # 7 days
    log_retention_duration: str = "interval 30 days"
    deleted_file_retention_duration: str = "interval 7 days"

    def __post_init__(self) -> None:
        """Validate Delta table configuration."""
        if not self.table_name or not re.match(r"^[a-zA-Z0-9_]+$", self.table_name):
            raise ValueError(f"Invalid table_name '{self.table_name}'. Must be alphanumeric with underscores.")
        if self.vacuum_retention_hours < 0:
            raise ValueError(f"vacuum_retention_hours must be >= 0, got {self.vacuum_retention_hours}")

    @property
    def full_table_identifier(self) -> str:
        """Return three-tier Unity Catalog table identifier: catalog.schema.table."""
        return f"{self.catalog}.{self.schema}.{self.table_name}"

    def to_table_properties(self) -> Dict[str, str]:
        """Generate Spark SQL Delta table properties."""
        return {
            "delta.enableChangeDataFeed": str(self.enable_change_data_feed).lower(),
            "delta.autoOptimize.optimizeWrite": str(self.auto_optimize_write).lower(),
            "delta.autoOptimize.autoCompact": str(self.auto_compact).lower(),
            "delta.logRetentionDuration": self.log_retention_duration,
            "delta.deletedFileRetentionDuration": self.deleted_file_retention_duration,
        }


# Canonical Delta Bronze Table Configurations
BRONZE_MARKET_TICKS_CONFIG = DeltaTableConfig(
    table_name="bronze_market_ticks",
    catalog="trading_risk",
    schema="bronze",
    partition_columns=["year", "month", "day", "asset_class"],
    z_order_columns=["ticker", "event_timestamp"],
    enable_change_data_feed=True,
    auto_optimize_write=True,
    auto_compact=True,
    vacuum_retention_hours=168,
)

BRONZE_TRADE_ORDERS_CONFIG = DeltaTableConfig(
    table_name="bronze_trade_orders",
    catalog="trading_risk",
    schema="bronze",
    partition_columns=["year", "month", "day", "portfolio_id"],
    z_order_columns=["order_id", "ticker", "timestamp"],
    enable_change_data_feed=True,
    auto_optimize_write=True,
    auto_compact=True,
    vacuum_retention_hours=168,
)

BRONZE_ROLLING_VWAP_CONFIG = DeltaTableConfig(
    table_name="bronze_rolling_vwap",
    catalog="trading_risk",
    schema="bronze",
    partition_columns=["year", "month", "day", "asset_class"],
    z_order_columns=["ticker", "window_start"],
    enable_change_data_feed=True,
    auto_optimize_write=True,
    auto_compact=True,
    vacuum_retention_hours=168,
)
