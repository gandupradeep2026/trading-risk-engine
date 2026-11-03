"""BigQuery Star-Schema Configuration & Partition/Clustering Specs (Day 22).

Defines strongly-typed configurations for BigQuery dataset provisioning,
table partitioning policies, clustering rules (max 4 columns), and cost controls
(require_partition_filter).
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class TablePartitionClusteringSpec(BaseModel):
    """Specification for table partitioning, clustering, and access controls."""

    table_name: str
    table_type: str = Field(default="FACT", description="DIMENSION or FACT")
    partition_column: Optional[str] = Field(
        default=None, description="Column used for partitioning (e.g. trade_date)"
    )
    partition_type: Optional[str] = Field(
        default=None, description="Partitioning granularity: DAY, HOUR, MONTH, YEAR"
    )
    require_partition_filter: bool = Field(
        default=False,
        description="Enforces that queries against this table must filter on partition column",
    )
    clustering_columns: List[str] = Field(
        default_factory=list,
        description="Ordered list of up to 4 clustering columns",
    )
    description: str = Field(default="", description="Table documentation")

    @field_validator("clustering_columns")
    @classmethod
    def validate_clustering_limit(cls, cols: List[str]) -> List[str]:
        if len(cols) > 4:
            raise ValueError(
                f"BigQuery supports a maximum of 4 clustering columns; received {len(cols)}: {cols}"
            )
        # Ensure distinct columns
        if len(cols) != len(set(cols)):
            raise ValueError(f"Clustering columns must be distinct; received: {cols}")
        return cols

    @field_validator("partition_type")
    @classmethod
    def validate_partition_type(cls, ptype: Optional[str]) -> Optional[str]:
        if ptype is not None:
            allowed = {"DAY", "HOUR", "MONTH", "YEAR"}
            if ptype.upper() not in allowed:
                raise ValueError(
                    f"Unsupported partition_type '{ptype}'. Allowed: {sorted(allowed)}"
                )
            return ptype.upper()
        return None

    @model_validator(mode="after")
    def validate_partition_consistency(self) -> TablePartitionClusteringSpec:
        if self.require_partition_filter and not self.partition_column:
            raise ValueError(
                f"Table '{self.table_name}' has require_partition_filter=True but partition_column is not specified."
            )
        if self.partition_column and not self.partition_type:
            self.partition_type = "DAY"
        return self


# -----------------------------------------------------------------------------
# CANONICAL STAR SCHEMA TABLE SPECS
# -----------------------------------------------------------------------------

DIM_TICKERS_SPEC = TablePartitionClusteringSpec(
    table_name="dim_tickers",
    table_type="DIMENSION",
    clustering_columns=["asset_class", "ticker"],
    description="Financial instrument dimension capturing tickers, asset classes, and contract lot sizes.",
)

DIM_PORTFOLIOS_SPEC = TablePartitionClusteringSpec(
    table_name="dim_portfolios",
    table_type="DIMENSION",
    clustering_columns=["desk", "portfolio_id"],
    description="Trading portfolio book dimension capturing books, desks, and risk mandate limits.",
)

DIM_TRADERS_SPEC = TablePartitionClusteringSpec(
    table_name="dim_traders",
    table_type="DIMENSION",
    clustering_columns=["desk", "trader_id"],
    description="Trading personnel dimension capturing authorized traders, roles, and single order limits.",
)

DIM_DATE_SPEC = TablePartitionClusteringSpec(
    table_name="dim_date",
    table_type="DIMENSION",
    clustering_columns=["year", "quarter"],
    description="Financial calendar dimension table capturing market sessions, holidays, and fiscal periods.",
)

FACT_TRADE_ORDERS_SPEC = TablePartitionClusteringSpec(
    table_name="fact_trade_orders",
    table_type="FACT",
    partition_column="trade_date",
    partition_type="DAY",
    require_partition_filter=True,
    clustering_columns=["portfolio_id", "ticker", "trader_id", "execution_status"],
    description="Fact table tracking executed trade orders, fills, fees, slippage, and notional exposure.",
)

FACT_MARKET_TICKS_SPEC = TablePartitionClusteringSpec(
    table_name="fact_market_ticks",
    table_type="FACT",
    partition_column="tick_date",
    partition_type="DAY",
    require_partition_filter=True,
    clustering_columns=["asset_class", "ticker"],
    description="Fact table storing granular high-frequency market tick prints, spreads, and order book top.",
)

FACT_RISK_STATS_SPEC = TablePartitionClusteringSpec(
    table_name="fact_risk_stats",
    table_type="FACT",
    partition_column="valuation_date",
    partition_type="DAY",
    require_partition_filter=True,
    clustering_columns=["portfolio_id", "risk_regime", "confidence_level"],
    description="Fact table storing institutional portfolio Value-at-Risk (VaR), CVaR, limits, and exposures.",
)

FACT_RISK_ALERTS_SPEC = TablePartitionClusteringSpec(
    table_name="fact_risk_alerts",
    table_type="FACT",
    partition_column="alert_date",
    partition_type="DAY",
    require_partition_filter=False,
    clustering_columns=["portfolio_id", "ticker", "severity", "alert_type"],
    description="Fact table storing streaming market anomalies, flash crash drawdowns, and limit breaches.",
)

STAR_SCHEMA_CATALOG: Dict[str, TablePartitionClusteringSpec] = {
    spec.table_name: spec
    for spec in [
        DIM_TICKERS_SPEC,
        DIM_PORTFOLIOS_SPEC,
        DIM_TRADERS_SPEC,
        DIM_DATE_SPEC,
        FACT_TRADE_ORDERS_SPEC,
        FACT_MARKET_TICKS_SPEC,
        FACT_RISK_STATS_SPEC,
        FACT_RISK_ALERTS_SPEC,
    ]
}

# -----------------------------------------------------------------------------
# MATERIALIZED VIEWS SPECS (DAY 24)
# -----------------------------------------------------------------------------

MV_REALTIME_MARKET_PERFORMANCE_SPEC = TablePartitionClusteringSpec(
    table_name="mv_realtime_market_performance",
    table_type="MATERIALIZED_VIEW",
    partition_column="tick_date",
    partition_type="DAY",
    require_partition_filter=False,
    clustering_columns=["asset_class", "ticker"],
    description="Real-time materialized view aggregating intraday market ticks and microstructure metrics.",
)

MV_REALTIME_TRADER_EXPOSURE_SPEC = TablePartitionClusteringSpec(
    table_name="mv_realtime_trader_exposure",
    table_type="MATERIALIZED_VIEW",
    partition_column="trade_date",
    partition_type="DAY",
    require_partition_filter=False,
    clustering_columns=["portfolio_id", "ticker", "trader_id"],
    description="Real-time materialized view aggregating intraday trade executions, notional volume, and slippage.",
)

MV_REALTIME_RISK_ALERTS_SPEC = TablePartitionClusteringSpec(
    table_name="mv_realtime_risk_alerts",
    table_type="MATERIALIZED_VIEW",
    partition_column="alert_date",
    partition_type="DAY",
    require_partition_filter=False,
    clustering_columns=["portfolio_id", "severity", "alert_type"],
    description="Real-time materialized view aggregating streaming risk alerts and anomaly breach metrics.",
)

MATERIALIZED_VIEWS_CATALOG: Dict[str, TablePartitionClusteringSpec] = {
    spec.table_name: spec
    for spec in [
        MV_REALTIME_MARKET_PERFORMANCE_SPEC,
        MV_REALTIME_TRADER_EXPOSURE_SPEC,
        MV_REALTIME_RISK_ALERTS_SPEC,
    ]
}


class BigQueryDatasetConfig(BaseModel):
    """Enterprise configuration for BigQuery Data Warehouse dataset."""

    project_id: str = Field(default="trading-risk-dev-001", description="GCP project ID")
    dataset_id: str = Field(
        default="trading_risk_dwh_dev", description="BigQuery dataset identifier"
    )
    dataset_name: str = Field(
        default="Trading Risk Enterprise Data Warehouse", description="Friendly name"
    )
    dataset_description: str = Field(
        default="Enterprise star schema analytical data warehouse for trading orders, market ticks, portfolio VaR, and risk analytics.",
        description="Dataset description",
    )
    location: str = Field(default="us-central1", description="GCP region or multi-region")
    environment: str = Field(default="dev", description="dev, staging, or prod")
    delete_contents_on_destroy: bool = Field(
        default=True, description="Allow table deletion on destroy"
    )
    require_partition_filter: bool = Field(
        default=True,
        description="Enforce mandatory WHERE clause partition filter on fact tables",
    )
    default_table_expiration_days: Optional[int] = Field(
        default=None, description="Expiration in days (None for permanent)"
    )
    tables: Dict[str, TablePartitionClusteringSpec] = Field(
        default_factory=lambda: dict(STAR_SCHEMA_CATALOG),
        description="Table catalog specifications",
    )
    labels: Dict[str, str] = Field(
        default_factory=dict, description="Resource labels"
    )

    @classmethod
    def from_env(cls) -> BigQueryDatasetConfig:
        """Instantiate configuration from environment variables."""
        env = os.getenv("ENVIRONMENT", "dev").lower()
        project_id = os.getenv("GCP_PROJECT_ID", f"trading-risk-{env}-001")
        dataset_id = os.getenv("BIGQUERY_DATASET_ID", f"trading_risk_dwh_{env}")
        location = os.getenv("GCP_REGION", "us-central1")
        delete_on_destroy = env != "prod"

        return cls(
            project_id=project_id,
            dataset_id=dataset_id,
            environment=env,
            location=location,
            delete_contents_on_destroy=delete_on_destroy,
            require_partition_filter=True,
            labels={"environment": env, "platform": "trading-risk-engine"},
        )

    def get_full_table_id(self, table_name: str) -> str:
        """Format standard GCP BigQuery full table reference (project.dataset.table)."""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"
