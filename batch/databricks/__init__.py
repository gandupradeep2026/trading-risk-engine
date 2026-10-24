"""Databricks Medallion Lakehouse module for Trading Risk Engine."""

from batch.databricks.bronze_ingestion import (
    BronzeIngestionResult,
    BronzeIngestionTelemetry,
    DeltaBronzeIngestionEngine,
    DeltaLogManager,
    compute_record_hash,
)
from batch.databricks.client import (
    DatabricksClient,
    DatabricksClientException,
    MockDatabricksClient,
    RunLifeCycleState,
    RunResultState,
)
from batch.databricks.config import (
    BRONZE_MARKET_TICKS_CONFIG,
    BRONZE_ROLLING_VWAP_CONFIG,
    BRONZE_TRADE_ORDERS_CONFIG,
    DEFAULT_DELTA_SPARK_CONF,
    DatabricksClusterConfig,
    DatabricksWorkspaceConfig,
    DeltaTableConfig,
)
from batch.databricks.schemas import (
    ARROW_DELTA_BRONZE_ORDER_SCHEMA,
    ARROW_DELTA_BRONZE_TICK_SCHEMA,
    ARROW_DELTA_BRONZE_VWAP_SCHEMA,
    DELTA_BRONZE_ORDER_DDL,
    DELTA_BRONZE_TICK_DDL,
    DELTA_BRONZE_VWAP_DDL,
    QuarantinedBronzeRecord,
    validate_bronze_order_record,
    validate_bronze_tick_record,
    validate_bronze_vwap_record,
)

__all__ = [
    # Config
    "DatabricksClusterConfig",
    "DatabricksWorkspaceConfig",
    "DeltaTableConfig",
    "DEFAULT_DELTA_SPARK_CONF",
    "BRONZE_MARKET_TICKS_CONFIG",
    "BRONZE_TRADE_ORDERS_CONFIG",
    "BRONZE_ROLLING_VWAP_CONFIG",
    # Schemas
    "DELTA_BRONZE_TICK_DDL",
    "DELTA_BRONZE_ORDER_DDL",
    "DELTA_BRONZE_VWAP_DDL",
    "ARROW_DELTA_BRONZE_TICK_SCHEMA",
    "ARROW_DELTA_BRONZE_ORDER_SCHEMA",
    "ARROW_DELTA_BRONZE_VWAP_SCHEMA",
    "QuarantinedBronzeRecord",
    "validate_bronze_tick_record",
    "validate_bronze_order_record",
    "validate_bronze_vwap_record",
    # Ingestion & Delta Log
    "BronzeIngestionResult",
    "BronzeIngestionTelemetry",
    "DeltaLogManager",
    "DeltaBronzeIngestionEngine",
    "compute_record_hash",
    # Client
    "DatabricksClient",
    "MockDatabricksClient",
    "RunLifeCycleState",
    "RunResultState",
    "DatabricksClientException",
]
