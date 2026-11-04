"""Trading Risk Engine: Data Warehouse (DWH) Package (Days 22-25)."""

from dwh.bigquery.config import (
    BigQueryConnectionConfig,
    BigQueryDatasetConfig,
    FEDERATED_VIEWS_CATALOG,
    MATERIALIZED_VIEWS_CATALOG,
    STAR_SCHEMA_CATALOG,
    TablePartitionClusteringSpec,
)
from dwh.bigquery.emulator import (
    BigQueryEmulatorError,
    BigQueryPartitionFilterRequiredError,
    BigQueryTelemetry,
    LocalBigQueryEmulator,
)
from dwh.bigquery.replication import (
    CloudSqlBigQueryReplicationRunner,
    REPLICATION_CATALOG,
    ReplicationConfig,
    ReplicationMode,
    ReplicationTelemetry,
    ReplicationWatermarkStore,
)
from dwh.bigquery.schema_manager import BigQuerySchemaManager
from dwh.dbt.config import DbtConfig
from dwh.dbt.project_manager import DbtProjectManager
from dwh.dbt.runner import DbtRunner, DbtRunResult, DbtTelemetry

__version__ = "1.2.0"

__all__ = [
    "BigQueryConnectionConfig",
    "BigQueryDatasetConfig",
    "BigQueryEmulatorError",
    "BigQueryPartitionFilterRequiredError",
    "BigQuerySchemaManager",
    "BigQueryTelemetry",
    "CloudSqlBigQueryReplicationRunner",
    "DbtConfig",
    "DbtProjectManager",
    "DbtRunResult",
    "DbtRunner",
    "DbtTelemetry",
    "FEDERATED_VIEWS_CATALOG",
    "LocalBigQueryEmulator",
    "MATERIALIZED_VIEWS_CATALOG",
    "REPLICATION_CATALOG",
    "ReplicationConfig",
    "ReplicationMode",
    "ReplicationTelemetry",
    "ReplicationWatermarkStore",
    "STAR_SCHEMA_CATALOG",
    "TablePartitionClusteringSpec",
]

