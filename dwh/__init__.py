"""Trading Risk Engine: Data Warehouse (DWH) Package (Days 22-24)."""

from dwh.bigquery.config import (
    BigQueryDatasetConfig,
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
from dwh.bigquery.schema_manager import BigQuerySchemaManager
from dwh.dbt.config import DbtConfig
from dwh.dbt.project_manager import DbtProjectManager
from dwh.dbt.runner import DbtRunner, DbtRunResult, DbtTelemetry

__version__ = "1.1.0"

__all__ = [
    "BigQueryDatasetConfig",
    "BigQueryEmulatorError",
    "BigQueryPartitionFilterRequiredError",
    "BigQuerySchemaManager",
    "BigQueryTelemetry",
    "DbtConfig",
    "DbtProjectManager",
    "DbtRunResult",
    "DbtRunner",
    "DbtTelemetry",
    "LocalBigQueryEmulator",
    "MATERIALIZED_VIEWS_CATALOG",
    "STAR_SCHEMA_CATALOG",
    "TablePartitionClusteringSpec",
]
