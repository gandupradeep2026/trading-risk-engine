"""Trading Risk Engine: Data Warehouse (DWH) Package (Days 22-26)."""

from dwh.bigquery.benchmark import (
    BENCHMARK_SCENARIOS_CATALOG,
    BenchmarkCategory,
    BenchmarkMetricResult,
    BenchmarkRunner,
    BenchmarkScenario,
    BenchmarkSuiteResult,
    BenchmarkTelemetry,
)
from dwh.bigquery.config import (
    BigQueryConnectionConfig,
    BigQueryDatasetConfig,
    FEDERATED_VIEWS_CATALOG,
    MATERIALIZED_VIEWS_CATALOG,
    STAR_SCHEMA_CATALOG,
    TablePartitionClusteringSpec,
)
from dwh.bigquery.cost_optimizer import (
    AntipatternFinding,
    BigQueryPricingModel,
    BudgetCheckResult,
    BudgetGuardrailManager,
    BudgetPolicy,
    CostEstimationResult,
    FindingSeverity,
    PricingTier,
    QueryAntipatternAnalyzer,
    RunawayQueryRemediator,
    StorageBillingModel,
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

__version__ = "1.3.0"

__all__ = [
    "AntipatternFinding",
    "BENCHMARK_SCENARIOS_CATALOG",
    "BenchmarkCategory",
    "BenchmarkMetricResult",
    "BenchmarkRunner",
    "BenchmarkScenario",
    "BenchmarkSuiteResult",
    "BenchmarkTelemetry",
    "BigQueryConnectionConfig",
    "BigQueryDatasetConfig",
    "BigQueryEmulatorError",
    "BigQueryPartitionFilterRequiredError",
    "BigQueryPricingModel",
    "BigQuerySchemaManager",
    "BigQueryTelemetry",
    "BudgetCheckResult",
    "BudgetGuardrailManager",
    "BudgetPolicy",
    "CloudSqlBigQueryReplicationRunner",
    "CostEstimationResult",
    "DbtConfig",
    "DbtProjectManager",
    "DbtRunResult",
    "DbtRunner",
    "DbtTelemetry",
    "FEDERATED_VIEWS_CATALOG",
    "FindingSeverity",
    "LocalBigQueryEmulator",
    "MATERIALIZED_VIEWS_CATALOG",
    "PricingTier",
    "QueryAntipatternAnalyzer",
    "REPLICATION_CATALOG",
    "ReplicationConfig",
    "ReplicationMode",
    "ReplicationTelemetry",
    "ReplicationWatermarkStore",
    "RunawayQueryRemediator",
    "STAR_SCHEMA_CATALOG",
    "StorageBillingModel",
    "TablePartitionClusteringSpec",
]
