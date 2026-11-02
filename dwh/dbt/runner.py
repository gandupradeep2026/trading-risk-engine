"""dbt Zero-Cost Execution Engine & Test Runner (Day 23).

Executes compiled dbt models against LocalBigQueryEmulator (DuckDB), verifies relations,
runs schema tests (unique, not_null, accepted_values, relationships), and records
operational telemetry in Prometheus text format.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Any, Dict, List, Optional, Set
import pyarrow as pa

from dwh.bigquery.emulator import LocalBigQueryEmulator
from dwh.dbt.config import DbtConfig
from dwh.dbt.project_manager import DbtModelNode, DbtProjectManager, DbtTestSpec


@dataclass
class DbtModelExecutionResult:
    """Result of an individual dbt model execution."""

    model_name: str
    layer: str
    materialized: str
    status: str  # 'SUCCESS' or 'FAILED'
    rows: int = 0
    duration_ms: float = 0.0
    error_message: Optional[str] = None


@dataclass
class DbtTestExecutionResult:
    """Result of an individual dbt schema test execution."""

    test_type: str
    model_name: str
    column_name: str
    status: str  # 'PASSED' or 'FAILED'
    duration_ms: float = 0.0
    failure_count: int = 0
    error_message: Optional[str] = None


@dataclass
class DbtTelemetry:
    """Operational telemetry store tracking dbt pipeline execution metrics."""

    models_executed: int = 0
    models_failed: int = 0
    tests_executed: int = 0
    tests_passed: int = 0
    tests_failed: int = 0
    total_execution_time_ms: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_model(self, success: bool, duration_ms: float) -> None:
        with self._lock:
            self.models_executed += 1
            if not success:
                self.models_failed += 1
            self.total_execution_time_ms += duration_ms

    def record_test(self, passed: bool, duration_ms: float) -> None:
        with self._lock:
            self.tests_executed += 1
            if passed:
                self.tests_passed += 1
            else:
                self.tests_failed += 1
            self.total_execution_time_ms += duration_ms

    def format_prometheus(self) -> str:
        """Render operational metrics in standard Prometheus text exposition format."""
        lines = [
            "# HELP dbt_models_executed_total Total dbt models executed",
            "# TYPE dbt_models_executed_total counter",
            f"dbt_models_executed_total {self.models_executed}",
            "# HELP dbt_models_failed_total Total failed dbt models",
            "# TYPE dbt_models_failed_total counter",
            f"dbt_models_failed_total {self.models_failed}",
            "# HELP dbt_tests_executed_total Total dbt schema tests evaluated",
            "# TYPE dbt_tests_executed_total counter",
            f"dbt_tests_executed_total {self.tests_executed}",
            "# HELP dbt_tests_passed_total Total passed dbt schema tests",
            "# TYPE dbt_tests_passed_total counter",
            f"dbt_tests_passed_total {self.tests_passed}",
            "# HELP dbt_tests_failed_total Total failed dbt schema tests",
            "# TYPE dbt_tests_failed_total counter",
            f"dbt_tests_failed_total {self.tests_failed}",
            "# HELP dbt_total_execution_time_seconds Total dbt execution wall-clock time in seconds",
            "# TYPE dbt_total_execution_time_seconds gauge",
            f"dbt_total_execution_time_seconds {self.total_execution_time_ms / 1000.0:.4f}",
        ]
        return "\n".join(lines) + "\n"


@dataclass
class DbtRunResult:
    """Consolidated summary of a dbt run and test evaluation invocation."""

    success: bool
    models_executed: int = 0
    models_failed: int = 0
    tests_executed: int = 0
    tests_passed: int = 0
    tests_failed: int = 0
    total_duration_ms: float = 0.0
    model_results: List[DbtModelExecutionResult] = field(default_factory=list)
    test_results: List[DbtTestExecutionResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "models_executed": self.models_executed,
            "models_failed": self.models_failed,
            "tests_executed": self.tests_executed,
            "tests_passed": self.tests_passed,
            "tests_failed": self.tests_failed,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "model_results": [
                {
                    "model_name": m.model_name,
                    "layer": m.layer,
                    "materialized": m.materialized,
                    "status": m.status,
                    "rows": m.rows,
                    "duration_ms": round(m.duration_ms, 2),
                    "error_message": m.error_message,
                }
                for m in self.model_results
            ],
            "test_results": [
                {
                    "test_type": t.test_type,
                    "model_name": t.model_name,
                    "column_name": t.column_name,
                    "status": t.status,
                    "duration_ms": round(t.duration_ms, 2),
                    "failure_count": t.failure_count,
                    "error_message": t.error_message,
                }
                for t in self.test_results
            ],
        }


class DbtRunner:
    """Production-grade zero-cost runner executing dbt transformations on DuckDB."""

    def __init__(
        self,
        emulator: Optional[LocalBigQueryEmulator] = None,
        project_manager: Optional[DbtProjectManager] = None,
        config: Optional[DbtConfig] = None,
    ) -> None:
        self.config = config or DbtConfig.from_env()
        self.emulator = emulator or LocalBigQueryEmulator(
            project_id=self.config.gcp_project_id,
            dataset_id=self.config.dataset_id,
        )
        self.project_manager = project_manager or DbtProjectManager(self.config)
        self.telemetry = DbtTelemetry()

    def run_models(self, selected_models: Optional[List[str]] = None) -> DbtRunResult:
        """Compile and execute selected dbt models in topological dependency order."""
        t_start = time.perf_counter()
        order = self.project_manager.get_execution_order(selected_models)
        results: List[DbtModelExecutionResult] = []
        overall_success = True

        for model_name in order:
            node = self.project_manager.models[model_name]
            m_start = time.perf_counter()
            try:
                compiled_sql = self.project_manager.compile_model_sql(
                    model_name, target_dataset=self.config.dataset_id
                )

                # Translate project and dataset naming if needed
                sql_exec = compiled_sql.replace("`", "")
                sql_exec = sql_exec.replace(f"{self.config.gcp_project_id}.", "")

                # Materialization: view or table
                target_relation = f"{self.config.dataset_id}.{model_name}"
                if node.materialized == "table":
                    ddl = f"CREATE OR REPLACE TABLE {target_relation} AS {sql_exec};"
                else:
                    ddl = f"CREATE OR REPLACE VIEW {target_relation} AS {sql_exec};"

                self.emulator.connection.execute(ddl)

                # Count rows
                count_res = self.emulator.connection.execute(
                    f"SELECT COUNT(*) FROM {target_relation};"
                ).fetchone()
                row_count = count_res[0] if count_res else 0

                duration_ms = (time.perf_counter() - m_start) * 1000.0
                res = DbtModelExecutionResult(
                    model_name=model_name,
                    layer=node.layer,
                    materialized=node.materialized,
                    status="SUCCESS",
                    rows=row_count,
                    duration_ms=duration_ms,
                )
                self.telemetry.record_model(True, duration_ms)
                results.append(res)
            except Exception as e:
                overall_success = False
                duration_ms = (time.perf_counter() - m_start) * 1000.0
                err_msg = str(e)
                res = DbtModelExecutionResult(
                    model_name=model_name,
                    layer=node.layer,
                    materialized=node.materialized,
                    status="FAILED",
                    duration_ms=duration_ms,
                    error_message=err_msg,
                )
                self.telemetry.record_model(False, duration_ms)
                results.append(res)
                # Halt on model execution failure
                break

        total_ms = (time.perf_counter() - t_start) * 1000.0
        return DbtRunResult(
            success=overall_success,
            models_executed=len(results),
            models_failed=sum(1 for r in results if r.status == "FAILED"),
            total_duration_ms=total_ms,
            model_results=results,
        )

    def run_tests(self, selected_models: Optional[List[str]] = None) -> DbtRunResult:
        """Evaluate all declared schema tests for selected models."""
        t_start = time.perf_counter()
        target_names: Optional[Set[str]] = None
        if selected_models and "all" not in selected_models:
            target_names = set(self.project_manager.get_execution_order(selected_models))

        tests_to_run = [
            t for t in self.project_manager.tests
            if target_names is None or t.model_name in target_names
        ]

        results: List[DbtTestExecutionResult] = []
        overall_success = True

        for test in tests_to_run:
            m_start = time.perf_counter()
            target_relation = f"{self.config.dataset_id}.{test.model_name}"
            test_passed = False
            fail_count = 0
            err_msg: Optional[str] = None

            try:
                if test.test_type == "not_null":
                    sql = f"SELECT COUNT(*) FROM {target_relation} WHERE {test.column_name} IS NULL;"
                    res = self.emulator.connection.execute(sql).fetchone()
                    fail_count = res[0] if res else 0
                    test_passed = (fail_count == 0)

                elif test.test_type == "unique":
                    sql = (
                        f"SELECT COUNT(*) FROM ("
                        f"  SELECT {test.column_name}, COUNT(*) "
                        f"  FROM {target_relation} "
                        f"  GROUP BY {test.column_name} "
                        f"  HAVING COUNT(*) > 1"
                        f") sub;"
                    )
                    res = self.emulator.connection.execute(sql).fetchone()
                    fail_count = res[0] if res else 0
                    test_passed = (fail_count == 0)

                elif test.test_type == "accepted_values":
                    val_list = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in (test.values or [])])
                    sql = (
                        f"SELECT COUNT(*) FROM {target_relation} "
                        f"WHERE {test.column_name} NOT IN ({val_list}) "
                        f"  AND {test.column_name} IS NOT NULL;"
                    )
                    res = self.emulator.connection.execute(sql).fetchone()
                    fail_count = res[0] if res else 0
                    test_passed = (fail_count == 0)

                elif test.test_type == "relationships":
                    parent_relation = f"{self.config.dataset_id}.{test.to_model}"
                    sql = (
                        f"SELECT COUNT(*) FROM {target_relation} c "
                        f"LEFT JOIN {parent_relation} p "
                        f"  ON c.{test.column_name} = p.{test.to_column} "
                        f"WHERE p.{test.to_column} IS NULL "
                        f"  AND c.{test.column_name} IS NOT NULL;"
                    )
                    res = self.emulator.connection.execute(sql).fetchone()
                    fail_count = res[0] if res else 0
                    test_passed = (fail_count == 0)

                else:
                    test_passed = True
            except Exception as e:
                test_passed = False
                fail_count = 1
                err_msg = str(e)

            duration_ms = (time.perf_counter() - m_start) * 1000.0
            if not test_passed:
                overall_success = False

            t_res = DbtTestExecutionResult(
                test_type=test.test_type,
                model_name=test.model_name,
                column_name=test.column_name,
                status="PASSED" if test_passed else "FAILED",
                duration_ms=duration_ms,
                failure_count=fail_count,
                error_message=err_msg,
            )
            self.telemetry.record_test(test_passed, duration_ms)
            results.append(t_res)

        total_ms = (time.perf_counter() - t_start) * 1000.0
        return DbtRunResult(
            success=overall_success,
            tests_executed=len(results),
            tests_passed=sum(1 for r in results if r.status == "PASSED"),
            tests_failed=sum(1 for r in results if r.status == "FAILED"),
            total_duration_ms=total_ms,
            test_results=results,
        )

    def run_all(self, selected_models: Optional[List[str]] = None) -> DbtRunResult:
        """Run all selected models followed by their associated schema tests."""
        t_start = time.perf_counter()
        model_run = self.run_models(selected_models)
        if not model_run.success:
            return model_run

        test_run = self.run_tests(selected_models)
        total_ms = (time.perf_counter() - t_start) * 1000.0

        return DbtRunResult(
            success=test_run.success,
            models_executed=model_run.models_executed,
            models_failed=model_run.models_failed,
            tests_executed=test_run.tests_executed,
            tests_passed=test_run.tests_passed,
            tests_failed=test_run.tests_failed,
            total_duration_ms=total_ms,
            model_results=model_run.model_results,
            test_results=test_run.test_results,
        )

    def fetch_arrow_table(self, model_name: str) -> pa.Table:
        """Fetch model data as an Apache Arrow Table for downstream verification."""
        target_relation = f"{self.config.dataset_id}.{model_name}"
        return self.emulator.connection.sql(f"SELECT * FROM {target_relation};").to_arrow_table()
