"""Great Expectations Data Quality Validation Engine.

Provides zero-cost, hermetic local validation execution against PyArrow tables,
Parquet datasets, Delta tables, and in-memory dictionaries:
1. Evaluates standard Great Expectations expectation types.
2. Produces standard Great Expectations JSON validation results dictionaries.
3. Generates standalone HTML Data Docs reports.
4. Tracks operational telemetry with Prometheus metrics formatting.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq

from data_quality.suite_definitions import (
    ExpectationConfig,
    ExpectationSuite,
    get_expectation_suite,
)


@dataclass
class DataQualityTelemetry:
    """Operational metrics store for data quality validation gates."""
    validations_total: int = 0
    validations_passed_total: int = 0
    validations_failed_total: int = 0
    expectations_evaluated_total: int = 0
    expectations_passed_total: int = 0
    expectations_failed_total: int = 0
    last_validation_duration_seconds: float = 0.0

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_validation(
        self,
        success: bool,
        evaluated: int,
        passed: int,
        failed: int,
        duration: float,
    ) -> None:
        with self._lock:
            self.validations_total += 1
            if success:
                self.validations_passed_total += 1
            else:
                self.validations_failed_total += 1
            self.expectations_evaluated_total += evaluated
            self.expectations_passed_total += passed
            self.expectations_failed_total += failed
            self.last_validation_duration_seconds = duration

    def reset(self) -> None:
        with self._lock:
            self.validations_total = 0
            self.validations_passed_total = 0
            self.validations_failed_total = 0
            self.expectations_evaluated_total = 0
            self.expectations_passed_total = 0
            self.expectations_failed_total = 0
            self.last_validation_duration_seconds = 0.0

    def format_prometheus(self, environment: str = "dev") -> str:
        """Format metrics as Prometheus text exposition string."""
        with self._lock:
            lines = [
                "# HELP dq_validations_total Total number of data quality validation runs executed",
                "# TYPE dq_validations_total counter",
                f'dq_validations_total{{env="{environment}"}} {self.validations_total}',
                "# HELP dq_validations_passed_total Total number of data quality validation runs passed",
                "# TYPE dq_validations_passed_total counter",
                f'dq_validations_passed_total{{env="{environment}"}} {self.validations_passed_total}',
                "# HELP dq_validations_failed_total Total number of data quality validation runs failed",
                "# TYPE dq_validations_failed_total counter",
                f'dq_validations_failed_total{{env="{environment}"}} {self.validations_failed_total}',
                "# HELP dq_expectations_evaluated_total Total number of individual expectations evaluated",
                "# TYPE dq_expectations_evaluated_total counter",
                f'dq_expectations_evaluated_total{{env="{environment}"}} {self.expectations_evaluated_total}',
                "# HELP dq_expectations_passed_total Total number of individual expectations passed",
                "# TYPE dq_expectations_passed_total counter",
                f'dq_expectations_passed_total{{env="{environment}"}} {self.expectations_passed_total}',
                "# HELP dq_expectations_failed_total Total number of individual expectations failed",
                "# TYPE dq_expectations_failed_total counter",
                f'dq_expectations_failed_total{{env="{environment}"}} {self.expectations_failed_total}',
                "# HELP dq_last_validation_duration_seconds Duration in seconds of the latest validation run",
                "# TYPE dq_last_validation_duration_seconds gauge",
                f'dq_last_validation_duration_seconds{{env="{environment}"}} {self.last_validation_duration_seconds:.4f}',
            ]
            return "\n".join(lines) + "\n"


@dataclass
class CheckpointValidationResult:
    """Consolidated validation result matching the Great Expectations specification."""
    success: bool
    suite_name: str
    run_id: str
    statistics: Dict[str, Any]
    results: List[Dict[str, Any]]
    meta: Dict[str, Any] = field(default_factory=dict)
    data_docs_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "suite_name": self.suite_name,
            "run_id": self.run_id,
            "statistics": dict(self.statistics),
            "results": list(self.results),
            "meta": dict(self.meta),
            "data_docs_path": self.data_docs_path,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)


class GreatExpectationsValidator:
    """Production validator executing Great Expectations test suites."""

    def __init__(
        self,
        expectations_dir: Optional[Union[str, Path]] = None,
        telemetry: Optional[DataQualityTelemetry] = None,
    ) -> None:
        self.expectations_dir = Path(expectations_dir) if expectations_dir else None
        self.telemetry = telemetry or DataQualityTelemetry()

    def validate(
        self,
        data: Union[pa.Table, List[Dict[str, Any]], Dict[str, List[Any]], str, Path, Any],
        suite: Union[str, ExpectationSuite],
        run_id: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        data_docs_dir: Optional[Union[str, Path]] = None,
    ) -> CheckpointValidationResult:
        """Validate an input dataset against an ExpectationSuite."""
        start_time = time.time()
        now_utc = datetime.now(timezone.utc)
        run_identifier = run_id or now_utc.strftime("%Y%m%d_%H%M%S_dq")

        # Resolve ExpectationSuite
        if isinstance(suite, str):
            suite_obj = get_expectation_suite(suite, self.expectations_dir)
        elif isinstance(suite, ExpectationSuite):
            suite_obj = suite
        else:
            raise TypeError(f"Expected str or ExpectationSuite, got {type(suite)}")

        # Convert data into standard in-memory column dictionary
        col_dict, row_count = self._load_data(data)

        # Evaluate expectations
        results: List[Dict[str, Any]] = []
        passed_count = 0
        failed_count = 0

        for exp in suite_obj.expectations:
            res = self._evaluate_expectation(exp, col_dict, row_count)
            results.append(res)
            if res["success"]:
                passed_count += 1
            else:
                failed_count += 1

        total_evaluated = len(results)
        suite_success = (failed_count == 0) and (total_evaluated > 0)
        success_percent = (passed_count / total_evaluated * 100.0) if total_evaluated > 0 else 0.0

        duration_sec = time.time() - start_time

        statistics = {
            "evaluated_expectations": total_evaluated,
            "successful_expectations": passed_count,
            "unsuccessful_expectations": failed_count,
            "success_percent": round(success_percent, 2),
            "duration_seconds": round(duration_sec, 4),
        }

        meta = {
            "great_expectations_version": "0.18.12",
            "expectation_suite_name": suite_obj.expectation_suite_name,
            "run_id": run_identifier,
            "data_asset_name": data_asset_name or suite_obj.meta.get("table", "unknown_asset"),
            "validation_time": now_utc.isoformat(),
        }

        docs_path_str: Optional[str] = None
        if data_docs_dir:
            docs_path_str = self.generate_data_docs(
                suite_name=suite_obj.expectation_suite_name,
                run_id=run_identifier,
                statistics=statistics,
                results=results,
                meta=meta,
                output_dir=data_docs_dir,
            )

        # Record telemetry
        self.telemetry.record_validation(
            success=suite_success,
            evaluated=total_evaluated,
            passed=passed_count,
            failed=failed_count,
            duration=duration_sec,
        )

        return CheckpointValidationResult(
            success=suite_success,
            suite_name=suite_obj.expectation_suite_name,
            run_id=run_identifier,
            statistics=statistics,
            results=results,
            meta=meta,
            data_docs_path=docs_path_str,
        )

    # -------------------------------------------------------------------------
    # DATA LOADING HELPERS
    # -------------------------------------------------------------------------

    def _load_data(
        self,
        data: Any,
    ) -> Tuple[Dict[str, List[Any]], int]:
        """Convert varied dataset formats into a unified column-oriented dictionary."""
        # 1. File path (Parquet)
        if isinstance(data, (str, Path)):
            p = Path(data)
            if not p.exists():
                raise FileNotFoundError(f"Data file does not exist: {p}")
            if p.is_dir():
                # Read all parquet files in directory or hive partitions
                parquet_files = list(p.rglob("*.parquet"))
                if not parquet_files:
                    raise FileNotFoundError(f"No .parquet files found in directory: {p}")
                table = pq.read_table(p)
            else:
                table = pq.read_table(p)
            return self._pyarrow_table_to_col_dict(table)

        # 2. PyArrow Table
        if isinstance(data, pa.Table):
            return self._pyarrow_table_to_col_dict(data)

        # 3. List of dictionaries (row-oriented)
        if isinstance(data, list):
            if len(data) == 0:
                return {}, 0
            if isinstance(data[0], dict):
                columns: Dict[str, List[Any]] = {}
                keys = list(data[0].keys())
                for k in keys:
                    columns[k] = [row.get(k) for row in data]
                return columns, len(data)
            raise ValueError("List must contain dictionary elements")

        # 4. Dict of lists (column-oriented)
        if isinstance(data, dict):
            row_count = 0
            for k, v in data.items():
                if isinstance(v, (list, tuple)):
                    row_count = max(row_count, len(v))
            return {k: list(v) if isinstance(v, (list, tuple)) else [v] for k, v in data.items()}, row_count

        # 5. Pandas DataFrame duck-typing
        if hasattr(data, "to_dict") and hasattr(data, "columns"):
            # to_dict(orient="list")
            cols = {col: list(data[col]) for col in data.columns}
            return cols, len(data)

        raise TypeError(f"Unsupported data format for validation: {type(data)}")

    def _pyarrow_table_to_col_dict(self, table: pa.Table) -> Tuple[Dict[str, List[Any]], int]:
        """Convert PyArrow Table to column list dictionary."""
        cols: Dict[str, List[Any]] = {}
        row_count = table.num_rows
        for col_name in table.column_names:
            col_chunked = table.column(col_name)
            cols[col_name] = col_chunked.to_pylist()
        return cols, row_count

    # -------------------------------------------------------------------------
    # EXPECTATION EVALUATION ENGINE
    # -------------------------------------------------------------------------

    def _evaluate_expectation(
        self,
        exp: ExpectationConfig,
        columns: Dict[str, List[Any]],
        row_count: int,
    ) -> Dict[str, Any]:
        """Evaluate a single expectation against dataset columns."""
        exp_type = exp.expectation_type
        kwargs = exp.kwargs

        handler = getattr(self, f"_eval_{exp_type}", None)
        if handler:
            return handler(kwargs, columns, row_count, exp)

        # Fallback unknown expectation
        return {
            "success": False,
            "expectation_config": exp.to_dict(),
            "result": {"observed_value": "UNKNOWN_EXPECTATION_TYPE"},
            "exception_info": {"raised_exception": False, "exception_message": f"Unsupported expectation: {exp_type}"},
        }

    def _eval_expect_table_row_count_to_be_between(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        min_v = kwargs.get("min_value")
        max_v = kwargs.get("max_value")
        success = True
        if min_v is not None and row_count < min_v:
            success = False
        if max_v is not None and row_count > max_v:
            success = False

        return {
            "success": success,
            "expectation_config": exp.to_dict(),
            "result": {
                "observed_value": row_count,
                "element_count": row_count,
            },
        }

    def _eval_expect_column_to_exist(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col = kwargs.get("column")
        exists = col in columns
        return {
            "success": exists,
            "expectation_config": exp.to_dict(),
            "result": {
                "observed_value": list(columns.keys()),
            },
        }

    def _eval_expect_table_columns_to_match_set(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        expected_set = set(kwargs.get("column_set", []))
        exact_match = kwargs.get("exact_match", True)
        actual_set = set(columns.keys())
        if exact_match:
            success = (actual_set == expected_set)
        else:
            success = expected_set.issubset(actual_set)

        return {
            "success": success,
            "expectation_config": exp.to_dict(),
            "result": {
                "observed_value": list(actual_set),
                "details": {
                    "missing": list(expected_set - actual_set),
                    "unexpected": list(actual_set - expected_set),
                },
            },
        }

    def _eval_expect_column_values_to_not_be_null(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col = kwargs.get("column")
        if col not in columns:
            return {
                "success": False,
                "expectation_config": exp.to_dict(),
                "result": {"observed_value": None, "element_count": 0, "unexpected_count": row_count},
            }

        values = columns[col]
        unexpected = [v for v in values if v is None]
        unexp_count = len(unexpected)
        success = (unexp_count == 0)

        return {
            "success": success,
            "expectation_config": exp.to_dict(),
            "result": {
                "element_count": len(values),
                "unexpected_count": unexp_count,
                "unexpected_percent": (unexp_count / len(values) * 100.0) if values else 0.0,
                "unexpected_values": unexpected[:20],
            },
        }

    def _eval_expect_column_values_to_be_unique(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col = kwargs.get("column")
        if col not in columns:
            return {
                "success": False,
                "expectation_config": exp.to_dict(),
                "result": {"observed_value": None, "element_count": 0, "unexpected_count": row_count},
            }

        values = columns[col]
        seen: set = set()
        duplicates = []
        for v in values:
            if v in seen:
                duplicates.append(v)
            else:
                seen.add(v)

        unexp_count = len(duplicates)
        return {
            "success": (unexp_count == 0),
            "expectation_config": exp.to_dict(),
            "result": {
                "element_count": len(values),
                "unexpected_count": unexp_count,
                "unexpected_percent": (unexp_count / len(values) * 100.0) if values else 0.0,
                "unexpected_values": duplicates[:20],
            },
        }

    def _eval_expect_column_values_to_be_in_set(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col = kwargs.get("column")
        value_set = set(kwargs.get("value_set", []))
        if col not in columns:
            return {
                "success": False,
                "expectation_config": exp.to_dict(),
                "result": {"observed_value": None, "unexpected_count": row_count},
            }

        values = columns[col]
        unexpected = [v for v in values if v not in value_set]
        unexp_count = len(unexpected)

        return {
            "success": (unexp_count == 0),
            "expectation_config": exp.to_dict(),
            "result": {
                "element_count": len(values),
                "unexpected_count": unexp_count,
                "unexpected_percent": (unexp_count / len(values) * 100.0) if values else 0.0,
                "unexpected_values": unexpected[:20],
            },
        }

    def _eval_expect_column_values_to_be_between(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col = kwargs.get("column")
        min_v = kwargs.get("min_value")
        max_v = kwargs.get("max_value")
        if col not in columns:
            return {
                "success": False,
                "expectation_config": exp.to_dict(),
                "result": {"observed_value": None, "unexpected_count": row_count},
            }

        values = columns[col]
        unexpected = []
        for v in values:
            if v is None:
                unexpected.append(v)
                continue
            try:
                num_v = float(v)
                if min_v is not None and num_v < min_v:
                    unexpected.append(v)
                elif max_v is not None and num_v > max_v:
                    unexpected.append(v)
            except (ValueError, TypeError):
                unexpected.append(v)

        unexp_count = len(unexpected)
        return {
            "success": (unexp_count == 0),
            "expectation_config": exp.to_dict(),
            "result": {
                "element_count": len(values),
                "unexpected_count": unexp_count,
                "unexpected_percent": (unexp_count / len(values) * 100.0) if values else 0.0,
                "unexpected_values": unexpected[:20],
            },
        }

    def _eval_expect_column_pair_values_a_to_be_greater_than_or_equal_to_b(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col_a = kwargs.get("column_A")
        col_b = kwargs.get("column_B")
        if col_a not in columns or col_b not in columns:
            return {
                "success": False,
                "expectation_config": exp.to_dict(),
                "result": {"observed_value": None, "unexpected_count": row_count},
            }

        vals_a = columns[col_a]
        vals_b = columns[col_b]
        unexpected = []

        for i in range(min(len(vals_a), len(vals_b))):
            va = vals_a[i]
            vb = vals_b[i]
            if va is None or vb is None:
                unexpected.append({"row": i, col_a: va, col_b: vb})
                continue
            try:
                fa = float(va)
                fb = float(vb)
                # allow minute epsilon for floating point representations
                if fa < (fb - 1e-9):
                    unexpected.append({"row": i, col_a: fa, col_b: fb})
            except (ValueError, TypeError):
                unexpected.append({"row": i, col_a: va, col_b: vb})

        unexp_count = len(unexpected)
        return {
            "success": (unexp_count == 0),
            "expectation_config": exp.to_dict(),
            "result": {
                "element_count": len(vals_a),
                "unexpected_count": unexp_count,
                "unexpected_percent": (unexp_count / len(vals_a) * 100.0) if vals_a else 0.0,
                "unexpected_values": unexpected[:20],
            },
        }

    def _eval_expect_column_values_to_match_regex(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col = kwargs.get("column")
        regex = kwargs.get("regex", ".*")
        pattern = re.compile(regex)

        if col not in columns:
            return {"success": False, "expectation_config": exp.to_dict(), "result": {"unexpected_count": row_count}}

        values = columns[col]
        unexpected = [v for v in values if v is None or not pattern.match(str(v))]
        unexp_count = len(unexpected)

        return {
            "success": (unexp_count == 0),
            "expectation_config": exp.to_dict(),
            "result": {
                "element_count": len(values),
                "unexpected_count": unexp_count,
                "unexpected_percent": (unexp_count / len(values) * 100.0) if values else 0.0,
                "unexpected_values": unexpected[:20],
            },
        }

    def _eval_expect_column_mean_to_be_between(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col = kwargs.get("column")
        min_v = kwargs.get("min_value")
        max_v = kwargs.get("max_value")
        if col not in columns or not columns[col]:
            return {"success": False, "expectation_config": exp.to_dict(), "result": {"observed_value": None}}

        valid_vals = [float(v) for v in columns[col] if v is not None]
        if not valid_vals:
            return {"success": False, "expectation_config": exp.to_dict(), "result": {"observed_value": None}}

        mean_val = sum(valid_vals) / len(valid_vals)
        success = True
        if min_v is not None and mean_val < min_v:
            success = False
        if max_v is not None and mean_val > max_v:
            success = False

        return {
            "success": success,
            "expectation_config": exp.to_dict(),
            "result": {"observed_value": mean_val},
        }

    def _eval_expect_column_sum_to_be_between(
        self,
        kwargs: Dict[str, Any],
        columns: Dict[str, List[Any]],
        row_count: int,
        exp: ExpectationConfig,
    ) -> Dict[str, Any]:
        col = kwargs.get("column")
        min_v = kwargs.get("min_value")
        max_v = kwargs.get("max_value")
        if col not in columns or not columns[col]:
            return {"success": False, "expectation_config": exp.to_dict(), "result": {"observed_value": None}}

        valid_vals = [float(v) for v in columns[col] if v is not None]
        sum_val = sum(valid_vals)
        success = True
        if min_v is not None and sum_val < min_v:
            success = False
        if max_v is not None and sum_val > max_v:
            success = False

        return {
            "success": success,
            "expectation_config": exp.to_dict(),
            "result": {"observed_value": sum_val},
        }

    # -------------------------------------------------------------------------
    # DATA DOCS GENERATOR
    # -------------------------------------------------------------------------

    def generate_data_docs(
        self,
        suite_name: str,
        run_id: str,
        statistics: Dict[str, Any],
        results: List[Dict[str, Any]],
        meta: Dict[str, Any],
        output_dir: Union[str, Path],
    ) -> str:
        """Generate static HTML Data Docs report."""
        out_p = Path(output_dir)
        out_p.mkdir(parents=True, exist_ok=True)
        html_file = out_p / f"{suite_name}_{run_id}.html"

        status_badge = (
            '<span style="background-color: #28a745; color: white; padding: 4px 12px; border-radius: 4px; font-weight: bold;">PASSED</span>'
            if statistics["unsuccessful_expectations"] == 0
            else '<span style="background-color: #dc3545; color: white; padding: 4px 12px; border-radius: 4px; font-weight: bold;">FAILED</span>'
        )

        rows = []
        for i, res in enumerate(results, 1):
            exp_cfg = res["expectation_config"]
            exp_type = exp_cfg["expectation_type"]
            kwargs_str = ", ".join(f"<b>{k}</b>={v}" for k, v in exp_cfg.get("kwargs", {}).items())
            is_pass = res["success"]
            res_badge = (
                '<span style="color: #28a745; font-weight: bold;">PASSED</span>'
                if is_pass
                else '<span style="color: #dc3545; font-weight: bold;">FAILED</span>'
            )
            obs = res.get("result", {}).get("observed_value")
            unexp = res.get("result", {}).get("unexpected_count")
            detail = f"Observed: {obs}" if obs is not None else f"Unexpected count: {unexp}"

            rows.append(f"""
                <tr style="border-bottom: 1px solid #e1e4e8;">
                    <td style="padding: 10px;">{i}</td>
                    <td style="padding: 10px;"><code>{exp_type}</code></td>
                    <td style="padding: 10px; font-size: 13px;">{kwargs_str}</td>
                    <td style="padding: 10px;">{res_badge}</td>
                    <td style="padding: 10px; font-size: 12px; color: #586069;">{detail}</td>
                </tr>
            """)

        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Great Expectations Data Docs - {suite_name}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 24px; background: #f6f8fa; color: #24292e; }}
        .container {{ max-width: 1100px; margin: 0 auto; background: #ffffff; padding: 32px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.12); }}
        .header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #eaecef; padding-bottom: 16px; margin-bottom: 24px; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }}
        .stat-card {{ background: #f1f8ff; border: 1px solid #c8e1ff; padding: 16px; border-radius: 6px; text-align: center; }}
        .stat-card h3 {{ margin: 0 0 8px 0; font-size: 14px; color: #0366d6; text-transform: uppercase; }}
        .stat-card p {{ margin: 0; font-size: 26px; font-weight: bold; color: #24292e; }}
        table {{ width: 100%; border-collapse: collapse; text-align: left; }}
        th {{ background: #fafbfc; border-bottom: 2px solid #e1e4e8; padding: 12px 10px; font-size: 13px; color: #586069; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <h1 style="margin: 0 0 6px 0; font-size: 24px;">Great Expectations Validation Report</h1>
                <p style="margin: 0; color: #586069; font-size: 14px;">Suite: <b>{suite_name}</b> | Run ID: <code>{run_id}</code></p>
            </div>
            <div>
                {status_badge}
            </div>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>Evaluated</h3>
                <p>{statistics["evaluated_expectations"]}</p>
            </div>
            <div class="stat-card" style="background: #dcffe4; border-color: #34d058;">
                <h3 style="color: #22863a;">Passed</h3>
                <p>{statistics["successful_expectations"]}</p>
            </div>
            <div class="stat-card" style="background: {'#ffeef0' if statistics['unsuccessful_expectations'] > 0 else '#f1f8ff'}; border-color: {'#d73a49' if statistics['unsuccessful_expectations'] > 0 else '#c8e1ff'};">
                <h3 style="color: {'#cb2431' if statistics['unsuccessful_expectations'] > 0 else '#0366d6'};">Failed</h3>
                <p>{statistics["unsuccessful_expectations"]}</p>
            </div>
            <div class="stat-card">
                <h3>Success Rate</h3>
                <p>{statistics["success_percent"]}%</p>
            </div>
        </div>

        <h2 style="font-size: 18px; margin-bottom: 16px;">Expectation Results Breakdown</h2>
        <table>
            <thead>
                <tr>
                    <th style="width: 40px;">#</th>
                    <th>Expectation Type</th>
                    <th>Parameters</th>
                    <th style="width: 100px;">Status</th>
                    <th>Diagnostics</th>
                </tr>
            </thead>
            <tbody>
                {"".join(rows)}
            </tbody>
        </table>
    </div>
</body>
</html>
"""
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        # Also write / update index.html in output directory
        index_file = out_p / "index.html"
        with open(index_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        return str(html_file)
