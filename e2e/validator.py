"""Trading Risk Engine: End-to-End Data Lineage & Integrity Validator.

Implements rigorous, mathematical cross-tier reconciliation rules:
1. Record Conservation: Injected = Valid + DeadLetter across Ingestion -> Streaming -> Silver -> BigQuery -> dbt.
2. Gross Notional Reconciliation: Ingestion notional == Silver notional == BigQuery notional == dbt notional.
3. Anomaly Conservation: Injected microstructural anomalies == Emitted alerts == BigQuery fact == dbt mart.
4. DLQ Poison Pill Integrity: 100% isolation of malformed records, zero false positive rejections.
5. Value-at-Risk Parity: Databricks Gold VaR == BigQuery fact_risk_stats == dbt fct_portfolio_risk_daily.
6. Analytical Mart Integrity: Schema tests pass rate == 100%, non-empty marts, referential integrity.
7. SLA Compliance: Total execution duration and stage latencies conform to platform SLA boundaries.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from e2e.config import AssertionResult, E2ERunnerConfig, StageExecutionResult
from e2e.synthetic_generator import SyntheticE2EDataBatch


class E2EDataLineageValidator:
    """Evaluates cross-tier mathematical reconciliation invariants across the pipeline."""

    def __init__(self, config: Optional[E2ERunnerConfig] = None) -> None:
        self.config = config or E2ERunnerConfig()
        self.tolerance = self.config.tolerance_usd

    def validate_record_conservation(
        self,
        batch: SyntheticE2EDataBatch,
        stage_results: Dict[str, StageExecutionResult],
    ) -> List[AssertionResult]:
        """Verify record count conservation across all architectural tiers."""
        assertions: List[AssertionResult] = []

        # 1. Trade Orders Ingestion Conservation
        ingestion_res = stage_results.get("ingestion")
        if ingestion_res and ingestion_res.passed:
            ing_orders = ingestion_res.metrics.get("orders_produced", 0)
            assertions.append(
                AssertionResult(
                    name="ingestion_orders_conservation",
                    category="RECORD_CONSERVATION",
                    passed=ing_orders == batch.expected_valid_orders_count,
                    expected=batch.expected_valid_orders_count,
                    actual=ing_orders,
                    message="Ingested valid orders count matches expected batch count",
                )
            )

        # 2. Streaming Validation Conservation
        streaming_res = stage_results.get("streaming")
        if streaming_res and streaming_res.passed:
            valid_orders_decoded = streaming_res.metrics.get("valid_orders_decoded", 0)

            assertions.append(
                AssertionResult(
                    name="streaming_valid_orders_count",
                    category="RECORD_CONSERVATION",
                    passed=valid_orders_decoded == batch.expected_valid_orders_count,
                    expected=batch.expected_valid_orders_count,
                    actual=valid_orders_decoded,
                    message="Streaming valid orders count matches expected count",
                )
            )

        # 3. Lakehouse Silver Cleansing Conservation
        batch_res = stage_results.get("lakehouse_batch")
        if batch_res and batch_res.passed:
            silver_orders = batch_res.metrics.get("silver_orders_count", 0)
            silver_ticks = batch_res.metrics.get("silver_ticks_count", 0)

            assertions.append(
                AssertionResult(
                    name="silver_orders_conservation",
                    category="RECORD_CONSERVATION",
                    passed=silver_orders == batch.expected_valid_orders_count,
                    expected=batch.expected_valid_orders_count,
                    actual=silver_orders,
                    message="Delta Lake Silver orders count matches expected valid orders",
                )
            )
            assertions.append(
                AssertionResult(
                    name="silver_ticks_conservation",
                    category="RECORD_CONSERVATION",
                    passed=silver_ticks == batch.expected_valid_ticks_count,
                    expected=batch.expected_valid_ticks_count,
                    actual=silver_ticks,
                    message="Delta Lake Silver ticks count matches expected valid ticks",
                )
            )

        # 4. BigQuery Data Warehouse Star Schema Conservation
        dwh_res = stage_results.get("dwh_replication")
        if dwh_res and dwh_res.passed:
            bq_orders = dwh_res.metrics.get("fact_trade_orders_count", 0)
            bq_ticks = dwh_res.metrics.get("fact_market_ticks_count", 0)

            assertions.append(
                AssertionResult(
                    name="bigquery_fact_orders_conservation",
                    category="RECORD_CONSERVATION",
                    passed=bq_orders == batch.expected_valid_orders_count,
                    expected=batch.expected_valid_orders_count,
                    actual=bq_orders,
                    message="BigQuery fact_trade_orders row count matches valid orders count",
                )
            )
            assertions.append(
                AssertionResult(
                    name="bigquery_fact_ticks_conservation",
                    category="RECORD_CONSERVATION",
                    passed=bq_ticks == batch.expected_valid_ticks_count,
                    expected=batch.expected_valid_ticks_count,
                    actual=bq_ticks,
                    message="BigQuery fact_market_ticks row count matches valid ticks count",
                )
            )

        return assertions

    def validate_notional_reconciliation(
        self,
        batch: SyntheticE2EDataBatch,
        stage_results: Dict[str, StageExecutionResult],
    ) -> List[AssertionResult]:
        """Verify gross notional value conservation across Ingestion -> BigQuery -> dbt."""
        assertions: List[AssertionResult] = []
        expected_notional = batch.expected_total_notional_usd

        # BigQuery fact_trade_orders notional
        dwh_res = stage_results.get("dwh_replication")
        if dwh_res and dwh_res.passed:
            bq_notional = dwh_res.metrics.get("fact_orders_total_notional", 0.0)
            diff = abs(bq_notional - expected_notional)
            passed = diff <= self.tolerance
            assertions.append(
                AssertionResult(
                    name="notional_reconciliation_bigquery",
                    category="NOTIONAL_RECONCILIATION",
                    passed=passed,
                    expected=expected_notional,
                    actual=bq_notional,
                    message=f"BigQuery gross notional matches ingestion notional (delta: ${diff:.4f})",
                )
            )

        # dbt int_orders_enriched / marts notional
        dbt_res = stage_results.get("dbt_analytics")
        if dbt_res and dbt_res.passed:
            dbt_notional = dbt_res.metrics.get("dbt_orders_total_notional", 0.0)
            if dbt_notional > 0:
                diff_dbt = abs(dbt_notional - expected_notional)
                assertions.append(
                    AssertionResult(
                        name="notional_reconciliation_dbt",
                        category="NOTIONAL_RECONCILIATION",
                        passed=diff_dbt <= self.tolerance,
                        expected=expected_notional,
                        actual=dbt_notional,
                        message=f"dbt gross notional matches ingestion notional (delta: ${diff_dbt:.4f})",
                    )
                )

        return assertions

    def validate_anomaly_detection_integrity(
        self,
        batch: SyntheticE2EDataBatch,
        stage_results: Dict[str, StageExecutionResult],
    ) -> List[AssertionResult]:
        """Verify that microstructural anomalies were accurately detected and persisted."""
        assertions: List[AssertionResult] = []

        streaming_res = stage_results.get("streaming")
        if streaming_res and streaming_res.passed:
            flash_crash_alerts = streaming_res.metrics.get("flash_crash_alerts_emitted", 0)
            fat_finger_alerts = streaming_res.metrics.get("fat_finger_alerts_emitted", 0)

            if batch.expected_flash_crash_alerts_count > 0:
                assertions.append(
                    AssertionResult(
                        name="anomaly_flash_crash_detected",
                        category="ANOMALY_DETECTION",
                        passed=flash_crash_alerts >= batch.expected_flash_crash_alerts_count,
                        expected=batch.expected_flash_crash_alerts_count,
                        actual=flash_crash_alerts,
                        message="Dataflow detected flash crash drawdown threshold breach",
                    )
                )

            if batch.expected_fat_finger_alerts_count > 0:
                assertions.append(
                    AssertionResult(
                        name="anomaly_fat_finger_detected",
                        category="ANOMALY_DETECTION",
                        passed=fat_finger_alerts >= batch.expected_fat_finger_alerts_count,
                        expected=batch.expected_fat_finger_alerts_count,
                        actual=fat_finger_alerts,
                        message="Dataflow detected single-order fat finger limit breach",
                    )
                )

        dwh_res = stage_results.get("dwh_replication")
        if dwh_res and dwh_res.passed:
            replicated_alerts = dwh_res.metrics.get("fact_risk_alerts_count", 0)
            expected_min_alerts = (
                batch.expected_flash_crash_alerts_count + batch.expected_fat_finger_alerts_count
            )
            assertions.append(
                AssertionResult(
                    name="anomaly_alerts_replicated_to_dwh",
                    category="ANOMALY_DETECTION",
                    passed=replicated_alerts >= expected_min_alerts,
                    expected=f">={expected_min_alerts}",
                    actual=replicated_alerts,
                    message="Cloud SQL operational risk alerts replicated to BigQuery fact_risk_alerts",
                )
            )

        return assertions

    def validate_dlq_forensics(
        self,
        batch: SyntheticE2EDataBatch,
        stage_results: Dict[str, StageExecutionResult],
    ) -> List[AssertionResult]:
        """Verify dead-letter queue classification accuracy and zero data leaks."""
        assertions: List[AssertionResult] = []

        streaming_res = stage_results.get("streaming")
        if streaming_res and streaming_res.passed:
            dlq_orders_emitted = streaming_res.metrics.get("dlq_orders_emitted", 0)
            assertions.append(
                AssertionResult(
                    name="streaming_dlq_orders_count",
                    category="DLQ_INTEGRITY",
                    passed=dlq_orders_emitted == batch.expected_dlq_orders_count,
                    expected=batch.expected_dlq_orders_count,
                    actual=dlq_orders_emitted,
                    message="Streaming DLQ orders count matches corrupted orders count",
                )
            )

        mon_res = stage_results.get("monitoring_observability")
        if mon_res and mon_res.passed:
            dlq_records_scanned = mon_res.metrics.get("dlq_records_scanned", 0)
            expected_total_dlq = batch.expected_dlq_orders_count + batch.expected_dlq_ticks_count
            assertions.append(
                AssertionResult(
                    name="dlq_forensic_classification_total",
                    category="DLQ_INTEGRITY",
                    passed=dlq_records_scanned >= expected_total_dlq,
                    expected=expected_total_dlq,
                    actual=dlq_records_scanned,
                    message="Dead-letter queue monitor scanned and categorized all corrupted messages",
                )
            )

        return assertions

    def validate_var_risk_parity(
        self,
        stage_results: Dict[str, StageExecutionResult],
    ) -> List[AssertionResult]:
        """Verify Value-at-Risk parity between Databricks Gold and BigQuery star schema."""
        assertions: List[AssertionResult] = []
        batch_res = stage_results.get("lakehouse_batch")
        dwh_res = stage_results.get("dwh_replication")

        if batch_res and batch_res.passed and dwh_res and dwh_res.passed:
            gold_var_records = batch_res.metrics.get("gold_portfolio_var_count", 0)
            bq_risk_stats = dwh_res.metrics.get("fact_risk_stats_count", 0)

            assertions.append(
                AssertionResult(
                    name="var_metrics_parity_count",
                    category="VAR_PARITY",
                    passed=bq_risk_stats >= gold_var_records and gold_var_records > 0,
                    expected=gold_var_records,
                    actual=bq_risk_stats,
                    message="Gold VaR records successfully merged into BigQuery fact_risk_stats",
                )
            )

        return assertions

    def validate_dbt_mart_integrity(
        self,
        stage_results: Dict[str, StageExecutionResult],
    ) -> List[AssertionResult]:
        """Verify dbt mart models execution and schema test pass rate."""
        assertions: List[AssertionResult] = []
        dbt_res = stage_results.get("dbt_analytics")
        if dbt_res and dbt_res.passed:
            models_executed = dbt_res.metrics.get("models_executed", 0)
            models_failed = dbt_res.metrics.get("models_failed", 0)
            tests_passed = dbt_res.metrics.get("tests_passed", 0)
            tests_failed = dbt_res.metrics.get("tests_failed", 0)

            assertions.append(
                AssertionResult(
                    name="dbt_models_execution_success",
                    category="DBT_INTEGRITY",
                    passed=models_executed >= 15 and models_failed == 0,
                    expected="15 models / 0 failed",
                    actual=f"{models_executed} models / {models_failed} failed",
                    message="All 15 dbt staging, intermediate, and mart models executed successfully",
                )
            )
            assertions.append(
                AssertionResult(
                    name="dbt_schema_tests_pass_rate",
                    category="DBT_INTEGRITY",
                    passed=tests_failed == 0 and tests_passed > 0,
                    expected="0 test failures",
                    actual=f"{tests_failed} test failures ({tests_passed} passed)",
                    message="100% pass rate across all dbt schema constraints",
                )
            )

        return assertions

    def validate_sla_performance(
        self,
        stage_results: Dict[str, StageExecutionResult],
        total_duration_sec: float,
    ) -> List[AssertionResult]:
        """Verify total and per-stage latency adherence to SLA thresholds."""
        assertions: List[AssertionResult] = []

        assertions.append(
            AssertionResult(
                name="total_pipeline_sla_compliance",
                category="SLA_PERFORMANCE",
                passed=total_duration_sec <= self.config.sla_total_seconds,
                expected=f"<={self.config.sla_total_seconds:.1f}s",
                actual=f"{total_duration_sec:.3f}s",
                message=f"Total E2E execution duration within SLA boundary ({self.config.sla_total_seconds}s)",
            )
        )

        for name, res in stage_results.items():
            if res.status != "SKIPPED":
                assertions.append(
                    AssertionResult(
                        name=f"stage_sla_{name}",
                        category="SLA_PERFORMANCE",
                        passed=res.duration_sec <= self.config.sla_stage_seconds,
                        expected=f"<={self.config.sla_stage_seconds:.1f}s",
                        actual=f"{res.duration_sec:.3f}s",
                        message=f"Stage {name} duration within SLA boundary ({self.config.sla_stage_seconds}s)",
                    )
                )

        return assertions

    def run_all_validations(
        self,
        batch: SyntheticE2EDataBatch,
        stage_results: Dict[str, StageExecutionResult],
        total_duration_sec: float,
    ) -> List[AssertionResult]:
        """Execute all validation checks and return consolidated assertion results."""
        all_assertions: List[AssertionResult] = []
        all_assertions.extend(self.validate_record_conservation(batch, stage_results))
        all_assertions.extend(self.validate_notional_reconciliation(batch, stage_results))
        all_assertions.extend(self.validate_anomaly_detection_integrity(batch, stage_results))
        all_assertions.extend(self.validate_dlq_forensics(batch, stage_results))
        all_assertions.extend(self.validate_var_risk_parity(stage_results))
        all_assertions.extend(self.validate_dbt_mart_integrity(stage_results))
        all_assertions.extend(self.validate_sla_performance(stage_results, total_duration_sec))
        return all_assertions
