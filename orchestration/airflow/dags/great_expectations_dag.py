"""Great Expectations Lakehouse Data Quality Orchestration DAG.

Orchestrates automated data quality validation gates across Silver and Gold Medallion layers:
1. Validates Silver Market Ticks suite (schema, non-crossed quotes, price bounds, spreads).
2. Validates Silver Trade Orders suite (uniqueness, BUY/SELL, valid order types, positive notional).
3. Validates Silver Rolling VWAP suite (Bollinger bands, price variance, volatility regimes).
4. Validates Gold Portfolio VaR suite (mathematical ordering VaR_99 >= VaR_95 >= 0, CVaR >= VaR).
5. Validates Gold Risk Features suite (normalized liquidity scores in [0, 1], order imbalance in [-1, 1]).
6. Publishes HTML Data Docs reports to GCS bucket for risk stakeholders.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

# Dual-mode import: uses native Apache Airflow in Cloud Composer,
# or pure-Python hermetic compatibility layer during local development and testing.
try:
    import airflow  # type: ignore
    from airflow import DAG  # type: ignore
    from airflow.operators.empty import EmptyOperator  # type: ignore
    from airflow.utils.task_group import TaskGroup  # type: ignore
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore
    from great_expectations_provider.operators.great_expectations import (  # type: ignore
        GreatExpectationsOperator,
    )
except (ImportError, ModuleNotFoundError):
    from orchestration.airflow.airflow_compat import (
        DAG,
        EmptyOperator,
        GreatExpectationsOperator,
        TaskGroup,
        TriggerRule,
    )

from orchestration.airflow.callbacks.failure_callbacks import (
    create_failure_callback,
    create_retry_callback,
    create_sla_miss_callback,
    create_success_callback,
)
from orchestration.airflow.config.great_expectations_config import GreatExpectationsConfig


def build_great_expectations_dag(
    config: Optional[GreatExpectationsConfig] = None,
) -> DAG:
    """Build and return the production Great Expectations quality gate DAG."""
    cfg = config or GreatExpectationsConfig.from_env()

    default_args = {
        "owner": "risk-data-platform",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": cfg.checkpoint_retry_limit,
        "retry_delay": timedelta(seconds=cfg.checkpoint_retry_delay_seconds),
        "retry_exponential_backoff": cfg.retry_exponential_backoff,
        "max_retry_delay": timedelta(seconds=cfg.max_retry_delay_seconds),
        "execution_timeout": timedelta(minutes=15),
        "on_failure_callback": create_failure_callback(),
        "on_retry_callback": create_retry_callback(),
    }

    dag = DAG(
        dag_id="great_expectations_quality_pipeline",
        description=(
            "Validates Great Expectations test suites on Silver and Gold Medallion layers: "
            "market ticks, trade orders, rolling VWAP, portfolio VaR, and risk features."
        ),
        schedule_interval="30 2 * * *",  # 02:30 UTC daily (post Databricks Gold processing)
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        sla=timedelta(minutes=30),
        sla_miss_callback=create_sla_miss_callback(),
        on_success_callback=create_success_callback(),
        tags=["market-risk", "data-quality", "great-expectations", "lakehouse", "tier-3"],
        doc_md=__doc__,
    )

    with dag:
        start_quality_gates = EmptyOperator(
            task_id="start_quality_gates",
            doc_md="Milestone marker signaling initiation of Great Expectations data quality gates.",
        )

        # ---------------------------------------------------------------------
        # TaskGroup: Silver Layer Data Quality Suites
        # ---------------------------------------------------------------------
        with TaskGroup("silver_quality_validations", tooltip="Silver Medallion Layer Validation") as silver_group:
            validate_silver_ticks = GreatExpectationsOperator(
                task_id="validate_silver_ticks",
                expectation_suite_name=cfg.silver_ticks_suite,
                data_asset_name="silver_market_ticks",
                data_asset_path=cfg.get_silver_ticks_path("{{ ds }}"),
                data_context_root_dir=cfg.data_context_root_dir,
                fail_task_on_validation_failure=cfg.fail_task_on_validation_failure,
                doc_md="Validates Silver market ticks for schema conformance and non-crossed quotes.",
            )

            validate_silver_orders = GreatExpectationsOperator(
                task_id="validate_silver_orders",
                expectation_suite_name=cfg.silver_orders_suite,
                data_asset_name="silver_trade_orders",
                data_asset_path=cfg.get_silver_orders_path("{{ ds }}"),
                data_context_root_dir=cfg.data_context_root_dir,
                fail_task_on_validation_failure=cfg.fail_task_on_validation_failure,
                doc_md="Validates Silver trade orders for primary key uniqueness and notional bounds.",
            )

            validate_silver_vwap = GreatExpectationsOperator(
                task_id="validate_silver_vwap",
                expectation_suite_name=cfg.silver_vwap_suite,
                data_asset_name="silver_rolling_vwap",
                data_asset_path=cfg.get_silver_vwap_path("{{ ds }}"),
                data_context_root_dir=cfg.data_context_root_dir,
                fail_task_on_validation_failure=cfg.fail_task_on_validation_failure,
                doc_md="Validates Silver rolling VWAP aggregations and Bollinger volatility bands.",
            )

        # ---------------------------------------------------------------------
        # TaskGroup: Gold Layer Data Quality Suites
        # ---------------------------------------------------------------------
        with TaskGroup("gold_quality_validations", tooltip="Gold Curated Risk Layer Validation") as gold_group:
            validate_gold_var = GreatExpectationsOperator(
                task_id="validate_gold_var",
                expectation_suite_name=cfg.gold_var_suite,
                data_asset_name="gold_portfolio_var",
                data_asset_path=cfg.get_gold_var_path("{{ ds }}"),
                data_context_root_dir=cfg.data_context_root_dir,
                fail_task_on_validation_failure=cfg.fail_task_on_validation_failure,
                doc_md="Validates Value-at-Risk matrices and mathematical invariants (VaR_99 >= VaR_95 >= 0).",
            )

            validate_gold_features = GreatExpectationsOperator(
                task_id="validate_gold_features",
                expectation_suite_name=cfg.gold_features_suite,
                data_asset_name="gold_risk_features",
                data_asset_path=cfg.get_gold_features_path("{{ ds }}"),
                data_context_root_dir=cfg.data_context_root_dir,
                fail_task_on_validation_failure=cfg.fail_task_on_validation_failure,
                doc_md="Validates Point-in-Time Risk Feature Store (normalized liquidity and order imbalance).",
            )

        publish_data_docs = GreatExpectationsOperator(
            task_id="publish_data_docs",
            checkpoint_name=cfg.master_checkpoint_name,
            expectation_suite_name=cfg.gold_var_suite,
            data_context_root_dir=cfg.data_context_root_dir,
            data_docs_dir=f"uncommitted/data_docs/{cfg.environment}",
            fail_task_on_validation_failure=False,
            doc_md="Compiles and publishes consolidated Great Expectations HTML Data Docs.",
        )

        end_quality_gates = EmptyOperator(
            task_id="end_quality_gates",
            doc_md="Milestone marker signaling successful validation across all data quality gates.",
        )

        start_quality_gates >> silver_group >> gold_group >> publish_data_docs >> end_quality_gates

    return dag


# Canonical DAG instance for Cloud Composer auto-discovery
great_expectations_dag = build_great_expectations_dag()
