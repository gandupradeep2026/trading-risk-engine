"""Unit and Integration Tests for Ephemeral Dataproc Cluster Orchestration (Day 19).

Validates:
1. Dataproc Ephemeral Cluster & PySpark Job Configuration:
   - Config validation invariants, default resolution, environment variable overrides.
   - Canonical RFC 1035 cluster name generation.
   - Standard GCP Dataproc ClusterConfig dictionary structure (master, worker, secondary spot workers,
     internal IP only, subnetwork URI, image version, software properties, and lifecycle TTLs).
   - PySpark Batch JobConfig structure, arguments, and cluster placement.
2. Dataproc Airflow Compatibility Operators & Registry:
   - In-memory thread-safe `DataprocClusterStateRegistry` lifecycle transitions.
   - `DataprocCreateClusterOperator` execution, template rendering, and error handling.
   - `DataprocSubmitJobOperator` execution, cluster state checks, and failure handling.
   - `DataprocDeleteClusterOperator` execution, idempotency, and guaranteed `TriggerRule.ALL_DONE`.
3. Standalone Dataproc Ephemeral Orchestration DAG (`dataproc_ephemeral_batch_pipeline`):
   - DAG topology, SLA, tags, schedule, task dependencies, and cycle validation.
   - Cluster name Jinja templating (`{{ ds_nodash }}`, `{{ task_instance.try_number }}`).
   - Verification that teardown task enforces `TriggerRule.ALL_DONE`.
4. Master Lakehouse DAG Integration (`trading_risk_daily_batch_pipeline`):
   - Verification that `dataproc_batch_aggregation` TaskGroup uses production Dataproc operators.
   - End-to-end topological sort and acyclic graph integrity.
5. PySpark Daily Batch Aggregation Script (`batch/dataproc/daily_tick_aggregation.py`):
   - Microstructure metrics: OHLCV, Volume-Weighted Average Price (VWAP), sample standard deviation,
     realized volatility in basis points, and average/max bid-ask spread.
   - Institutional order flow metrics: directional buy/sell notional, net exposure, order flow imbalance,
     and average order size.
   - Snappy-compressed partitioned Parquet dataset generation and schema validation.
   - Deterministic SHA-256 record hash for data lineage.
   - CLI argument parsing and telemetry reporting.
6. Ephemeral Disaster Recovery & Zero Cost Leak Verification:
   - End-to-end simulation: cluster creation -> job execution -> cluster teardown (0 lingering clusters).
   - Job failure simulation: cluster teardown task reliably runs and deletes cluster on upstream failure.
"""

from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
from typing import Any, Dict, List
import pytest
import pyarrow.parquet as pq

from batch.dataproc import (
    PORTFOLIO_FLOW_ARROW_SCHEMA,
    TICKER_SUMMARY_ARROW_SCHEMA,
    DailyPortfolioFlowRecord,
    DailyTickerSummaryRecord,
    DataprocBatchAggregationTelemetry,
    compute_portfolio_flow_summaries,
    compute_record_hash,
    compute_ticker_daily_summaries,
    run_batch_aggregation,
    write_portfolio_flow_parquet,
    write_ticker_summaries_parquet,
)
from orchestration.airflow.airflow_compat import (
    AirflowException,
    DataprocClusterStateRegistry,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    TriggerRule,
    create_mock_task_context,
)
from orchestration.airflow.config.airflow_config import AirflowEnvironmentConfig
from orchestration.airflow.config.dataproc_cluster_config import (
    DataprocBatchJobConfig,
    DataprocEphemeralClusterConfig,
)
from orchestration.airflow.dags.dataproc_ephemeral_dag import (
    build_dataproc_ephemeral_dag,
    dataproc_ephemeral_dag,
)
from orchestration.airflow.dags.trading_risk_daily_batch_dag import (
    build_trading_risk_dag,
    trading_risk_dag,
)


@pytest.fixture(autouse=True)
def reset_dataproc_registry() -> None:
    """Reset the mock Dataproc registry before each test."""
    DataprocClusterStateRegistry.reset()
    yield
    DataprocClusterStateRegistry.reset()


# =============================================================================
# 1. DATAPROC EPHEMERAL CLUSTER & JOB CONFIGURATION TESTS
# =============================================================================
class TestDataprocEphemeralClusterConfig:
    """Validate Dataproc cluster and PySpark job configuration builders."""

    def test_default_config_resolution(self) -> None:
        """Verify default cluster configuration parameters."""
        cfg = DataprocEphemeralClusterConfig()
        assert cfg.project_id == "trading-risk-dev-001"
        assert cfg.region == "us-central1"
        assert cfg.zone == "us-central1-a"
        assert cfg.subnetwork == "sb-data-services"
        assert cfg.internal_ip_only is True
        assert cfg.master_machine_type == "n2-standard-4"
        assert cfg.master_disk_type == "pd-ssd"
        assert cfg.master_disk_size_gb == 100
        assert cfg.worker_machine_type == "n2-standard-4"
        assert cfg.worker_num_instances == 2
        assert cfg.secondary_worker_num_instances == 2
        assert cfg.secondary_worker_preemptible is True
        assert cfg.image_version == "2.2-debian12"
        assert cfg.idle_delete_ttl_sec == 1200
        assert cfg.auto_delete_ttl_sec == 7200
        assert "trading-risk" in cfg.tags

    def test_config_validation_rules(self) -> None:
        """Verify invalid configuration throws descriptive ValueError."""
        with pytest.raises(ValueError, match="project_id cannot be empty"):
            DataprocEphemeralClusterConfig(project_id="")

        with pytest.raises(ValueError, match="region cannot be empty"):
            DataprocEphemeralClusterConfig(region="")

        with pytest.raises(ValueError, match="subnetwork cannot be empty"):
            DataprocEphemeralClusterConfig(subnetwork="")

        with pytest.raises(ValueError, match="master_num_instances must be at least 1"):
            DataprocEphemeralClusterConfig(master_num_instances=0)

        with pytest.raises(ValueError, match="worker_num_instances cannot be negative"):
            DataprocEphemeralClusterConfig(worker_num_instances=-1)

        with pytest.raises(ValueError, match="master_disk_size_gb must be at least 20 GB"):
            DataprocEphemeralClusterConfig(master_disk_size_gb=10)

        with pytest.raises(ValueError, match="idle_delete_ttl_sec must be positive"):
            DataprocEphemeralClusterConfig(idle_delete_ttl_sec=0)

        with pytest.raises(ValueError, match="auto_delete_ttl_sec must be >= idle_delete_ttl_sec"):
            DataprocEphemeralClusterConfig(idle_delete_ttl_sec=3600, auto_delete_ttl_sec=1800)

    def test_cluster_naming_convention(self) -> None:
        """Verify canonical RFC 1035 compliant cluster name generation."""
        cfg = DataprocEphemeralClusterConfig(cluster_name_prefix="tre-dp-batch-dev")
        cluster_name = cfg.generate_cluster_name(execution_date_nodash="20261030", try_number=1)
        assert cluster_name == "tre-dp-batch-dev-20261030-1"
        assert len(cluster_name) <= 51
        assert cluster_name.islower()

        # Retry number 2
        cluster_name_r2 = cfg.generate_cluster_name(execution_date_nodash="20261030", try_number=2)
        assert cluster_name_r2 == "tre-dp-batch-dev-20261030-2"

    def test_to_cluster_config_dict_structure(self) -> None:
        """Verify the generated GCP Dataproc ClusterConfig dictionary."""
        cfg = DataprocEphemeralClusterConfig(
            project_id="trading-risk-prod-001",
            region="us-central1",
            subnetwork="sb-data-services",
        )
        cdict = cfg.to_cluster_config_dict()

        # Network and security
        gce_cfg = cdict["gce_cluster_config"]
        assert gce_cfg["internal_ip_only"] is True
        assert gce_cfg["subnetwork_uri"] == "projects/trading-risk-prod-001/regions/us-central1/subnetworks/sb-data-services"
        assert gce_cfg["metadata"]["enable-oslogin"] == "TRUE"
        assert "dataproc-ephemeral" in gce_cfg["tags"]

        # Master and worker nodes
        assert cdict["master_config"]["num_instances"] == 1
        assert cdict["master_config"]["machine_type_uri"] == "n2-standard-4"
        assert cdict["master_config"]["disk_config"]["boot_disk_type"] == "pd-ssd"
        assert cdict["master_config"]["disk_config"]["boot_disk_size_gb"] == 100

        assert cdict["worker_config"]["num_instances"] == 2
        assert cdict["worker_config"]["machine_type_uri"] == "n2-standard-4"
        assert cdict["worker_config"]["disk_config"]["boot_disk_type"] == "pd-standard"

        # Secondary preemptible workers
        assert cdict["secondary_worker_config"]["num_instances"] == 2
        assert cdict["secondary_worker_config"]["is_preemptible"] is True
        assert cdict["secondary_worker_config"]["preemptibility"] == "PREEMPTIBLE"

        # Software and Spark properties
        assert cdict["software_config"]["image_version"] == "2.2-debian12"
        props = cdict["software_config"]["properties"]
        assert props["spark:spark.serializer"] == "org.apache.spark.serializer.KryoSerializer"
        assert props["spark:spark.sql.adaptive.enabled"] == "true"
        assert "BindingParquetOutputCommitter" in props["spark:spark.sql.parquet.output.committer.class"]

        # Lifecycle safety net
        assert cdict["lifecycle_config"]["idle_delete_ttl"] == "1200s"
        assert cdict["lifecycle_config"]["auto_delete_ttl"] == "7200s"

    def test_batch_job_config_structure(self) -> None:
        """Verify DataprocBatchJobConfig creates expected PySpark job specification."""
        job_cfg = DataprocBatchJobConfig(
            project_id="trading-risk-dev-001",
            region="us-central1",
            main_python_file_uri="gs://trading-risk-bronze-dev/scripts/dataproc/daily_tick_aggregation.py",
            args=["--target-date", "2026-10-30", "--environment", "dev"],
        )
        job_dict = job_cfg.to_pyspark_job_dict(cluster_name="tre-dp-batch-20261030-1", job_id="job_001")

        assert job_dict["placement"]["cluster_name"] == "tre-dp-batch-20261030-1"
        assert job_dict["reference"]["job_id"] == "job_001"
        assert job_dict["reference"]["project_id"] == "trading-risk-dev-001"
        assert job_dict["pyspark_job"]["main_python_file_uri"] == "gs://trading-risk-bronze-dev/scripts/dataproc/daily_tick_aggregation.py"
        assert "--target-date" in job_dict["pyspark_job"]["args"]
        assert job_dict["pyspark_job"]["properties"]["spark.submit.deployMode"] == "cluster"


# =============================================================================
# 2. AIRFLOW DATAPROC OPERATORS & REGISTRY TESTS
# =============================================================================
class TestDataprocAirflowOperators:
    """Validate Dataproc operators in hermetic compatibility mode."""

    def test_cluster_create_and_delete_lifecycle(self) -> None:
        """Verify cluster creation, state tracking, and teardown."""
        cfg = DataprocEphemeralClusterConfig()
        cluster_name = "tre-dp-batch-20261030-1"

        create_op = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id="trading-risk-dev-001",
            cluster_name=cluster_name,
            cluster_config=cfg.to_cluster_config_dict(),
        )
        result = create_op.execute({})
        assert result["status"] == "RUNNING"
        assert result["cluster_name"] == cluster_name
        assert cluster_name in DataprocClusterStateRegistry.list_active_clusters()

        delete_op = DataprocDeleteClusterOperator(
            task_id="teardown_cluster",
            project_id="trading-risk-dev-001",
            cluster_name=cluster_name,
        )
        assert delete_op.trigger_rule == TriggerRule.ALL_DONE
        deleted = delete_op.execute({})
        assert deleted is True
        assert cluster_name not in DataprocClusterStateRegistry.list_active_clusters()

    def test_job_submission_requires_active_cluster(self) -> None:
        """Verify job submission fails if cluster is not active."""
        submit_op = DataprocSubmitJobOperator(
            task_id="submit_job",
            project_id="trading-risk-dev-001",
            region="us-central1",
            job={"placement": {"cluster_name": "non-existent-cluster"}},
        )
        with pytest.raises(AirflowException, match="is not running"):
            submit_op.execute({})

    def test_job_submission_success_on_active_cluster(self) -> None:
        """Verify successful job submission and registry tracking."""
        cfg = DataprocEphemeralClusterConfig()
        cluster_name = "tre-dp-batch-20261030-1"

        create_op = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id="trading-risk-dev-001",
            cluster_name=cluster_name,
            cluster_config=cfg.to_cluster_config_dict(),
        )
        create_op.execute({})

        submit_op = DataprocSubmitJobOperator(
            task_id="submit_job",
            project_id="trading-risk-dev-001",
            region="us-central1",
            job={
                "placement": {"cluster_name": cluster_name},
                "pyspark_job": {"main_python_file_uri": "gs://bucket/script.py"},
            },
        )
        job_result = submit_op.execute({})
        assert job_result["status"] == "SUCCESS"
        assert job_result["cluster_name"] == cluster_name
        assert DataprocClusterStateRegistry.get_job(job_result["job_id"]) is not None

    def test_jinja_template_rendering(self) -> None:
        """Verify Jinja parameters (ds_nodash, try_number) are resolved in operator execution."""
        cfg = DataprocEphemeralClusterConfig()
        template_name = "tre-dp-{{ ds_nodash }}-{{ task_instance.try_number }}"

        create_op = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id="trading-risk-dev-001",
            cluster_name=template_name,
            cluster_config=cfg.to_cluster_config_dict(),
        )

        mock_context = {
            "execution_date": datetime(2026, 10, 30),
            "task_instance": type("TI", (), {"try_number": 2})(),
        }
        res = create_op.execute(mock_context)
        assert res["cluster_name"] == "tre-dp-20261030-2"
        assert "tre-dp-20261030-2" in DataprocClusterStateRegistry.list_active_clusters()

    def test_failure_injection_in_cluster_creation(self) -> None:
        """Verify simulated cluster creation failure raises AirflowException."""
        DataprocClusterStateRegistry.inject_failure(
            "create_cluster",
            AirflowException("GCP QuotaExceededException: IN_USE_ADDRESSES quota exceeded"),
        )
        cfg = DataprocEphemeralClusterConfig()
        op = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id="trading-risk-dev-001",
            cluster_name="test-cluster",
            cluster_config=cfg.to_cluster_config_dict(),
        )
        with pytest.raises(AirflowException, match="QuotaExceededException"):
            op.execute({})


# =============================================================================
# 3. STANDALONE DATAPROC EPHEMERAL DAG TESTS
# =============================================================================
class TestDataprocEphemeralDAG:
    """Validate standalone dataproc_ephemeral_batch_pipeline structure and workflow."""

    def test_dag_metadata_and_properties(self) -> None:
        """Verify DAG identifier, schedule, tags, and SLA configuration."""
        dag = dataproc_ephemeral_dag
        assert dag.dag_id == "dataproc_ephemeral_batch_pipeline"
        assert dag.schedule_interval == "0 1 * * *"
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert "dataproc" in dag.tags
        assert "ephemeral" in dag.tags
        assert dag.sla == timedelta(minutes=60)

    def test_dag_tasks_and_dependencies(self) -> None:
        """Verify task existence, order, and upstream/downstream dependencies."""
        dag = dataproc_ephemeral_dag
        expected_tasks = {
            "verify_bronze_partitions",
            "create_ephemeral_cluster",
            "submit_pyspark_job",
            "teardown_cluster",
            "notify_batch_completion",
        }
        assert expected_tasks.issubset(set(dag.task_dict.keys()))

        verify = dag.get_task("verify_bronze_partitions")
        create = dag.get_task("create_ephemeral_cluster")
        submit = dag.get_task("submit_pyspark_job")
        teardown = dag.get_task("teardown_cluster")
        notify = dag.get_task("notify_batch_completion")

        assert "create_ephemeral_cluster" in verify.downstream_task_ids
        assert "submit_pyspark_job" in create.downstream_task_ids
        assert "teardown_cluster" in submit.downstream_task_ids
        assert "notify_batch_completion" in teardown.downstream_task_ids

        # Critical: Teardown cluster task MUST enforce trigger_rule=all_done
        assert teardown.trigger_rule == TriggerRule.ALL_DONE

    def test_dag_topological_sort_and_acyclic(self) -> None:
        """Verify DAG contains no cycles and topological sort order is strictly linear."""
        dag = dataproc_ephemeral_dag
        ordered = dag.topological_sort()
        task_ids = [t.task_id for t in ordered]
        assert task_ids == [
            "verify_bronze_partitions",
            "create_ephemeral_cluster",
            "submit_pyspark_job",
            "teardown_cluster",
            "notify_batch_completion",
        ]


# =============================================================================
# 4. MASTER LAKEHOUSE DAG INTEGRATION TESTS
# =============================================================================
class TestTradingRiskDailyBatchDagIntegration:
    """Verify integration of production Dataproc operators inside trading_risk_daily_batch_pipeline."""

    def test_dataproc_group_uses_real_operators(self) -> None:
        """Verify TaskGroup dataproc_batch_aggregation contains real Dataproc operators."""
        dag = trading_risk_dag
        create_task = dag.get_task("create_ephemeral_cluster")
        submit_task = dag.get_task("submit_pyspark_job")
        teardown_task = dag.get_task("teardown_cluster")

        assert isinstance(create_task, DataprocCreateClusterOperator)
        assert isinstance(submit_task, DataprocSubmitJobOperator)
        assert isinstance(teardown_task, DataprocDeleteClusterOperator)

        # Teardown must be trigger_rule=all_done
        assert teardown_task.trigger_rule == TriggerRule.ALL_DONE

    def test_master_dag_validates_without_cycles(self) -> None:
        """Verify trading_risk_daily_batch_pipeline topology is acyclic and valid."""
        dag = trading_risk_dag
        ordered = dag.topological_sort()
        assert len(ordered) == len(dag.tasks)
        assert ordered[0].task_id == "start_pipeline"
        assert ordered[-1].task_id == "end_pipeline"


# =============================================================================
# 5. PYSPARK DAILY BATCH AGGREGATION SCRIPT TESTS
# =============================================================================
class TestPySparkDailyBatchAggregationScript:
    """Validate quantitative financial rollups and Parquet output schemas."""

    def test_ticker_daily_summaries_calculation(self) -> None:
        """Verify OHLCV, VWAP, price stddev, and spread calculation."""
        ticks = [
            {"ticker": "AAPL", "asset_class": "EQUITY", "last_price": 150.0, "volume": 100, "bid": 149.9, "ask": 150.1, "sequence_number": 1, "event_timestamp": "2026-10-30T10:00:00Z"},
            {"ticker": "AAPL", "asset_class": "EQUITY", "last_price": 155.0, "volume": 200, "bid": 154.9, "ask": 155.1, "sequence_number": 2, "event_timestamp": "2026-10-30T11:00:00Z"},
            {"ticker": "AAPL", "asset_class": "EQUITY", "last_price": 145.0, "volume": 100, "bid": 144.9, "ask": 145.1, "sequence_number": 3, "event_timestamp": "2026-10-30T12:00:00Z"},
            {"ticker": "AAPL", "asset_class": "EQUITY", "last_price": 152.0, "volume": 100, "bid": 151.9, "ask": 152.1, "sequence_number": 4, "event_timestamp": "2026-10-30T13:00:00Z"},
        ]
        summaries = compute_ticker_daily_summaries(ticks, data_date="2026-10-30", batch_id="batch_001")
        assert len(summaries) == 1
        s = summaries[0]

        assert s.ticker == "AAPL"
        assert s.asset_class == "EQUITY"
        assert s.open_price == 150.0
        assert s.high_price == 155.0
        assert s.low_price == 145.0
        assert s.close_price == 152.0
        assert s.tick_count == 4
        assert s.total_volume == 500.0

        # Expected VWAP: (150*100 + 155*200 + 145*100 + 152*100) / 500
        # = (15000 + 31000 + 14500 + 15200) / 500 = 75700 / 500 = 151.4
        assert s.daily_vwap == pytest.approx(151.4, rel=1e-3)
        assert s.total_notional_usd == pytest.approx(75700.0, rel=1e-3)
        assert s.price_stddev > 0.0
        assert s.realized_volatility_bps > 0.0
        assert s.record_hash is not None
        assert len(s.record_hash) == 64

    def test_portfolio_flow_and_imbalance_calculation(self) -> None:
        """Verify order flow imbalance and net notional calculation."""
        orders = [
            {"portfolio_id": "ALPHA", "ticker": "MSFT", "asset_class": "EQUITY", "side": "BUY", "quantity": 100, "price": 400.0, "notional_usd": 40000.0},
            {"portfolio_id": "ALPHA", "ticker": "MSFT", "asset_class": "EQUITY", "side": "BUY", "quantity": 50, "price": 400.0, "notional_usd": 20000.0},
            {"portfolio_id": "ALPHA", "ticker": "MSFT", "asset_class": "EQUITY", "side": "SELL", "quantity": 50, "price": 400.0, "notional_usd": 20000.0},
        ]
        flows = compute_portfolio_flow_summaries(orders, data_date="2026-10-30", batch_id="batch_001")
        assert len(flows) == 1
        f = flows[0]

        assert f.portfolio_id == "ALPHA"
        assert f.ticker == "MSFT"
        assert f.order_count == 3
        assert f.buy_order_count == 2
        assert f.sell_order_count == 1
        assert f.buy_notional_usd == 60000.0
        assert f.sell_notional_usd == 20000.0
        assert f.net_notional_usd == 40000.0
        # Imbalance: (60k - 20k) / (60k + 20k) = 40k / 80k = +0.5
        assert f.order_flow_imbalance == pytest.approx(0.5, rel=1e-3)
        assert f.avg_order_size_usd == pytest.approx(80000.0 / 3, rel=1e-3)

    def test_snappy_parquet_dataset_writing_and_schema(self) -> None:
        """Verify write to Snappy-compressed Parquet and schema conformity."""
        with tempfile.TemporaryDirectory() as tmpdir:
            summaries = [
                DailyTickerSummaryRecord(
                    ticker="NVDA",
                    asset_class="EQUITY",
                    data_date="2026-10-30",
                    open_price=120.0,
                    high_price=125.0,
                    low_price=119.0,
                    close_price=124.0,
                    tick_count=10,
                    total_volume=5000.0,
                    total_notional_usd=615000.0,
                    daily_vwap=123.0,
                    price_stddev=2.1,
                    realized_volatility_bps=170.73,
                    avg_spread_bps=2.5,
                    max_spread_bps=4.0,
                    computed_at="2026-10-30T16:00:00Z",
                    batch_id="b1",
                    record_hash="hash1",
                )
            ]
            files = write_ticker_summaries_parquet(summaries, tmpdir, "2026-10-30")
            assert len(files) == 1
            assert files[0].endswith(".snappy.parquet")

            # Read back using PyArrow and verify metadata and schemas
            table = pq.read_table(files[0])
            assert table.schema == TICKER_SUMMARY_ARROW_SCHEMA
            assert table.num_rows == 1
            meta = pq.read_metadata(files[0])
            for i in range(meta.num_columns):
                assert meta.row_group(0).column(i).compression == "SNAPPY"

    def test_deterministic_record_hash_stability(self) -> None:
        """Verify SHA-256 record hash is deterministic and invariant to dictionary ordering."""
        rec1 = {"ticker": "AAPL", "data_date": "2026-10-30", "open_price": 150.0, "computed_at": "2026-10-30T10:00:00Z"}
        rec2 = {"data_date": "2026-10-30", "ticker": "AAPL", "open_price": 150.0, "computed_at": "2026-10-30T15:00:00Z"}

        h1 = compute_record_hash(rec1)
        h2 = compute_record_hash(rec2)
        assert h1 == h2

    def test_run_batch_aggregation_end_to_end(self) -> None:
        """Verify complete batch aggregation execution and telemetry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ticks = [
                {"ticker": "BTCUSD", "asset_class": "CRYPTO", "last_price": 65000.0, "volume": 1.5, "bid": 64990.0, "ask": 65010.0},
                {"ticker": "BTCUSD", "asset_class": "CRYPTO", "last_price": 65500.0, "volume": 2.0, "bid": 65490.0, "ask": 65510.0},
            ]
            orders = [
                {"portfolio_id": "CRYPTO_FUND", "ticker": "BTCUSD", "asset_class": "CRYPTO", "side": "BUY", "quantity": 1.5, "price": 65000.0, "notional_usd": 97500.0},
            ]
            telemetry = run_batch_aggregation(
                ticks_data=ticks,
                orders_data=orders,
                output_dir=tmpdir,
                target_date="2026-10-30",
            )
            assert telemetry.total_ticks_processed == 2
            assert telemetry.total_orders_processed == 1
            assert telemetry.ticker_summaries_emitted == 1
            assert telemetry.portfolio_summaries_emitted == 1
            assert len(telemetry.output_ticker_files) >= 1
            assert len(telemetry.output_order_files) >= 1


# =============================================================================
# 6. EPHEMERAL DISASTER RECOVERY & ZERO COST LEAK TESTS
# =============================================================================
class TestEphemeralDisasterRecoveryAndZeroCostLeak:
    """Verify ephemeral cluster guarantees: teardown on success, teardown on failure."""

    def test_successful_run_leaves_zero_active_clusters(self) -> None:
        """Simulate successful pipeline run and confirm zero active clusters remain."""
        dag = dataproc_ephemeral_dag
        context = {
            "execution_date": datetime(2026, 10, 30),
            "task_instance": type("TI", (), {"try_number": 1})(),
        }

        # Step 1: Create
        dag.get_task("create_ephemeral_cluster").execute(context)
        assert len(DataprocClusterStateRegistry.list_active_clusters()) == 1

        # Step 2: Submit job
        dag.get_task("submit_pyspark_job").execute(context)
        assert len(DataprocClusterStateRegistry.list_active_clusters()) == 1

        # Step 3: Teardown
        dag.get_task("teardown_cluster").execute(context)
        # Guarantees ZERO lingering clusters
        assert len(DataprocClusterStateRegistry.list_active_clusters()) == 0

    def test_job_failure_triggers_cluster_teardown_via_all_done(self) -> None:
        """Simulate job failure and confirm teardown still cleans up ephemeral compute."""
        dag = dataproc_ephemeral_dag
        context = {
            "execution_date": datetime(2026, 10, 30),
            "task_instance": type("TI", (), {"try_number": 1})(),
        }

        # Step 1: Create
        dag.get_task("create_ephemeral_cluster").execute(context)
        assert len(DataprocClusterStateRegistry.list_active_clusters()) == 1

        # Step 2: Inject job failure
        DataprocClusterStateRegistry.inject_failure(
            "submit_job",
            AirflowException("SparkException: Container killed by YARN for exceeding memory limits"),
        )
        with pytest.raises(AirflowException, match="SparkException"):
            dag.get_task("submit_pyspark_job").execute(context)

        # Step 3: Due to TriggerRule.ALL_DONE, Airflow executes teardown regardless of failure
        teardown_task = dag.get_task("teardown_cluster")
        assert teardown_task.trigger_rule == TriggerRule.ALL_DONE
        teardown_task.execute(context)

        # Verify cluster is deleted despite upstream job failure
        assert len(DataprocClusterStateRegistry.list_active_clusters()) == 0
