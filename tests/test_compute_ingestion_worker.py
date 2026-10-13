"""Compute Engine and Ingestion Worker Validation Tests (Day 04).

Validates:
- Compute Engine Module: Terraform HCL definitions, zero-external IP enforcement,
  Shielded VM configuration, least-privilege IAM service account attachment,
  and network tagging.
- Systemd service unit structure, resource boundaries, restart policies, and user configuration.
- Host provisioning shell script and startup script syntax, kernel tuning, and Docker configuration.
- Ingestion worker Dockerfile hardening (non-root USER, HEALTHCHECK, slim base).
- IngestionWorker Python daemon: telemetry metrics, synthetic order generation,
  retry/backoff error handling, and embedded HTTP /health & /metrics endpoints.
"""

import configparser
import json
from pathlib import Path
import re
import socket
import time
import urllib.request
import pytest

from ingestion.pubsub_client import LocalPubSubMock, TradeOrder
from ingestion.worker import IngestionWorker, WorkerConfig, WorkerMetrics

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_COMPUTE_DIR = REPO_ROOT / "terraform" / "modules" / "compute"
TF_ROOT_DIR = REPO_ROOT / "terraform"
INGESTION_DIR = REPO_ROOT / "ingestion"


def get_free_port() -> int:
    """Find a dynamically free TCP port for testing HTTP endpoints."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class TestTerraformComputeModule:
    """Validate Compute Engine Terraform HCL module definitions and security."""

    def test_compute_module_files_exist(self) -> None:
        """Verify all critical compute module files exist."""
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf", "scripts/startup.sh"]
        for filename in required:
            file_path = TF_COMPUTE_DIR / filename
            assert file_path.is_file(), f"Missing required file: {file_path}"
            assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

    def test_balanced_braces_in_compute_module(self) -> None:
        """Verify balanced HCL block braces in compute module."""
        for tf_file in TF_COMPUTE_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_file.name}"

    def test_zero_public_ip_enforced(self) -> None:
        """Verify the VM has zero public IPv4 allocation (no access_config block)."""
        main_tf = (TF_COMPUTE_DIR / "main.tf").read_text(encoding="utf-8")
        assert "network_interface {" in main_tf
        assert "subnetwork = var.subnetwork_id" in main_tf
        # Ensure access_config is not present in network_interface (ignoring comments)
        net_block = re.search(r"network_interface\s*\{([^}]+)\}", main_tf)
        assert net_block is not None
        code_without_comments = re.sub(r"#.*", "", net_block.group(1))
        assert "access_config" not in code_without_comments, (
            "VM must NOT define access_config (zero public IP required for compliance)"
        )


    def test_shielded_vm_enabled(self) -> None:
        """Verify Shielded VM security settings are enforced."""
        main_tf = (TF_COMPUTE_DIR / "main.tf").read_text(encoding="utf-8")
        assert "shielded_instance_config" in main_tf
        assert "enable_secure_boot          = true" in main_tf
        assert "enable_vtpm                 = true" in main_tf
        assert "enable_integrity_monitoring = true" in main_tf

    def test_least_privilege_service_account_bound(self) -> None:
        """Verify service account binding is parameterized to ingestion service account."""
        main_tf = (TF_COMPUTE_DIR / "main.tf").read_text(encoding="utf-8")
        assert "service_account {" in main_tf
        assert "email  = var.service_account_email" in main_tf

    def test_network_tags_and_metadata(self) -> None:
        """Verify network tags for IAP SSH and OS Login metadata are present."""
        main_tf = (TF_COMPUTE_DIR / "main.tf").read_text(encoding="utf-8")
        assert "tags = var.network_tags" in main_tf
        assert 'enable-oslogin             = "TRUE"' in main_tf
        assert "startup-script = templatefile" in main_tf

    def test_root_terraform_wires_compute_module(self) -> None:
        """Verify root main.tf provisions the compute module with network and IAM links."""
        root_main = (TF_ROOT_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'module "compute"' in root_main
        assert "module.network.ingestion_subnet_id" in root_main
        assert "module.pubsub.ingestion_service_account_email" in root_main


class TestSystemdAndProvisioningScripts:
    """Validate systemd unit and shell provisioning scripts."""

    def test_systemd_unit_structure_and_limits(self) -> None:
        """Verify trading-ingestion.service file conforms to systemd unit specs."""
        unit_path = INGESTION_DIR / "systemd" / "trading-ingestion.service"
        assert unit_path.is_file()

        content = unit_path.read_text(encoding="utf-8")
        # Systemd sections
        assert "[Unit]" in content
        assert "[Service]" in content
        assert "[Install]" in content

        # Robustness & Lifecycle
        assert "Restart=always" in content
        assert "RestartSec=5s" in content
        assert "KillMode=mixed" in content
        assert "TimeoutStopSec=30" in content

        # User security & cgroups
        assert "User=riskworker" in content
        assert "MemoryMax=4G" in content
        assert "CPUQuota=200%" in content

        # Docker container commands
        assert "ExecStartPre=" in content
        assert "ExecStart=" in content
        assert "ExecStop=" in content

    def test_provisioning_script_syntax_and_sysctl(self) -> None:
        """Verify host provisioning script contains kernel tuning and safety options."""
        script_path = INGESTION_DIR / "scripts" / "provision_worker.sh"
        assert script_path.is_file()
        content = script_path.read_text(encoding="utf-8")

        assert content.startswith("#!/usr/bin/env bash")
        assert "set -euo pipefail" in content
        assert "net.core.rmem_max = 16777216" in content
        assert "net.ipv4.tcp_rmem" in content
        assert "trading-ingestion.service" in content

    def test_compute_startup_script(self) -> None:
        """Verify startup.sh in compute module includes required bootstrap steps."""
        startup_path = TF_COMPUTE_DIR / "scripts" / "startup.sh"
        assert startup_path.is_file()
        content = startup_path.read_text(encoding="utf-8")

        assert content.startswith("#!/usr/bin/env bash")
        assert "set -euo pipefail" in content
        assert "docker" in content
        assert "riskworker" in content
        assert "systemctl enable trading-ingestion.service" in content


class TestIngestionContainerArtifacts:
    """Validate Dockerfile and Docker Compose configurations."""

    def test_dockerfile_security_and_hardening(self) -> None:
        """Verify Dockerfile uses non-root user and specifies healthcheck."""
        dockerfile_path = INGESTION_DIR / "Dockerfile"
        assert dockerfile_path.is_file()
        content = dockerfile_path.read_text(encoding="utf-8")

        assert "FROM python:3.12-slim" in content
        assert "USER riskworker" in content
        assert "HEALTHCHECK" in content
        assert "ENTRYPOINT" in content
        assert "ingestion.worker" in content

    def test_docker_compose_configuration(self) -> None:
        """Verify docker-compose.yml defines the ingestion-worker service."""
        compose_path = INGESTION_DIR / "docker-compose.yml"
        assert compose_path.is_file()
        content = compose_path.read_text(encoding="utf-8")

        assert "services:" in content
        assert "ingestion-worker:" in content
        assert "8080:8080" in content
        assert "healthcheck:" in content


class TestIngestionWorkerRuntime:
    """Validate Python IngestionWorker daemon logic and HTTP endpoints."""

    def test_worker_config_defaults_and_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify configuration parsing from environment variables."""
        monkeypatch.setenv("PROJECT_ID", "test-risk-project")
        monkeypatch.setenv("MARKET_FEED_TICKERS", "NVDA,TSLA,SPY")
        monkeypatch.setenv("METRICS_PORT", "9090")
        monkeypatch.setenv("SIMULATION_MODE", "true")

        config = WorkerConfig.from_env()
        assert config.project_id == "test-risk-project"
        assert config.tickers == ["NVDA", "TSLA", "SPY"]
        assert config.metrics_port == 9090
        assert config.simulation_mode is True

    def test_synthetic_order_generation(self) -> None:
        """Verify synthetic trade order generation creates valid TradeOrder."""
        config = WorkerConfig(tickers=["AAPL", "MSFT"])
        worker = IngestionWorker(config=config)

        order = worker.generate_synthetic_order()
        assert isinstance(order, TradeOrder)
        assert order.ticker in ["AAPL", "MSFT"]
        assert order.quantity > 0
        assert order.price > 0
        assert order.notional_usd == round(order.quantity * order.price, 2)
        assert order.side in ("BUY", "SELL")

    def test_order_publishing_with_mock(self) -> None:
        """Verify publishing records success in telemetry metrics."""
        mock_pubsub = LocalPubSubMock()
        topic = "projects/test/topics/orders"
        mock_pubsub.create_topic(topic)

        config = WorkerConfig(topic_id=topic)
        worker = IngestionWorker(config=config, publisher=mock_pubsub)

        order = TradeOrder.create(
            portfolio_id="PORT-TEST",
            ticker="GOOGL",
            asset_class="EQUITY",
            side="BUY",
            order_type="LIMIT",
            quantity=10.0,
            price=150.0,
        )

        msg_id = worker.publish_order(order)
        assert msg_id is not None
        assert len(mock_pubsub.topics[topic]) == 1

        snapshot = worker.metrics.get_snapshot()
        assert snapshot["orders_published"] == 1
        assert snapshot["notional_usd_total"] == 1500.0
        assert snapshot["last_order_id"] == order.order_id
        assert snapshot["errors_count"] == 0

    def test_order_publishing_retry_and_backoff_on_failure(self) -> None:
        """Verify exponential backoff retry logic on transient failure."""

        class FailingPublisher:
            def __init__(self, fail_count: int) -> None:
                self.calls = 0
                self.fail_count = fail_count

            def publish(self, topic: str, data: bytes, attributes: dict) -> str:
                self.calls += 1
                if self.calls <= self.fail_count:
                    raise ConnectionResetError("Transient network failure")
                return "msg-success-after-retry"

        # Should succeed on 2nd attempt with max_retries=3
        config = WorkerConfig(max_retries=3, initial_backoff_sec=0.01)
        worker = IngestionWorker(config=config, publisher=FailingPublisher(fail_count=1))

        order = TradeOrder.create("PORT-1", "NVDA", "EQUITY", "BUY", "LIMIT", 1.0, 500.0)
        msg_id = worker.publish_order(order)
        assert msg_id == "msg-success-after-retry"
        assert worker.metrics.orders_published == 1

        # Should raise RuntimeError if it exceeds max_retries
        failing_worker = IngestionWorker(config=config, publisher=FailingPublisher(fail_count=5))
        with pytest.raises(RuntimeError, match="Failed to publish order"):
            failing_worker.publish_order(order)
        assert failing_worker.metrics.errors_count == 1

    def test_embedded_http_health_and_metrics_endpoints(self) -> None:
        """Verify embedded HTTP server answers /health and /metrics requests."""
        port = get_free_port()
        config = WorkerConfig(metrics_port=port, metrics_host="127.0.0.1")
        worker = IngestionWorker(config=config)

        # Start HTTP server
        worker.start_http_server()
        worker.metrics.set_status("RUNNING")

        try:
            # Test /health endpoint
            health_url = f"http://127.0.0.1:{port}/health"
            with urllib.request.urlopen(health_url, timeout=3.0) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["status"] == "RUNNING"
                assert data["healthy"] is True
                assert "uptime_seconds" in data

            # Test /metrics endpoint
            metrics_url = f"http://127.0.0.1:{port}/metrics"
            with urllib.request.urlopen(metrics_url, timeout=3.0) as resp:
                assert resp.status == 200
                text = resp.read().decode("utf-8")
                assert "trading_ingestion_orders_published_total" in text
                assert "trading_ingestion_healthy 1" in text

        finally:
            worker.stop_http_server()

    def test_graceful_worker_lifecycle(self) -> None:
        """Verify start, run_cycle, and clean stop of IngestionWorker."""
        port = get_free_port()
        config = WorkerConfig(
            metrics_port=port,
            metrics_host="127.0.0.1",
            publish_interval_seconds=0.05,
        )
        worker = IngestionWorker(config=config)

        worker.start(run_in_background=True)
        time.sleep(0.15)  # Allow a few cycles to run

        snapshot = worker.metrics.get_snapshot()
        assert snapshot["orders_published"] >= 1
        assert snapshot["status"] == "RUNNING"

        # Graceful stop
        worker.stop()
        assert worker.metrics.status == "STOPPED"
