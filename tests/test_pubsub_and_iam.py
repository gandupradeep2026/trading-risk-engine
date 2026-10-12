"""Pub/Sub and IAM Architecture Validation Tests (Day 03).

Validates:
- Pub/Sub Module: Topic definitions, DLQ dead_letter_policy configuration,
  exponential backoff retry policy, and IAM least-privilege role bindings.
- Zero-cost Python TradeOrder schema validation and serialization.
- In-memory LocalPubSubMock DLQ routing after max delivery attempts.
"""

from pathlib import Path
import re
import pytest
from ingestion.pubsub_client import LocalPubSubMock, TradeOrder

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_PUBSUB_DIR = REPO_ROOT / "terraform" / "modules" / "pubsub"
TF_ROOT_DIR = REPO_ROOT / "terraform"


class TestTerraformPubSubModule:
    """Validate Cloud Pub/Sub Terraform HCL definitions and IAM roles."""

    def test_pubsub_module_files_exist(self) -> None:
        """Verify all critical pubsub module files exist."""
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf"]
        for filename in required:
            file_path = TF_PUBSUB_DIR / filename
            assert file_path.is_file(), f"Missing required file: {file_path}"
            assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

    def test_dlq_and_retry_policy_configured(self) -> None:
        """Verify that subscriptions define dead_letter_policy and retry_policy."""
        main_tf = (TF_PUBSUB_DIR / "main.tf").read_text(encoding="utf-8")
        assert "dead_letter_policy" in main_tf, "Subscription must define dead_letter_policy"
        assert "max_delivery_attempts = var.max_delivery_attempts" in main_tf
        assert "retry_policy" in main_tf
        assert "minimum_backoff" in main_tf

    def test_iam_service_accounts_defined(self) -> None:
        """Verify dedicated service accounts for Ingestion and Dataflow."""
        main_tf = (TF_PUBSUB_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'resource "google_service_account" "ingestion_producer"' in main_tf
        assert 'resource "google_service_account" "dataflow_worker"' in main_tf
        assert "roles/pubsub.publisher" in main_tf
        assert "roles/pubsub.subscriber" in main_tf

    def test_balanced_braces_in_pubsub_module(self) -> None:
        """Verify balanced HCL block braces in Pub/Sub module."""
        for tf_file in TF_PUBSUB_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_file.name}"


class TestTradeOrderSchemaAndValidation:
    """Validate TradeOrder data structure and schema constraints."""

    def test_trade_order_creation_and_notional(self) -> None:
        """Verify valid creation and automatic notional USD calculation."""
        order = TradeOrder.create(
            portfolio_id="PORT-EQ-001",
            ticker="AAPL",
            asset_class="EQUITY",
            side="BUY",
            order_type="LIMIT",
            quantity=100.0,
            price=175.50,
        )
        assert order.notional_usd == 17550.00
        assert order.side == "BUY"
        assert order.ticker == "AAPL"
        assert order.order_id is not None

    def test_invalid_order_rejection(self) -> None:
        """Ensure negative quantities or invalid sides raise ValueError."""
        with pytest.raises(ValueError, match="strictly positive"):
            TradeOrder.create("PORT-1", "AAPL", "EQUITY", "BUY", "LIMIT", quantity=-10, price=100)

        with pytest.raises(ValueError, match="strictly positive"):
            TradeOrder.create("PORT-1", "AAPL", "EQUITY", "BUY", "LIMIT", quantity=10, price=-5)

        with pytest.raises(ValueError, match="Invalid order side"):
            TradeOrder.create("PORT-1", "AAPL", "EQUITY", "HOLD", "LIMIT", quantity=10, price=100)

    def test_json_roundtrip(self) -> None:
        """Verify JSON serialization and deserialization."""
        order = TradeOrder.create(
            portfolio_id="PORT-FX-002",
            ticker="EURUSD",
            asset_class="FX",
            side="SELL",
            order_type="MARKET",
            quantity=50000.0,
            price=1.0850,
        )
        json_str = order.to_json()
        restored = TradeOrder.from_json(json_str)
        assert restored.order_id == order.order_id
        assert restored.notional_usd == order.notional_usd
        assert restored.side == "SELL"


class TestLocalPubSubMockAndDLQ:
    """Validate zero-cost in-memory mock Pub/Sub with DLQ delivery."""

    def test_successful_message_processing(self) -> None:
        """Ensure successfully processed messages are acknowledged."""
        mock = LocalPubSubMock(max_delivery_attempts=3)
        mock.create_subscription("sub-orders", "top-orders", dlq_topic="top-orders-dlq")

        order = TradeOrder.create("PORT-1", "NVDA", "EQUITY", "BUY", "LIMIT", 10.0, 500.0)
        mock.publish("top-orders", order.to_json().encode("utf-8"))

        processed = []

        def handler(data, attrs):
            processed.append(TradeOrder.from_json(data.decode("utf-8")))

        assert mock.process_message("sub-orders", handler) is True
        assert len(processed) == 1
        assert processed[0].ticker == "NVDA"
        # Queue should now be empty
        assert len(mock.subscriptions["sub-orders"]["messages"]) == 0

    def test_dlq_routing_after_max_attempts(self) -> None:
        """Ensure failed messages are routed to DLQ after exceeding max attempts."""
        mock = LocalPubSubMock(max_delivery_attempts=3)
        mock.create_subscription("sub-orders", "top-orders", dlq_topic="top-orders-dlq")

        order = TradeOrder.create("PORT-1", "TSLA", "EQUITY", "BUY", "LIMIT", 5.0, 250.0)
        mock.publish("top-orders", order.to_json().encode("utf-8"))

        def failing_handler(data, attrs):
            raise RuntimeError("Database connection timed out")

        # Attempt 1 -> fails, remains in queue
        assert mock.process_message("sub-orders", failing_handler) is False
        assert len(mock.subscriptions["sub-orders"]["messages"]) == 1

        # Attempt 2 -> fails, remains in queue
        assert mock.process_message("sub-orders", failing_handler) is False
        assert len(mock.subscriptions["sub-orders"]["messages"]) == 1

        # Attempt 3 -> fails, reaches max attempts, routed to DLQ
        assert mock.process_message("sub-orders", failing_handler) is False
        assert len(mock.subscriptions["sub-orders"]["messages"]) == 0
        assert len(mock.topics["top-orders-dlq"]) == 1
        assert len(mock.dlq_messages) == 1
