"""Zero-Cost Local Unit and Integration Test Suite for Cloud Dataflow Order Pipeline (Day 08).

Validates:
- RiskEnginePipelineOptions: CLI argument parsing, canonical GCP naming, validation rules.
- DeadLetterRecord: dataclass invariants, JSON roundtrip, PubsubMessage conversion with attributes.
- DecodeAndValidateTradeOrderFn:
  - Valid trade order parsing from PubsubMessage, bytes, string, and dictionary.
  - Event-time timestamp extraction (ISO 8601 & numeric epoch).
  - Defensive UTF-8 handling and UnicodeDecodeError routing to DLQ.
  - Malformed JSON syntax and non-dict JSON root routing to DLQ.
  - Missing mandatory field detection and MissingFieldError routing to DLQ.
  - Financial domain invariant validation (positive price, positive quantity, valid sides/types).
  - Metric counters and distributions.
- FormatDeadLetterForPubSubFn: serialization to PubsubMessage for deadletter topic publication.
- Apache Beam DirectRunner / TestPipeline Integration:
  - End-to-end processing of pure valid trade order streams.
  - End-to-end processing of mixed dirty streams: deterministic routing into valid_orders vs deadletter_records.
  - Preservation of source attributes in dead-letter records.
  - DAG construction checks (subscription source, topic source, DLQ sink enablement).
"""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List
import pytest

import apache_beam as beam
from apache_beam import pvalue, window
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from ingestion.pubsub_client import TradeOrder
from streaming.dataflow.decoders import (
    TAG_DEADLETTER,
    TAG_VALID_ORDERS,
    DeadLetterRecord,
    DecodeAndValidateOrders,
    DecodeAndValidateTradeOrderFn,
    FormatDeadLetterForPubSubFn,
)
from streaming.dataflow.options import RiskEnginePipelineOptions
from streaming.dataflow.pipeline import DataflowPipelineBundle, build_pipeline


# -----------------------------------------------------------------------------
# 1. PIPELINE OPTIONS & CONFIGURATION TESTS
# -----------------------------------------------------------------------------
class TestRiskEnginePipelineOptions:
    """Validate pipeline options parsing, default values, and GCP naming rules."""

    def test_default_options(self) -> None:
        """Verify default configuration flags."""
        opts = RiskEnginePipelineOptions([])
        assert opts.environment == "dev"
        assert opts.project_id == "trading-risk-engine-dev"
        assert opts.input_subscription is None
        assert opts.input_topic is None
        assert opts.deadletter_topic is None
        assert opts.window_duration_seconds == 300
        assert opts.window_slide_seconds == 60
        assert opts.max_anomaly_drop_threshold == 0.05
        assert opts.enable_attributes is True
        assert opts.enable_deadletter_sink is True

    def test_custom_cli_flags_parsing(self) -> None:
        """Verify CLI argument parsing overrides defaults."""
        flags = [
            "--environment=prod",
            "--project_id=prod-risk-platform",
            "--input_subscription=projects/prod-risk-platform/subscriptions/sub-custom",
            "--deadletter_topic=projects/prod-risk-platform/topics/top-dlq-custom",
            "--window_duration_seconds=600",
            "--window_slide_seconds=120",
            "--max_anomaly_drop_threshold=0.10",
        ]
        opts = RiskEnginePipelineOptions(flags)
        assert opts.environment == "prod"
        assert opts.project_id == "prod-risk-platform"
        assert opts.input_subscription == "projects/prod-risk-platform/subscriptions/sub-custom"
        assert opts.deadletter_topic == "projects/prod-risk-platform/topics/top-dlq-custom"
        assert opts.window_duration_seconds == 600
        assert opts.window_slide_seconds == 120
        assert opts.max_anomaly_drop_threshold == 0.10

    def test_canonical_resource_names_generation(self) -> None:
        """Verify canonical GCP names match Terraform module standards."""
        names = RiskEnginePipelineOptions.get_canonical_resource_names("my-project", "dev")
        assert names["trade_orders_topic"] == "projects/my-project/topics/top-trade-orders-dev"
        assert names["trade_orders_sub"] == "projects/my-project/subscriptions/sub-trade-orders-dataflow-dev"
        assert names["trade_orders_dlq"] == "projects/my-project/topics/top-trade-orders-dlq-dev"
        assert names["risk_alerts_topic"] == "projects/my-project/topics/top-risk-alerts-dev"
        assert names["bronze_bucket"] == "gs://trading-risk-bronze-dev"

    def test_resolve_defaults_populates_canonical_paths(self) -> None:
        """Verify resolve_defaults auto-populates missing topics and subscriptions."""
        opts = RiskEnginePipelineOptions(["--environment=staging", "--project_id=risk-staging"])
        opts.resolve_defaults()
        assert opts.input_subscription == "projects/risk-staging/subscriptions/sub-trade-orders-dataflow-staging"
        assert opts.deadletter_topic == "projects/risk-staging/topics/top-trade-orders-dlq-staging"
        assert opts.output_alerts_topic == "projects/risk-staging/topics/top-risk-alerts-staging"
        assert "trading-risk-bronze-staging" in opts.gcs_output_path

    def test_validation_rejects_invalid_environment(self) -> None:
        """Verify invalid environment name raises ValueError or SystemExit."""
        with pytest.raises(SystemExit):
            RiskEnginePipelineOptions(["--environment=unknown"])

        opts = RiskEnginePipelineOptions([])
        opts.environment = "unknown"
        with pytest.raises(ValueError, match="Invalid environment"):
            opts.validate_options(is_test=True)

    def test_validation_rejects_invalid_window_params(self) -> None:
        """Verify invalid window durations and anomaly thresholds raise ValueError."""
        with pytest.raises(ValueError, match="window_duration_seconds must be positive"):
            RiskEnginePipelineOptions(["--window_duration_seconds=0"]).validate_options(is_test=True)

        with pytest.raises(ValueError, match="window_slide_seconds cannot be greater"):
            RiskEnginePipelineOptions([
                "--window_duration_seconds=60",
                "--window_slide_seconds=120",
            ]).validate_options(is_test=True)

        with pytest.raises(ValueError, match="max_anomaly_drop_threshold must be strictly between 0 and 1"):
            RiskEnginePipelineOptions(["--max_anomaly_drop_threshold=1.5"]).validate_options(is_test=True)

    def test_validation_enforces_input_source_in_non_test(self) -> None:
        """Verify missing both subscription and topic raises ValueError."""
        opts = RiskEnginePipelineOptions([])
        with pytest.raises(ValueError, match="Must specify either --input_subscription or --input_topic"):
            opts.validate_options(is_test=False)


# -----------------------------------------------------------------------------
# 2. DEAD-LETTER RECORD ENVELOPE TESTS
# -----------------------------------------------------------------------------
class TestDeadLetterRecord:
    """Validate DeadLetterRecord data model and conversion utilities."""

    def test_deadletter_creation_and_dict(self) -> None:
        """Verify field population and dictionary serialization."""
        dlq = DeadLetterRecord(
            payload="{'broken': true}",
            error_type="JSONDecodeError",
            error_message="Invalid JSON syntax",
            error_details={"line": 1, "col": 5},
            timestamp="2026-10-17T11:00:00Z",
            source_attributes={"env": "dev"},
            retryable=False,
            ordering_key="PORT-1#AAPL",
            original_message_id="msg-101",
        )
        as_dict = dlq.to_dict()
        assert as_dict["error_type"] == "JSONDecodeError"
        assert as_dict["ordering_key"] == "PORT-1#AAPL"
        assert as_dict["original_message_id"] == "msg-101"

    def test_deadletter_json_roundtrip(self) -> None:
        """Verify JSON serialization and deserialization roundtrip."""
        dlq = DeadLetterRecord(
            payload="malformed_data",
            error_type="ValidationError",
            error_message="Quantity must be strictly positive",
            error_details={"quantity": -10},
            timestamp="2026-10-17T11:00:00Z",
        )
        json_str = dlq.to_json()
        reconstructed = DeadLetterRecord.from_json(json_str)
        assert reconstructed.error_type == dlq.error_type
        assert reconstructed.payload == dlq.payload
        assert reconstructed.error_details == dlq.error_details

    def test_deadletter_to_pubsub_message(self) -> None:
        """Verify conversion into a PubsubMessage with diagnostic headers."""
        dlq = DeadLetterRecord(
            payload='{"bad": 1}',
            error_type="MissingFieldError",
            error_message="Missing ticker",
            error_details={"missing": ["ticker"]},
            timestamp="2026-10-17T11:00:00Z",
            source_attributes={"env": "prod", "trader": "T-100"},
            ordering_key="PORT-9#TSLA",
            original_message_id="pubsub-msg-55",
        )
        msg = dlq.to_pubsub_message()
        assert isinstance(msg, PubsubMessage)
        assert msg.ordering_key == "PORT-9#TSLA"
        assert msg.attributes["error_type"] == "MissingFieldError"
        assert msg.attributes["source"] == "dataflow-trade-decoder"
        assert msg.attributes["orig_env"] == "prod"
        assert msg.attributes["orig_trader"] == "T-100"
        assert msg.attributes["original_message_id"] == "pubsub-msg-55"


# -----------------------------------------------------------------------------
# 3. DO-FN UNIT TESTS (DIRECT DECODER INVOCATION)
# -----------------------------------------------------------------------------
class TestDecodeAndValidateTradeOrderFnUnit:
    """Validate DecodeAndValidateTradeOrderFn in isolation across valid & corrupt inputs."""

    @pytest.fixture
    def decoder(self) -> DecodeAndValidateTradeOrderFn:
        """Fixture providing a fresh decoder instance."""
        return DecodeAndValidateTradeOrderFn()

    @pytest.fixture
    def valid_order_dict(self) -> Dict[str, Any]:
        """Fixture returning a valid canonical trade order dictionary."""
        return {
            "order_id": "ORD-12345",
            "portfolio_id": "PORT-ALPHA",
            "ticker": "AAPL",
            "asset_class": "EQUITY",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": 100.0,
            "price": 182.50,
            "notional_usd": 18250.0,
            "timestamp": "2026-10-17T11:00:00+00:00",
            "trader_id": "ALGO-EXEC-01",
            "execution_status": "FILLED",
            "sequence_number": 42,
        }

    def test_valid_pubsub_message_decoding(
        self, decoder: DecodeAndValidateTradeOrderFn, valid_order_dict: Dict[str, Any]
    ) -> None:
        """Verify successful decoding of a PubsubMessage with attributes."""
        msg_bytes = json.dumps(valid_order_dict).encode("utf-8")
        msg = PubsubMessage(
            data=msg_bytes,
            attributes={"environment": "dev", "order_id": "ORD-12345"},
            ordering_key="PORT-ALPHA#AAPL",
            message_id="msg-999",
        )

        results = list(decoder.process(msg))
        assert len(results) == 1
        ts_val = results[0]
        assert isinstance(ts_val, window.TimestampedValue)
        order = ts_val.value
        assert isinstance(order, TradeOrder)
        assert order.order_id == "ORD-12345"
        assert order.ticker == "AAPL"
        assert order.side == "BUY"
        assert order.quantity == 100.0
        assert order.price == 182.50
        assert order.notional_usd == 18250.0
        assert ts_val.timestamp == datetime.fromisoformat("2026-10-17T11:00:00+00:00").timestamp()

    def test_valid_raw_bytes_decoding(
        self, decoder: DecodeAndValidateTradeOrderFn, valid_order_dict: Dict[str, Any]
    ) -> None:
        """Verify decoding from raw UTF-8 JSON bytes."""
        msg_bytes = json.dumps(valid_order_dict).encode("utf-8")
        results = list(decoder.process(msg_bytes))
        assert len(results) == 1
        order = results[0].value
        assert order.ticker == "AAPL"

    def test_valid_raw_string_and_dict_decoding(
        self, decoder: DecodeAndValidateTradeOrderFn, valid_order_dict: Dict[str, Any]
    ) -> None:
        """Verify decoding from str and dict representations."""
        # String
        results_str = list(decoder.process(json.dumps(valid_order_dict)))
        assert len(results_str) == 1
        assert results_str[0].value.order_id == "ORD-12345"

        # Dict
        results_dict = list(decoder.process(valid_order_dict))
        assert len(results_dict) == 1
        assert results_dict[0].value.order_id == "ORD-12345"

    def test_epoch_timestamp_support(
        self, decoder: DecodeAndValidateTradeOrderFn, valid_order_dict: Dict[str, Any]
    ) -> None:
        """Verify numeric epoch seconds and epoch milliseconds are correctly converted."""
        # Epoch seconds
        valid_order_dict["timestamp"] = 1776416400.0
        res_sec = list(decoder.process(valid_order_dict))
        assert len(res_sec) == 1
        assert res_sec[0].timestamp == 1776416400.0

        # Epoch milliseconds
        valid_order_dict["timestamp"] = 1776416400000
        res_ms = list(decoder.process(valid_order_dict))
        assert len(res_ms) == 1
        assert res_ms[0].timestamp == 1776416400.0

    def test_corrupt_non_utf8_bytes_routes_to_dlq(
        self, decoder: DecodeAndValidateTradeOrderFn
    ) -> None:
        """Verify invalid non-UTF-8 bytes trigger UnicodeDecodeError and route to DLQ."""
        bad_bytes = b"\x80\x81\xff\xfe\x00\x01\xfa"
        results = list(decoder.process(bad_bytes))
        assert len(results) == 1
        out = results[0]
        assert isinstance(out, pvalue.TaggedOutput)
        assert out.tag == TAG_DEADLETTER
        dlq_rec: DeadLetterRecord = out.value
        assert dlq_rec.error_type == "UnicodeDecodeError"
        assert dlq_rec.payload == bad_bytes.hex()

    def test_malformed_json_syntax_routes_to_dlq(
        self, decoder: DecodeAndValidateTradeOrderFn
    ) -> None:
        """Verify malformed JSON syntax triggers JSONDecodeError and routes to DLQ."""
        bad_json = b"{'order_id': 'ORD-1', ticker: AAPL, price: 100"  # unquoted & unclosed
        results = list(decoder.process(bad_json))
        assert len(results) == 1
        out = results[0]
        assert isinstance(out, pvalue.TaggedOutput)
        assert out.tag == TAG_DEADLETTER
        assert out.value.error_type == "JSONDecodeError"

    def test_json_root_not_dict_routes_to_dlq(
        self, decoder: DecodeAndValidateTradeOrderFn
    ) -> None:
        """Verify valid JSON with non-dict root triggers JSONDecodeError."""
        bad_json = b"[1, 2, 3, 4]"
        results = list(decoder.process(bad_json))
        assert len(results) == 1
        out = results[0]
        assert isinstance(out, pvalue.TaggedOutput)
        assert out.tag == TAG_DEADLETTER
        assert out.value.error_type == "JSONDecodeError"
        assert "Expected JSON object (dict)" in out.value.error_message

    def test_missing_mandatory_fields_routes_to_dlq(
        self, decoder: DecodeAndValidateTradeOrderFn, valid_order_dict: Dict[str, Any]
    ) -> None:
        """Verify omission of critical fields triggers MissingFieldError."""
        del valid_order_dict["ticker"]
        del valid_order_dict["price"]

        results = list(decoder.process(valid_order_dict))
        assert len(results) == 1
        out = results[0]
        assert isinstance(out, pvalue.TaggedOutput)
        assert out.tag == TAG_DEADLETTER
        dlq_rec = out.value
        assert dlq_rec.error_type == "MissingFieldError"
        assert "ticker" in dlq_rec.error_details["missing_fields"]
        assert "price" in dlq_rec.error_details["missing_fields"]

    @pytest.mark.parametrize(
        "field_name,bad_val,expected_err",
        [
            ("price", -50.0, "positive number"),
            ("price", 0.0, "positive number"),
            ("price", "not-a-number", "positive number"),
            ("quantity", -10.0, "positive number"),
            ("quantity", 0.0, "positive number"),
            ("quantity", "bad-qty", "positive number"),
            ("side", "HOLD", "Must be BUY or SELL"),
            ("side", "", "Must be BUY or SELL"),
            ("order_type", "CANCEL", "Must be MARKET, LIMIT"),
            ("ticker", "   ", "non-empty string"),
            ("portfolio_id", "", "non-empty string"),
            ("timestamp", "not-a-date", "Invalid ISO 8601"),
        ],
    )
    def test_domain_rule_violations_route_to_dlq(
        self,
        decoder: DecodeAndValidateTradeOrderFn,
        valid_order_dict: Dict[str, Any],
        field_name: str,
        bad_val: Any,
        expected_err: str,
    ) -> None:
        """Verify financial domain rule violations trigger ValidationError with details."""
        valid_order_dict[field_name] = bad_val
        results = list(decoder.process(valid_order_dict))
        assert len(results) == 1
        out = results[0]
        assert isinstance(out, pvalue.TaggedOutput)
        assert out.tag == TAG_DEADLETTER
        dlq_rec = out.value
        assert dlq_rec.error_type == "ValidationError"
        assert expected_err in dlq_rec.error_message

    def test_unsupported_input_type_routes_to_dlq(
        self, decoder: DecodeAndValidateTradeOrderFn
    ) -> None:
        """Verify feeding unexpected types routes to DLQ."""
        results = list(decoder.process(12345))
        assert len(results) == 1
        out = results[0]
        assert out.tag == TAG_DEADLETTER
        assert out.value.error_type == "UnsupportedInputTypeError"


# -----------------------------------------------------------------------------
# 4. DEAD-LETTER FORMATTER TESTS
# -----------------------------------------------------------------------------
class TestFormatDeadLetterForPubSubFn:
    """Validate formatting DeadLetterRecord objects into PubsubMessage instances."""

    def test_format_dead_letter_record(self) -> None:
        """Verify conversion to PubsubMessage."""
        formatter = FormatDeadLetterForPubSubFn()
        record = DeadLetterRecord(
            payload="bad-data",
            error_type="JSONDecodeError",
            error_message="Syntax error",
            error_details={},
            timestamp="2026-10-17T11:00:00Z",
            ordering_key="KEY-1",
        )
        msgs = list(formatter.process(record))
        assert len(msgs) == 1
        msg = msgs[0]
        assert isinstance(msg, PubsubMessage)
        assert msg.ordering_key == "KEY-1"
        assert msg.attributes["error_type"] == "JSONDecodeError"

    def test_format_dict_dead_letter(self) -> None:
        """Verify conversion from dictionary."""
        formatter = FormatDeadLetterForPubSubFn()
        data = {
            "payload": "raw-data",
            "error_type": "ValidationError",
            "error_message": "Negative quantity",
            "error_details": {},
            "timestamp": "2026-10-17T11:00:00Z",
        }
        msgs = list(formatter.process(data))
        assert len(msgs) == 1
        assert msgs[0].attributes["error_type"] == "ValidationError"


# -----------------------------------------------------------------------------
# 5. BEAM PIPELINE INTEGRATION TESTS (DIRECT RUNNER / TESTPIPELINE)
# -----------------------------------------------------------------------------
class TestBeamPipelineDirectRunner:
    """End-to-end hermetic testing of the Apache Beam pipeline DAG using TestPipeline."""

    def test_pipeline_processes_clean_orders(self) -> None:
        """Verify an entire batch of valid orders passes through to valid_orders."""
        valid_orders = [
            {
                "order_id": f"ORD-CLEAN-{i}",
                "portfolio_id": "PORT-GROWTH",
                "ticker": ticker,
                "asset_class": "EQUITY",
                "side": "BUY",
                "order_type": "LIMIT",
                "quantity": 50.0 * (i + 1),
                "price": 100.0 + i,
                "notional_usd": round(50.0 * (i + 1) * (100.0 + i), 2),
                "timestamp": f"2026-10-17T11:0{i}:00Z",
            }
            for i, ticker in enumerate(["AAPL", "MSFT", "NVDA", "GOOGL"])
        ]
        input_elements = [json.dumps(o).encode("utf-8") for o in valid_orders]

        with BeamTestPipeline() as p:
            source = p | "CreateInput" >> beam.Create(input_elements)
            outputs = source | "DecodeAndValidate" >> DecodeAndValidateOrders()

            # Extract order IDs for deterministic assertion
            order_ids = outputs[TAG_VALID_ORDERS] | "ExtractIDs" >> beam.Map(lambda o: o.order_id)
            deadletter_count = outputs[TAG_DEADLETTER] | "CountDLQ" >> beam.combiners.Count.Globally()

            assert_that(
                order_ids,
                equal_to(["ORD-CLEAN-0", "ORD-CLEAN-1", "ORD-CLEAN-2", "ORD-CLEAN-3"]),
                label="CheckValidOrderIDs",
            )
            assert_that(deadletter_count, equal_to([0]), label="CheckZeroDLQ")

    def test_pipeline_mixed_stream_with_dlq_routing(self) -> None:
        """Verify mixed stream properly branches valid orders and deadletter records."""
        good_order_1 = json.dumps({
            "order_id": "ORD-GOOD-1",
            "portfolio_id": "PORT-1",
            "ticker": "AAPL",
            "asset_class": "EQUITY",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": 10.0,
            "price": 180.0,
            "timestamp": "2026-10-17T11:00:00Z",
        }).encode("utf-8")

        good_order_2 = json.dumps({
            "order_id": "ORD-GOOD-2",
            "portfolio_id": "PORT-2",
            "ticker": "BTCUSD",
            "asset_class": "CRYPTO",
            "side": "SELL",
            "order_type": "MARKET",
            "quantity": 1.5,
            "price": 65000.0,
            "timestamp": "2026-10-17T11:01:00Z",
        }).encode("utf-8")

        # Corrupted elements
        corrupt_utf8 = b"\xff\xfe\x00\x12\xab\xcd"
        malformed_json = b"{'order_id': 'BAD', ticker: missing-quotes"
        missing_fields = json.dumps({"order_id": "BAD-2", "quantity": 10}).encode("utf-8")
        negative_price = json.dumps({
            "order_id": "BAD-3",
            "portfolio_id": "PORT-1",
            "ticker": "TSLA",
            "asset_class": "EQUITY",
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": 10.0,
            "price": -150.0,
            "timestamp": "2026-10-17T11:00:00Z",
        }).encode("utf-8")
        invalid_side = json.dumps({
            "order_id": "BAD-4",
            "portfolio_id": "PORT-1",
            "ticker": "TSLA",
            "asset_class": "EQUITY",
            "side": "HOLD",
            "order_type": "MARKET",
            "quantity": 10.0,
            "price": 150.0,
            "timestamp": "2026-10-17T11:00:00Z",
        }).encode("utf-8")

        stream = [
            good_order_1,
            corrupt_utf8,
            good_order_2,
            malformed_json,
            missing_fields,
            negative_price,
            invalid_side,
        ]

        with BeamTestPipeline() as p:
            source = p | "CreateMixedStream" >> beam.Create(stream)
            outputs = source | "DecodeAndValidate" >> DecodeAndValidateOrders()

            good_tickers = outputs[TAG_VALID_ORDERS] | "MapTickers" >> beam.Map(lambda o: o.ticker)
            dlq_errors = outputs[TAG_DEADLETTER] | "MapErrors" >> beam.Map(lambda d: d.error_type)

            assert_that(good_tickers, equal_to(["AAPL", "BTCUSD"]), label="CheckValidTickers")
            assert_that(
                dlq_errors,
                equal_to([
                    "UnicodeDecodeError",
                    "JSONDecodeError",
                    "MissingFieldError",
                    "ValidationError",
                    "ValidationError",
                ]),
                label="CheckDLQErrors",
            )

    def test_pipeline_preserves_pubsub_attributes_in_dlq(self) -> None:
        """Verify PubsubMessage source attributes are retained in dead-letter records."""
        bad_msg = PubsubMessage(
            data=b"invalid-json-body",
            attributes={"client_id": "FIX-ENGINE-01", "trace_id": "trace-xyz"},
            ordering_key="KEY-PORT-1",
        )

        with BeamTestPipeline() as p:
            source = p | "CreatePubsubSource" >> beam.Create([bad_msg])
            outputs = source | "DecodeAndValidate" >> DecodeAndValidateOrders()

            dlq_record = outputs[TAG_DEADLETTER] | "ExtractRecord" >> beam.Map(lambda r: (
                r.error_type,
                r.ordering_key,
                r.source_attributes.get("client_id"),
                r.source_attributes.get("trace_id"),
            ))

            assert_that(
                dlq_record,
                equal_to([("JSONDecodeError", "KEY-PORT-1", "FIX-ENGINE-01", "trace-xyz")]),
                label="CheckPreservedAttributes",
            )


# -----------------------------------------------------------------------------
# 6. PIPELINE BUILDER & DAG TOPOLOGY TESTS
# -----------------------------------------------------------------------------
class TestPipelineDAGConstruction:
    """Validate Dataflow pipeline DAG construction across configuration variants."""

    def test_build_pipeline_with_test_input_pcollection(self) -> None:
        """Verify build_pipeline integrates seamlessly with TestPipeline."""
        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--deadletter_topic=projects/trading-risk-dev/topics/top-dlq",
        ])

        with BeamTestPipeline() as p:
            input_pcoll = p | "CreateTestInput" >> beam.Create([
                json.dumps({
                    "order_id": "ORD-DAG-01",
                    "portfolio_id": "PORT-DAG",
                    "ticker": "MSFT",
                    "asset_class": "EQUITY",
                    "side": "BUY",
                    "order_type": "MARKET",
                    "quantity": 25.0,
                    "price": 420.0,
                    "timestamp": "2026-10-17T11:00:00Z",
                }).encode("utf-8")
            ])

            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=input_pcoll)
            assert isinstance(bundle, DataflowPipelineBundle)
            assert bundle.valid_orders is not None
            assert bundle.deadletter_records is not None
            assert bundle.dlq_messages is not None

            order_count = bundle.valid_orders | "CountOrders" >> beam.combiners.Count.Globally()
            assert_that(order_count, equal_to([1]))

    def test_build_pipeline_without_deadletter_sink(self) -> None:
        """Verify DLQ sink transform is skipped when enable_deadletter_sink is False."""
        opts = RiskEnginePipelineOptions([
            "--environment=dev",
            "--deadletter_topic=projects/trading-risk-dev/topics/top-dlq",
        ])
        opts.enable_deadletter_sink = False

        with BeamTestPipeline() as p:
            input_pcoll = p | "CreateInput" >> beam.Create([b"{}"])
            bundle = build_pipeline(opts, pipeline=p, test_input_pcoll=input_pcoll)
            assert bundle.dlq_messages is None

    def test_build_pipeline_missing_source_raises_error(self) -> None:
        """Verify build_pipeline raises ValueError when no input source is specified."""
        opts = RiskEnginePipelineOptions(["--environment=dev"])
        # No test input and no subscription/topic
        with pytest.raises(ValueError, match="Streaming pipeline requires either --input_subscription or --input_topic"):
            with BeamTestPipeline() as p:
                build_pipeline(opts, pipeline=p)


# -----------------------------------------------------------------------------
# 7. INTERACTIVE VISUAL DASHBOARD SERVER TESTS
# -----------------------------------------------------------------------------
class TestStreamingDashboardServer:
    """Validate the local interactive visual dashboard state engine and HTTP endpoints."""

    def test_dashboard_engine_state_step_and_fault_injection(self) -> None:
        """Verify DashboardEngineState drives MarketFeed, OrderFeed, and Beam DoFn."""
        from streaming.dashboard_server import DashboardEngineState

        engine = DashboardEngineState(environment="dev", seed=101)
        initial = engine.get_snapshot()
        assert initial["metrics"]["valid_orders"] == 5
        assert initial["metrics"]["dlq_total"] == 2

        # Step simulation
        after_step = engine.step(tick_count=4)
        assert after_step["metrics"]["total_ticks"] == 4
        assert after_step["metrics"]["valid_orders"] == 6

        # Inject faults across all error categories
        for fault_type, expected_err in [
            ("unicode", "UnicodeDecodeError"),
            ("json", "JSONDecodeError"),
            ("missing", "MissingFieldError"),
            ("negative_price", "ValidationError"),
            ("invalid_side", "ValidationError"),
        ]:
            out = engine.inject_event(fault_type)
            assert out["result"]["ok"] is False
            assert out["result"]["dlq"]["error_type"] == expected_err

        # Custom valid payload
        custom_good = json.dumps({
            "order_id": "ORD-DASH-99",
            "portfolio_id": "PORT-DASH",
            "ticker": "NVDA",
            "asset_class": "EQUITY",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": 10.0,
            "price": 140.0,
        })
        custom_out = engine.inject_event("custom", custom_payload=custom_good)
        assert custom_out["result"]["ok"] is True
        assert custom_out["result"]["order"]["order_id"] == "ORD-DASH-99"

        # Reset counters
        reset_snap = engine.reset()
        assert reset_snap["metrics"]["valid_orders"] == 0
        assert reset_snap["metrics"]["dlq_total"] == 0

    def test_dashboard_http_server_endpoints(self) -> None:
        """Verify HTTP GET/POST endpoints on StreamingDashboardServer."""
        import urllib.request
        from streaming.dashboard_server import StreamingDashboardServer

        server = StreamingDashboardServer(host="127.0.0.1", port=0, environment="dev")
        port = server.start()
        base_url = f"http://127.0.0.1:{port}"

        try:
            # 1. GET / (HTML UI)
            with urllib.request.urlopen(f"{base_url}/", timeout=5) as resp:
                assert resp.status == 200
                html = resp.read().decode("utf-8")
                assert "trading-risk-engine" in html

            # 2. GET /health
            with urllib.request.urlopen(f"{base_url}/health", timeout=5) as resp:
                assert resp.status == 200
                health = json.loads(resp.read().decode("utf-8"))
                assert health["status"] == "healthy"

            # 3. POST /api/step
            req_step = urllib.request.Request(f"{base_url}/api/step", data=b"{}", method="POST")
            with urllib.request.urlopen(req_step, timeout=5) as resp:
                assert resp.status == 200
                state = json.loads(resp.read().decode("utf-8"))
                assert state["metrics"]["total_ticks"] >= 3

            # 4. POST /api/inject
            inject_body = json.dumps({"type": "json"}).encode("utf-8")
            req_inject = urllib.request.Request(
                f"{base_url}/api/inject",
                data=inject_body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req_inject, timeout=5) as resp:
                assert resp.status == 200
                injected = json.loads(resp.read().decode("utf-8"))
                assert injected["result"]["ok"] is False
                assert injected["result"]["dlq"]["error_type"] == "JSONDecodeError"
        finally:
            server.stop()

