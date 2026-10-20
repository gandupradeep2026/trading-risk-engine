"""Testing and Simulation Utilities for Cloud Dataflow Streaming Pipelines.

Provides mock event generators, synthetic corruption fixtures, fluent TestStream builders,
custom Beam assert matchers, and scenario presets for financial risk engine testing.
"""

from streaming.dataflow.testing.matchers import (
    contains_subset,
    elements_satisfy,
    has_count,
    matches_set,
)
from streaming.dataflow.testing.mock_events import (
    CorruptionType,
    create_corrupt_message,
    create_fat_finger_event,
    create_flash_crash_event_sequence,
    create_mock_order_bytes,
    create_mock_order_dict,
    create_mock_pubsub_message,
    create_multi_asset_portfolio_stream,
    create_notional_breach_event_sequence,
    create_sell_pressure_event_sequence,
)
from streaming.dataflow.testing.test_stream_builder import (
    StreamScenarioBuilder,
    build_dlq_corruption_scenario,
    build_flash_crash_scenario,
    build_happy_path_scenario,
    build_multi_portfolio_scenario,
)

__all__ = [
    "CorruptionType",
    "create_mock_order_dict",
    "create_mock_order_bytes",
    "create_mock_pubsub_message",
    "create_corrupt_message",
    "create_flash_crash_event_sequence",
    "create_fat_finger_event",
    "create_sell_pressure_event_sequence",
    "create_notional_breach_event_sequence",
    "create_multi_asset_portfolio_stream",
    "StreamScenarioBuilder",
    "build_happy_path_scenario",
    "build_flash_crash_scenario",
    "build_dlq_corruption_scenario",
    "build_multi_portfolio_scenario",
    "matches_set",
    "has_count",
    "contains_subset",
    "elements_satisfy",
]
