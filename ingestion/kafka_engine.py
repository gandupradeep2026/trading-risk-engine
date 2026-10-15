"""Trading Risk Engine - Kafka Tick Streaming Engine Module Entrypoint.

Exports and runs the multi-threaded Kafka market tick streaming engine.
"""

from ingestion.streaming_engine import (
    EngineTelemetry,
    MultiThreadedTickEngine,
    StreamingEngineConfig,
    StreamingMetricsHandler,
    main,
)

__all__ = [
    "MultiThreadedTickEngine",
    "StreamingEngineConfig",
    "EngineTelemetry",
    "StreamingMetricsHandler",
    "main",
]

if __name__ == "__main__":
    main()
