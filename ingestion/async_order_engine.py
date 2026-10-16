"""Production Asynchronous Trade Order Streaming Engine Daemon.

Coordinates:
- OrderFeedGenerator generating multi-asset order flow.
- Bounded asyncio.Queue decoupling order ingestion from async network publishing.
- Pool of async worker coroutines publishing to Cloud Pub/Sub via AsyncTradeOrderProducer.
- Dynamic backpressure management preventing queue saturation or memory exhaustion.
- Embedded HTTP server exporting /health and /metrics (Prometheus).
- Clean graceful shutdown handling for Systemd and Docker container lifecycles.
"""

import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional
import urllib.parse

from ingestion.order_feed import OrderFeedGenerator
from ingestion.pubsub.async_producer import (
    AsyncProducerConfig,
    AsyncTradeOrderProducer,
    OrderValidationError,
    PubSubPublishError,
)
from ingestion.pubsub_client import TradeOrder

logger = logging.getLogger("async_order_engine")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


@dataclass
class EngineConfig:
    """Configuration parameters for the asynchronous streaming engine."""

    rate_orders_per_sec: float = 25.0
    worker_count: int = 4
    queue_capacity: int = 500
    metrics_port: int = 8081
    metrics_host: str = "0.0.0.0"
    producer_config: AsyncProducerConfig = field(default_factory=AsyncProducerConfig)

    @classmethod
    def from_env(cls) -> "EngineConfig":
        """Instantiate engine configuration from environment variables."""
        return cls(
            rate_orders_per_sec=float(os.getenv("ORDER_RATE_PER_SEC", "25.0")),
            worker_count=int(os.getenv("WORKER_COUNT", "4")),
            queue_capacity=int(os.getenv("QUEUE_CAPACITY", "500")),
            metrics_port=int(os.getenv("METRICS_PORT", "8081")),
            metrics_host=os.getenv("METRICS_HOST", "0.0.0.0"),
            producer_config=AsyncProducerConfig.from_env(),
        )


class EngineMetricsHTTPHandler(BaseHTTPRequestHandler):
    """Lightweight HTTP handler serving /health and /metrics endpoints."""

    engine_instance: Optional["AsyncOrderEngine"] = None

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default HTTP stderr logs."""
        return

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if self.engine_instance is None:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b'{"error": "engine not initialized"}\n')
            return

        producer_snapshot = self.engine_instance.producer.telemetry.get_snapshot()
        queue_size = self.engine_instance.order_queue.qsize()

        if path in ("/health", "/healthz"):
            is_healthy = self.engine_instance.is_running and producer_snapshot["orders_failed"] < 1000
            status_code = 200 if is_healthy else 503
            body = json.dumps(
                {
                    "status": "UP" if is_healthy else "DOWN",
                    "running": self.engine_instance.is_running,
                    "queue_size": queue_size,
                    "queue_capacity": self.engine_instance.config.queue_capacity,
                    "producer": producer_snapshot,
                },
                indent=2,
            ).encode("utf-8")

            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif path == "/metrics":
            prom_text = self.engine_instance.producer.telemetry.format_prometheus()
            extra_lines = [
                "# HELP trading_queue_size_current Current items buffered in memory queue",
                "# TYPE trading_queue_size_current gauge",
                f"trading_queue_size_current {queue_size}",
                "# HELP trading_engine_workers_total Total concurrent publisher workers",
                "# TYPE trading_engine_workers_total gauge",
                f"trading_engine_workers_total {self.engine_instance.config.worker_count}",
            ]
            full_metrics = (prom_text + "\n".join(extra_lines) + "\n").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(full_metrics)))
            self.end_headers()
            self.wfile.write(full_metrics)

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "endpoint not found"}\n')


class AsyncOrderEngine:
    """Asynchronous high-frequency order streaming engine."""

    def __init__(
        self,
        config: Optional[EngineConfig] = None,
        producer: Optional[AsyncTradeOrderProducer] = None,
        feed: Optional[OrderFeedGenerator] = None,
    ) -> None:
        self.config = config or EngineConfig.from_env()
        self.producer = producer or AsyncTradeOrderProducer(config=self.config.producer_config)
        self.feed = feed or OrderFeedGenerator()
        self.order_queue: asyncio.Queue[Optional[TradeOrder]] = asyncio.Queue(
            maxsize=self.config.queue_capacity
        )
        self.is_running: bool = False
        self._stop_event = asyncio.Event()
        self._generator_task: Optional[asyncio.Task] = None
        self._worker_tasks: List[asyncio.Task] = []
        self._http_server: Optional[HTTPServer] = None
        self._http_thread: Optional[threading.Thread] = None

    def start_http_server(self) -> None:
        """Start the embedded health and metrics server in a daemon thread."""
        handler_class = EngineMetricsHTTPHandler
        handler_class.engine_instance = self

        server_address = (self.config.metrics_host, self.config.metrics_port)
        self._http_server = HTTPServer(server_address, handler_class)

        def serve():
            logger.info(
                f"Engine HTTP health & metrics listening on http://{self.config.metrics_host}:{self.config.metrics_port}"
            )
            try:
                if self._http_server:
                    self._http_server.serve_forever()
            except Exception as ex:
                logger.debug(f"HTTP server closed: {ex}")

        self._http_thread = threading.Thread(target=serve, daemon=True, name="EngineHTTPServer")
        self._http_thread.start()

    def stop_http_server(self) -> None:
        """Stop HTTP server cleanly."""
        if self._http_server:
            try:
                self._http_server.shutdown()
                self._http_server.server_close()
            except Exception as ex:
                logger.debug(f"Error stopping HTTP server: {ex}")
            self._http_server = None

    async def _order_generator_loop(self) -> None:
        """Continuously generate trade orders and push to bounded async queue."""
        interval = 1.0 / max(0.1, self.config.rate_orders_per_sec)
        logger.info(
            f"Order generator started: target rate {self.config.rate_orders_per_sec} orders/sec (interval={interval:.4f}s)"
        )

        while not self._stop_event.is_set():
            try:
                order = self.feed.generate_order()
                # Put with backpressure: blocks if queue is full
                await self.order_queue.put(order)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as ex:
                logger.error(f"Error in order generator loop: {ex}")
                await asyncio.sleep(0.1)

        logger.info("Order generator loop terminated.")

    async def _publisher_worker_loop(self, worker_id: int) -> None:
        """Pull orders from queue and publish to Cloud Pub/Sub asynchronously."""
        logger.debug(f"Publisher worker-{worker_id} started.")

        while True:
            try:
                order = await self.order_queue.get()
                if order is None:  # Poison pill
                    self.order_queue.task_done()
                    break

                try:
                    await self.producer.publish(order)
                except (OrderValidationError, PubSubPublishError) as pub_err:
                    logger.warning(f"Worker-{worker_id} publish handled error: {pub_err}")
                except Exception as ex:
                    logger.error(f"Worker-{worker_id} unexpected publish exception: {ex}")
                finally:
                    self.order_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as ex:
                logger.error(f"Worker-{worker_id} error: {ex}")

        logger.debug(f"Publisher worker-{worker_id} stopped.")

    async def start(self) -> None:
        """Start the asynchronous streaming engine."""
        self.is_running = True
        self._stop_event.clear()
        self.start_http_server()

        # Spawn publisher worker fleet
        self._worker_tasks = [
            asyncio.create_task(self._publisher_worker_loop(i + 1), name=f"Worker-{i+1}")
            for i in range(self.config.worker_count)
        ]

        # Spawn generator task
        self._generator_task = asyncio.create_task(
            self._order_generator_loop(), name="OrderGenerator"
        )

        logger.info(
            f"AsyncOrderEngine initialized with {self.config.worker_count} workers, "
            f"queue capacity {self.config.queue_capacity}, target {self.config.rate_orders_per_sec} ops/s"
        )

    async def stop(self) -> None:
        """Gracefully stop the streaming engine with complete buffer drainage."""
        if not self.is_running:
            return

        logger.info("Initiating graceful shutdown of AsyncOrderEngine...")
        self.is_running = False
        self._stop_event.set()

        # 1. Cancel and await generator task
        if self._generator_task and not self._generator_task.done():
            self._generator_task.cancel()
            try:
                await self._generator_task
            except asyncio.CancelledError:
                pass

        # 2. Inject poison pills for worker tasks
        for _ in range(self.config.worker_count):
            await self.order_queue.put(None)

        # 3. Wait for all queued orders to be processed
        try:
            await asyncio.wait_for(self.order_queue.join(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Timed out waiting for queue to drain; terminating remaining workers.")

        # 4. Wait for worker tasks to finish
        for task in self._worker_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)

        # 5. Flush and close producer
        await self.producer.aclose()

        # 6. Stop HTTP server
        self.stop_http_server()

        logger.info("AsyncOrderEngine gracefully terminated.")

    def setup_signals(self) -> None:
        """Attach POSIX signal handlers to trigger graceful stop."""
        loop = asyncio.get_event_loop()

        def _handle_signal():
            logger.info("Received termination signal, scheduling engine stop...")
            asyncio.create_task(self.stop())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle_signal)
            except NotImplementedError:
                # Windows event loop doesn't support add_signal_handler
                signal.signal(sig, lambda s, f: asyncio.run_coroutine_threadsafe(self.stop(), loop))


async def run_engine_async(duration_seconds: Optional[float] = None) -> None:
    """Run the engine asynchronously for an optional fixed duration or until interrupted."""
    config = EngineConfig.from_env()
    engine = AsyncOrderEngine(config=config)
    engine.setup_signals()
    await engine.start()

    try:
        if duration_seconds is not None:
            await asyncio.sleep(duration_seconds)
            await engine.stop()
        else:
            while engine.is_running:
                await asyncio.sleep(1.0)
    finally:
        if engine.is_running:
            await engine.stop()


def main() -> None:
    """CLI entrypoint for running the asynchronous order producer engine."""
    parser = argparse.ArgumentParser(description="Trading Risk Engine - Async Order Producer")
    parser.add_argument("--rate", type=float, default=25.0, help="Target orders per second")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent publisher workers")
    parser.add_argument("--port", type=int, default=8081, help="HTTP metrics and health check port")
    parser.add_argument("--duration", type=float, default=None, help="Run duration in seconds (optional)")
    parser.add_argument("--sim", action="store_true", help="Force simulation mode")
    args = parser.parse_args()

    os.environ["ORDER_RATE_PER_SEC"] = str(args.rate)
    os.environ["WORKER_COUNT"] = str(args.workers)
    os.environ["METRICS_PORT"] = str(args.port)
    if args.sim:
        os.environ["SIMULATION_MODE"] = "true"

    try:
        asyncio.run(run_engine_async(duration_seconds=args.duration))
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")


if __name__ == "__main__":
    main()
