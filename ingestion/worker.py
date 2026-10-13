"""Trading Risk Engine - Production Ingestion Worker Daemon.

Provides:
- High-throughput market data and trade order ingestion worker.
- Resilient publishing to Cloud Pub/Sub with retry policies and exponential backoff.
- Zero-cost simulation mode and local emulator integration.
- Embedded lightweight HTTP health check and Prometheus-compatible metrics server.
- Graceful shutdown handling for Systemd and Docker container lifecycles.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse

from ingestion.pubsub_client import LocalPubSubMock, TradeOrder

# Configure structured logging
logger = logging.getLogger("trading_ingestion_worker")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


@dataclass
class WorkerConfig:
    """Runtime configuration parameters for the ingestion worker."""

    project_id: str = "trading-risk-dev-001"
    topic_id: str = "projects/trading-risk-dev-001/topics/top-trade-orders"
    tickers: List[str] = field(
        default_factory=lambda: ["AAPL", "GOOGL", "MSFT", "AMZN", "NVDA", "TSLA", "JPM", "GS"]
    )
    metrics_port: int = 8080
    metrics_host: str = "0.0.0.0"
    publish_interval_seconds: float = 1.0
    simulation_mode: bool = True
    emulator_host: Optional[str] = None
    environment: str = "dev"
    max_retries: int = 3
    initial_backoff_sec: float = 0.5
    backoff_multiplier: float = 2.0

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        """Instantiate configuration from environment variables."""
        tickers_raw = os.getenv("MARKET_FEED_TICKERS", "AAPL,GOOGL,MSFT,AMZN,NVDA,TSLA,JPM,GS")
        tickers_list = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]

        return cls(
            project_id=os.getenv("PROJECT_ID", "trading-risk-dev-001"),
            topic_id=os.getenv(
                "PUBSUB_TOPIC_ID",
                "projects/trading-risk-dev-001/topics/top-trade-orders",
            ),
            tickers=tickers_list,
            metrics_port=int(os.getenv("METRICS_PORT", "8080")),
            metrics_host=os.getenv("METRICS_HOST", "0.0.0.0"),
            publish_interval_seconds=float(os.getenv("PUBLISH_INTERVAL_SECONDS", "1.0")),
            simulation_mode=os.getenv("SIMULATION_MODE", "true").lower() in ("true", "1", "yes"),
            emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
            environment=os.getenv("ENVIRONMENT", "dev"),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            initial_backoff_sec=float(os.getenv("INITIAL_BACKOFF_SEC", "0.5")),
            backoff_multiplier=float(os.getenv("BACKOFF_MULTIPLIER", "2.0")),
        )


class WorkerMetrics:
    """Thread-safe telemetry metrics store for ingestion worker."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.start_time: float = time.time()
        self.orders_published: int = 0
        self.notional_usd_total: float = 0.0
        self.errors_count: int = 0
        self.last_order_id: Optional[str] = None
        self.last_published_timestamp: Optional[str] = None
        self.status: str = "INITIALIZING"

    def record_success(self, order_id: str, notional_usd: float) -> None:
        """Record successful message publication."""
        with self._lock:
            self.orders_published += 1
            self.notional_usd_total = round(self.notional_usd_total + notional_usd, 2)
            self.last_order_id = order_id
            self.last_published_timestamp = datetime.now(timezone.utc).isoformat()

    def record_error(self) -> None:
        """Record publishing failure or error condition."""
        with self._lock:
            self.errors_count += 1

    def set_status(self, status: str) -> None:
        """Set current lifecycle status."""
        with self._lock:
            self.status = status

    def get_snapshot(self) -> Dict[str, Any]:
        """Obtain consistent snapshot of current metrics."""
        with self._lock:
            uptime = max(0.0, time.time() - self.start_time)
            return {
                "status": self.status,
                "uptime_seconds": round(uptime, 2),
                "orders_published": self.orders_published,
                "notional_usd_total": round(self.notional_usd_total, 2),
                "errors_count": self.errors_count,
                "last_order_id": self.last_order_id,
                "last_published_timestamp": self.last_published_timestamp,
                "healthy": self.status in ("RUNNING", "INITIALIZING"),
            }


class MetricsHTTPRequestHandler(BaseHTTPRequestHandler):
    """Lightweight HTTP request handler serving /health and /metrics."""

    worker_instance: Optional["IngestionWorker"] = None

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default HTTP server stderr output."""
        return

    def do_GET(self) -> None:
        """Handle incoming GET requests."""
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if self.worker_instance is None:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b'{"error": "worker not initialized"}\n')
            return

        snapshot = self.worker_instance.metrics.get_snapshot()

        if path in ("/health", "/healthz"):
            status_code = 200 if snapshot["healthy"] else 503
            body = json.dumps(snapshot, indent=2).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif path == "/metrics":
            # Prometheus formatted metrics
            lines = [
                "# HELP trading_ingestion_orders_published_total Total trade orders published",
                "# TYPE trading_ingestion_orders_published_total counter",
                f"trading_ingestion_orders_published_total {snapshot['orders_published']}",
                "# HELP trading_ingestion_notional_usd_total Cumulative USD notional published",
                "# TYPE trading_ingestion_notional_usd_total counter",
                f"trading_ingestion_notional_usd_total {snapshot['notional_usd_total']:.2f}",
                "# HELP trading_ingestion_publish_errors_total Total errors encountered",
                "# TYPE trading_ingestion_publish_errors_total counter",
                f"trading_ingestion_publish_errors_total {snapshot['errors_count']}",
                "# HELP trading_ingestion_uptime_seconds Ingestion worker uptime in seconds",
                "# TYPE trading_ingestion_uptime_seconds gauge",
                f"trading_ingestion_uptime_seconds {snapshot['uptime_seconds']:.2f}",
                "# HELP trading_ingestion_healthy Health indicator (1=UP, 0=DOWN)",
                "# TYPE trading_ingestion_healthy gauge",
                f"trading_ingestion_healthy {1 if snapshot['healthy'] else 0}",
            ]
            body = ("\n".join(lines) + "\n").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "not found"}\n')


class IngestionWorker:
    """Production Ingestion Worker orchestrating tick generation and Pub/Sub publishing."""

    def __init__(
        self,
        config: Optional[WorkerConfig] = None,
        publisher: Any = None,
    ) -> None:
        self.config = config or WorkerConfig.from_env()
        self.metrics = WorkerMetrics()
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._http_server: Optional[HTTPServer] = None
        self._http_thread: Optional[threading.Thread] = None

        # Setup publisher client
        if publisher is not None:
            self.publisher = publisher
        elif self.config.simulation_mode or self.config.emulator_host:
            # Use LocalPubSubMock for zero-cost testing
            mock = LocalPubSubMock()
            mock.create_topic(self.config.topic_id)
            self.publisher = mock
        else:
            try:
                from google.cloud import pubsub_v1  # type: ignore

                self.publisher = pubsub_v1.PublisherClient()
            except ImportError:
                logger.warning(
                    "google-cloud-pubsub not available, falling back to LocalPubSubMock"
                )
                mock = LocalPubSubMock()
                mock.create_topic(self.config.topic_id)
                self.publisher = mock

        self.portfolio_ids = ["PORT-EQUITY-01", "PORT-MACRO-02", "PORT-TECH-03", "PORT-QUANT-04"]
        self.asset_classes = ["EQUITY", "ETF", "CRYPTO", "FX"]

    def generate_synthetic_order(self) -> TradeOrder:
        """Generate a realistic trade order execution."""
        ticker = random.choice(self.config.tickers)
        portfolio_id = random.choice(self.portfolio_ids)
        asset_class = "EQUITY"
        side = random.choice(["BUY", "SELL"])
        order_type = random.choice(["MARKET", "LIMIT"])
        quantity = round(random.uniform(5.0, 500.0), 2)
        price = round(random.uniform(25.0, 450.0), 2)

        return TradeOrder.create(
            portfolio_id=portfolio_id,
            ticker=ticker,
            asset_class=asset_class,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
        )

    def publish_order(self, order: TradeOrder) -> str:
        """Publish a trade order with retry logic and exponential backoff.

        Raises:
            RuntimeError: If publication fails after exceeding max_retries.
        """
        payload = order.to_json().encode("utf-8")
        attributes = {
            "ticker": order.ticker,
            "side": order.side,
            "portfolio_id": order.portfolio_id,
            "environment": self.config.environment,
        }

        last_error: Optional[Exception] = None
        backoff = self.config.initial_backoff_sec

        for attempt in range(1, self.config.max_retries + 1):
            try:
                # Handle both LocalPubSubMock and google-cloud-pubsub PublisherClient
                if hasattr(self.publisher, "publish"):
                    res = self.publisher.publish(
                        self.config.topic_id, data=payload, attributes=attributes
                    )
                    # Live client returns a Future with .result()
                    if hasattr(res, "result"):
                        msg_id = str(res.result(timeout=10.0))
                    else:
                        msg_id = str(res)
                else:
                    raise AttributeError("Publisher instance has no publish method.")

                self.metrics.record_success(order.order_id, order.notional_usd)
                return msg_id

            except Exception as ex:
                last_error = ex
                logger.warning(
                    f"Publish attempt {attempt}/{self.config.max_retries} failed: {ex}. Retrying in {backoff:.2f}s..."
                )
                if attempt < self.config.max_retries:
                    time.sleep(backoff)
                    backoff *= self.config.backoff_multiplier

        self.metrics.record_error()
        raise RuntimeError(
            f"Failed to publish order {order.order_id} after {self.config.max_retries} attempts: {last_error}"
        ) from last_error

    def run_cycle(self) -> Optional[TradeOrder]:
        """Execute a single ingestion-publish cycle."""
        try:
            order = self.generate_synthetic_order()
            self.publish_order(order)
            return order
        except Exception as ex:
            logger.error(f"Error in run_cycle: {ex}")
            return None

    def start_http_server(self) -> None:
        """Start the embedded health and metrics server in a daemon thread."""
        handler_class = MetricsHTTPRequestHandler
        handler_class.worker_instance = self

        server_address = (self.config.metrics_host, self.config.metrics_port)
        self._http_server = HTTPServer(server_address, handler_class)

        def serve():
            logger.info(
                f"Metrics and health HTTP server listening on http://{self.config.metrics_host}:{self.config.metrics_port}"
            )
            try:
                if self._http_server:
                    self._http_server.serve_forever()
            except Exception as ex:
                logger.debug(f"HTTP server terminated: {ex}")

        self._http_thread = threading.Thread(target=serve, daemon=True, name="MetricsHTTPServer")
        self._http_thread.start()

    def stop_http_server(self) -> None:
        """Shutdown the embedded HTTP server."""
        if self._http_server:
            try:
                self._http_server.shutdown()
                self._http_server.server_close()
            except Exception as ex:
                logger.debug(f"Error closing HTTP server: {ex}")
            self._http_server = None

    def _worker_loop(self) -> None:
        """Continuous execution loop."""
        self.metrics.set_status("RUNNING")
        logger.info(
            f"Ingestion worker running. Publishing every {self.config.publish_interval_seconds}s to {self.config.topic_id}"
        )

        while not self._stop_event.is_set():
            self.run_cycle()
            self._stop_event.wait(timeout=self.config.publish_interval_seconds)

        self.metrics.set_status("STOPPED")
        logger.info("Ingestion worker execution loop finished.")

    def start(self, run_in_background: bool = True) -> None:
        """Start the ingestion worker and telemetry server."""
        self.start_http_server()
        self._stop_event.clear()

        if run_in_background:
            self._worker_thread = threading.Thread(
                target=self._worker_loop, daemon=True, name="IngestionWorkerLoop"
            )
            self._worker_thread.start()
        else:
            self._worker_loop()

    def stop(self) -> None:
        """Gracefully stop the worker and release resources."""
        self.metrics.set_status("STOPPING")
        logger.info("Stopping ingestion worker daemon...")
        self._stop_event.set()

        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=5.0)

        self.stop_http_server()
        self.metrics.set_status("STOPPED")
        logger.info("Ingestion worker daemon stopped cleanly.")

    def setup_signals(self) -> None:
        """Register POSIX signal handlers for graceful shutdown."""

        def _signal_handler(signum: int, frame: Any) -> None:
            signame = signal.Signals(signum).name
            logger.info(f"Received signal {signame} ({signum}), initiating graceful shutdown...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)


def main() -> None:
    """CLI entry point for running the containerized ingestion worker."""
    config = WorkerConfig.from_env()
    worker = IngestionWorker(config=config)
    worker.setup_signals()

    logger.info(
        f"Starting Trading Risk Ingestion Worker [Env: {config.environment}, Topic: {config.topic_id}]"
    )
    # Run synchronously in main thread
    worker.start(run_in_background=False)


if __name__ == "__main__":
    main()
