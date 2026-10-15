"""Trading Risk Engine - Production Multi-Threaded Kafka Ingestion Engine.

Provides:
- High-throughput multi-threaded tick ingestion engine streaming real-time market data into Kafka.
- Worker pool architecture with decoupled feed generation and Kafka producer workers.
- Bounded thread-safe buffer with backpressure management to prevent memory exhaustion.
- Dynamic dead-letter queue (DLQ) routing for corrupted, crossed, or unparseable quotes.
- Prometheus-compatible metrics HTTP server (/metrics) and health endpoint (/health).
- Graceful shutdown with in-flight buffer draining, poison-pill thread joins, and producer flushing.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional
import urllib.parse

from ingestion.kafka.producer import KafkaProducerConfig, KafkaTickProducer, LocalKafkaProducerMock
from ingestion.market_feed import DEFAULT_MARKET_UNIVERSE, MarketFeedGenerator
from ingestion.market_tick import MarketTick

logger = logging.getLogger("kafka_streaming_engine")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "streaming_engine", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


@dataclass
class StreamingEngineConfig:
    """Runtime configuration for the multi-threaded streaming ingestion engine."""

    bootstrap_servers: str = "localhost:9092"
    topic: str = "market.ticks.v1"
    deadletter_topic: str = "market.ticks.deadletter"
    tickers: List[str] = field(
        default_factory=lambda: [
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "JPM", "GS",
            "SPY", "QQQ", "EURUSD", "GBPUSD", "USDJPY", "BTCUSD", "ETHUSD", "XAUUSD", "BRENT"
        ]
    )
    num_feed_threads: int = 4
    num_producer_threads: int = 4
    buffer_capacity: int = 10000
    tick_interval_ms: float = 10.0  # Time between ticks per feed worker
    simulation_mode: bool = True
    metrics_port: int = 8081
    metrics_host: str = "0.0.0.0"
    environment: str = "dev"
    batch_size: int = 16384
    linger_ms: int = 10
    compression_type: str = "lz4"

    @classmethod
    def from_env(cls) -> "StreamingEngineConfig":
        """Instantiate engine configuration from environment variables."""
        tickers_raw = os.getenv(
            "MARKET_FEED_TICKERS",
            "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,JPM,GS,SPY,QQQ,EURUSD,GBPUSD,USDJPY,BTCUSD,ETHUSD,XAUUSD,BRENT",
        )
        tickers_list = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]

        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TICKS_TOPIC", "market.ticks.v1"),
            deadletter_topic=os.getenv("KAFKA_DEADLETTER_TOPIC", "market.ticks.deadletter"),
            tickers=tickers_list,
            num_feed_threads=int(os.getenv("NUM_FEED_THREADS", "4")),
            num_producer_threads=int(os.getenv("NUM_PRODUCER_THREADS", "4")),
            buffer_capacity=int(os.getenv("BUFFER_CAPACITY", "10000")),
            tick_interval_ms=float(os.getenv("TICK_INTERVAL_MS", "10.0")),
            simulation_mode=os.getenv("SIMULATION_MODE", "true").lower() in ("true", "1", "yes"),
            metrics_port=int(os.getenv("METRICS_PORT", "8081")),
            metrics_host=os.getenv("METRICS_HOST", "0.0.0.0"),
            environment=os.getenv("ENVIRONMENT", "dev"),
            batch_size=int(os.getenv("KAFKA_BATCH_SIZE", "16384")),
            linger_ms=int(os.getenv("KAFKA_LINGER_MS", "10")),
            compression_type=os.getenv("KAFKA_COMPRESSION_TYPE", "lz4"),
        )


class EngineTelemetry:
    """Thread-safe telemetry store tracking throughput, latencies, and backpressure."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.start_time: float = time.time()
        self.ticks_generated_total: int = 0
        self.ticks_published_total: int = 0
        self.ticks_failed_total: int = 0
        self.ticks_deadlettered_total: int = 0
        self.bytes_published_total: int = 0
        self.buffer_current_size: int = 0
        self.buffer_high_watermark: int = 0
        self.backpressure_events_total: int = 0
        self.last_tick_id: Optional[str] = None
        self.last_published_timestamp: Optional[str] = None
        self.status: str = "INITIALIZING"

    def record_generated(self) -> None:
        """Record tick generation event."""
        with self._lock:
            self.ticks_generated_total += 1

    def record_published(self, tick_bytes: int, tick_id: str) -> None:
        """Record successful tick publication."""
        with self._lock:
            self.ticks_published_total += 1
            self.bytes_published_total += tick_bytes
            self.last_tick_id = tick_id
            self.last_published_timestamp = datetime.now(timezone.utc).isoformat()

    def record_failed(self) -> None:
        """Record publish failure."""
        with self._lock:
            self.ticks_failed_total += 1

    def record_deadlettered(self) -> None:
        """Record tick routed to Dead-Letter Queue."""
        with self._lock:
            self.ticks_deadlettered_total += 1

    def record_backpressure(self, queue_size: int) -> None:
        """Record a backpressure event when buffer capacity is reached."""
        with self._lock:
            self.backpressure_events_total += 1
            if queue_size > self.buffer_high_watermark:
                self.buffer_high_watermark = queue_size

    def update_buffer_size(self, queue_size: int) -> None:
        """Update current buffer watermark."""
        with self._lock:
            self.buffer_current_size = queue_size
            if queue_size > self.buffer_high_watermark:
                self.buffer_high_watermark = queue_size

    def set_status(self, status: str) -> None:
        """Update engine operational status."""
        with self._lock:
            self.status = status

    def get_snapshot(self) -> Dict[str, Any]:
        """Return consistent snapshot of current telemetry metrics."""
        with self._lock:
            uptime = max(0.001, time.time() - self.start_time)
            ticks_per_sec = round(self.ticks_published_total / uptime, 2)
            kbytes_per_sec = round((self.bytes_published_total / 1024.0) / uptime, 2)

            return {
                "status": self.status,
                "healthy": self.status in ("RUNNING", "INITIALIZING"),
                "uptime_seconds": round(uptime, 2),
                "ticks_generated_total": self.ticks_generated_total,
                "ticks_published_total": self.ticks_published_total,
                "ticks_failed_total": self.ticks_failed_total,
                "ticks_deadlettered_total": self.ticks_deadlettered_total,
                "bytes_published_total": self.bytes_published_total,
                "throughput_ticks_per_sec": ticks_per_sec,
                "throughput_kb_per_sec": kbytes_per_sec,
                "buffer_current_size": self.buffer_current_size,
                "buffer_high_watermark": self.buffer_high_watermark,
                "backpressure_events_total": self.backpressure_events_total,
                "last_tick_id": self.last_tick_id,
                "last_published_timestamp": self.last_published_timestamp,
            }

    def get_prometheus_metrics(self) -> str:
        """Generate Prometheus exposition format string."""
        s = self.get_snapshot()
        lines = [
            "# HELP kafka_streaming_ticks_generated_total Total market ticks generated by feed threads",
            "# TYPE kafka_streaming_ticks_generated_total counter",
            f"kafka_streaming_ticks_generated_total {s['ticks_generated_total']}",
            "# HELP kafka_streaming_ticks_published_total Total ticks successfully published to Kafka",
            "# TYPE kafka_streaming_ticks_published_total counter",
            f"kafka_streaming_ticks_published_total {s['ticks_published_total']}",
            "# HELP kafka_streaming_ticks_failed_total Total failed tick produce attempts",
            "# TYPE kafka_streaming_ticks_failed_total counter",
            f"kafka_streaming_ticks_failed_total {s['ticks_failed_total']}",
            "# HELP kafka_streaming_ticks_deadlettered_total Total malformed ticks routed to DLQ",
            "# TYPE kafka_streaming_ticks_deadlettered_total counter",
            f"kafka_streaming_ticks_deadlettered_total {s['ticks_deadlettered_total']}",
            "# HELP kafka_streaming_bytes_published_total Total bytes published to Kafka",
            "# TYPE kafka_streaming_bytes_published_total counter",
            f"kafka_streaming_bytes_published_total {s['bytes_published_total']}",
            "# HELP kafka_streaming_buffer_current_size Current items in in-memory queue",
            "# TYPE kafka_streaming_buffer_current_size gauge",
            f"kafka_streaming_buffer_current_size {s['buffer_current_size']}",
            "# HELP kafka_streaming_buffer_high_watermark Highest queue size reached",
            "# TYPE kafka_streaming_buffer_high_watermark gauge",
            f"kafka_streaming_buffer_high_watermark {s['buffer_high_watermark']}",
            "# HELP kafka_streaming_backpressure_events_total Count of queue-full backpressure events",
            "# TYPE kafka_streaming_backpressure_events_total counter",
            f"kafka_streaming_backpressure_events_total {s['backpressure_events_total']}",
            "# HELP kafka_streaming_throughput_ticks_per_sec Real-time publishing rate",
            "# TYPE kafka_streaming_throughput_ticks_per_sec gauge",
            f"kafka_streaming_throughput_ticks_per_sec {s['throughput_ticks_per_sec']:.2f}",
            "# HELP kafka_streaming_uptime_seconds Engine uptime in seconds",
            "# TYPE kafka_streaming_uptime_seconds gauge",
            f"kafka_streaming_uptime_seconds {s['uptime_seconds']:.2f}",
            "# HELP kafka_streaming_healthy Engine health indicator (1=UP, 0=DOWN)",
            "# TYPE kafka_streaming_healthy gauge",
            f"kafka_streaming_healthy {1 if s['healthy'] else 0}",
        ]
        return "\n".join(lines) + "\n"


class StreamingMetricsHandler(BaseHTTPRequestHandler):
    """Embedded HTTP server handling /health and /metrics requests."""

    engine_instance: Optional["MultiThreadedTickEngine"] = None

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default HTTP stderr logs."""
        return

    def do_GET(self) -> None:
        """Handle GET requests for health and metrics."""
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if self.engine_instance is None:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b'{"error": "engine not initialized"}\n')
            return

        snapshot = self.engine_instance.telemetry.get_snapshot()

        if path in ("/health", "/healthz"):
            status_code = 200 if snapshot["healthy"] else 503
            body = json.dumps(snapshot, indent=2).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif path == "/metrics":
            prom_text = self.engine_instance.telemetry.get_prometheus_metrics()
            body = prom_text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "not found"}\n')


class MultiThreadedTickEngine:
    """Production Multi-Threaded Real-Time Tick Ingestion Engine for Apache Kafka."""

    def __init__(
        self,
        config: Optional[StreamingEngineConfig] = None,
        producer: Optional[KafkaTickProducer] = None,
        feed_generator: Optional[MarketFeedGenerator] = None,
    ) -> None:
        self.config = config or StreamingEngineConfig.from_env()
        self.telemetry = EngineTelemetry()
        self.feed_generator = feed_generator or MarketFeedGenerator(tickers=self.config.tickers)

        # Initialize producer
        if producer is not None:
            self.producer = producer
        else:
            prod_config = KafkaProducerConfig(
                bootstrap_servers=self.config.bootstrap_servers,
                topic=self.config.topic,
                deadletter_topic=self.config.deadletter_topic,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                compression_type=self.config.compression_type,
                simulation_mode=self.config.simulation_mode,
            )
            self.producer = KafkaTickProducer(config=prod_config)

        # Thread-safe bounded message queue for decoupled pipeline
        self._queue: queue.Queue[Optional[Any]] = queue.Queue(maxsize=self.config.buffer_capacity)
        self._stop_event = threading.Event()
        self._feed_threads: List[threading.Thread] = []
        self._producer_threads: List[threading.Thread] = []
        self._http_server: Optional[HTTPServer] = None
        self._http_thread: Optional[threading.Thread] = None

    def _feed_worker_loop(self, worker_id: int, assigned_tickers: List[str]) -> None:
        """Feed generator worker thread loop producing simulated ticks into the bounded queue."""
        logger.info(
            f"Feed worker {worker_id} started. Monitoring {len(assigned_tickers)} tickers: {assigned_tickers}"
        )
        sleep_sec = max(0.001, self.config.tick_interval_ms / 1000.0)

        while not self._stop_event.is_set():
            for ticker in assigned_tickers:
                if self._stop_event.is_set():
                    break
                try:
                    tick = self.feed_generator.simulate_tick(ticker)
                    self.telemetry.record_generated()

                    # Push to queue with backpressure handling
                    try:
                        self._queue.put(tick, timeout=0.2)
                        self.telemetry.update_buffer_size(self._queue.qsize())
                    except queue.Full:
                        self.telemetry.record_backpressure(self._queue.qsize())
                        logger.warning(
                            f"Feed worker {worker_id}: Buffer full ({self.config.buffer_capacity}). Applying backpressure..."
                        )
                        time.sleep(0.05)

                except Exception as ex:
                    logger.error(f"Feed worker {worker_id} error generating tick for {ticker}: {ex}")

                time.sleep(sleep_sec)

        logger.info(f"Feed worker {worker_id} stopped cleanly.")

    def _producer_worker_loop(self, worker_id: int) -> None:
        """Producer worker thread pulling ticks from the buffer and publishing to Kafka."""
        logger.info(f"Producer worker {worker_id} started.")

        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue

            # Poison pill received for clean shutdown
            if item is None:
                self._queue.task_done()
                break

            self.telemetry.update_buffer_size(self._queue.qsize())

            try:
                # Process tick
                if isinstance(item, MarketTick):
                    payload_bytes = len(item.to_json().encode("utf-8"))
                    self.producer.send_tick(item)
                    self.telemetry.record_published(payload_bytes, item.trade_id)
                elif isinstance(item, dict):
                    # Check if it's a raw dict that can be converted to MarketTick or routed to DLQ
                    try:
                        tick = MarketTick.create(
                            ticker=item["ticker"],
                            asset_class=item["asset_class"],
                            bid=item["bid"],
                            ask=item["ask"],
                            last_price=item["last_price"],
                            volume=item["volume"],
                            trade_id=item.get("trade_id"),
                            timestamp=item.get("timestamp"),
                            sequence_number=item.get("sequence_number", 0),
                        )
                        payload_bytes = len(tick.to_json().encode("utf-8"))
                        self.producer.send_tick(tick)
                        self.telemetry.record_published(payload_bytes, tick.trade_id)
                    except Exception as val_err:
                        # Validation failure: route to Dead-Letter Queue
                        logger.warning(f"Corrupt quote detected: {val_err}. Routing to DLQ.")
                        self.producer.send_deadletter(
                            payload=item,
                            error_reason=str(val_err),
                            original_key=item.get("ticker", "UNKNOWN"),
                        )
                        self.telemetry.record_deadlettered()
                else:
                    # Unrecognized payload type
                    logger.error(f"Unrecognized payload in buffer: {item}. Routing to DLQ.")
                    self.producer.send_deadletter(
                        payload=str(item),
                        error_reason="Unrecognized payload type",
                        original_key="UNKNOWN",
                    )
                    self.telemetry.record_deadlettered()

            except Exception as ex:
                self.telemetry.record_failed()
                logger.error(f"Producer worker {worker_id} error publishing tick: {ex}")
            finally:
                self._queue.task_done()

        logger.info(f"Producer worker {worker_id} stopped cleanly.")

    def inject_corrupt_tick(
        self,
        corruption_type: str = "crossed",
        ticker: Optional[str] = None,
    ) -> None:
        """Inject a corrupted tick into the processing pipeline to verify DLQ routing."""
        corrupt_dict = self.feed_generator.simulate_corrupt_tick(
            ticker=ticker, corruption_type=corruption_type
        )
        self._queue.put(corrupt_dict)

    def start_http_server(self) -> None:
        """Start the embedded health and Prometheus metrics HTTP server."""
        StreamingMetricsHandler.engine_instance = self
        server_address = (self.config.metrics_host, self.config.metrics_port)
        self._http_server = HTTPServer(server_address, StreamingMetricsHandler)

        def serve() -> None:
            logger.info(
                f"Kafka streaming engine metrics server listening on http://{self.config.metrics_host}:{self.config.metrics_port}"
            )
            try:
                if self._http_server:
                    self._http_server.serve_forever()
            except Exception as ex:
                logger.debug(f"HTTP server terminated: {ex}")

        self._http_thread = threading.Thread(
            target=serve, daemon=True, name="StreamingMetricsHTTPServer"
        )
        self._http_thread.start()

    def stop_http_server(self) -> None:
        """Stop the embedded metrics server."""
        if self._http_server:
            try:
                self._http_server.shutdown()
                self._http_server.server_close()
            except Exception as ex:
                logger.debug(f"Error stopping HTTP server: {ex}")
            self._http_server = None

    def start(self, run_in_background: bool = True) -> None:
        """Launch the multi-threaded feed and producer worker fleet."""
        self.telemetry.set_status("INITIALIZING")
        self.start_http_server()
        self._stop_event.clear()

        # Partition ticker list across feed worker threads
        tickers = self.config.tickers
        n_feeds = max(1, min(self.config.num_feed_threads, len(tickers)))
        chunks = [tickers[i::n_feeds] for i in range(n_feeds)]

        for i, chunk in enumerate(chunks):
            if chunk:
                t = threading.Thread(
                    target=self._feed_worker_loop,
                    args=(i + 1, chunk),
                    name=f"FeedWorker-{i+1}",
                    daemon=True,
                )
                self._feed_threads.append(t)
                t.start()

        # Launch producer worker threads
        n_producers = max(1, self.config.num_producer_threads)
        for j in range(n_producers):
            t = threading.Thread(
                target=self._producer_worker_loop,
                args=(j + 1,),
                name=f"ProducerWorker-{j+1}",
                daemon=True,
            )
            self._producer_threads.append(t)
            t.start()

        self.telemetry.set_status("RUNNING")
        logger.info(
            f"MultiThreadedTickEngine running with {len(self._feed_threads)} feed workers and "
            f"{len(self._producer_threads)} producer workers streaming to '{self.config.topic}'."
        )

        if not run_in_background:
            try:
                while not self._stop_event.is_set():
                    time.sleep(1.0)
            except KeyboardInterrupt:
                self.stop()

    def stop(self, drain_timeout: float = 5.0) -> None:
        """Gracefully terminate all workers, flush buffers, and close the producer."""
        self.telemetry.set_status("STOPPING")
        logger.info("Initiating graceful shutdown of Kafka streaming engine...")
        self._stop_event.set()

        # Join feed threads first
        for t in self._feed_threads:
            if t.is_alive():
                t.join(timeout=2.0)
        self._feed_threads.clear()

        # Push poison pills to release producer threads blocked on queue.get()
        for _ in self._producer_threads:
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                pass

        # Join producer threads
        for t in self._producer_threads:
            if t.is_alive():
                t.join(timeout=drain_timeout)
        self._producer_threads.clear()

        # Flush and close producer
        try:
            self.producer.flush(timeout=2.0)
            self.producer.close(timeout=2.0)
        except Exception as ex:
            logger.warning(f"Error closing producer: {ex}")

        # Stop HTTP server
        self.stop_http_server()

        self.telemetry.set_status("STOPPED")
        snapshot = self.telemetry.get_snapshot()
        logger.info(
            f"Kafka streaming engine stopped cleanly. Published: {snapshot['ticks_published_total']} ticks, "
            f"DLQ: {snapshot['ticks_deadlettered_total']}, Rate: {snapshot['throughput_ticks_per_sec']} ticks/sec."
        )

    def setup_signals(self) -> None:
        """Register POSIX signal handlers for graceful container/systemd shutdown."""

        def _signal_handler(signum: int, frame: Any) -> None:
            signame = signal.Signals(signum).name
            logger.info(f"Received signal {signame} ({signum}). Gracefully stopping engine...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)


def main() -> None:
    """CLI execution entrypoint for multi-threaded tick ingestion engine."""
    config = StreamingEngineConfig.from_env()
    engine = MultiThreadedTickEngine(config=config)
    engine.setup_signals()

    logger.info(
        f"Starting Trading Risk Kafka Streaming Engine [Topic: {config.topic}, Servers: {config.bootstrap_servers}, "
        f"Feed Workers: {config.num_feed_threads}, Producer Workers: {config.num_producer_threads}]"
    )
    engine.start(run_in_background=False)


if __name__ == "__main__":
    main()
