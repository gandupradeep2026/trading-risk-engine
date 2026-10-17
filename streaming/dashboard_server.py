"""Real-Time Interactive Web Dashboard Server for Trading Risk Engine.

Provides a zero-cost local visual control plane backed directly by:
- MarketFeedGenerator (GBM + Ornstein-Uhlenbeck multi-asset tick simulator)
- DeterministicPartitioner (Kafka 6-partition murmur-style hash router)
- OrderFeedGenerator (Institutional multi-asset trade execution generator)
- DecodeAndValidateTradeOrderFn (Apache Beam Dataflow DoFn decoder & DLQ router)

Endpoints:
- GET  /           : Interactive HTML5 + Tailwind visual dashboard UI
- GET  /health     : JSON health check probe
- GET  /api/state  : Current telemetry, price series, valid orders, and DLQ logs
- POST /api/step   : Step live market ticks and order stream through Beam DoFn
- POST /api/inject : Inject valid, corrupt, or custom JSON payloads into Beam DoFn
- POST /api/reset  : Reset telemetry counters and event buffers

Usage:
    python -m streaming.dashboard_server --port 8050
"""

import argparse
from collections import deque
from dataclasses import asdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import threading
from typing import Any, Deque, Dict, List, Optional, Union

from apache_beam import pvalue, window
from apache_beam.io.gcp.pubsub import PubsubMessage

from ingestion.market_feed import MarketFeedGenerator
from ingestion.market_tick import DeterministicPartitioner
from ingestion.order_feed import OrderFeedGenerator
from ingestion.pubsub_client import TradeOrder
from streaming.dataflow.decoders import (
    TAG_DEADLETTER,
    DeadLetterRecord,
    DecodeAndValidateTradeOrderFn,
)

logger = logging.getLogger(__name__)

TRACKED_TICKERS = ["NVDA", "AAPL", "MSFT", "SPY", "BTCUSD", "ETHUSD", "EURUSD", "XAUUSD"]


class DashboardEngineState:
    """Thread-safe state coordinator driving real ingestion feeds and Beam DoFn decoding."""

    def __init__(self, environment: str = "dev", seed: int = 42) -> None:
        self._lock = threading.Lock()
        self.environment = environment
        self.market_feed = MarketFeedGenerator(tickers=TRACKED_TICKERS, seed=seed)
        self.order_feed = OrderFeedGenerator(seed=seed)
        self.decoder_dofn = DecodeAndValidateTradeOrderFn()
        self.partitioner = DeterministicPartitioner()

        self._init_counters()
        self._warm_up_initial_data()

    def _init_counters(self) -> None:
        """Initialize or reset all telemetry metrics and rolling buffers."""
        self.total_ticks: int = 0
        self.tick_dlq: int = 0
        self.messages_received: int = 0
        self.valid_orders: int = 0
        self.total_notional: float = 0.0
        self.payload_bytes: int = 0
        self.dlq_total: int = 0
        self.last_error_type: str = "None"

        self.dlq_by_type: Dict[str, int] = {
            "UnicodeDecodeError": 0,
            "JSONDecodeError": 0,
            "MissingFieldError": 0,
            "ValidationError": 0,
            "UnsupportedInputTypeError": 0,
        }
        self.notional_by_asset: Dict[str, float] = {
            "EQUITY": 0.0,
            "ETF": 0.0,
            "CRYPTO": 0.0,
            "FX": 0.0,
            "COMMODITY": 0.0,
        }

        self.valid_orders_log: Deque[Dict[str, Any]] = deque(maxlen=10)
        self.dlq_log: Deque[Dict[str, Any]] = deque(maxlen=10)

        self.price_history: Dict[str, List[float]] = {t: [] for t in TRACKED_TICKERS}
        self.latest_ticks: Dict[str, Dict[str, Any]] = {}

        for ticker in TRACKED_TICKERS:
            for _ in range(40):
                tick = self.market_feed.simulate_tick(ticker)
                self.price_history[ticker].append(tick.last_price)
            part = self.partitioner.get_partition(tick.partition_key, num_partitions=6)
            self.latest_ticks[ticker] = {
                "ticker": tick.ticker,
                "asset_class": tick.asset_class,
                "price": tick.last_price,
                "bid": tick.bid,
                "ask": tick.ask,
                "spread": tick.spread,
                "partition": part,
            }

    def _warm_up_initial_data(self) -> None:
        """Seed the dashboard with initial valid orders and sample DLQ events."""
        for _ in range(5):
            self.inject_event("valid")
        self.inject_event("json")
        self.inject_event("negative_price")

    def reset(self) -> Dict[str, Any]:
        """Reset all counters and logs in a thread-safe manner."""
        with self._lock:
            self._init_counters()
            return self._build_snapshot_unlocked()

    def process_Order_element(
        self,
        raw_element: Union[PubsubMessage, bytes, str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Execute the real Apache Beam DecodeAndValidateTradeOrderFn on an input element."""
        self.messages_received += 1
        if isinstance(raw_element, PubsubMessage):
            self.payload_bytes += len(raw_element.data or b"")
        elif isinstance(raw_element, bytes):
            self.payload_bytes += len(raw_element)
        elif isinstance(raw_element, str):
            self.payload_bytes += len(raw_element.encode("utf-8"))
        elif isinstance(raw_element, dict):
            self.payload_bytes += len(json.dumps(raw_element).encode("utf-8"))

        results = list(self.decoder_dofn.process(raw_element))
        outcome: Dict[str, Any] = {"ok": False}

        for res in results:
            if isinstance(res, window.TimestampedValue):
                order: TradeOrder = res.value
                self.valid_orders += 1
                self.total_notional = round(self.total_notional + order.notional_usd, 2)
                if order.asset_class in self.notional_by_asset:
                    self.notional_by_asset[order.asset_class] = round(
                        self.notional_by_asset[order.asset_class] + order.notional_usd, 2
                    )
                order_dict = asdict(order)
                order_dict["event_timestamp"] = float(res.timestamp)
                self.valid_orders_log.appendleft(order_dict)
                outcome = {"ok": True, "order": order_dict}
            elif isinstance(res, pvalue.TaggedOutput) and res.tag == TAG_DEADLETTER:
                dlq_rec: DeadLetterRecord = res.value
                self.dlq_total += 1
                self.last_error_type = dlq_rec.error_type
                self.dlq_by_type[dlq_rec.error_type] = (
                    self.dlq_by_type.get(dlq_rec.error_type, 0) + 1
                )
                dlq_dict = dlq_rec.to_dict()
                self.dlq_log.appendleft(dlq_dict)
                outcome = {"ok": False, "dlq": dlq_dict}

        return outcome

    def step(self, tick_count: int = 3) -> Dict[str, Any]:
        """Advance market ticks and process a new streaming trade order."""
        with self._lock:
            for _ in range(tick_count):
                tick = self.market_feed.simulate_tick()
                self.total_ticks += 1
                if tick.ticker in self.price_history:
                    self.price_history[tick.ticker].append(tick.last_price)
                    if len(self.price_history[tick.ticker]) > 60:
                        self.price_history[tick.ticker].pop(0)
                    part = self.partitioner.get_partition(tick.partition_key, num_partitions=6)
                    self.latest_ticks[tick.ticker] = {
                        "ticker": tick.ticker,
                        "asset_class": tick.asset_class,
                        "price": tick.last_price,
                        "bid": tick.bid,
                        "ask": tick.ask,
                        "spread": tick.spread,
                        "partition": part,
                    }

            order = self.order_feed.generate_order(ticker=self._pick_tracked_ticker())
            msg = PubsubMessage(
                data=order.to_json().encode("utf-8"),
                attributes=order.get_attributes(environment=self.environment),
                ordering_key=order.get_ordering_key(),
            )
            self.process_Order_element(msg)
            return self._build_snapshot_unlocked()

    def _pick_tracked_ticker(self) -> str:
        """Select a ticker from the tracked dashboard universe."""
        idx = self.messages_received % len(TRACKED_TICKERS)
        return TRACKED_TICKERS[idx]

    def inject_event(
        self,
        event_type: str,
        custom_payload: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Inject a valid, fault, or custom order event into the Beam DoFn decoder."""
        with self._lock:
            etype = event_type.strip().lower()
            if etype == "valid":
                order = self.order_feed.generate_order(ticker=self._pick_tracked_ticker())
                msg = PubsubMessage(
                    data=order.to_json().encode("utf-8"),
                    attributes=order.get_attributes(environment=self.environment),
                    ordering_key=order.get_ordering_key(),
                )
                result = self.process_Order_element(msg)
            elif etype == "unicode":
                msg = PubsubMessage(
                    data=b"\xff\xfe\x00\x81\xab\xcd\xef",
                    attributes={"environment": self.environment, "fault": "unicode"},
                    ordering_key="BINARY#CORRUPT",
                )
                result = self.process_Order_element(msg)
            elif etype == "json":
                msg = PubsubMessage(
                    data=b'{"order_id": "ORD-BROKEN", "ticker": "NVDA", UNCLOSED_JSON',
                    attributes={"environment": self.environment, "fault": "json"},
                    ordering_key="PORT-ALPHA#NVDA",
                )
                result = self.process_Order_element(msg)
            elif etype == "missing":
                raw = json.dumps({"order_id": "ORD-MISSING", "portfolio_id": "PORT-ARB"}).encode("utf-8")
                msg = PubsubMessage(
                    data=raw,
                    attributes={"environment": self.environment, "fault": "missing_fields"},
                    ordering_key="PORT-ARB#MISSING",
                )
                result = self.process_Order_element(msg)
            elif etype == "negative_price":
                raw = json.dumps({
                    "order_id": f"ORD-NEG-{self.messages_received + 1}",
                    "portfolio_id": "PORT-DELTA",
                    "ticker": "AAPL",
                    "asset_class": "EQUITY",
                    "side": "BUY",
                    "order_type": "LIMIT",
                    "quantity": 100.0,
                    "price": -185.50,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }).encode("utf-8")
                msg = PubsubMessage(
                    data=raw,
                    attributes={"environment": self.environment, "fault": "negative_price"},
                    ordering_key="PORT-DELTA#AAPL",
                )
                result = self.process_Order_element(msg)
            elif etype == "invalid_side":
                raw = json.dumps({
                    "order_id": f"ORD-SIDE-{self.messages_received + 1}",
                    "portfolio_id": "PORT-MACRO",
                    "ticker": "BTCUSD",
                    "asset_class": "CRYPTO",
                    "side": "HOLD",
                    "order_type": "MARKET",
                    "quantity": 1.5,
                    "price": 67500.0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }).encode("utf-8")
                msg = PubsubMessage(
                    data=raw,
                    attributes={"environment": self.environment, "fault": "invalid_side"},
                    ordering_key="PORT-MACRO#BTCUSD",
                )
                result = self.process_Order_element(msg)
            elif etype == "custom":
                payload_str = custom_payload or ""
                msg = PubsubMessage(
                    data=payload_str.encode("utf-8"),
                    attributes={"environment": self.environment, "source": "custom-ui-tester"},
                    ordering_key="CUSTOM#TEST",
                )
                result = self.process_Order_element(msg)
            else:
                raise ValueError(f"Unsupported injection event_type: '{event_type}'")

            return {
                "result": result,
                "state": self._build_snapshot_unlocked(),
            }

    def get_snapshot(self) -> Dict[str, Any]:
        """Return a thread-safe snapshot of current dashboard state."""
        with self._lock:
            return self._build_snapshot_unlocked()

    def _build_snapshot_unlocked(self) -> Dict[str, Any]:
        return {
            "environment": self.environment,
            "metrics": {
                "total_ticks": self.total_ticks,
                "tick_dlq": self.tick_dlq,
                "messages_received": self.messages_received,
                "valid_orders": self.valid_orders,
                "total_notional": self.total_notional,
                "payload_bytes": self.payload_bytes,
                "dlq_total": self.dlq_total,
                "last_error_type": self.last_error_type,
                "dlq_by_type": dict(self.dlq_by_type),
                "notional_by_asset": dict(self.notional_by_asset),
            },
            "latest_ticks": dict(self.latest_ticks),
            "price_history": {k: list(v) for k, v in self.price_history.items()},
            "valid_orders_log": list(self.valid_orders_log),
            "dlq_log": list(self.dlq_log),
        }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Trading Risk Engine — Live Streaming & Dataflow Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 antialiased p-6 select-none">
  <div class="max-w-7xl mx-auto space-y-6">
    <!-- Top Header -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
      <div class="space-y-1">
        <div class="flex items-center gap-3 flex-wrap">
          <span id="statusDot" class="inline-block w-3 h-3 rounded-full bg-emerald-400 animate-pulse"></span>
          <h1 class="text-xl font-bold tracking-tight text-white">
            trading-risk-engine • Live Python Dataflow &amp; Streaming Control Plane
          </h1>
          <span class="px-2.5 py-0.5 text-xs font-semibold rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
            Day 01–08 Live Engine
          </span>
        </div>
        <p class="text-sm text-slate-400">
          Connected to Python <code class="font-mono text-slate-300">MarketFeedGenerator</code>, <code class="font-mono text-slate-300">OrderFeedGenerator</code> &amp; Apache Beam <code class="font-mono text-slate-300">DecodeAndValidateTradeOrderFn</code>
        </p>
      </div>
      <div class="flex flex-wrap items-center gap-2">
        <button id="toggleStreamBtn" onclick="toggleStream()" class="px-3.5 py-2 rounded-lg text-xs font-semibold bg-indigo-600 text-white hover:bg-indigo-500 transition cursor-pointer">
          Pause Live Polling
        </button>
        <button onclick="resetServer()" class="px-3 py-2 rounded-lg text-xs font-medium bg-slate-800 text-slate-200 border border-slate-700 hover:bg-slate-700 transition cursor-pointer">
          Reset Engine Counters
        </button>
      </div>
    </div>

    <!-- KPI Row -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 space-y-2">
        <div class="flex items-center justify-between">
          <span class="text-xs font-semibold uppercase tracking-wider text-slate-400">Kafka Market Ticks</span>
          <span class="text-xs font-mono px-2 py-0.5 rounded bg-blue-500/15 text-blue-400">market.ticks.v1</span>
        </div>
        <div class="flex items-baseline justify-between pt-1">
          <div id="kpiTotalTicks" class="text-2xl font-bold font-mono">0</div>
          <div class="text-xs font-mono text-emerald-400">6 Partitions</div>
        </div>
      </div>

      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 space-y-2">
        <div class="flex items-center justify-between">
          <span class="text-xs font-semibold uppercase tracking-wider text-slate-400">Pub/Sub Ingress</span>
          <span class="text-xs font-mono px-2 py-0.5 rounded bg-indigo-500/15 text-indigo-400">top-trade-orders-dev</span>
        </div>
        <div class="flex items-baseline justify-between pt-1">
          <div id="kpiMessagesReceived" class="text-2xl font-bold font-mono">0</div>
          <div id="kpiPayloadBytes" class="text-xs font-mono text-slate-400">0.0 KB</div>
        </div>
      </div>

      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 space-y-2">
        <div class="flex items-center justify-between">
          <span class="text-xs font-semibold uppercase tracking-wider text-slate-400">Beam Validated Orders</span>
          <span class="text-xs font-mono px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-400">TAG_VALID_ORDERS</span>
        </div>
        <div class="flex items-baseline justify-between pt-1">
          <div id="kpiValidOrders" class="text-2xl font-bold font-mono text-emerald-400">0</div>
          <div id="kpiTotalNotional" class="text-xs font-mono font-semibold text-slate-200">$0</div>
        </div>
      </div>

      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 space-y-2">
        <div class="flex items-center justify-between">
          <span class="text-xs font-semibold uppercase tracking-wider text-slate-400">Beam Dead-Letter Queue</span>
          <span class="text-xs font-mono px-2 py-0.5 rounded bg-rose-500/15 text-rose-400">TAG_DEADLETTER</span>
        </div>
        <div class="flex items-baseline justify-between pt-1">
          <div id="kpiDlqCount" class="text-2xl font-bold font-mono text-rose-400">0</div>
          <div id="kpiLastErrorType" class="text-xs font-mono text-rose-400">None</div>
        </div>
      </div>
    </div>

    <!-- Fault Injection & Custom DoFn Tester -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
        <h2 class="text-base font-semibold text-white">Live Apache Beam DoFn Fault Injection</h2>
        <div class="flex flex-wrap gap-2">
          <button onclick="injectEvent('valid')" class="px-3 py-2 rounded-lg text-xs font-semibold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 hover:bg-emerald-500/25 cursor-pointer">+ Inject Valid Order</button>
          <button onclick="injectEvent('unicode')" class="px-3 py-2 rounded-lg text-xs font-semibold bg-rose-500/15 text-rose-400 border border-rose-500/30 hover:bg-rose-500/25 cursor-pointer">⚡ Corrupt UTF-8</button>
          <button onclick="injectEvent('json')" class="px-3 py-2 rounded-lg text-xs font-semibold bg-rose-500/15 text-rose-400 border border-rose-500/30 hover:bg-rose-500/25 cursor-pointer">⚡ Malformed JSON</button>
          <button onclick="injectEvent('missing')" class="px-3 py-2 rounded-lg text-xs font-semibold bg-amber-500/15 text-amber-400 border border-amber-500/30 hover:bg-amber-500/25 cursor-pointer">⚡ Missing Fields</button>
          <button onclick="injectEvent('negative_price')" class="px-3 py-2 rounded-lg text-xs font-semibold bg-amber-500/15 text-amber-400 border border-amber-500/30 hover:bg-amber-500/25 cursor-pointer">⚡ Negative Price</button>
          <button onclick="injectEvent('invalid_side')" class="px-3 py-2 rounded-lg text-xs font-semibold bg-amber-500/15 text-amber-400 border border-amber-500/30 hover:bg-amber-500/25 cursor-pointer">⚡ Side="HOLD"</button>
        </div>
        <canvas id="priceChartCanvas" width="700" height="160" class="w-full h-40 rounded-lg bg-slate-950 border border-slate-800"></canvas>
      </div>

      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 flex flex-col justify-between space-y-3">
        <div>
          <h2 class="text-base font-semibold text-white">Custom Payload DoFn Tester</h2>
          <p class="text-xs text-slate-400">Send raw JSON to Python <code class="font-mono">DecodeAndValidateTradeOrderFn</code>:</p>
        </div>
        <textarea id="customJsonInput" rows="5" class="w-full font-mono text-xs p-2.5 rounded-lg bg-slate-950 text-slate-100 border border-slate-800 select-text">{"order_id":"ORD-999","portfolio_id":"PORT-ALPHA","ticker":"NVDA","asset_class":"EQUITY","side":"BUY","order_type":"LIMIT","quantity":100,"price":138.5}</textarea>
        <button onclick="sendCustomPayload()" class="w-full py-2 px-3 rounded-lg text-xs font-semibold bg-indigo-600 text-white hover:bg-indigo-500 cursor-pointer">Execute on Python Beam DoFn</button>
        <div id="customTestResult" class="text-xs font-mono p-2 rounded bg-slate-950 border border-slate-800 text-slate-400 truncate">Ready...</div>
      </div>
    </div>

    <!-- Tables -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3">
        <h2 class="text-base font-semibold text-emerald-400">Validated Orders (TAG_VALID_ORDERS)</h2>
        <div class="overflow-x-auto">
          <table class="w-full text-left text-xs font-mono">
            <thead>
              <tr class="border-b border-slate-800 text-slate-400">
                <th class="py-2">Order ID</th>
                <th class="py-2">Portfolio</th>
                <th class="py-2">Ticker</th>
                <th class="py-2">Side</th>
                <th class="py-2 text-right">Notional</th>
              </tr>
            </thead>
            <tbody id="validOrdersTableBody" class="divide-y divide-slate-800"></tbody>
          </table>
        </div>
      </div>

      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3">
        <h2 class="text-base font-semibold text-rose-400">Dead-Letter Queue (TAG_DEADLETTER)</h2>
        <div class="overflow-x-auto">
          <table class="w-full text-left text-xs font-mono">
            <thead>
              <tr class="border-b border-slate-800 text-slate-400">
                <th class="py-2">Error Type</th>
                <th class="py-2">Ordering Key</th>
                <th class="py-2">Diagnostic Message</th>
              </tr>
            </thead>
            <tbody id="dlqTableBody" class="divide-y divide-slate-800"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    let polling = true;
    let pollTimer = null;

    async function stepServer() {
      if (!polling) return;
      const res = await fetch("/api/step", { method: "POST" });
      const state = await res.json();
      renderState(state);
    }

    async function injectEvent(type) {
      const res = await fetch("/api/inject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type })
      });
      const data = await res.json();
      renderState(data.state);
    }

    async function sendCustomPayload() {
      const payload = document.getElementById("customJsonInput").value;
      const res = await fetch("/api/inject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type: "custom", payload })
      });
      const data = await res.json();
      const box = document.getElementById("customTestResult");
      if (data.result.ok) {
        box.className = "text-xs font-mono p-2 rounded bg-emerald-500/15 border border-emerald-500/30 text-emerald-400 truncate";
        box.textContent = "VALID: " + data.result.order.order_id + " ($" + data.result.order.notional_usd + ")";
      } else {
        box.className = "text-xs font-mono p-2 rounded bg-rose-500/15 border border-rose-500/30 text-rose-400 truncate";
        box.textContent = "DLQ [" + data.result.dlq.error_type + "]: " + data.result.dlq.error_message;
      }
      renderState(data.state);
    }

    async function resetServer() {
      const res = await fetch("/api/reset", { method: "POST" });
      const state = await res.json();
      renderState(state);
    }

    function toggleStream() {
      polling = !polling;
      document.getElementById("toggleStreamBtn").textContent = polling ? "Pause Live Polling" : "Resume Live Polling";
    }

    function renderState(state) {
      const m = state.metrics;
      document.getElementById("kpiTotalTicks").textContent = m.total_ticks.toLocaleString();
      document.getElementById("kpiMessagesReceived").textContent = m.messages_received.toLocaleString();
      document.getElementById("kpiPayloadBytes").textContent = (m.payload_bytes / 1024).toFixed(1) + " KB";
      document.getElementById("kpiValidOrders").textContent = m.valid_orders.toLocaleString();
      document.getElementById("kpiTotalNotional").textContent = "$" + Math.round(m.total_notional).toLocaleString();
      document.getElementById("kpiDlqCount").textContent = m.dlq_total.toLocaleString();
      document.getElementById("kpiLastErrorType").textContent = m.last_error_type;

      document.getElementById("validOrdersTableBody").innerHTML = state.valid_orders_log.slice(0, 7).map(o => `
        <tr>
          <td class="py-1.5 font-semibold">${o.order_id}</td>
          <td class="py-1.5 text-slate-400">${o.portfolio_id}</td>
          <td class="py-1.5 font-bold">${o.ticker}</td>
          <td class="py-1.5 text-emerald-400">${o.side}</td>
          <td class="py-1.5 text-right">$${o.notional_usd.toLocaleString()}</td>
        </tr>`).join("");

      document.getElementById("dlqTableBody").innerHTML = state.dlq_log.slice(0, 7).map(d => `
        <tr>
          <td class="py-1.5 text-rose-400 font-semibold">${d.error_type}</td>
          <td class="py-1.5 text-slate-400">${d.ordering_key || ""}</td>
          <td class="py-1.5 truncate max-w-[240px]">${d.error_message}</td>
        </tr>`).join("");

      const series = (state.price_history && state.price_history.NVDA) || [];
      const canvas = document.getElementById("priceChartCanvas");
      if (canvas && series.length > 1) {
        const ctx = canvas.getContext("2d");
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        const minP = Math.min(...series), maxP = Math.max(...series);
        const range = (maxP - minP) || 1;
        ctx.beginPath();
        series.forEach((p, idx) => {
          const x = (idx / (series.length - 1)) * (canvas.width - 20) + 10;
          const y = canvas.height - ((p - minP) / range) * (canvas.height - 24) - 12;
          if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        });
        ctx.strokeStyle = "#10b981";
        ctx.lineWidth = 2;
        ctx.stroke();
      }
    }

    fetch("/api/state").then(r => r.json()).then(renderState);
    pollTimer = setInterval(stepServer, 800);
  </script>
</body>
</html>
"""


class StreamingDashboardServer:
    """Embedded HTTP server exposing the interactive Streaming & Dataflow UI and JSON APIs."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8050,
        environment: str = "dev",
    ) -> None:
        self.host = host
        self.port = port
        self.state = DashboardEngineState(environment=environment)
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> int:
        """Start the dashboard HTTP server in a background daemon thread and return bound port."""
        engine_state = self.state

        class RequestHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path in ("/", "/index.html"):
                    body = DASHBOARD_HTML.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path == "/health":
                    self._send_json(200, {"status": "healthy", "service": "trading-risk-dashboard"})
                elif self.path == "/api/state":
                    self._send_json(200, engine_state.get_snapshot())
                else:
                    self._send_json(404, {"error": "Not Found"})

            def do_POST(self) -> None:
                if self.path == "/api/step":
                    snapshot = engine_state.step()
                    self._send_json(200, snapshot)
                elif self.path == "/api/reset":
                    snapshot = engine_state.reset()
                    self._send_json(200, snapshot)
                elif self.path == "/api/inject":
                    length = int(self.headers.get("Content-Length", "0"))
                    raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
                    try:
                        req = json.loads(raw_body)
                        etype = req.get("type", "valid")
                        custom_payload = req.get("payload")
                        out = engine_state.inject_event(etype, custom_payload=custom_payload)
                        self._send_json(200, out)
                    except Exception as exc:
                        self._send_json(400, {"error": str(exc)})
                else:
                    self._send_json(404, {"error": "Not Found"})

            def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, format: str, *args: Any) -> None:
                return  # Suppress noisy stdout access logs during tests

        self._server = ThreadingHTTPServer((self.host, self.port), RequestHandler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="trading-risk-dashboard-http",
            daemon=True,
        )
        self._thread.start()
        logger.info("Streaming Visual Dashboard listening on http://%s:%d", self.host, self.port)
        return self.port

    def stop(self) -> None:
        """Stop the HTTP server and release socket resources."""
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
            self._thread = None


def main() -> None:
    """Run the standalone visual dashboard server until interrupted."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Trading Risk Engine Visual Dashboard Server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8050, help="Bind port (default: 8050)")
    parser.add_argument("--environment", default="dev", choices=["dev", "staging", "prod"])
    args = parser.parse_args()

    server = StreamingDashboardServer(host=args.host, port=args.port, environment=args.environment)
    bound_port = server.start()
    logger.info("Open http://%s:%d in your browser. Press Ctrl+C to exit.", args.host, bound_port)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        logger.info("Shutting down visual dashboard server...")
        server.stop()


if __name__ == "__main__":
    main()
