"""Real-Time Quantitative Systems & Dataflow Telemetry Console for Trading Risk Engine.

Provides a zero-cost local engineering control plane backed directly by:
- MarketFeedGenerator (GBM + Ornstein-Uhlenbeck multi-asset tick simulator)
- DeterministicPartitioner (Kafka 6-partition MD5 hash router)
- OrderFeedGenerator (Institutional multi-asset trade execution generator)
- DecodeAndValidateTradeOrderFn (Apache Beam Dataflow DoFn decoder & DLQ router)

Endpoints:
- GET  /           : Institutional terminal UI (HTML5 + Canvas + Keyboard shortcuts)
- GET  /health     : JSON health check probe
- GET  /api/state  : Full telemetry state, partition offsets, L1 quotes, orders, and DLQ logs
- POST /api/step   : Step live market ticks and order stream through Beam DoFn
- POST /api/inject : Inject valid, fault vector, or custom JSON payloads into Beam DoFn
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
import time
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
        self.started_at = time.time()
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
        self.last_error_type: str = "NONE"

        self.partition_offsets: Dict[str, int] = {f"P{i}": 0 for i in range(6)}

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

        self.valid_orders_log: Deque[Dict[str, Any]] = deque(maxlen=15)
        self.dlq_log: Deque[Dict[str, Any]] = deque(maxlen=15)

        self.price_history: Dict[str, List[float]] = {t: [] for t in TRACKED_TICKERS}
        self.open_prices: Dict[str, float] = {}
        self.latest_ticks: Dict[str, Dict[str, Any]] = {}

        for ticker in TRACKED_TICKERS:
            for i in range(48):
                tick = self.market_feed.simulate_tick(ticker)
                if i == 0:
                    self.open_prices[ticker] = tick.last_price
                self.price_history[ticker].append(tick.last_price)
            part = self.partitioner.get_partition(tick.partition_key, num_partitions=6)
            self.partition_offsets[f"P{part}"] += 48
            open_p = self.open_prices[ticker]
            chg_pct = ((tick.last_price - open_p) / open_p) * 100.0 if open_p else 0.0
            spread_bps = (tick.spread / tick.mid_price) * 10000.0 if tick.mid_price else 0.0
            self.latest_ticks[ticker] = {
                "ticker": tick.ticker,
                "asset_class": tick.asset_class,
                "price": tick.last_price,
                "bid": tick.bid,
                "ask": tick.ask,
                "spread": tick.spread,
                "spread_bps": round(spread_bps, 2),
                "chg_pct": round(chg_pct, 2),
                "partition": part,
                "partition_key": tick.partition_key,
                "seq": tick.sequence_number,
            }

    def _warm_up_initial_data(self) -> None:
        """Seed initial valid orders and sample DLQ events."""
        for _ in range(5):
            self.inject_event("valid")
        self.inject_event("json")
        self.inject_event("negative_price")

    def reset(self) -> Dict[str, Any]:
        """Reset all counters and logs in a thread-safe manner."""
        with self._lock:
            self._init_counters()
            return self._build_snapshot_unlocked()

    def process_order_element(
        self,
        raw_element: Union[PubsubMessage, bytes, str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Execute the real Apache Beam DecodeAndValidateTradeOrderFn on an input element."""
        t0 = time.perf_counter()
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
        exec_us = round((time.perf_counter() - t0) * 1_000_000, 1)
        outcome: Dict[str, Any] = {"ok": False, "exec_us": exec_us}

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
                order_dict["dofn_exec_us"] = exec_us
                order_dict["ordering_key"] = order.get_ordering_key()
                self.valid_orders_log.appendleft(order_dict)
                outcome = {"ok": True, "order": order_dict, "exec_us": exec_us}
            elif isinstance(res, pvalue.TaggedOutput) and res.tag == TAG_DEADLETTER:
                dlq_rec: DeadLetterRecord = res.value
                self.dlq_total += 1
                self.last_error_type = dlq_rec.error_type
                self.dlq_by_type[dlq_rec.error_type] = (
                    self.dlq_by_type.get(dlq_rec.error_type, 0) + 1
                )
                dlq_dict = dlq_rec.to_dict()
                dlq_dict["dofn_exec_us"] = exec_us
                self.dlq_log.appendleft(dlq_dict)
                outcome = {"ok": False, "dlq": dlq_dict, "exec_us": exec_us}

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
                    self.partition_offsets[f"P{part}"] += 1
                    open_p = self.open_prices.get(tick.ticker, tick.last_price)
                    chg_pct = ((tick.last_price - open_p) / open_p) * 100.0 if open_p else 0.0
                    spread_bps = (tick.spread / tick.mid_price) * 10000.0 if tick.mid_price else 0.0
                    self.latest_ticks[tick.ticker] = {
                        "ticker": tick.ticker,
                        "asset_class": tick.asset_class,
                        "price": tick.last_price,
                        "bid": tick.bid,
                        "ask": tick.ask,
                        "spread": tick.spread,
                        "spread_bps": round(spread_bps, 2),
                        "chg_pct": round(chg_pct, 2),
                        "partition": part,
                        "partition_key": tick.partition_key,
                        "seq": tick.sequence_number,
                    }

            order = self.order_feed.generate_order(ticker=self._pick_tracked_ticker())
            msg = PubsubMessage(
                data=order.to_json().encode("utf-8"),
                attributes=order.get_attributes(environment=self.environment),
                ordering_key=order.get_ordering_key(),
            )
            self.process_order_element(msg)
            return self._build_snapshot_unlocked()

    def _pick_tracked_ticker(self) -> str:
        idx = self.messages_received % len(TRACKED_TICKERS)
        return TRACKED_TICKERS[idx]

    def inject_event(
        self,
        event_type: str,
        custom_payload: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Inject a valid, fault vector, or custom order event into the Beam DoFn decoder."""
        with self._lock:
            etype = event_type.strip().lower()
            if etype == "valid":
                order = self.order_feed.generate_order(ticker=self._pick_tracked_ticker())
                msg = PubsubMessage(
                    data=order.to_json().encode("utf-8"),
                    attributes=order.get_attributes(environment=self.environment),
                    ordering_key=order.get_ordering_key(),
                )
                result = self.process_order_element(msg)
            elif etype == "unicode":
                msg = PubsubMessage(
                    data=b"\xff\xfe\x00\x81\xab\xcd\xef",
                    attributes={"environment": self.environment, "vector": "VEC-01_UTF8_CORRUPT"},
                    ordering_key="BINARY#RAW",
                )
                result = self.process_order_element(msg)
            elif etype == "json":
                msg = PubsubMessage(
                    data=b'{"order_id": "ORD-BROKEN", "ticker": "NVDA", "price": 138.50, EOF_TRUNCATED',
                    attributes={"environment": self.environment, "vector": "VEC-02_JSON_TRUNC"},
                    ordering_key="PORT-ALPHA#NVDA",
                )
                result = self.process_order_element(msg)
            elif etype == "missing":
                raw = json.dumps({"order_id": "ORD-MISSING", "portfolio_id": "PORT-STAT-ARB"}).encode("utf-8")
                msg = PubsubMessage(
                    data=raw,
                    attributes={"environment": self.environment, "vector": "VEC-03_MISSING_REQ"},
                    ordering_key="PORT-STAT-ARB#NULL",
                )
                result = self.process_order_element(msg)
            elif etype == "negative_price":
                raw = json.dumps({
                    "order_id": f"ORD-NEG-{self.messages_received + 1}",
                    "portfolio_id": "PORT-DELTA-ONE",
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
                    attributes={"environment": self.environment, "vector": "VEC-04_DOMAIN_PRICE"},
                    ordering_key="PORT-DELTA-ONE#AAPL",
                )
                result = self.process_order_element(msg)
            elif etype == "invalid_side":
                raw = json.dumps({
                    "order_id": f"ORD-SIDE-{self.messages_received + 1}",
                    "portfolio_id": "PORT-GLOBAL-MACRO",
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
                    attributes={"environment": self.environment, "vector": "VEC-05_ENUM_SIDE"},
                    ordering_key="PORT-GLOBAL-MACRO#BTCUSD",
                )
                result = self.process_order_element(msg)
            elif etype == "custom":
                payload_str = custom_payload or ""
                msg = PubsubMessage(
                    data=payload_str.encode("utf-8"),
                    attributes={"environment": self.environment, "source": "dofn-wire-evaluator"},
                    ordering_key="MANUAL#EVAL",
                )
                result = self.process_order_element(msg)
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
            "uptime_sec": round(time.time() - self.started_at, 1),
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
                "partition_offsets": dict(self.partition_offsets),
            },
            "latest_ticks": dict(self.latest_ticks),
            "price_history": {k: list(v) for k, v in self.price_history.items()},
            "valid_orders_log": list(self.valid_orders_log),
            "dlq_log": list(self.dlq_log),
        }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta name="application-name" content="trading-risk-engine" />
  <title>TRADING-RISK-ENGINE // TELEMETRY &amp; BEAM DOFN INSPECTOR</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    * { box-sizing: border-box; }
    body {
      background-color: #090b0e;
      color: #d4d8df;
      font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
      font-variant-numeric: tabular-nums;
      margin: 0;
      padding: 0;
    }
    .panel {
      background-color: #0d1015;
      border: 1px solid #1c222d;
    }
    .panel-header {
      background-color: #11161d;
      border-bottom: 1px solid #1c222d;
      padding: 6px 10px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: #8b95a5;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .btn-tech {
      background-color: #151b24;
      border: 1px solid #262f3d;
      color: #c9d1d9;
      padding: 4px 9px;
      font-size: 11px;
      font-weight: 500;
      cursor: pointer;
      transition: background-color 0.1s, border-color 0.1s;
    }
    .btn-tech:hover {
      background-color: #1f2733;
      border-color: #3b485e;
      color: #ffffff;
    }
    .btn-fault {
      background-color: #191215;
      border: 1px solid #3d1e24;
      color: #f87171;
      padding: 4px 8px;
      font-size: 11px;
      cursor: pointer;
    }
    .btn-fault:hover {
      background-color: #28171c;
      border-color: #692b36;
    }
    table { width: 100%; border-collapse: collapse; font-size: 11px; }
    th {
      text-align: left;
      padding: 5px 8px;
      color: #6e7887;
      font-weight: 600;
      border-bottom: 1px solid #1c222d;
      background-color: #0b0e12;
      white-space: nowrap;
    }
    td {
      padding: 4px 8px;
      border-bottom: 1px solid #141921;
      white-space: nowrap;
    }
    tr.interactive-row:hover td {
      background-color: #151b24;
      cursor: pointer;
    }
    tr.selected-row td {
      background-color: #172130;
    }
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: #090b0e; }
    ::-webkit-scrollbar-thumb { background: #262f3d; }
  </style>
</head>
<body class="min-h-screen flex flex-col">
  <!-- TOP SYSTEM STATUS BAR -->
  <header class="bg-[#0d1015] border-b border-[#1c222d] px-3 py-1.5 flex flex-wrap items-center justify-between gap-2 text-xs">
    <div class="flex items-center gap-4">
      <div class="flex items-center gap-2">
        <span id="sysIndicator" class="inline-block w-2 h-2 bg-emerald-500"></span>
        <span class="font-bold tracking-wider text-white">TRADING-RISK-ENGINE</span>
        <span class="text-[#6e7887]">//</span>
        <span class="text-[#8b95a5]">BEAM-2.77.0 DIRECT_RUNNER</span>
      </div>
      <div class="hidden md:flex items-center gap-3 text-[#6e7887] border-l border-[#1c222d] pl-4">
        <span>VPC: <strong class="text-[#c9d1d9] font-normal">vpc-trading-risk</strong></span>
        <span>SUBNET: <strong class="text-[#c9d1d9] font-normal">10.10.20.0/24</strong></span>
        <span>ENV: <strong id="hdrEnv" class="text-emerald-400 font-normal">dev</strong></span>
      </div>
    </div>

    <div class="flex items-center gap-2">
      <span id="utcClock" class="text-[#8b95a5] pr-2 border-r border-[#1c222d]">UTC --:--:--.---</span>
      <button id="togglePollBtn" onclick="togglePolling()" class="btn-tech">[SPACE] STATE: RUNNING</button>
      <button onclick="stepOnce()" class="btn-tech">[S] STEP +1</button>
      <button onclick="resetEngine()" class="btn-tech">[R] FLUSH STATE</button>
    </div>
  </header>

  <!-- TELEMETRY STRIP -->
  <section class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 border-b border-[#1c222d] bg-[#0b0e12]">
    <div class="px-3 py-2 border-r border-[#1c222d]">
      <div class="text-[10px] text-[#6e7887] uppercase">KAFKA_TICKS_IN</div>
      <div id="mTotalTicks" class="text-sm font-bold text-white mt-0.5">0</div>
    </div>
    <div class="px-3 py-2 border-r border-[#1c222d]">
      <div class="text-[10px] text-[#6e7887] uppercase">PUBSUB_MSGS_IN</div>
      <div id="mMsgsIn" class="text-sm font-bold text-white mt-0.5">0</div>
    </div>
    <div class="px-3 py-2 border-r border-[#1c222d]">
      <div class="text-[10px] text-[#6e7887] uppercase">WIRE_BYTES_RX</div>
      <div id="mBytesRx" class="text-sm font-bold text-[#c9d1d9] mt-0.5">0 B</div>
    </div>
    <div class="px-3 py-2 border-r border-[#1c222d]">
      <div class="text-[10px] text-[#6e7887] uppercase">TAG_VALID_ORDERS</div>
      <div id="mValidOrders" class="text-sm font-bold text-emerald-400 mt-0.5">0</div>
    </div>
    <div class="px-3 py-2 border-r border-[#1c222d]">
      <div class="text-[10px] text-[#6e7887] uppercase">YIELD_PASS_RATE</div>
      <div id="mYieldRate" class="text-sm font-bold text-emerald-400 mt-0.5">100.00%</div>
    </div>
    <div class="px-3 py-2 border-r border-[#1c222d]">
      <div class="text-[10px] text-[#6e7887] uppercase">CUM_NOTIONAL_USD</div>
      <div id="mNotional" class="text-sm font-bold text-white mt-0.5">$0.00</div>
    </div>
    <div class="px-3 py-2 border-r border-[#1c222d]">
      <div class="text-[10px] text-[#6e7887] uppercase">TAG_DEADLETTER</div>
      <div id="mDlqTotal" class="text-sm font-bold text-rose-400 mt-0.5">0</div>
    </div>
    <div class="px-3 py-2">
      <div class="text-[10px] text-[#6e7887] uppercase">LAST_EXCEPTION</div>
      <div id="mLastErr" class="text-sm font-bold text-amber-400 mt-0.5 truncate">NONE</div>
    </div>
  </section>

  <!-- MAIN 3-COLUMN WORKSPACE -->
  <main class="flex-1 grid grid-cols-1 lg:grid-cols-12 gap-2 p-2">
    <!-- LEFT COL (4): L1 ORDERBOOK QUOTES & KAFKA PARTITION OFFSETS -->
    <div class="lg:col-span-4 flex flex-col gap-2">
      <div class="panel flex-1 flex flex-col">
        <div class="panel-header">
          <span>TIER-1 // L1 MARKET QUOTES (market.ticks.v1)</span>
          <span class="text-[10px] text-[#6e7887]">CLICK ROW TO FOCUS</span>
        </div>
        <div class="overflow-x-auto">
          <table>
            <thead>
              <tr>
                <th>SYM</th>
                <th>CLS</th>
                <th>PART</th>
                <th class="text-right">BID</th>
                <th class="text-right">ASK</th>
                <th class="text-right">LAST</th>
                <th class="text-right">SPRD</th>
                <th class="text-right">CHG%</th>
              </tr>
            </thead>
            <tbody id="l1QuoteTable"></tbody>
          </table>
        </div>
      </div>

      <!-- KAFKA KRAFT PARTITION MATRIX -->
      <div class="panel">
        <div class="panel-header">
          <span>KAFKA KRAFT LOG END OFFSETS (MD5 PARTITIONER)</span>
          <span class="text-[10px] text-emerald-400">6 PARTITIONS // LZ4</span>
        </div>
        <div id="partitionGrid" class="grid grid-cols-6 divide-x divide-[#1c222d] text-center py-2"></div>
      </div>

      <!-- ASSET CLASS NOTIONAL LEDGER -->
      <div class="panel">
        <div class="panel-header">
          <span>BOOK EXPOSURE BY ASSET CLASS (VALIDATED)</span>
          <span class="text-[10px] text-[#6e7887]">USD NOTIONAL</span>
        </div>
        <div id="assetExposureList" class="p-2.5 space-y-2"></div>
      </div>
    </div>

    <!-- CENTER COL (5): MICROSTRUCTURE CHART & BEAM DAG STAGE TELEMETRY -->
    <div class="lg:col-span-5 flex flex-col gap-2">
      <div class="panel flex flex-col">
        <div class="panel-header">
          <span id="chartTitle">MICROSTRUCTURE SERIES // NVDA (EQUITY)</span>
          <div class="flex items-center gap-3 text-[10px]">
            <span class="text-emerald-400">— LAST PRICE</span>
            <span class="text-sky-400">-- EMA(12)</span>
          </div>
        </div>
        <div class="p-2 bg-[#090b0e]">
          <canvas id="microChart" width="600" height="185" class="w-full h-[185px] block"></canvas>
        </div>
        <div id="chartStatsBar" class="grid grid-cols-4 divide-x divide-[#1c222d] border-t border-[#1c222d] bg-[#0b0e12] text-[11px] px-2 py-1.5">
          <div class="px-2"><span class="text-[#6e7887]">MIN:</span> <span id="cMin">--</span></div>
          <div class="px-2"><span class="text-[#6e7887]">MAX:</span> <span id="cMax">--</span></div>
          <div class="px-2"><span class="text-[#6e7887]">SPREAD:</span> <span id="cSprd">--</span></div>
          <div class="px-2"><span class="text-[#6e7887]">SEQ_ID:</span> <span id="cSeq">--</span></div>
        </div>
      </div>

      <!-- APACHE BEAM PTRANSFORM STAGE TABLE -->
      <div class="panel flex-1 flex flex-col">
        <div class="panel-header">
          <span>TIER-2 // APACHE BEAM PTRANSFORM DAG TELEMETRY</span>
          <span class="text-[10px] text-sky-400">streaming/dataflow/pipeline.py</span>
        </div>
        <div class="overflow-x-auto">
          <table>
            <thead>
              <tr>
                <th>STAGE / PTRANSFORM</th>
                <th>IO / TAG</th>
                <th class="text-right">IN</th>
                <th class="text-right">OUT</th>
                <th class="text-right">STATUS</th>
              </tr>
            </thead>
            <tbody id="beamDagTable"></tbody>
          </table>
        </div>

        <!-- DLQ ERROR COUNTER BREAKDOWN -->
        <div class="border-t border-[#1c222d] p-2.5 bg-[#0b0e12] mt-auto">
          <div class="text-[10px] text-[#6e7887] uppercase mb-1.5">DOFN EXCEPTION CLASS DISTRIBUTION (beam.metrics.Metrics)</div>
          <div id="dlqClassGrid" class="grid grid-cols-4 gap-2 text-xs"></div>
        </div>
      </div>
    </div>

    <!-- RIGHT COL (3): DETERMINISTIC FAULT VECTORS & DOFN WIRE EVALUATOR -->
    <div class="lg:col-span-3 flex flex-col gap-2">
      <div class="panel p-2.5 space-y-2">
        <div class="text-[11px] font-semibold uppercase tracking-wider text-[#8b95a5] border-b border-[#1c222d] pb-1.5 flex justify-between">
          <span>SYNTHETIC INJECTION VECTORS</span>
          <span class="text-[10px] text-[#6e7887]">HOTKEYS [1-6]</span>
        </div>
        <div class="grid grid-cols-1 gap-1.5">
          <button onclick="injectVector('valid')" class="btn-tech text-left flex justify-between items-center">
            <span>[1] VALID_INSTITUTIONAL_ORDER</span>
            <span class="text-emerald-400 text-[10px]">MAIN_TAG</span>
          </button>
          <button onclick="injectVector('unicode')" class="btn-fault text-left flex justify-between items-center">
            <span>[2] VEC_01: UTF8_HEX_CORRUPT</span>
            <span class="text-[10px]">0xff 0xfe</span>
          </button>
          <button onclick="injectVector('json')" class="btn-fault text-left flex justify-between items-center">
            <span>[3] VEC_02: JSON_EOF_TRUNC</span>
            <span class="text-[10px]">SYNTAX</span>
          </button>
          <button onclick="injectVector('missing')" class="btn-fault text-left flex justify-between items-center">
            <span>[4] VEC_03: SCHEMA_MISSING_REQ</span>
            <span class="text-[10px]">NULL_FLD</span>
          </button>
          <button onclick="injectVector('negative_price')" class="btn-fault text-left flex justify-between items-center">
            <span>[5] VEC_04: DOMAIN_NEG_PRICE</span>
            <span class="text-[10px]">PX &lt;= 0</span>
          </button>
          <button onclick="injectVector('invalid_side')" class="btn-fault text-left flex justify-between items-center">
            <span>[6] VEC_05: ENUM_INVALID_SIDE</span>
            <span class="text-[10px]">SIDE=HOLD</span>
          </button>
        </div>
      </div>

      <!-- RAW JSON DOFN HARNESS -->
      <div class="panel flex-1 flex flex-col">
        <div class="panel-header">
          <span>DOFN WIRE-FRAME EVALUATOR</span>
          <button onclick="resetCustomJson()" class="text-[10px] text-sky-400 hover:underline cursor-pointer">LOAD_TEMPLATE</button>
        </div>
        <div class="p-2 flex-1 flex flex-col gap-2">
          <textarea id="rawJsonEditor" rows="7" spellcheck="false" class="w-full flex-1 bg-[#07090c] text-[#c9d1d9] border border-[#1c222d] p-2 text-[11px] font-mono focus:outline-none focus:border-sky-500"></textarea>
          <button onclick="evaluateCustomJson()" class="btn-tech w-full py-1.5 font-semibold text-sky-400 border-sky-900/60">
            EXECUTE DecodeAndValidateTradeOrderFn.process()
          </button>
          <div id="evalOutputBox" class="bg-[#07090c] border border-[#1c222d] p-2 text-[11px] text-[#8b95a5] break-all">
            Awaiting manual wire payload...
          </div>
        </div>
      </div>
    </div>
  </main>

  <!-- BOTTOM SPLIT STREAM LOGS & PACKET INSPECTOR -->
  <section class="grid grid-cols-1 lg:grid-cols-12 gap-2 px-2 pb-2">
    <!-- VALIDATED ORDERS TAPE -->
    <div class="lg:col-span-5 panel">
      <div class="panel-header">
        <span class="text-emerald-400">STREAM // TAG_VALID_ORDERS (TimestampedValue&lt;TradeOrder&gt;)</span>
        <span class="text-[10px] text-[#6e7887]">CLICK ROW TO INSPECT PACKET</span>
      </div>
      <div class="overflow-x-auto max-h-[190px]">
        <table>
          <thead>
            <tr>
              <th>ORDER_ID</th>
              <th>PORTFOLIO</th>
              <th>SYM</th>
              <th>SIDE</th>
              <th class="text-right">QTY</th>
              <th class="text-right">PRICE</th>
              <th class="text-right">NOTIONAL</th>
              <th class="text-right">DOFN_US</th>
            </tr>
          </thead>
          <tbody id="validStreamBody"></tbody>
        </table>
      </div>
    </div>

    <!-- DEADLETTER TAPE -->
    <div class="lg:col-span-4 panel">
      <div class="panel-header">
        <span class="text-rose-400">STREAM // TAG_DEADLETTER (DeadLetterRecord)</span>
        <span class="text-[10px] text-[#6e7887]">CLICK ROW TO INSPECT</span>
      </div>
      <div class="overflow-x-auto max-h-[190px]">
        <table>
          <thead>
            <tr>
              <th>EXCEPTION_TYPE</th>
              <th>ORDERING_KEY</th>
              <th>DIAGNOSTIC</th>
            </tr>
          </thead>
          <tbody id="dlqStreamBody"></tbody>
        </table>
      </div>
    </div>

    <!-- RAW PACKET / ENVELOPE INSPECTOR -->
    <div class="lg:col-span-3 panel flex flex-col">
      <div class="panel-header">
        <span>WIRE PACKET INSPECTOR</span>
        <span id="inspectorBadge" class="text-[10px] text-sky-400">SELECT ANY EVENT</span>
      </div>
      <pre id="packetInspectorPre" class="p-2 text-[10px] text-[#9ca3af] overflow-auto max-h-[190px] m-0 bg-[#07090c] flex-1">Click any row in TAG_VALID_ORDERS or TAG_DEADLETTER to inspect full PubsubMessage attributes, event-time epoch, and JSON structure.</pre>
    </div>
  </section>

  <script>
    let isPolling = true;
    let selectedSymbol = "NVDA";
    let lastState = null;

    function updateClock() {
      const now = new Date();
      document.getElementById("utcClock").textContent = "UTC " + now.toISOString().slice(11, 23);
    }
    setInterval(updateClock, 100);
    updateClock();

    async function fetchState() {
      const r = await fetch("/api/state");
      lastState = await r.json();
      render(lastState);
    }

    async function stepOnce() {
      const r = await fetch("/api/step", { method: "POST" });
      lastState = await r.json();
      render(lastState);
    }

    async function injectVector(type) {
      const r = await fetch("/api/inject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type })
      });
      const data = await r.json();
      lastState = data.state;
      render(lastState);
      inspectObject(data.result.ok ? "VALID_ORDER" : "DEADLETTER_RECORD", data.result.ok ? data.result.order : data.result.dlq);
    }

    async function evaluateCustomJson() {
      const payload = document.getElementById("rawJsonEditor").value;
      const r = await fetch("/api/inject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type: "custom", payload })
      });
      const data = await r.json();
      lastState = data.state;
      const box = document.getElementById("evalOutputBox");
      if (data.result.ok) {
        box.className = "bg-[#07130e] border border-emerald-800/70 p-2 text-[11px] text-emerald-400 break-all";
        box.textContent = `[PASS // ${data.result.exec_us}us] TimestampedValue(epoch=${data.result.order.event_timestamp.toFixed(3)}, id=${data.result.order.order_id}, notional=$${data.result.order.notional_usd.toLocaleString()})`;
        inspectObject("VALID_ORDER", data.result.order);
      } else {
        box.className = "bg-[#180b0e] border border-rose-800/70 p-2 text-[11px] text-rose-400 break-all";
        box.textContent = `[DLQ // ${data.result.exec_us}us] ${data.result.dlq.error_type}: ${data.result.dlq.error_message}`;
        inspectObject("DEADLETTER_RECORD", data.result.dlq);
      }
      render(lastState);
    }

    async function resetEngine() {
      const r = await fetch("/api/reset", { method: "POST" });
      lastState = await r.json();
      render(lastState);
    }

    function togglePolling() {
      isPolling = !isPolling;
      const btn = document.getElementById("togglePollBtn");
      const ind = document.getElementById("sysIndicator");
      if (isPolling) {
        btn.textContent = "[SPACE] STATE: RUNNING";
        ind.className = "inline-block w-2 h-2 bg-emerald-500";
      } else {
        btn.textContent = "[SPACE] STATE: HALTED";
        ind.className = "inline-block w-2 h-2 bg-amber-500";
      }
    }

    function selectSymbol(sym) {
      selectedSymbol = sym;
      if (lastState) render(lastState);
    }

    function inspectObject(kind, obj) {
      document.getElementById("inspectorBadge").textContent = kind;
      document.getElementById("packetInspectorPre").textContent = JSON.stringify(obj, null, 2);
    }

    function resetCustomJson() {
      document.getElementById("rawJsonEditor").value = JSON.stringify({
        order_id: "ORD-MIT-904",
        portfolio_id: "PORT-STAT-ARB",
        ticker: "NVDA",
        asset_class: "EQUITY",
        side: "BUY",
        order_type: "LIMIT",
        quantity: 250,
        price: 138.75,
        timestamp: new Date().toISOString()
      }, null, 2);
    }

    function drawMicroChart(state) {
      const canvas = document.getElementById("microChart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width, h = canvas.height;
      ctx.clearRect(0, 0, w, h);

      const series = (state.price_history && state.price_history[selectedSymbol]) || [];
      const info = (state.latest_ticks && state.latest_ticks[selectedSymbol]) || {};
      document.getElementById("chartTitle").textContent = `MICROSTRUCTURE SERIES // ${selectedSymbol} (${info.asset_class || "EQUITY"}) // PART P${info.partition ?? 0}`;

      if (series.length < 2) return;
      const dec = selectedSymbol === "EURUSD" ? 4 : 2;
      const minP = Math.min(...series), maxP = Math.max(...series);
      const pad = (maxP - minP) * 0.15 || 0.01;
      const low = minP - pad, high = maxP + pad;

      document.getElementById("cMin").textContent = minP.toFixed(dec);
      document.getElementById("cMax").textContent = maxP.toFixed(dec);
      document.getElementById("cSprd").textContent = (info.spread_bps ?? 0) + " bps";
      document.getElementById("cSeq").textContent = "#" + (info.seq ?? 0);

      // Grid lines
      ctx.strokeStyle = "#141922";
      ctx.lineWidth = 1;
      for (let i = 1; i <= 4; i++) {
        const y = (h / 5) * i;
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
      }
      for (let i = 1; i <= 6; i++) {
        const x = (w / 7) * i;
        ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke();
      }

      // Compute EMA(12)
      let ema = [];
      const k = 2 / (12 + 1);
      series.forEach((p, idx) => {
        if (idx === 0) ema.push(p);
        else ema.push(p * k + ema[idx - 1] * (1 - k));
      });

      // Draw EMA(12)
      ctx.beginPath();
      ema.forEach((p, idx) => {
        const x = (idx / (series.length - 1)) * (w - 65) + 8;
        const y = h - ((p - low) / (high - low)) * (h - 24) - 12;
        if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = "#38bdf8";
      ctx.setLineDash([3, 3]);
      ctx.lineWidth = 1.2;
      ctx.stroke();
      ctx.setLineDash([]);

      // Draw Price Curve
      ctx.beginPath();
      series.forEach((p, idx) => {
        const x = (idx / (series.length - 1)) * (w - 65) + 8;
        const y = h - ((p - low) / (high - low)) * (h - 24) - 12;
        if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = "#10b981";
      ctx.lineWidth = 1.6;
      ctx.stroke();

      // Axis labels
      ctx.fillStyle = "#6e7887";
      ctx.font = "10px monospace";
      ctx.fillText(high.toFixed(dec), w - 54, 14);
      ctx.fillText(((high + low) / 2).toFixed(dec), w - 54, h / 2 + 3);
      ctx.fillText(low.toFixed(dec), w - 54, h - 6);
    }

    function render(state) {
      const m = state.metrics;
      document.getElementById("hdrEnv").textContent = state.environment;
      document.getElementById("mTotalTicks").textContent = m.total_ticks.toLocaleString();
      document.getElementById("mMsgsIn").textContent = m.messages_received.toLocaleString();
      document.getElementById("mBytesRx").textContent = (m.payload_bytes / 1024).toFixed(2) + " KB";
      document.getElementById("mValidOrders").textContent = m.valid_orders.toLocaleString();
      const passRate = m.messages_received > 0 ? ((m.valid_orders / m.messages_received) * 100).toFixed(2) : "100.00";
      document.getElementById("mYieldRate").textContent = passRate + "%";
      document.getElementById("mNotional").textContent = "$" + m.total_notional.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      document.getElementById("mDlqTotal").textContent = m.dlq_total.toLocaleString();
      document.getElementById("mLastErr").textContent = m.last_error_type;

      // 1. L1 Quotes Table
      const l1Body = document.getElementById("l1QuoteTable");
      l1Body.innerHTML = Object.values(state.latest_ticks).map(t => {
        const dec = t.ticker === "EURUSD" ? 4 : 2;
        const chgColor = t.chg_pct >= 0 ? "text-emerald-400" : "text-rose-400";
        const selClass = t.ticker === selectedSymbol ? "selected-row" : "";
        return `<tr class="interactive-row ${selClass}" onclick="selectSymbol('${t.ticker}')">
          <td class="font-bold text-white">${t.ticker}</td>
          <td class="text-[#6e7887]">${t.asset_class.slice(0, 4)}</td>
          <td class="text-sky-400">P${t.partition}</td>
          <td class="text-right">${t.bid.toFixed(dec)}</td>
          <td class="text-right">${t.ask.toFixed(dec)}</td>
          <td class="text-right font-semibold text-white">${t.price.toFixed(dec)}</td>
          <td class="text-right text-[#8b95a5]">${t.spread_bps.toFixed(1)}</td>
          <td class="text-right ${chgColor}">${t.chg_pct >= 0 ? "+" : ""}${t.chg_pct.toFixed(2)}%</td>
        </tr>`;
      }).join("");

      // 2. Kafka Partition Matrix
      const pGrid = document.getElementById("partitionGrid");
      pGrid.innerHTML = Object.entries(m.partition_offsets || {}).map(([p, off]) => `
        <div class="px-1">
          <div class="text-[10px] text-[#6e7887]">${p}</div>
          <div class="text-xs font-semibold text-[#c9d1d9] mt-0.5">${off}</div>
        </div>
      `).join("");

      // 3. Asset Class Exposure
      const totalNotional = Math.max(1, m.total_notional);
      document.getElementById("assetExposureList").innerHTML = Object.entries(m.notional_by_asset).map(([cls, val]) => {
        const pct = ((val / totalNotional) * 100).toFixed(1);
        return `<div class="space-y-1">
          <div class="flex justify-between text-[11px]">
            <span class="text-[#8b95a5]">${cls}</span>
            <span class="text-white">$${Math.round(val).toLocaleString()} <span class="text-[#6e7887]">(${pct}%)</span></span>
          </div>
          <div class="w-full h-1 bg-[#141922]">
            <div class="h-full bg-emerald-500" style="width:${Math.min(100, pct)}%"></div>
          </div>
        </div>`;
      }).join("");

      // 4. Beam DAG Table
      const dagRows = [
        ["ReadFromPubSubSubscription", `sub-trade-orders-dataflow-${state.environment}`, m.messages_received, m.messages_received, "OK"],
        ["DecodeAndValidateOrderEvents", "ParDo(DecodeAndValidateTradeOrderFn)", m.messages_received, m.valid_orders + m.dlq_total, "OK"],
        ["Branch::MainOutput", "TAG_VALID_ORDERS (TimestampedValue)", m.valid_orders, m.valid_orders, "OK"],
        ["Branch::TaggedOutput", "TAG_DEADLETTER (DeadLetterRecord)", m.dlq_total, m.dlq_total, m.dlq_total > 0 ? "ROUTED" : "IDLE"],
        ["WriteToDeadLetterTopic", `top-trade-orders-dlq-${state.environment}`, m.dlq_total, m.dlq_total, "OK"]
      ];
      document.getElementById("beamDagTable").innerHTML = dagRows.map(r => `
        <tr>
          <td class="font-semibold text-[#c9d1d9]">${r[0]}</td>
          <td class="text-[#8b95a5]">${r[1]}</td>
          <td class="text-right">${r[2]}</td>
          <td class="text-right">${r[3]}</td>
          <td class="text-right ${r[4] === 'ROUTED' ? 'text-amber-400' : 'text-emerald-400'}">${r[4]}</td>
        </tr>
      `).join("");

      // 5. DLQ Exception Class Breakdown
      const b = m.dlq_by_type;
      document.getElementById("dlqClassGrid").innerHTML = [
        ["UTF8_ERR", b.UnicodeDecodeError || 0],
        ["JSON_ERR", b.JSONDecodeError || 0],
        ["MISSING_FLD", b.MissingFieldError || 0],
        ["DOMAIN_VAL", b.ValidationError || 0]
      ].map(([k, v]) => `
        <div class="bg-[#0d1015] border border-[#1c222d] px-2 py-1 flex justify-between">
          <span class="text-[#6e7887] text-[10px]">${k}</span>
          <span class="${v > 0 ? 'text-rose-400 font-bold' : 'text-[#6e7887]'}">${v}</span>
        </div>
      `).join("");

      // 6. Validated Stream Tape
      document.getElementById("validStreamBody").innerHTML = state.valid_orders_log.map((o, idx) => `
        <tr class="interactive-row" onclick='inspectObject("VALID_ORDER", lastState.valid_orders_log[${idx}])'>
          <td class="text-white font-semibold">${o.order_id}</td>
          <td class="text-[#8b95a5]">${o.portfolio_id}</td>
          <td class="text-sky-400 font-bold">${o.ticker}</td>
          <td class="${o.side === 'BUY' ? 'text-emerald-400' : 'text-amber-400'}">${o.side}</td>
          <td class="text-right">${o.quantity}</td>
          <td class="text-right">${o.price}</td>
          <td class="text-right text-white">$${o.notional_usd.toLocaleString()}</td>
          <td class="text-right text-[#6e7887]">${o.dofn_exec_us ?? 12.0}us</td>
        </tr>
      `).join("");

      // 7. DLQ Stream Tape
      document.getElementById("dlqStreamBody").innerHTML = state.dlq_log.map((d, idx) => `
        <tr class="interactive-row" onclick='inspectObject("DEADLETTER_RECORD", lastState.dlq_log[${idx}])'>
          <td class="text-rose-400 font-semibold">${d.error_type}</td>
          <td class="text-[#8b95a5]">${d.ordering_key || "N/A"}</td>
          <td class="text-[#c9d1d9] truncate max-w-[200px]">${d.error_message}</td>
        </tr>
      `).join("");

      drawMicroChart(state);
    }

    // Keyboard Shortcuts
    window.addEventListener("keydown", (e) => {
      if (document.activeElement && document.activeElement.tagName === "TEXTAREA") return;
      if (e.code === "Space") { e.preventDefault(); togglePolling(); }
      else if (e.key === "s" || e.key === "S") stepOnce();
      else if (e.key === "r" || e.key === "R") resetEngine();
      else if (e.key === "1") injectVector("valid");
      else if (e.key === "2") injectVector("unicode");
      else if (e.key === "3") injectVector("json");
      else if (e.key === "4") injectVector("missing");
      else if (e.key === "5") injectVector("negative_price");
      else if (e.key === "6") injectVector("invalid_side");
    });

    resetCustomJson();
    fetchState();
    setInterval(() => { if (isPolling) stepOnce(); }, 900);
  </script>
</body>
</html>
"""


class StreamingDashboardServer:
    """Embedded HTTP server exposing the Quantitative Systems & Dataflow UI and JSON APIs."""

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
                return

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
